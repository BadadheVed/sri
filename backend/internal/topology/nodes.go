package topology

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// NamespaceInfo describes a namespace and how many topology services it has.
type NamespaceInfo struct {
	Name     string `json:"name"`
	Services int    `json:"services"`
}

// NodeProvider supplies the known service nodes per namespace.
type NodeProvider interface {
	Namespaces() []NamespaceInfo
	NodesIn(ns string) []Node
}

// EndpointResolver is optionally implemented by a NodeProvider to map a
// Beyla edge endpoint (a namespace and a workload or Service name) to the
// node ID it belongs to. Without it, BuildSnapshot uses "ns/name".
type EndpointResolver interface {
	ResolveEndpoint(ns, name string) string
}

// ServerResolver is optionally implemented by a NodeProvider to map the raw
// server host of a client-side edge (EdgeKey with an empty ServerNS: a DNS
// name, Service ClusterIP or pod IP) to a node ID. Without it, BuildSnapshot
// resolves only "<svc>.<ns>.svc[.<domain>]" names and maps everything else to
// an "external/<host>" stub.
type ServerResolver interface {
	ResolveServer(clientNS, host string) string
}

// ExternalNamespace is the namespace of stub nodes for server hosts that do
// not resolve to anything in the cluster (node ID "external/<host>").
const ExternalNamespace = "external"

func externalID(host string) string { return ExternalNamespace + "/" + host }

// splitServiceDNS parses in-cluster Service DNS forms (case-insensitive,
// trailing dot allowed): "<svc>.<ns>.svc[.<cluster domain>]" (full=true),
// "<svc>.<ns>" and bare "<svc>" (ns empty). Other shapes yield svc "".
func splitServiceDNS(host string) (svc, ns string, full bool) {
	parts := strings.Split(strings.TrimSuffix(strings.ToLower(host), "."), ".")
	for _, p := range parts {
		if p == "" {
			return "", "", false
		}
	}
	switch {
	case len(parts) >= 3 && parts[2] == "svc":
		return parts[0], parts[1], true
	case len(parts) == 2:
		return parts[0], parts[1], false
	case len(parts) == 1:
		return parts[0], "", false
	}
	return "", "", false
}

// defaultResolveServer is BuildSnapshot's fallback without a ServerResolver.
func defaultResolveServer(_, host string) string {
	if net.ParseIP(host) == nil {
		if svc, ns, full := splitServiceDNS(host); full {
			return ns + "/" + svc
		}
	}
	return externalID(host)
}

// SyncWaiter is optionally implemented by a NodeProvider whose data is
// loaded asynchronously (informer caches).
type SyncWaiter interface {
	// WaitSynced blocks until the provider's caches have synced (true) or
	// ctx ends (false).
	WaitSynced(ctx context.Context) bool
}

// DefaultSyncWarnAfter is how long Start waits for the informer caches
// before logging an error (it keeps waiting afterwards).
const DefaultSyncWarnAfter = 30 * time.Second

// K8sNodeProvider serves nodes from informer caches (no API call per query).
type K8sNodeProvider struct {
	// SyncWarnAfter overrides DefaultSyncWarnAfter (set before Start).
	SyncWarnAfter time.Duration

	factory informers.SharedInformerFactory
	svcInf  cache.SharedIndexInformer
	nsInf   cache.SharedIndexInformer
	podInf  cache.SharedIndexInformer

	// gen is bumped whenever a Service or a pod's labels/owners/IPs change;
	// the per-namespace alias maps and the IP map are rebuilt lazily when it
	// moves.
	gen       atomic.Uint64
	aliasMu   sync.Mutex
	aliasGen  uint64
	aliasByNS map[string]map[string]string // ns -> workload name -> Service node ID
	ipToNode  map[string]string            // Service ClusterIP / pod IP -> node ID (nil until built)
}

// NewK8sNodeProvider builds a provider over Service, Namespace and Pod
// informers. Pods are only used to map Services to the workload names Beyla
// reports and pod IPs to Services, so they are stripped to
// name/labels/owners/hostNetwork/pod IPs to bound memory.
func NewK8sNodeProvider(clientset kubernetes.Interface, resync time.Duration) *K8sNodeProvider {
	f := informers.NewSharedInformerFactory(clientset, resync)
	p := &K8sNodeProvider{
		factory: f,
		svcInf:  f.Core().V1().Services().Informer(),
		nsInf:   f.Core().V1().Namespaces().Informer(),
		podInf:  f.Core().V1().Pods().Informer(),
	}
	_ = p.podInf.SetTransform(func(o interface{}) (interface{}, error) {
		pod, ok := o.(*corev1.Pod)
		if !ok {
			return o, nil
		}
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace:       pod.Namespace,
			Name:            pod.Name,
			Labels:          pod.Labels,
			OwnerReferences: pod.OwnerReferences,
			ResourceVersion: pod.ResourceVersion,
		},
			Spec:   corev1.PodSpec{HostNetwork: pod.Spec.HostNetwork},
			Status: corev1.PodStatus{PodIP: pod.Status.PodIP, PodIPs: pod.Status.PodIPs},
		}, nil
	})
	bump := func(interface{}) { p.gen.Add(1) }
	_, _ = p.svcInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: bump, DeleteFunc: bump,
		UpdateFunc: func(_, _ interface{}) { p.gen.Add(1) },
	})
	_, _ = p.podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: bump, DeleteFunc: bump,
		UpdateFunc: func(o, n interface{}) {
			op, ok1 := o.(*corev1.Pod)
			np, ok2 := n.(*corev1.Pod)
			if ok1 && ok2 && reflect.DeepEqual(op.Labels, np.Labels) && reflect.DeepEqual(op.OwnerReferences, np.OwnerReferences) &&
				op.Spec.HostNetwork == np.Spec.HostNetwork && reflect.DeepEqual(op.Status.PodIPs, np.Status.PodIPs) && op.Status.PodIP == np.Status.PodIP {
				return
			}
			p.gen.Add(1)
		},
	})
	return p
}

// Start runs the informers and blocks until their caches have synced or ctx
// ends. If the caches have not synced after SyncWarnAfter it logs an error
// (and keeps waiting) so a stuck sync (e.g. missing RBAC) is visible.
func (p *K8sNodeProvider) Start(ctx context.Context) error {
	p.factory.Start(ctx.Done())
	warnAfter := p.SyncWarnAfter
	if warnAfter <= 0 {
		warnAfter = DefaultSyncWarnAfter
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTimer(warnAfter)
		defer t.Stop()
		select {
		case <-t.C:
			slog.Error("topology: informer caches have not synced yet; check RBAC (services, namespaces, pods: get/list/watch) and API server reachability",
				"waited", warnAfter.String())
		case <-done:
		case <-ctx.Done():
		}
	}()
	for typ, ok := range p.factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("topology: informer cache for %v failed to sync", typ)
		}
	}
	return nil
}

// Synced reports whether all informer caches have synced.
func (p *K8sNodeProvider) Synced() bool {
	return p.svcInf.HasSynced() && p.nsInf.HasSynced() && p.podInf.HasSynced()
}

// WaitSynced implements SyncWaiter.
func (p *K8sNodeProvider) WaitSynced(ctx context.Context) bool {
	if p.Synced() {
		return true
	}
	return cache.WaitForCacheSync(ctx.Done(), p.svcInf.HasSynced, p.nsInf.HasSynced, p.podInf.HasSynced)
}

// included reports whether a service is a topology node (not headless, not default/kubernetes).
func included(s *corev1.Service) bool {
	if s.Spec.ClusterIP == corev1.ClusterIPNone {
		return false
	}
	return !(s.Namespace == "default" && s.Name == "kubernetes")
}

// Namespaces returns all namespaces sorted by name with their service counts.
func (p *K8sNodeProvider) Namespaces() []NamespaceInfo {
	counts := map[string]int{}
	for _, o := range p.svcInf.GetStore().List() {
		if s, ok := o.(*corev1.Service); ok && included(s) {
			counts[s.Namespace]++
		}
	}
	var out []NamespaceInfo
	for _, o := range p.nsInf.GetStore().List() {
		if n, ok := o.(*corev1.Namespace); ok {
			out = append(out, NamespaceInfo{Name: n.Name, Services: counts[n.Name]})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// NodesIn returns the service nodes of a namespace sorted by name.
func (p *K8sNodeProvider) NodesIn(ns string) []Node {
	objs, err := p.svcInf.GetIndexer().ByIndex(cache.NamespaceIndex, ns)
	if err != nil {
		return nil
	}
	var out []Node
	for _, o := range objs {
		s, ok := o.(*corev1.Service)
		if !ok || !included(s) {
			continue
		}
		ports := make([]Port, 0, len(s.Spec.Ports))
		for _, sp := range s.Spec.Ports {
			ports = append(ports, Port{Port: sp.Port, Protocol: string(sp.Protocol), TargetPort: sp.TargetPort.String()})
		}
		out = append(out, Node{ID: s.Namespace + "/" + s.Name, Namespace: s.Namespace, Name: s.Name, Ports: ports})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ResolveEndpoint implements EndpointResolver. Beyla labels edges with
// workload names (the Deployment/StatefulSet/DaemonSet owning the pod),
// while nodes are Services. Resolution order:
//  1. a Service named name in ns (exact match wins);
//  2. a Service in ns whose selector matches pods owned by workload name —
//     if several do, the first by Service name (deterministic);
//  3. otherwise "ns/name" (just name when ns is empty).
//
// Headless Services take part in resolution even though they are not
// listed as nodes, so edges to e.g. a StatefulSet database land on a stub
// named after its Service.
func (p *K8sNodeProvider) ResolveEndpoint(ns, name string) string {
	id := nodeID(ns, name)
	if ns == "" {
		return id
	}
	if _, exists, err := p.svcInf.GetStore().GetByKey(ns + "/" + name); err == nil && exists {
		return id
	}
	if svcID, ok := p.aliasesIn(ns)[name]; ok {
		return svcID
	}
	return id
}

// aliasesIn returns ns's workload-name -> Service node ID map, rebuilding
// the cache when a Service or pod changed since it was built.
func (p *K8sNodeProvider) aliasesIn(ns string) map[string]string {
	p.aliasMu.Lock()
	defer p.aliasMu.Unlock()
	p.refreshLocked()
	if m, ok := p.aliasByNS[ns]; ok {
		return m
	}
	m := p.buildAliases(ns)
	p.aliasByNS[ns] = m
	return m
}

// refreshLocked drops the cached alias and IP maps when gen moved. Caller
// holds aliasMu.
func (p *K8sNodeProvider) refreshLocked() {
	if g := p.gen.Load(); p.aliasByNS == nil || g != p.aliasGen {
		p.aliasByNS, p.aliasGen, p.ipToNode = map[string]map[string]string{}, g, nil
	}
}

// ResolveServer implements ServerResolver. host is a client-side edge's
// server address without port. Resolution:
//  1. an IP: a Service ClusterIP -> that Service (including
//     default/kubernetes, which BuildSnapshot then shows as a stub); else a
//     pod IP -> the first Service by name in the pod's namespace whose
//     selector matches the pod, else the pod's workload ("ns/workload");
//     when several pods share an IP (hostNetwork), non-hostNetwork pods win,
//     then the first by namespace/name;
//  2. "<svc>.<ns>.svc[.<cluster domain>]" -> "ns/svc";
//  3. "<svc>.<ns>" -> "ns/svc", and bare "<svc>" -> "clientNS/svc", only if
//     that Service exists;
//  4. otherwise "external/<host>".
func (p *K8sNodeProvider) ResolveServer(clientNS, host string) string {
	if ip := net.ParseIP(host); ip != nil {
		if id, ok := p.ipIndex()[ip.String()]; ok {
			return id
		}
		return externalID(host)
	}
	svc, ns, full := splitServiceDNS(host)
	switch {
	case svc == "":
		return externalID(host)
	case full:
		return ns + "/" + svc
	case ns == "":
		ns = clientNS
	}
	if ns != "" {
		if _, exists, err := p.svcInf.GetStore().GetByKey(ns + "/" + svc); err == nil && exists {
			return ns + "/" + svc
		}
	}
	return externalID(host)
}

// ipIndex returns the cached IP -> node ID map, rebuilding it when a
// Service or pod changed since it was built.
func (p *K8sNodeProvider) ipIndex() map[string]string {
	p.aliasMu.Lock()
	defer p.aliasMu.Unlock()
	p.refreshLocked()
	if p.ipToNode == nil {
		p.ipToNode = p.buildIPIndex()
	}
	return p.ipToNode
}

func normIP(s string) string {
	if ip := net.ParseIP(s); ip != nil {
		return ip.String()
	}
	return ""
}

func (p *K8sNodeProvider) buildIPIndex() map[string]string {
	m := map[string]string{}
	svcsByNS := map[string][]*corev1.Service{}
	var svcs []*corev1.Service
	for _, o := range p.svcInf.GetStore().List() {
		if s, ok := o.(*corev1.Service); ok {
			svcs = append(svcs, s)
		}
	}
	sort.Slice(svcs, func(i, j int) bool {
		if svcs[i].Namespace != svcs[j].Namespace {
			return svcs[i].Namespace < svcs[j].Namespace
		}
		return svcs[i].Name < svcs[j].Name
	})
	for _, s := range svcs {
		if len(s.Spec.Selector) > 0 {
			svcsByNS[s.Namespace] = append(svcsByNS[s.Namespace], s)
		}
		for _, ip := range append([]string{s.Spec.ClusterIP}, s.Spec.ClusterIPs...) {
			if k := normIP(ip); k != "" {
				if _, taken := m[k]; !taken {
					m[k] = s.Namespace + "/" + s.Name
				}
			}
		}
	}
	var pods []*corev1.Pod
	for _, o := range p.podInf.GetStore().List() {
		if pod, ok := o.(*corev1.Pod); ok {
			pods = append(pods, pod)
		}
	}
	sort.Slice(pods, func(i, j int) bool {
		a, b := pods[i], pods[j]
		if a.Spec.HostNetwork != b.Spec.HostNetwork {
			return !a.Spec.HostNetwork
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	for _, pod := range pods {
		ips := []string{pod.Status.PodIP}
		for _, pip := range pod.Status.PodIPs {
			ips = append(ips, pip.IP)
		}
		var target string
		for _, ip := range ips {
			k := normIP(ip)
			if k == "" {
				continue
			}
			if _, taken := m[k]; taken {
				continue
			}
			if target == "" {
				target = nodeID(pod.Namespace, workloadName(pod))
				for _, s := range svcsByNS[pod.Namespace] {
					if labels.SelectorFromSet(s.Spec.Selector).Matches(labels.Set(pod.Labels)) {
						target = s.Namespace + "/" + s.Name
						break
					}
				}
			}
			m[k] = target
		}
	}
	return m
}

func (p *K8sNodeProvider) buildAliases(ns string) map[string]string {
	m := map[string]string{}
	svcObjs, err := p.svcInf.GetIndexer().ByIndex(cache.NamespaceIndex, ns)
	if err != nil {
		return m
	}
	podObjs, err := p.podInf.GetIndexer().ByIndex(cache.NamespaceIndex, ns)
	if err != nil {
		return m
	}
	svcs := make([]*corev1.Service, 0, len(svcObjs))
	for _, o := range svcObjs {
		if s, ok := o.(*corev1.Service); ok && len(s.Spec.Selector) > 0 {
			svcs = append(svcs, s)
		}
	}
	sort.Slice(svcs, func(i, j int) bool { return svcs[i].Name < svcs[j].Name })
	for _, s := range svcs {
		sel := labels.SelectorFromSet(s.Spec.Selector)
		for _, o := range podObjs {
			pod, ok := o.(*corev1.Pod)
			if !ok || !sel.Matches(labels.Set(pod.Labels)) {
				continue
			}
			if w := workloadName(pod); w != "" {
				if _, taken := m[w]; !taken {
					m[w] = s.Namespace + "/" + s.Name
				}
			}
		}
	}
	return m
}

// workloadName is the name Beyla reports for a pod's workload: the owning
// Deployment for ReplicaSet-owned pods (the ReplicaSet name minus its
// "-<pod-template-hash>" suffix), else the controller owner's name
// (StatefulSet, DaemonSet, Job, ...), else the pod's own name.
func workloadName(pod *corev1.Pod) string {
	var owner *metav1.OwnerReference
	for i := range pod.OwnerReferences {
		ref := &pod.OwnerReferences[i]
		if ref.Controller != nil && *ref.Controller {
			owner = ref
			break
		}
	}
	if owner == nil && len(pod.OwnerReferences) > 0 {
		owner = &pod.OwnerReferences[0]
	}
	if owner == nil {
		return pod.Name
	}
	if owner.Kind != "ReplicaSet" {
		return owner.Name
	}
	if h := pod.Labels["pod-template-hash"]; h != "" && strings.HasSuffix(owner.Name, "-"+h) {
		return strings.TrimSuffix(owner.Name, "-"+h)
	}
	if i := strings.LastIndex(owner.Name, "-"); i > 0 {
		return owner.Name[:i]
	}
	return owner.Name
}
