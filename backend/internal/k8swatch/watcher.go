// backend/internal/k8swatch/watcher.go
package k8swatch

import (
	"context"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"sre-platform/backend/internal/signal"
)

// knownReasons maps a K8s Event's Reason field to our internal failure-mode
// taxonomy, for reasons kubelet never reuses across unrelated failure
// classes.
//
// "Failed" and "BackOff" are deliberately not in this map: kubelet reuses
// both of those verbatim for multiple, unrelated problems — "Failed" for
// image pull errors, missing secrets/configmaps, volume mount failures,
// etc., and "BackOff" for both a crash-looping container ("Back-off
// restarting failed container ...") and a stalled image pull ("Back-off
// pulling image ..."). Both are handled separately below by inspecting the
// event Message instead of trusting Reason alone.
var knownReasons = map[string]string{
	"FailedScheduling": "SchedulingFailed",
	"Unhealthy":        "ProbeFailure",
}

type Watcher struct {
	clientset     kubernetes.Interface
	onSignal      func(signal.Signal)
	selfNamespace string
}

// selfNamespace is the namespace this backend itself is deployed into
// (POD_NAMESPACE via the Downward API — see settings.SelfNamespace). Events
// about objects in that namespace are dropped before ever becoming a
// signal, so SAGE never watches, diagnoses, or remediates itself — an empty
// selfNamespace disables the filter entirely (nothing is excluded).
func NewWatcher(clientset kubernetes.Interface, onSignal func(signal.Signal), selfNamespace string) *Watcher {
	return &Watcher{clientset: clientset, onSignal: onSignal, selfNamespace: selfNamespace}
}

func (w *Watcher) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactory(w.clientset, 30*time.Second)
	eventInformer := factory.Core().V1().Events().Informer()
	_, err := eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: w.HandleAddEvent,
	})
	if err != nil {
		return err
	}

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	<-ctx.Done()
	return nil
}

func (w *Watcher) HandleAddEvent(obj any) {
	ev, ok := obj.(*corev1.Event)
	if !ok {
		return
	}
	failureMode, known := knownReasons[ev.Reason]
	if !known && ev.Reason == "Failed" && strings.Contains(ev.Message, "pull image") {
		failureMode, known = "ImagePullError", true
	}
	if !known && ev.Reason == "BackOff" {
		// See knownReasons' doc comment: "BackOff" alone is ambiguous
		// between a crash loop and a stalled image pull. A signal
		// mislabeled CrashLoopBackOff for what's actually an image pull
		// problem sends ai/ toward restart_pod, which fixes nothing.
		if strings.Contains(ev.Message, "pulling image") {
			failureMode, known = "ImagePullError", true
		} else {
			failureMode, known = "CrashLoopBackOff", true
		}
	}
	if !known || ev.InvolvedObject.Kind != "Pod" {
		return
	}
	if w.selfNamespace != "" && ev.InvolvedObject.Namespace == w.selfNamespace {
		return
	}

	labels := map[string]string{}
	groupKey := ev.InvolvedObject.Namespace + "/" + ev.InvolvedObject.Kind + "/" + ev.InvolvedObject.Name
	pod, err := w.clientset.CoreV1().Pods(ev.InvolvedObject.Namespace).Get(context.Background(), ev.InvolvedObject.Name, metav1.GetOptions{})
	if err == nil {
		labels = pod.Labels
		// A ReplicaSet-managed pod gets deleted and recreated under a brand
		// new name every time it's remediated, so grouping restart attempts
		// by pod name would never actually catch a repeatedly-failing
		// workload. Group by the owning controller instead, when there is
		// one — it stays the same across those recreations.
		if len(pod.OwnerReferences) > 0 {
			owner := pod.OwnerReferences[0]
			groupKey = ev.InvolvedObject.Namespace + "/" + owner.Kind + "/" + owner.Name
		}
	} else {
		slog.Warn("failed to fetch pod labels, continuing without them", "namespace", ev.InvolvedObject.Namespace, "name", ev.InvolvedObject.Name, "error", err)
	}

	slog.Info("signal detected", "source", signal.SourceK8sEvent, "failure_mode", failureMode, "namespace", ev.InvolvedObject.Namespace, "kind", ev.InvolvedObject.Kind, "name", ev.InvolvedObject.Name, "group_key", groupKey)

	w.onSignal(signal.Signal{
		Source:    signal.SourceK8sEvent,
		Type:      failureMode,
		Severity:  "warning",
		Namespace: ev.InvolvedObject.Namespace,
		Kind:      ev.InvolvedObject.Kind,
		Name:      ev.InvolvedObject.Name,
		Labels:    labels,
		Timestamp: ev.LastTimestamp.Time,
		Raw:       ev.Message,
		GroupKey:  groupKey,
	})
}
