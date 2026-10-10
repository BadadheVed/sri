// backend/internal/beylascrape/discover.go
package beylascrape

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DiscoverPods lists the currently Running Beyla pods matching
// Config.PodSelector (in Config.Namespace, or every namespace if empty)
// and returns their IPs. A pod without an assigned IP, or not in the
// Running phase (e.g. Pending, or a stale IP retained while Failed or
// Terminating), is excluded — there's nothing to scrape at either.
func (s *Source) DiscoverPods(ctx context.Context) ([]string, error) {
	targets, err := s.discoverTargets(ctx)
	if err != nil {
		return nil, err
	}
	ips := make([]string, 0, len(targets))
	for _, t := range targets {
		ips = append(ips, t.ip)
	}
	return ips, nil
}

// podTarget is one scrapeable Beyla pod: key is its stable identity
// ("namespace/name"), ip where to scrape it.
type podTarget struct {
	key, ip string
}

// discoverTargets is DiscoverPods keeping each pod's identity, so callers
// can track per-pod state (two pods never share a key even if an IP is
// reused).
func (s *Source) discoverTargets(ctx context.Context) ([]podTarget, error) {
	list, err := s.clientset.CoreV1().Pods(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: s.cfg.PodSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("beylascrape: listing Beyla pods: %w", err)
	}

	out := make([]podTarget, 0, len(list.Items))
	for _, pod := range list.Items {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		out = append(out, podTarget{key: pod.Namespace + "/" + pod.Name, ip: pod.Status.PodIP})
	}
	return out, nil
}
