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
	list, err := s.clientset.CoreV1().Pods(s.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: s.cfg.PodSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("beylascrape: listing Beyla pods: %w", err)
	}

	ips := make([]string, 0, len(list.Items))
	for _, pod := range list.Items {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		ips = append(ips, pod.Status.PodIP)
	}
	return ips, nil
}
