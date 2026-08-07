package verify

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// CheckPodHealthy polls until at least one pod matching labels in namespace
// reports Ready, or timeout elapses.
//
// An empty podLabels map is treated as unverifiable and returns (false, nil)
// immediately without querying the API. labels.SelectorFromSet on an empty
// map produces an empty selector string, which the Kubernetes API interprets
// as "match every pod in the namespace" — so without this guard, any
// unrelated healthy pod in the namespace would make this return true, and
// the reconcile loop would record a remediation as "resolved" having
// verified nothing real. This happens whenever the watcher's live pod Get
// call fails and leaves Labels as an empty map (see k8swatch/watcher.go).
func CheckPodHealthy(ctx context.Context, clientset kubernetes.Interface, namespace string, podLabels map[string]string, timeout, pollInterval time.Duration) (bool, error) {
	if len(podLabels) == 0 {
		return false, nil
	}
	deadline := time.Now().Add(timeout)
	selector := labels.SelectorFromSet(podLabels).String()

	for {
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			if isReady(p) {
				return true, nil
			}
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func isReady(p corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
