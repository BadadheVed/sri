package verify

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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

// CheckDeploymentRolledOut polls the named Deployment until its rollout has
// finished (UpdatedReplicas and ReadyReplicas both equal the desired
// replica count, and the controller has observed the latest spec
// generation) or timeout elapses. Used by the three Deployment-targeting
// actions; CheckPodHealthy remains restart_pod's own check since it
// verifies a specific replacement pod by label, not a whole rollout.
//
// An empty deploymentName is treated as unverifiable and returns (false,
// nil) immediately, mirroring CheckPodHealthy's empty-labels guard.
func CheckDeploymentRolledOut(ctx context.Context, clientset kubernetes.Interface, namespace, deploymentName string, timeout, pollInterval time.Duration) (bool, error) {
	if deploymentName == "" {
		return false, nil
	}
	deadline := time.Now().Add(timeout)
	for {
		d, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if rolledOut(d) {
			return true, nil
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

func rolledOut(d *appsv1.Deployment) bool {
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	return d.Status.ObservedGeneration >= d.Generation &&
		d.Status.UpdatedReplicas == desired &&
		d.Status.ReadyReplicas == desired
}
