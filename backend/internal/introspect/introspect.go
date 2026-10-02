// This package is read-only by construction — it has no method that calls
// Delete/Update/Patch/Create on anything. mcp-readonly-server (cmd/mcp-readonly-server)
// only ever exposes functions defined here as MCP tools, so this file is
// the actual enforcement point for "ai/ never mutates the cluster": there
// is simply nothing to call that would.
package introspect

import (
	"context"
	"io"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"sre-platform/backend/internal/k8sresolve"
)

// GetPodLogs returns up to tailLines of the named pod's most recent log
// output.
func GetPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, name string, tailLines int64) (string, error) {
	req := clientset.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{TailLines: &tailLines})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()

	data, err := io.ReadAll(stream)
	if err != nil {
		return string(data), err
	}
	return string(data), nil
}

type EventSummary struct {
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Type      string `json:"type"`
	Count     int32  `json:"count"`
	Timestamp string `json:"timestamp"`
}

// GetPodEvents returns every Event in namespace whose InvolvedObject is the
// named pod. Filtered client-side rather than via a field selector — field
// selector support for Events is inconsistent across API server versions
// and isn't implemented by the fake clientset used in tests.
func GetPodEvents(ctx context.Context, clientset kubernetes.Interface, namespace, name string) ([]EventSummary, error) {
	events, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []EventSummary
	for _, ev := range events.Items {
		if ev.InvolvedObject.Kind != "Pod" || ev.InvolvedObject.Name != name {
			continue
		}
		out = append(out, EventSummary{
			Reason: ev.Reason, Message: ev.Message, Type: ev.Type, Count: ev.Count,
			Timestamp: ev.LastTimestamp.String(),
		})
	}
	return out, nil
}

type PodSummary struct {
	Phase      string             `json:"phase"`
	Containers []ContainerSummary `json:"containers"`
}

type ContainerSummary struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restart_count"`
	State        string `json:"state"`
	Reason       string `json:"reason"`
}

// DescribePod returns a compact summary of the named pod's current status —
// enough for an LLM investigation loop to reason about without needing the
// full Pod object.
func DescribePod(ctx context.Context, clientset kubernetes.Interface, namespace, name string) (PodSummary, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return PodSummary{}, err
	}
	summary := PodSummary{Phase: string(pod.Status.Phase)}
	for _, cs := range pod.Status.ContainerStatuses {
		c := ContainerSummary{Name: cs.Name, Ready: cs.Ready, RestartCount: cs.RestartCount}
		switch {
		case cs.State.Waiting != nil:
			c.State, c.Reason = "waiting", cs.State.Waiting.Reason
		case cs.State.Running != nil:
			c.State = "running"
		case cs.State.Terminated != nil:
			c.State, c.Reason = "terminated", cs.State.Terminated.Reason
		}
		summary.Containers = append(summary.Containers, c)
	}
	return summary, nil
}

type DeploymentSummary struct {
	DeploymentName    string `json:"deployment_name"`
	CurrentImage      string `json:"current_image"`
	DesiredReplicas   int32  `json:"desired_replicas"`
	ReadyReplicas     int32  `json:"ready_replicas"`
	AvailableReplicas int32  `json:"available_replicas"`
	CurrentRevision   int64  `json:"current_revision"`
	// PreviousRevision is nil when no older revision exists (e.g. this is
	// the deployment's first-ever revision) — the clearest signal an LLM
	// has that rollback_deployment has nothing to roll back to.
	PreviousRevision *RevisionInfo `json:"previous_revision,omitempty"`
}

type RevisionInfo struct {
	Revision int64  `json:"revision"`
	Image    string `json:"image"`
}

// DescribeDeployment resolves podName's owning Deployment and summarizes
// its current rollout state plus, if one exists, the image the
// immediately-previous revision ran — concrete evidence an investigation
// can use to tell "this image was just changed and broke" (favors
// rollback_deployment) apart from "this never worked" (favors none),
// rather than guessing from a pod-level event label alone.
func DescribeDeployment(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) (DeploymentSummary, error) {
	deploymentName, err := k8sresolve.OwningDeployment(ctx, clientset, namespace, podName)
	if err != nil {
		return DeploymentSummary{}, err
	}
	deployment, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return DeploymentSummary{}, err
	}

	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	summary := DeploymentSummary{
		DeploymentName:    deploymentName,
		DesiredReplicas:   desired,
		ReadyReplicas:     deployment.Status.ReadyReplicas,
		AvailableReplicas: deployment.Status.AvailableReplicas,
	}
	if len(deployment.Spec.Template.Spec.Containers) > 0 {
		summary.CurrentImage = deployment.Spec.Template.Spec.Containers[0].Image
	}

	currentRevision, err := k8sresolve.Revision(deployment.Annotations)
	if err != nil {
		// No revision annotation at all — return what's known about
		// current state, just without revision history.
		return summary, nil
	}
	summary.CurrentRevision = currentRevision

	rsList, err := clientset.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(deployment.Spec.Selector.MatchLabels).String(),
	})
	if err != nil {
		return summary, err
	}
	var previous *appsv1.ReplicaSet
	var previousRevision int64
	for i := range rsList.Items {
		rs := &rsList.Items[i]
		if !k8sresolve.OwnedBy(rs.OwnerReferences, deployment.UID) {
			continue
		}
		rev, err := k8sresolve.Revision(rs.Annotations)
		if err != nil {
			continue
		}
		if rev < currentRevision && rev > previousRevision {
			previous, previousRevision = rs, rev
		}
	}
	if previous != nil && len(previous.Spec.Template.Spec.Containers) > 0 {
		summary.PreviousRevision = &RevisionInfo{
			Revision: previousRevision,
			Image:    previous.Spec.Template.Spec.Containers[0].Image,
		}
	}
	return summary, nil
}
