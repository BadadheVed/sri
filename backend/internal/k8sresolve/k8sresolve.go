// Package k8sresolve holds Pod->ReplicaSet->Deployment resolution and
// revision-annotation parsing shared by execute.Executor (which mutates)
// and mcp-readonly-server's describe_deployment tool (which only reads) —
// extracted here so the two don't carry two copies of the same walk.
package k8sresolve

import (
	"context"
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// RevisionAnnotation is the key the deployment controller itself sets on
// every ReplicaSet it owns, recording that ReplicaSet's revision number.
const RevisionAnnotation = "deployment.kubernetes.io/revision"

// OwningDeployment walks Pod -> ReplicaSet -> Deployment. Callers only ever
// have a Pod name (see correlate.Incident.Name — always a pod name, never a
// Deployment name), so this is the one place that resolution happens.
func OwningDeployment(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) (string, error) {
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("resolve owning deployment: get pod %s/%s: %w", namespace, podName, err)
	}
	rsOwner := ownerOfKind(pod.OwnerReferences, "ReplicaSet")
	if rsOwner == nil {
		return "", fmt.Errorf("resolve owning deployment: pod %s/%s has no ReplicaSet owner (not Deployment-managed)", namespace, podName)
	}
	rs, err := clientset.AppsV1().ReplicaSets(namespace).Get(ctx, rsOwner.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("resolve owning deployment: get replicaset %s/%s: %w", namespace, rsOwner.Name, err)
	}
	deployOwner := ownerOfKind(rs.OwnerReferences, "Deployment")
	if deployOwner == nil {
		return "", fmt.Errorf("resolve owning deployment: replicaset %s/%s has no Deployment owner", namespace, rs.Name)
	}
	return deployOwner.Name, nil
}

func ownerOfKind(refs []metav1.OwnerReference, kind string) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Kind == kind {
			return &refs[i]
		}
	}
	return nil
}

// Revision parses the deployment-controller-set RevisionAnnotation off a
// Deployment's or ReplicaSet's annotations.
func Revision(annotations map[string]string) (int64, error) {
	raw, ok := annotations[RevisionAnnotation]
	if !ok {
		return 0, fmt.Errorf("missing %s annotation", RevisionAnnotation)
	}
	rev, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed %s annotation %q: %w", RevisionAnnotation, raw, err)
	}
	return rev, nil
}

// OwnedBy reports whether refs contains an owner reference with the given
// UID — UID, not name, is the only safe equality check for ownership.
func OwnedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}
