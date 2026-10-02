package k8sresolve_test

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/k8sresolve"
)

func TestOwningDeployment_ResolvesPodThroughReplicaSetToDeployment(t *testing.T) {
	depUID := types.UID("dep-uid-1")
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: depUID}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
		Name: "web-abc123", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "web-abc123-xyz", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc123"}},
	}}
	clientset := fake.NewSimpleClientset(deployment, rs, pod)

	name, err := k8sresolve.OwningDeployment(context.Background(), clientset, "default", "web-abc123-xyz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "web" {
		t.Errorf("expected deployment name %q, got %q", "web", name)
	}
}

func TestOwningDeployment_ErrorsWhenPodHasNoReplicaSetOwner(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"}}
	clientset := fake.NewSimpleClientset(pod)

	_, err := k8sresolve.OwningDeployment(context.Background(), clientset, "default", "web-1")
	if err == nil || !strings.Contains(err.Error(), "ReplicaSet") {
		t.Fatalf("expected a ReplicaSet-owner error, got %v", err)
	}
}

func TestRevision_ParsesAnnotation(t *testing.T) {
	rev, err := k8sresolve.Revision(map[string]string{k8sresolve.RevisionAnnotation: "3"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rev != 3 {
		t.Errorf("expected revision 3, got %d", rev)
	}
}

func TestRevision_ErrorsWhenAnnotationMissing(t *testing.T) {
	_, err := k8sresolve.Revision(map[string]string{})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected a missing-annotation error, got %v", err)
	}
}

func TestOwnedBy_MatchesOnUIDNotName(t *testing.T) {
	uid := types.UID("dep-uid-1")
	refs := []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: uid}}

	if !k8sresolve.OwnedBy(refs, uid) {
		t.Error("expected OwnedBy to match on UID")
	}
	if k8sresolve.OwnedBy(refs, types.UID("different-uid")) {
		t.Error("expected OwnedBy to reject a different UID")
	}
}
