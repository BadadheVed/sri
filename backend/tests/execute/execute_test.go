package execute_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/execute"
)

func TestRestartPod_DeletesExistingPod(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"},
	})

	e := execute.NewExecutor(clientset)
	if err := e.RestartPod(ctx, "default", "web-1"); err != nil {
		t.Fatalf("RestartPod: %v", err)
	}

	_, err := clientset.CoreV1().Pods("default").Get(ctx, "web-1", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected pod to be deleted, got err=%v", err)
	}
}

func TestRestartPod_IdempotentWhenAlreadyGone(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()

	e := execute.NewExecutor(clientset)
	if err := e.RestartPod(ctx, "default", "already-gone"); err != nil {
		t.Fatalf("expected RestartPod to succeed idempotently on a missing pod, got: %v", err)
	}
}
