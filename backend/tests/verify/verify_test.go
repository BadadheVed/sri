package verify_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/verify"
)

func readyPod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func TestCheckPodHealthy_ReturnsTrueWhenReadyPodExists(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"app": "web"}
	clientset := fake.NewSimpleClientset(readyPod("web-2", labels))

	healthy, err := verify.CheckPodHealthy(ctx, clientset, "default", labels, 2*time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckPodHealthy: %v", err)
	}
	if !healthy {
		t.Error("expected healthy=true when a Ready pod matches the labels")
	}
}

func TestCheckPodHealthy_ReturnsFalseWhenNoPodBecomesReady(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"app": "web"}
	clientset := fake.NewSimpleClientset()

	healthy, err := verify.CheckPodHealthy(ctx, clientset, "default", labels, 200*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckPodHealthy: %v", err)
	}
	if healthy {
		t.Error("expected healthy=false when no matching pod exists within the timeout")
	}
}

// TestCheckPodHealthy_ReturnsFalseImmediatelyWhenLabelsEmpty guards against
// an empty podLabels map (e.g. when the watcher's live pod Get call failed
// and left Labels as an empty map) producing an empty label selector, which
// Kubernetes interprets as "match every pod in the namespace." A Ready pod
// is deliberately seeded so the test would fail if CheckPodHealthy were
// accidentally matching it instead of treating the check as unverifiable.
func TestCheckPodHealthy_ReturnsFalseImmediatelyWhenLabelsEmpty(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset(readyPod("web-2", map[string]string{"app": "web"}))

	start := time.Now()
	healthy, err := verify.CheckPodHealthy(ctx, clientset, "default", map[string]string{}, 2*time.Second, 50*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("CheckPodHealthy: %v", err)
	}
	if healthy {
		t.Error("expected healthy=false when podLabels is empty, even though a Ready pod exists in the namespace")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("expected CheckPodHealthy to return immediately (no polling) for empty labels, took %v", elapsed)
	}
}

func rolledOutDeployment(name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1, UpdatedReplicas: replicas, ReadyReplicas: replicas,
		},
	}
}

func TestCheckDeploymentRolledOut_ReturnsTrueWhenFullyRolledOut(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset(rolledOutDeployment("web", 3))

	rolledOut, err := verify.CheckDeploymentRolledOut(ctx, clientset, "default", "web", 2*time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckDeploymentRolledOut: %v", err)
	}
	if !rolledOut {
		t.Error("expected rolledOut=true when deployment is fully rolled out")
	}
}

func TestCheckDeploymentRolledOut_ReturnsFalseWhenReadyReplicasBehind(t *testing.T) {
	ctx := context.Background()
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: ptrInt32(3)},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1, UpdatedReplicas: 3, ReadyReplicas: 1,
		},
	}
	clientset := fake.NewSimpleClientset(deployment)

	rolledOut, err := verify.CheckDeploymentRolledOut(ctx, clientset, "default", "web", 200*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckDeploymentRolledOut: %v", err)
	}
	if rolledOut {
		t.Error("expected rolledOut=false when ReadyReplicas < desired")
	}
}

func TestCheckDeploymentRolledOut_ReturnsFalseWhenObservedGenerationStale(t *testing.T) {
	ctx := context.Background()
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: ptrInt32(3)},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 0, UpdatedReplicas: 3, ReadyReplicas: 3,
		},
	}
	clientset := fake.NewSimpleClientset(deployment)

	rolledOut, err := verify.CheckDeploymentRolledOut(ctx, clientset, "default", "web", 200*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("CheckDeploymentRolledOut: %v", err)
	}
	if rolledOut {
		t.Error("expected rolledOut=false when ObservedGeneration is stale")
	}
}

func TestCheckDeploymentRolledOut_ReturnsFalseImmediatelyWhenNameEmpty(t *testing.T) {
	ctx := context.Background()
	clientset := fake.NewSimpleClientset()

	start := time.Now()
	rolledOut, err := verify.CheckDeploymentRolledOut(ctx, clientset, "default", "", 2*time.Second, 50*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("CheckDeploymentRolledOut: %v", err)
	}
	if rolledOut {
		t.Error("expected rolledOut=false when deploymentName is empty")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("expected CheckDeploymentRolledOut to return immediately (no polling) for empty name, took %v", elapsed)
	}
}

func ptrInt32(v int32) *int32 {
	return &v
}
