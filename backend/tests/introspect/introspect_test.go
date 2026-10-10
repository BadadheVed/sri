package introspect_test

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/introspect"
)

func TestGetPodLogs_ReturnsWithoutErrorForExistingPod(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"}}
	clientset := fake.NewSimpleClientset(pod)

	// client-go's fake clientset has no real log backend behind GetLogs —
	// this only proves the call plumbs through end to end without an
	// unexpected error. Real log content is only ever exercised against a
	// live cluster (see this plan's Verification section).
	if _, err := introspect.GetPodLogs(context.Background(), clientset, "default", "web-1", 100); err != nil {
		t.Fatalf("GetPodLogs: %v", err)
	}
}

func TestGetPodEvents_FiltersToTheNamedPod(t *testing.T) {
	matching := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "web-1.abc", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "BackOff", Message: "Back-off restarting failed container", Type: "Warning", Count: 3,
	}
	unrelated := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: "other.abc", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "other-pod"},
		Reason:         "Scheduled", Type: "Normal",
	}
	clientset := fake.NewSimpleClientset(matching, unrelated)

	events, err := introspect.GetPodEvents(context.Background(), clientset, "default", "web-1")
	if err != nil {
		t.Fatalf("GetPodEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event for web-1, got %d: %+v", len(events), events)
	}
	if events[0].Reason != "BackOff" || events[0].Count != 3 {
		t.Errorf("unexpected event: %+v", events[0])
	}
}

func TestDescribePod_SummarizesContainerState(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "default"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "app", Ready: false, RestartCount: 4,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				},
			},
		},
	}
	clientset := fake.NewSimpleClientset(pod)

	summary, err := introspect.DescribePod(context.Background(), clientset, "default", "web-1")
	if err != nil {
		t.Fatalf("DescribePod: %v", err)
	}
	if summary.Phase != "Running" {
		t.Errorf("expected phase Running, got %q", summary.Phase)
	}
	if len(summary.Containers) != 1 || summary.Containers[0].RestartCount != 4 || summary.Containers[0].Reason != "CrashLoopBackOff" {
		t.Fatalf("unexpected container summary: %+v", summary.Containers)
	}
}

func TestDescribeDeployment_ReportsCurrentStateAndPreviousRevisionImage(t *testing.T) {
	depUID := types.UID("dep-uid-1")
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: depUID,
			Annotations: map[string]string{"deployment.kubernetes.io/revision": "2"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "app:bad-tag"}}},
			},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 0, AvailableReplicas: 0},
	}
	rsOld := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-old", Namespace: "default", Labels: map[string]string{"app": "web"},
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": "1"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "app:v1"}}}},
		},
	}
	rsCurrent := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-new", Namespace: "default", Labels: map[string]string{"app": "web"},
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": "2"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "web-new-xyz", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-new"}},
	}}
	clientset := fake.NewSimpleClientset(deployment, rsOld, rsCurrent, pod)

	summary, err := introspect.DescribeDeployment(context.Background(), clientset, "default", "web-new-xyz")
	if err != nil {
		t.Fatalf("DescribeDeployment: %v", err)
	}
	if summary.DeploymentName != "web" || summary.CurrentImage != "app:bad-tag" || summary.CurrentRevision != 2 {
		t.Fatalf("unexpected current-state summary: %+v", summary)
	}
	if summary.PreviousRevision == nil || summary.PreviousRevision.Revision != 1 || summary.PreviousRevision.Image != "app:v1" {
		t.Fatalf("expected previous revision 1 with image app:v1, got %+v", summary.PreviousRevision)
	}
}

func TestDescribeDeployment_PreviousRevisionNilWhenNoneExists(t *testing.T) {
	depUID := types.UID("dep-uid-1")
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web", Namespace: "default", UID: depUID,
			Annotations: map[string]string{"deployment.kubernetes.io/revision": "1"},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "app:v1"}}},
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-abc", Namespace: "default", Labels: map[string]string{"app": "web"},
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": "1"},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "web-abc-xyz", Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc"}},
	}}
	clientset := fake.NewSimpleClientset(deployment, rs, pod)

	summary, err := introspect.DescribeDeployment(context.Background(), clientset, "default", "web-abc-xyz")
	if err != nil {
		t.Fatalf("DescribeDeployment: %v", err)
	}
	if summary.PreviousRevision != nil {
		t.Fatalf("expected no previous revision, got %+v", summary.PreviousRevision)
	}
}
