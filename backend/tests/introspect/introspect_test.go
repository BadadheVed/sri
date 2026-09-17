package introspect_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
