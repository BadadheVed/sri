package k8swatch_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sre-platform/backend/internal/k8swatch"
	"sre-platform/backend/internal/signal"
)

func TestWatcher_HandleAddEvent_EmitsCrashLoopSignal(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "web-1", Namespace: "default",
			Labels: map[string]string{"app": "web"},
		},
	}
	clientset := fake.NewSimpleClientset(pod)

	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1.abc", Namespace: "default"},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod", Namespace: "default", Name: "web-1",
		},
		Reason:        "BackOff",
		Message:       "Back-off restarting failed container",
		LastTimestamp: metav1.NewTime(time.Now()),
	}

	w.HandleAddEvent(ev)

	if len(got) != 1 {
		t.Fatalf("expected 1 signal emitted, got %d", len(got))
	}
	if got[0].Type != "CrashLoopBackOff" {
		t.Errorf("expected type CrashLoopBackOff, got %q", got[0].Type)
	}
	if got[0].Labels["app"] != "web" {
		t.Errorf("expected pod labels to be fetched, got %v", got[0].Labels)
	}
}

func TestWatcher_HandleAddEvent_IgnoresUnrelatedReasons(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	ev := &corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Scheduled",
		LastTimestamp:  metav1.NewTime(time.Now()),
	}
	w.HandleAddEvent(ev)

	if len(got) != 0 {
		t.Errorf("expected no signal for an unrelated event reason, got %d", len(got))
	}
}

func TestWatcher_HandleAddEvent_EmitsSchedulingFailedSignal(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "FailedScheduling",
		Message:        "0/3 nodes are available: insufficient cpu",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 1 || got[0].Type != "SchedulingFailed" {
		t.Fatalf("expected 1 SchedulingFailed signal, got %+v", got)
	}
}

func TestWatcher_HandleAddEvent_EmitsProbeFailureSignal(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Unhealthy",
		Message:        "Readiness probe failed: HTTP probe failed with statuscode: 503",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 1 || got[0].Type != "ProbeFailure" {
		t.Fatalf("expected 1 ProbeFailure signal, got %+v", got)
	}
}

func TestWatcher_HandleAddEvent_EmitsImagePullErrorSignal(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Failed",
		Message:        "Failed to pull image \"bad/image:latest\": rpc error: code = NotFound",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 1 || got[0].Type != "ImagePullError" {
		t.Fatalf("expected 1 ImagePullError signal, got %+v", got)
	}
}

func TestWatcher_HandleAddEvent_IgnoresUnrelatedFailedReason(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	var got []signal.Signal
	w := k8swatch.NewWatcher(clientset, func(s signal.Signal) { got = append(got, s) })

	// Reason "Failed" with a message that has nothing to do with image
	// pulls must NOT be swept into ImagePullError by the substring check.
	w.HandleAddEvent(&corev1.Event{
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "web-1"},
		Reason:         "Failed",
		Message:        "Error: secret \"app-config\" not found",
		LastTimestamp:  metav1.NewTime(time.Now()),
	})

	if len(got) != 0 {
		t.Errorf("expected no signal for an unrelated Failed reason, got %+v", got)
	}
}
