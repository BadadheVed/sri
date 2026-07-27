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
