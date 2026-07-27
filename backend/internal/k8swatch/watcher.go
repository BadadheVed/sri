// backend/internal/k8swatch/watcher.go
package k8swatch

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"sre-platform/backend/internal/signal"
)

// knownReasons maps a K8s Event's Reason field to our internal failure-mode
// taxonomy. Kubernetes itself emits "BackOff" (not "CrashLoopBackOff") for a
// crash-looping container; the pod status condition uses the longer name.
var knownReasons = map[string]string{
	"BackOff": "CrashLoopBackOff",
}

type Watcher struct {
	clientset kubernetes.Interface
	onSignal  func(signal.Signal)
}

func NewWatcher(clientset kubernetes.Interface, onSignal func(signal.Signal)) *Watcher {
	return &Watcher{clientset: clientset, onSignal: onSignal}
}

func (w *Watcher) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactory(w.clientset, 30*time.Second)
	eventInformer := factory.Core().V1().Events().Informer()
	_, err := eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: w.HandleAddEvent,
	})
	if err != nil {
		return err
	}

	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	<-ctx.Done()
	return nil
}

func (w *Watcher) HandleAddEvent(obj any) {
	ev, ok := obj.(*corev1.Event)
	if !ok {
		return
	}
	failureMode, known := knownReasons[ev.Reason]
	if !known || ev.InvolvedObject.Kind != "Pod" {
		return
	}

	labels := map[string]string{}
	pod, err := w.clientset.CoreV1().Pods(ev.InvolvedObject.Namespace).Get(context.Background(), ev.InvolvedObject.Name, metav1.GetOptions{})
	if err == nil {
		labels = pod.Labels
	}

	w.onSignal(signal.Signal{
		Source:    signal.SourceK8sEvent,
		Type:      failureMode,
		Severity:  "warning",
		Namespace: ev.InvolvedObject.Namespace,
		Kind:      ev.InvolvedObject.Kind,
		Name:      ev.InvolvedObject.Name,
		Labels:    labels,
		Timestamp: ev.LastTimestamp.Time,
		Raw:       ev.Message,
	})
}
