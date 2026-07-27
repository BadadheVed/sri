package execute

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Executor struct {
	clientset kubernetes.Interface
}

func NewExecutor(clientset kubernetes.Interface) *Executor {
	return &Executor{clientset: clientset}
}

// RestartPod deletes the pod so its owning controller (ReplicaSet/Deployment)
// recreates it. Deleting an already-absent pod is treated as success, since a
// remediation action must be safe to retry.
func (e *Executor) RestartPod(ctx context.Context, namespace, name string) error {
	err := e.clientset.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
