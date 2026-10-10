package execute

import (
	"context"
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"sre-platform/backend/internal/k8sresolve"
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

// ScaleDeployment resolves podName's owning Deployment and sets its replica
// count via the scale subresource (so RBAC only needs `update` on
// deployments/scale, not on the Deployment object itself).
func (e *Executor) ScaleDeployment(ctx context.Context, namespace, podName string, replicas int32) (string, error) {
	deploymentName, err := k8sresolve.OwningDeployment(ctx, e.clientset, namespace, podName)
	if err != nil {
		return "", err
	}
	scale, err := e.clientset.AppsV1().Deployments(namespace).GetScale(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return deploymentName, fmt.Errorf("scale deployment: get scale for %s/%s: %w", namespace, deploymentName, err)
	}
	scale.Spec.Replicas = replicas
	if _, err := e.clientset.AppsV1().Deployments(namespace).UpdateScale(ctx, deploymentName, scale, metav1.UpdateOptions{}); err != nil {
		return deploymentName, fmt.Errorf("scale deployment: update scale for %s/%s: %w", namespace, deploymentName, err)
	}
	return deploymentName, nil
}

// PatchResources bumps memory and/or CPU limits on every container in the
// owning Deployment's pod template. At least one of memoryLimit/cpuLimit
// must be non-empty — ai/ enforces this upstream (see investigate.py); this
// re-checks as defense-in-depth. Uses a strategic-merge patch keyed on
// container name so requests, and whichever of memory/cpu wasn't given, are
// left untouched.
func (e *Executor) PatchResources(ctx context.Context, namespace, podName, memoryLimit, cpuLimit string) (string, error) {
	if memoryLimit == "" && cpuLimit == "" {
		return "", fmt.Errorf("patch resources: at least one of memoryLimit or cpuLimit is required")
	}
	deploymentName, err := k8sresolve.OwningDeployment(ctx, e.clientset, namespace, podName)
	if err != nil {
		return "", err
	}
	deployment, err := e.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return deploymentName, fmt.Errorf("patch resources: get deployment %s/%s: %w", namespace, deploymentName, err)
	}

	limits := map[string]string{}
	if memoryLimit != "" {
		if _, err := resource.ParseQuantity(memoryLimit); err != nil {
			return deploymentName, fmt.Errorf("patch resources: invalid memory quantity %q: %w", memoryLimit, err)
		}
		limits["memory"] = memoryLimit
	}
	if cpuLimit != "" {
		if _, err := resource.ParseQuantity(cpuLimit); err != nil {
			return deploymentName, fmt.Errorf("patch resources: invalid cpu quantity %q: %w", cpuLimit, err)
		}
		limits["cpu"] = cpuLimit
	}

	containerPatches := make([]map[string]any, 0, len(deployment.Spec.Template.Spec.Containers))
	for _, c := range deployment.Spec.Template.Spec.Containers {
		containerPatches = append(containerPatches, map[string]any{
			"name":      c.Name,
			"resources": map[string]any{"limits": limits},
		})
	}
	patch := map[string]any{
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": containerPatches}}},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return deploymentName, fmt.Errorf("patch resources: marshal patch: %w", err)
	}
	if _, err := e.clientset.AppsV1().Deployments(namespace).Patch(ctx, deploymentName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return deploymentName, fmt.Errorf("patch resources: patch deployment %s/%s: %w", namespace, deploymentName, err)
	}
	return deploymentName, nil
}

// RollbackDeployment reverts the Deployment owning podName to the
// ReplicaSet revision immediately below its current one — the same
// algorithm `kubectl rollout undo` uses, reimplemented here because
// client-go has no standalone Rollback API (the old extensions/v1beta1
// Rollback subresource is long gone): list every ReplicaSet the Deployment
// owns, read each one's "deployment.kubernetes.io/revision" annotation
// (set by the deployment controller itself, not user-managed), and replace
// the Deployment's spec.template with the ReplicaSet's whose revision is
// the highest one still below the Deployment's own current revision.
// Revision numbers are not guaranteed contiguous (pruned by
// revisionHistoryLimit), so "previous" means "closest below current," not
// "current - 1".
//
// This uses a full Get+Update (not a Patch) deliberately: a strategic
// merge patch on containers/env/volumes only ever adds or overwrites by
// name, it can't remove an entry that existed in the live template but not
// the target one — which is exactly what a real rollback needs to do if a
// container/volume was added between the two revisions. RetryOnConflict
// handles the case where the Deployment changed between our Get and
// Update.
func (e *Executor) RollbackDeployment(ctx context.Context, namespace, podName string) (string, error) {
	deploymentName, err := k8sresolve.OwningDeployment(ctx, e.clientset, namespace, podName)
	if err != nil {
		return "", err
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := e.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("rollback deployment: get deployment %s/%s: %w", namespace, deploymentName, err)
		}
		currentRevision, err := k8sresolve.Revision(deployment.Annotations)
		if err != nil {
			return fmt.Errorf("rollback deployment: deployment %s/%s: %w", namespace, deploymentName, err)
		}

		rsList, err := e.clientset.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set(deployment.Spec.Selector.MatchLabels).String(),
		})
		if err != nil {
			return fmt.Errorf("rollback deployment: list replicasets for %s/%s: %w", namespace, deploymentName, err)
		}

		var target *appsv1.ReplicaSet
		var targetRevision int64
		for i := range rsList.Items {
			rs := &rsList.Items[i]
			if !k8sresolve.OwnedBy(rs.OwnerReferences, deployment.UID) {
				continue
			}
			rev, err := k8sresolve.Revision(rs.Annotations)
			if err != nil {
				continue // unlabeled/malformed RS — skip rather than fail the whole rollback
			}
			if rev < currentRevision && rev > targetRevision {
				target, targetRevision = rs, rev
			}
		}
		if target == nil {
			return fmt.Errorf("rollback deployment: no revision older than current (%d) found for %s/%s, nothing to roll back to", currentRevision, namespace, deploymentName)
		}

		deployment.Spec.Template = *target.Spec.Template.DeepCopy()
		if deployment.Annotations == nil {
			deployment.Annotations = map[string]string{}
		}
		deployment.Annotations["kubernetes.io/change-cause"] = fmt.Sprintf("sage rollback to revision %d", targetRevision)

		_, err = e.clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		return err
	})
	return deploymentName, err
}
