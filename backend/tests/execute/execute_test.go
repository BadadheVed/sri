package execute_test

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

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

// deploymentsGVR is the GroupVersionResource fake.NewSimpleClientset registers
// Deployment objects under; used by newScaleAwareClientset below to address
// the object tracker directly.
var deploymentsGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

// newScaleAwareClientset builds a fake clientset that also supports the
// Deployments().GetScale/UpdateScale calls ScaleDeployment relies on.
//
// The generic fake object tracker's default reaction (k8s.io/client-go
// /testing.ObjectReaction) does not know about the "scale" subresource for
// typed clients: it dispatches purely on the Go action type (Get/Update),
// ignoring the subresource string, so a bare GetScale call fetches the
// stored object by its "deployments" GVR and then type-asserts it straight
// to *autoscalingv1.Scale — which panics ("interface conversion:
// runtime.Object is *v1.Deployment, not *v1.Scale"). This was confirmed by
// running GetScale/UpdateScale against a bare fake.NewSimpleClientset()
// before writing this test.
//
// This is a fake-clientset test-infrastructure gap, not a bug in
// ScaleDeployment: against a real API server (or envtest), the deployments/
// scale subresource is a real, distinct object and GetScale/UpdateScale
// work as written. To exercise ScaleDeployment's real logic here we bridge
// the gap with two PrependReactors that translate the scale subresource
// verbs into reads/writes of the underlying Deployment's Spec.Replicas,
// using clientset.Tracker() directly (not the clientset methods) to avoid
// re-entering the already-locked reactor chain.
func newScaleAwareClientset(objects ...runtime.Object) *fake.Clientset {
	clientset := fake.NewSimpleClientset(objects...)

	clientset.PrependReactor("get", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(k8stesting.GetAction)
		if !ok || getAction.GetSubresource() != "scale" {
			return false, nil, nil
		}
		obj, err := clientset.Tracker().Get(deploymentsGVR, getAction.GetNamespace(), getAction.GetName())
		if err != nil {
			return true, nil, err
		}
		dep := obj.(*appsv1.Deployment)
		var replicas int32
		if dep.Spec.Replicas != nil {
			replicas = *dep.Spec.Replicas
		}
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: dep.Name, Namespace: dep.Namespace, UID: dep.UID, ResourceVersion: dep.ResourceVersion},
			Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
			Status:     autoscalingv1.ScaleStatus{Replicas: dep.Status.Replicas},
		}, nil
	})

	clientset.PrependReactor("update", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateAction, ok := action.(k8stesting.UpdateAction)
		if !ok || updateAction.GetSubresource() != "scale" {
			return false, nil, nil
		}
		scale, ok := updateAction.GetObject().(*autoscalingv1.Scale)
		if !ok {
			return false, nil, nil
		}
		obj, err := clientset.Tracker().Get(deploymentsGVR, scale.Namespace, scale.Name)
		if err != nil {
			return true, nil, err
		}
		dep := obj.(*appsv1.Deployment)
		newReplicas := scale.Spec.Replicas
		dep.Spec.Replicas = &newReplicas
		if err := clientset.Tracker().Update(deploymentsGVR, dep, scale.Namespace); err != nil {
			return true, nil, err
		}
		return true, scale, nil
	})

	return clientset
}

func TestScaleDeployment_ResolvesOwnerAndUpdatesReplicaCount(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")
	var initialReplicas int32 = 2

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: depUID},
		Spec:       appsv1.DeploymentSpec{Replicas: &initialReplicas},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc123",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-abc123-xyz",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc123"}},
		},
	}

	clientset := newScaleAwareClientset(deployment, rs, pod)
	e := execute.NewExecutor(clientset)

	name, err := e.ScaleDeployment(ctx, "default", "web-abc123-xyz", 5)
	if err != nil {
		t.Fatalf("ScaleDeployment: %v", err)
	}
	if name != "web" {
		t.Errorf("expected resolved deployment name %q, got %q", "web", name)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after scale: %v", err)
	}
	if updated.Spec.Replicas == nil || *updated.Spec.Replicas != 5 {
		t.Errorf("expected replicas == 5, got %v", updated.Spec.Replicas)
	}
}

func TestScaleDeployment_ErrorsWhenPodHasNoReplicaSetOwner(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "default"},
	}
	clientset := fake.NewSimpleClientset(pod)
	e := execute.NewExecutor(clientset)

	_, err := e.ScaleDeployment(ctx, "default", "orphan", 3)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "ReplicaSet") {
		t.Errorf("expected error to mention ReplicaSet, got: %v", err)
	}
}

// containerFixture builds a container with distinct, known request/limit
// values so a test can assert precisely which fields PatchResources left
// untouched.
func containerFixture(name string) corev1.Container {
	return corev1.Container{
		Name: name,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
	}
}

// deploymentOwnerChain builds a Deployment plus an owning ReplicaSet and Pod
// so resolveOwningDeployment(ctx, ..., podName) resolves to deploymentName.
func deploymentOwnerChain(deploymentName string, depUID types.UID, containers []corev1.Container) (*appsv1.Deployment, *appsv1.ReplicaSet, *corev1.Pod, string) {
	rsName := deploymentName + "-abc123"
	podName := rsName + "-xyz"
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: "default", UID: depUID},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: containers},
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            rsName,
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: deploymentName, UID: depUID}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            podName,
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: rsName}},
		},
	}
	return deployment, rs, pod, podName
}

func TestPatchResources_UpdatesLimitsOnAllContainersPreservingRequests(t *testing.T) {
	ctx := context.Background()
	containers := []corev1.Container{containerFixture("app"), containerFixture("sidecar")}
	deployment, rs, pod, podName := deploymentOwnerChain("web", types.UID("dep-uid-1"), containers)

	clientset := fake.NewSimpleClientset(deployment, rs, pod)
	e := execute.NewExecutor(clientset)

	name, err := e.PatchResources(ctx, "default", podName, "1Gi", "")
	if err != nil {
		t.Fatalf("PatchResources: %v", err)
	}
	if name != "web" {
		t.Errorf("expected resolved deployment name %q, got %q", "web", name)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after patch: %v", err)
	}

	wantMemory := resource.MustParse("1Gi")
	wantCPULimit := resource.MustParse("1")
	wantCPURequest := resource.MustParse("500m")
	wantMemoryRequest := resource.MustParse("256Mi")
	for _, c := range updated.Spec.Template.Spec.Containers {
		if got := c.Resources.Limits[corev1.ResourceMemory]; got.Cmp(wantMemory) != 0 {
			t.Errorf("container %s: expected memory limit %s, got %s", c.Name, wantMemory.String(), got.String())
		}
		if got := c.Resources.Limits[corev1.ResourceCPU]; got.Cmp(wantCPULimit) != 0 {
			t.Errorf("container %s: expected cpu limit unchanged at %s, got %s", c.Name, wantCPULimit.String(), got.String())
		}
		if got := c.Resources.Requests[corev1.ResourceCPU]; got.Cmp(wantCPURequest) != 0 {
			t.Errorf("container %s: expected cpu request unchanged at %s, got %s", c.Name, wantCPURequest.String(), got.String())
		}
		if got := c.Resources.Requests[corev1.ResourceMemory]; got.Cmp(wantMemoryRequest) != 0 {
			t.Errorf("container %s: expected memory request unchanged at %s, got %s", c.Name, wantMemoryRequest.String(), got.String())
		}
	}
}

func TestPatchResources_RejectsInvalidQuantity(t *testing.T) {
	ctx := context.Background()
	containers := []corev1.Container{containerFixture("app")}
	deployment, rs, pod, podName := deploymentOwnerChain("web", types.UID("dep-uid-1"), containers)

	clientset := fake.NewSimpleClientset(deployment, rs, pod)
	e := execute.NewExecutor(clientset)

	_, err := e.PatchResources(ctx, "default", podName, "not-a-quantity", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid memory quantity") {
		t.Errorf("expected error to mention invalid memory quantity, got: %v", err)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after failed patch: %v", err)
	}
	wantMemory := resource.MustParse("512Mi")
	got := updated.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]
	if got.Cmp(wantMemory) != 0 {
		t.Errorf("expected deployment to be unchanged (memory limit %s), got %s", wantMemory.String(), got.String())
	}
}

func TestPatchResources_RejectsWhenBothLimitsEmpty(t *testing.T) {
	ctx := context.Background()
	// Deliberately empty: PatchResources must reject before ever calling
	// resolveOwningDeployment (which would otherwise fail with a "get pod"
	// error against this empty clientset, masking the real assertion).
	clientset := fake.NewSimpleClientset()
	e := execute.NewExecutor(clientset)

	_, err := e.PatchResources(ctx, "default", "irrelevant-pod", "", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "memoryLimit") || !strings.Contains(err.Error(), "cpuLimit") {
		t.Errorf("expected error to mention memoryLimit/cpuLimit (proving it returned before resolving the pod), got: %v", err)
	}
}

// replicaSetFixture builds a ReplicaSet carrying the deployment's selector
// labels (required for RollbackDeployment's List(LabelSelector=...) call to
// find it), a revision annotation, and an owner reference back to the
// deployment's UID (isOwnedBy checks UID equality, not just name).
func replicaSetFixture(name, revision, image string, depUID types.UID) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       "default",
			Labels:          map[string]string{"app": "web"},
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": revision},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", UID: depUID}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: image}}},
			},
		},
	}
}

// deploymentFixture builds the Deployment under rollback, at the given
// current revision, with the given current image and a selector matching
// replicaSetFixture's labels.
func deploymentFixture(currentRevision, currentImage string, depUID types.UID) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "web",
			Namespace:   "default",
			UID:         depUID,
			Annotations: map[string]string{"deployment.kubernetes.io/revision": currentRevision},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: currentImage}}},
			},
		},
	}
}

func podOwnedByReplicaSet(rsName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            rsName + "-xyz",
			Namespace:       "default",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: rsName}},
		},
	}
}

func TestRollbackDeployment_ReturnsToImmediatelyPreviousRevision(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")

	deployment := deploymentFixture("3", "app:v3", depUID)
	rs1 := replicaSetFixture("web-rev1", "1", "app:v1", depUID)
	rs2 := replicaSetFixture("web-rev2", "2", "app:v2", depUID)
	rs3 := replicaSetFixture("web-rev3", "3", "app:v3", depUID)
	pod := podOwnedByReplicaSet("web-rev3")

	clientset := fake.NewSimpleClientset(deployment, rs1, rs2, rs3, pod)
	e := execute.NewExecutor(clientset)

	name, err := e.RollbackDeployment(ctx, "default", pod.Name)
	if err != nil {
		t.Fatalf("RollbackDeployment: %v", err)
	}
	if name != "web" {
		t.Errorf("expected resolved deployment name %q, got %q", "web", name)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after rollback: %v", err)
	}
	if got := updated.Spec.Template.Spec.Containers[0].Image; got != "app:v2" {
		t.Errorf("expected rollback to app:v2 (the immediately previous revision), got %s", got)
	}
}

func TestRollbackDeployment_SkipsNonContiguousRevisionsCorrectly(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")

	// Revisions 2 and 3 are absent (pruned by revisionHistoryLimit); only 1
	// and 4 remain, with the deployment currently at 4. The closest
	// revision below current is 1, not "current - 1".
	deployment := deploymentFixture("4", "app:v4", depUID)
	rs1 := replicaSetFixture("web-rev1", "1", "app:v1", depUID)
	rs4 := replicaSetFixture("web-rev4", "4", "app:v4", depUID)
	pod := podOwnedByReplicaSet("web-rev4")

	clientset := fake.NewSimpleClientset(deployment, rs1, rs4, pod)
	e := execute.NewExecutor(clientset)

	_, err := e.RollbackDeployment(ctx, "default", pod.Name)
	if err != nil {
		t.Fatalf("RollbackDeployment: %v", err)
	}

	updated, err := clientset.AppsV1().Deployments("default").Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after rollback: %v", err)
	}
	if got := updated.Spec.Template.Spec.Containers[0].Image; got != "app:v1" {
		t.Errorf("expected rollback to skip pruned revisions 2/3 and land on app:v1, got %s", got)
	}
}

func TestRollbackDeployment_ErrorsWhenNoOlderRevisionExists(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")

	// Only the current revision's ReplicaSet exists - nothing older to roll
	// back to.
	deployment := deploymentFixture("1", "app:v1", depUID)
	rs1 := replicaSetFixture("web-rev1", "1", "app:v1", depUID)
	pod := podOwnedByReplicaSet("web-rev1")

	clientset := fake.NewSimpleClientset(deployment, rs1, pod)
	e := execute.NewExecutor(clientset)

	_, err := e.RollbackDeployment(ctx, "default", pod.Name)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "nothing to roll back to") {
		t.Errorf("expected error to mention 'nothing to roll back to', got: %v", err)
	}
}

func TestRollbackDeployment_ErrorsWhenDeploymentMissingRevisionAnnotation(t *testing.T) {
	ctx := context.Background()
	depUID := types.UID("dep-uid-1")

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: depUID},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}
	rs := replicaSetFixture("web-rev1", "1", "app:v1", depUID)
	pod := podOwnedByReplicaSet("web-rev1")

	clientset := fake.NewSimpleClientset(deployment, rs, pod)
	e := execute.NewExecutor(clientset)

	_, err := e.RollbackDeployment(ctx, "default", pod.Name)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing") || !strings.Contains(err.Error(), "revision") {
		t.Errorf("expected error to mention the missing revision annotation, got: %v", err)
	}
}
