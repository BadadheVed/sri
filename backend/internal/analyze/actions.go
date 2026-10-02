package analyze

const (
	ActionRestartPod         = "restart_pod"
	ActionScaleDeployment    = "scale_deployment"
	ActionPatchResources     = "patch_resources"
	ActionRollbackDeployment = "rollback_deployment"
	ActionNone               = "none"
)

// ValidActions is the complete wire vocabulary diagnosis.go and
// reconcile.go both check against — the single source of truth so the two
// never drift.
var ValidActions = map[string]bool{
	ActionRestartPod:         true,
	ActionScaleDeployment:    true,
	ActionPatchResources:     true,
	ActionRollbackDeployment: true,
	ActionNone:               true,
}
