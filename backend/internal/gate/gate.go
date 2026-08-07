package gate

type Mode string

const (
	ModeAuto   Mode = "auto"
	ModeManual Mode = "manual"
)

// safetyFloor lists action types that always require approval, regardless of
// mode. This cannot be overridden by configuration — see design doc §2, §6.
//
// It is unexported so importing packages cannot mutate or clear it at
// runtime (Go maps are reference types, so an exported var would let any
// caller do gate.SafetyFloor["delete_pvc"] = false and silently defeat the
// hard safety floor). Use IsInSafetyFloor for read-only access.
var safetyFloor = map[string]bool{
	"delete_pvc":    true,
	"scale_to_zero": true,
	"delete_node":   true,
}

// IsInSafetyFloor reports whether action is a member of the hard safety
// floor — actions that always require approval regardless of mode or
// config. This is the only way for other packages to inspect the floor.
func IsInSafetyFloor(action string) bool {
	return safetyFloor[action]
}

type Decision struct {
	RequiresApproval bool
	Reason           string
}

// Evaluate fails CLOSED: approval is required for anything that is not
// exactly ModeAuto. This means a typo'd or unrecognized mode string (e.g.
// "Manual", "manual-approve", "off", or "") requires approval rather than
// silently falling through to auto-approve — this system deletes Kubernetes
// pods, so an operator misconfiguration must never be interpreted as
// permission to act unattended. settings.Load validates Mode at startup so
// this branch should be unreachable in the running binary; it exists as
// defense-in-depth for any other caller that constructs a Settings/calls
// Evaluate directly (e.g. tests).
func Evaluate(action string, mode Mode) Decision {
	if IsInSafetyFloor(action) {
		return Decision{RequiresApproval: true, Reason: "hard_safety_floor"}
	}
	if mode == ModeAuto {
		return Decision{RequiresApproval: false, Reason: "auto_approved"}
	}
	if mode == ModeManual {
		return Decision{RequiresApproval: true, Reason: "manual_mode"}
	}
	return Decision{RequiresApproval: true, Reason: "unrecognized_mode"}
}
