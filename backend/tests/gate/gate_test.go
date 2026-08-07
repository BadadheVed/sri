package gate_test

import (
	"testing"

	"sre-platform/backend/internal/gate"
)

func TestEvaluate_SafetyFloorAlwaysRequiresApproval(t *testing.T) {
	floorActions := []string{"delete_pvc", "scale_to_zero", "delete_node"}
	for _, action := range floorActions {
		t.Run(action, func(t *testing.T) {
			d := gate.Evaluate(action, gate.ModeAuto)
			if !d.RequiresApproval {
				t.Errorf("expected %s to require approval even in auto mode", action)
			}
			if d.Reason != "hard_safety_floor" {
				t.Errorf("expected reason hard_safety_floor, got %q", d.Reason)
			}
		})
	}
}

func TestEvaluate_ManualModeRequiresApproval(t *testing.T) {
	d := gate.Evaluate("restart_pod", gate.ModeManual)
	if !d.RequiresApproval {
		t.Error("expected restart_pod to require approval in manual mode")
	}
	if d.Reason != "manual_mode" {
		t.Errorf("expected reason manual_mode, got %q", d.Reason)
	}
}

func TestEvaluate_AutoModeAllowsNonFloorAction(t *testing.T) {
	d := gate.Evaluate("restart_pod", gate.ModeAuto)
	if d.RequiresApproval {
		t.Error("expected restart_pod in auto mode to not require approval")
	}
	if d.Reason != "auto_approved" {
		t.Errorf("expected reason auto_approved, got %q", d.Reason)
	}
}

// TestEvaluate_UnrecognizedModeRequiresApproval guards against the gate
// failing open on a typo'd or garbage mode string (e.g. "Manual",
// "manual-approve", "off", or an empty value from an unset env var). Only
// gate.ModeAuto may allow an action through unattended; anything else,
// recognized or not, must require approval.
func TestEvaluate_UnrecognizedModeRequiresApproval(t *testing.T) {
	garbageModes := []gate.Mode{"Manual", "manual-approve", "off", ""}
	for _, mode := range garbageModes {
		t.Run(string(mode), func(t *testing.T) {
			d := gate.Evaluate("restart_pod", mode)
			if !d.RequiresApproval {
				t.Errorf("expected restart_pod to require approval for unrecognized mode %q (fail closed)", mode)
			}
			if d.Reason != "unrecognized_mode" {
				t.Errorf("expected reason unrecognized_mode, got %q", d.Reason)
			}
		})
	}
}
