package analyze

import "sre-platform/backend/internal/correlate"

type Diagnosis struct {
	FailureMode       string
	RecommendedAction string
	Confidence        float64
}

type Analyzer interface {
	Analyze(inc correlate.Incident) (*Diagnosis, bool)
}

type CrashLoopAnalyzer struct{}

func (CrashLoopAnalyzer) Analyze(inc correlate.Incident) (*Diagnosis, bool) {
	for _, s := range inc.Signals {
		if s.Type == "CrashLoopBackOff" {
			return &Diagnosis{
				FailureMode:       "CrashLoopBackOff",
				RecommendedAction: "restart_pod",
				Confidence:        0.9,
			}, true
		}
	}
	return nil, false
}
