// Diagnosis is produced by ai/ (see docs/superpowers/specs/2026-08-08-ai-diagnosis-service-design.md)
// and delivered to backend/ over the HTTP callback in httpserver.NewRouter's
// diagnosis route. This package now holds only that wire-format shape — the
// analyzers that used to live here (CrashLoopAnalyzer) were ported to
// ai/ai/analyzers/.
package analyze

type Diagnosis struct {
	FailureMode       string
	RecommendedAction string
	Confidence        float64
}
