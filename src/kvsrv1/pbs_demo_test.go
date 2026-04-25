package kvsrv

import (
	"testing"
)

func TestPBSDemoPlotsFromRunningCluster(t *testing.T) {
	opts := DefaultPBSDemoOptions()
	opts.OutputDir = t.TempDir()

	output, err := RunPBSDemo(opts)
	if err != nil {
		t.Fatalf("RunPBSDemo failed: %v", err)
	}

	if err := assertPBSDemoPlotExists(output.Plots.DeltaPPath); err != nil {
		t.Fatalf("delta plot check failed: %v", err)
	}
	if err := assertPBSDemoPlotExists(output.Plots.KPPath); err != nil {
		t.Fatalf("k plot check failed: %v", err)
	}
	if output.Plots.SeriesConfigCSVPath == "" {
		t.Fatalf("expected series config CSV path to be populated")
	}
	baseline, ok := output.Stats["observe_baseline"]
	if !ok {
		t.Fatalf("expected baseline scenario stats")
	}
	if baseline.ReadOK == 0 {
		t.Fatalf("expected demo to record successful reads")
	}
	if baseline.WriteOK == 0 {
		t.Fatalf("expected demo to record successful writes")
	}

	t.Logf("generated PBS plots from running cluster: %s and %s", output.Plots.DeltaPPath, output.Plots.KPPath)
}
