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
	if output.Stats.ReadOK == 0 {
		t.Fatalf("expected demo to record successful reads")
	}
	if output.Stats.WriteOK == 0 {
		t.Fatalf("expected demo to record successful writes")
	}

	t.Logf("generated PBS plots from running cluster: %s and %s", output.Plots.DeltaPPath, output.Plots.KPPath)
}
