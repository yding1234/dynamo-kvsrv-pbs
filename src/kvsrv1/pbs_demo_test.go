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

	if err := assertPBSDemoPlotExists(output.DeltaPPath); err != nil {
		t.Fatalf("delta plot check failed: %v", err)
	}
	if err := assertPBSDemoPlotExists(output.KPPath); err != nil {
		t.Fatalf("k plot check failed: %v", err)
	}

	t.Logf("generated PBS plots from running cluster: %s and %s", output.DeltaPPath, output.KPPath)
}
