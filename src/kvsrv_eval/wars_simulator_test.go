package kvsrv_eval

import (
	"math/rand"
	"testing"
	"time"
)

func TestSimulateDeltaPStrictQuorumIsAlwaysConsistent(t *testing.T) {
	samplers, err := NewWARSSamplers(WARSTrace{
		WriteRequests: []time.Duration{time.Millisecond, 10 * time.Millisecond},
		WriteAcks:     []time.Duration{time.Millisecond, 5 * time.Millisecond},
		ReadRequests:  []time.Duration{time.Millisecond, 3 * time.Millisecond},
		ReadResponses: []time.Duration{time.Millisecond, 4 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("failed to build samplers: %v", err)
	}

	result, err := SimulateDeltaP(SimulationConfig{
		NumReplicas: 3,
		ReadQuorum:  2,
		WriteQuorum: 2,
		Delta:       0,
		Iterations:  500,
		RNG:         rand.New(rand.NewSource(1)),
	}, samplers)
	if err != nil {
		t.Fatalf("SimulateDeltaP failed: %v", err)
	}

	if result.Probability != 1.0 {
		t.Fatalf("strict quorum should always be consistent, got %v", result.Probability)
	}
}

func TestSimulateDeltaPImprovesWithLargerDelta(t *testing.T) {
	samplers, err := NewWARSSamplers(WARSTrace{
		WriteRequests: []time.Duration{time.Millisecond, 20 * time.Millisecond},
		WriteAcks:     []time.Duration{0},
		ReadRequests:  []time.Duration{0},
		ReadResponses: []time.Duration{0},
	})
	if err != nil {
		t.Fatalf("failed to build samplers: %v", err)
	}

	base := SimulationConfig{
		NumReplicas: 3,
		ReadQuorum:  1,
		WriteQuorum: 1,
		Iterations:  5000,
	}
	early, err := SimulateDeltaP(SimulationConfig{
		NumReplicas: base.NumReplicas,
		ReadQuorum:  base.ReadQuorum,
		WriteQuorum: base.WriteQuorum,
		Delta:       0,
		Iterations:  base.Iterations,
		RNG:         rand.New(rand.NewSource(2)),
	}, samplers)
	if err != nil {
		t.Fatalf("SimulateDeltaP early failed: %v", err)
	}
	later, err := SimulateDeltaP(SimulationConfig{
		NumReplicas: base.NumReplicas,
		ReadQuorum:  base.ReadQuorum,
		WriteQuorum: base.WriteQuorum,
		Delta:       25 * time.Millisecond,
		Iterations:  base.Iterations,
		RNG:         rand.New(rand.NewSource(2)),
	}, samplers)
	if err != nil {
		t.Fatalf("SimulateDeltaP later failed: %v", err)
	}

	if later.Probability < early.Probability {
		t.Fatalf("expected larger delta to not reduce consistency: early=%v later=%v", early.Probability, later.Probability)
	}
	if later.Probability == early.Probability {
		t.Fatalf("expected larger delta to improve consistency in this workload: early=%v later=%v", early.Probability, later.Probability)
	}
}
