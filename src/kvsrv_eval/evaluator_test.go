package kvsrv_eval

import (
	"math/rand"
	"testing"
	"time"
)

func TestEvaluateKPMonotonicInK(t *testing.T) {
	baseConfig := SimulationConfig{
		NumReplicas: 3,
		ReadQuorum:  1,
		WriteQuorum: 1,
	}

	results, err := EvaluateKPSweep(baseConfig, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("EvaluateKPSweep failed: %v", err)
	}

	if results[0].Probability <= 0 || results[0].Probability >= 1 {
		t.Fatalf("expected K=1 probability to be strictly between 0 and 1, got %v", results[0].Probability)
	}
	if results[1].Probability < results[0].Probability {
		t.Fatalf("expected K=2 to be at least as consistent as K=1: %v < %v", results[1].Probability, results[0].Probability)
	}
	if results[2].Probability < results[1].Probability {
		t.Fatalf("expected K=3 to be at least as consistent as K=2: %v < %v", results[2].Probability, results[1].Probability)
	}
	if results[2].Probability <= results[0].Probability {
		t.Fatalf("expected larger K to improve consistency in this quorum setup: K=1 %v, K=3 %v", results[0].Probability, results[2].Probability)
	}
}

func TestEvaluateKPRejectsInvalidConfig(t *testing.T) {
	_, err := EvaluateKP(SimulationConfig{
		NumReplicas: 3,
		ReadQuorum:  1,
		WriteQuorum: 1,
		K:           0,
	})
	if err == nil {
		t.Fatalf("expected invalid K to be rejected")
	}
}

func TestEvaluateDeltaPFromTracerRejectsNilTracer(t *testing.T) {
	_, err := EvaluateDeltaPFromTracer(nil, SimulationConfig{
		NumReplicas: 3,
		ReadQuorum:  1,
		WriteQuorum: 1,
		Delta:       0,
		Iterations:  10,
	})
	if err == nil {
		t.Fatalf("expected nil tracer to be rejected")
	}
}

func TestObservedTraceSupportsDeltaAndKSweeps(t *testing.T) {
	base := time.Unix(1_000, 0)
	tracer := NewTracer()

	// Two observed write latencies create a realistic "fast replica / slow
	// replica" trace, which lets larger delta values improve the observed
	// consistency probability.
	if err := tracer.ObserveWrite(MessageTrace{
		SentAt:      base,
		ArrivedAt:   base.Add(1 * time.Millisecond),
		RespondedAt: base.Add(1 * time.Millisecond),
		ReceivedAt:  base.Add(1 * time.Millisecond),
	}); err != nil {
		t.Fatalf("ObserveWrite failed: %v", err)
	}
	if err := tracer.ObserveWrite(MessageTrace{
		SentAt:      base,
		ArrivedAt:   base.Add(20 * time.Millisecond),
		RespondedAt: base.Add(20 * time.Millisecond),
		ReceivedAt:  base.Add(20 * time.Millisecond),
	}); err != nil {
		t.Fatalf("ObserveWrite failed: %v", err)
	}
	if err := tracer.ObserveRead(MessageTrace{
		SentAt:      base.Add(50 * time.Millisecond),
		ArrivedAt:   base.Add(50 * time.Millisecond),
		RespondedAt: base.Add(50 * time.Millisecond),
		ReceivedAt:  base.Add(50 * time.Millisecond),
	}); err != nil {
		t.Fatalf("ObserveRead failed: %v", err)
	}

	samplers, err := tracer.GetSamplers()
	if err != nil {
		t.Fatalf("GetSamplers failed: %v", err)
	}
	if got := samplers.WriteRequest.Samples(); len(got) != 2 || got[0] != 1*time.Millisecond || got[1] != 20*time.Millisecond {
		t.Fatalf("unexpected exported write request samples: %v", got)
	}

	trace := tracer.Trace()
	deltaResults, err := EvaluateDeltaPSweep(trace, SimulationConfig{
		NumReplicas: 3,
		ReadQuorum:  1,
		WriteQuorum: 1,
		Iterations:  5000,
		RNG:         rand.New(rand.NewSource(7)),
	}, []time.Duration{0, 25 * time.Millisecond})
	if err != nil {
		t.Fatalf("EvaluateDeltaPSweep failed: %v", err)
	}

	if deltaResults[1].Probability < deltaResults[0].Probability {
		t.Fatalf("expected larger delta to not reduce consistency: early=%v later=%v", deltaResults[0].Probability, deltaResults[1].Probability)
	}
	if deltaResults[1].Probability == deltaResults[0].Probability {
		t.Fatalf("expected larger delta to improve consistency: early=%v later=%v", deltaResults[0].Probability, deltaResults[1].Probability)
	}

	kResults, err := EvaluateKPSweep(SimulationConfig{
		NumReplicas: 3,
		ReadQuorum:  1,
		WriteQuorum: 1,
	}, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("EvaluateKPSweep failed: %v", err)
	}
	if kResults[2].Probability <= kResults[0].Probability {
		t.Fatalf("expected K sweep to improve consistency: K=1 %v, K=3 %v", kResults[0].Probability, kResults[2].Probability)
	}
}
