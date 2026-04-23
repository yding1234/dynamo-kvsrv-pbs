package kvsrv_eval

import (
	"math/rand"
	"testing"
	"time"
)

func TestNewWARSSamplersFromTraceRejectsEmptyTrace(t *testing.T) {
	_, err := NewWARSSamplers(WARSTrace{})
	if err == nil {
		t.Fatalf("expected empty trace to be rejected")
	}
}

func TestEmpiricalSamplerReturnsObservedSamples(t *testing.T) {
	sampler, err := NewEmpiricalSampler([]time.Duration{time.Millisecond, 2 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewEmpiricalSampler failed: %v", err)
	}

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 20; i++ {
		got := sampler.GetSample(rng)
		if got != time.Millisecond && got != 2*time.Millisecond {
			t.Fatalf("sampler returned value outside observed set: %v", got)
		}
	}
}

func TestPBSCollectorObserveWriteAndRead(t *testing.T) {
	base := time.Unix(100, 0)
	collector := NewPBSCollector()

	err := collector.ObserveWriteLatency(MessageTrace{
		SentAt:      base,
		ArrivedAt:   base.Add(3 * time.Millisecond),
		RespondedAt: base.Add(8 * time.Millisecond),
		ReceivedAt:  base.Add(11 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("ObserveWrite failed: %v", err)
	}
	err = collector.ObserveReadLatency(MessageTrace{
		SentAt:      base.Add(20 * time.Millisecond),
		ArrivedAt:   base.Add(24 * time.Millisecond),
		RespondedAt: base.Add(25 * time.Millisecond),
		ReceivedAt:  base.Add(31 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("ObserveRead failed: %v", err)
	}

	trace := collector.Trace()
	if got, want := trace.WriteRequests, []time.Duration{3 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected write request samples: got=%v want=%v", got, want)
	}
	if got, want := trace.WriteAcks, []time.Duration{3 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected write ack samples: got=%v want=%v", got, want)
	}
	if got, want := trace.ReadRequests, []time.Duration{4 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected read request samples: got=%v want=%v", got, want)
	}
	if got, want := trace.ReadResponses, []time.Duration{6 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected read response samples: got=%v want=%v", got, want)
	}
}

func TestPBSCollectorGetSamplersUsesTrace(t *testing.T) {
	collector := NewPBSCollector()
	if err := collector.AddWriteSample(2*time.Millisecond, 5*time.Millisecond); err != nil {
		t.Fatalf("AddWriteSample failed: %v", err)
	}
	if err := collector.AddReadSample(7*time.Millisecond, 11*time.Millisecond); err != nil {
		t.Fatalf("AddReadSample failed: %v", err)
	}

	samplers, err := collector.GetSamplers()
	if err != nil {
		t.Fatalf("Samplers failed: %v", err)
	}
	if got := samplers.WriteRequest.Samples(); len(got) != 1 || got[0] != 2*time.Millisecond {
		t.Fatalf("unexpected write request sampler samples: %v", got)
	}
	if got := samplers.WriteAck.Samples(); len(got) != 1 || got[0] != 5*time.Millisecond {
		t.Fatalf("unexpected write ack sampler samples: %v", got)
	}
	if got := samplers.ReadRequest.Samples(); len(got) != 1 || got[0] != 7*time.Millisecond {
		t.Fatalf("unexpected read request sampler samples: %v", got)
	}
	if got := samplers.ReadResponse.Samples(); len(got) != 1 || got[0] != 11*time.Millisecond {
		t.Fatalf("unexpected read response sampler samples: %v", got)
	}
}

func TestPBSCollectorObserveMultipleTraces(t *testing.T) {
	base := time.Unix(200, 0)
	collector := NewPBSCollector()

	writeTraces := []MessageTrace{
		{
			SentAt:      base,
			ArrivedAt:   base.Add(2 * time.Millisecond),
			RespondedAt: base.Add(5 * time.Millisecond),
			ReceivedAt:  base.Add(8 * time.Millisecond),
		},
		{
			SentAt:      base.Add(10 * time.Millisecond),
			ArrivedAt:   base.Add(14 * time.Millisecond),
			RespondedAt: base.Add(18 * time.Millisecond),
			ReceivedAt:  base.Add(21 * time.Millisecond),
		},
	}
	readTraces := []MessageTrace{
		{
			SentAt:      base.Add(30 * time.Millisecond),
			ArrivedAt:   base.Add(33 * time.Millisecond),
			RespondedAt: base.Add(34 * time.Millisecond),
			ReceivedAt:  base.Add(40 * time.Millisecond),
		},
		{
			SentAt:      base.Add(50 * time.Millisecond),
			ArrivedAt:   base.Add(51 * time.Millisecond),
			RespondedAt: base.Add(55 * time.Millisecond),
			ReceivedAt:  base.Add(57 * time.Millisecond),
		},
	}

	for _, trace := range writeTraces {
		if err := collector.ObserveWriteLatency(trace); err != nil {
			t.Fatalf("ObserveWriteLatency failed: %v", err)
		}
	}
	for _, trace := range readTraces {
		if err := collector.ObserveReadLatency(trace); err != nil {
			t.Fatalf("ObserveReadLatency failed: %v", err)
		}
	}

	trace := collector.Trace()
	if got, want := trace.WriteRequests, []time.Duration{2 * time.Millisecond, 4 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected write request samples: got=%v want=%v", got, want)
	}
	if got, want := trace.WriteAcks, []time.Duration{3 * time.Millisecond, 3 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected write ack samples: got=%v want=%v", got, want)
	}
	if got, want := trace.ReadRequests, []time.Duration{3 * time.Millisecond, 1 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected read request samples: got=%v want=%v", got, want)
	}
	if got, want := trace.ReadResponses, []time.Duration{6 * time.Millisecond, 2 * time.Millisecond}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected read response samples: got=%v want=%v", got, want)
	}
}
