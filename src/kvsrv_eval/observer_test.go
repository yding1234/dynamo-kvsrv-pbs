package kvsrv_eval

import (
	"testing"
	"time"

	"dynamo-kvsrv/kvsrv/rpc"
	"dynamo-kvsrv/kvsrv/vclock"
)

func makeObservedObject(value string, timestamp uint64, versions map[string]uint64) rpc.Object {
	vc := vclock.NewVClock()
	for node, version := range versions {
		vc.SetVersion(node, version)
	}
	return rpc.Object{
		Value: value,
		Context: rpc.Context{
			VC:        vc,
			Timestamp: timestamp,
			ETag:      value,
		},
	}
}

func TestIsConsistentAcceptsNewerCanonicalSibling(t *testing.T) {
	target := makeObservedObject("v1", 10, map[string]uint64{"writer": 1})
	newer := makeObservedObject("v2", 20, map[string]uint64{"writer": 2})

	if !IsConsistent(target, []rpc.Object{newer}) {
		t.Fatalf("expected consistency when the canonical sibling is newer")
	}
}

func TestIsConsistentRejectsOlderCanonicalSibling(t *testing.T) {
	target := makeObservedObject("v1", 10, map[string]uint64{"writer": 1})
	older := makeObservedObject("v0", 5, map[string]uint64{})

	if IsConsistent(target, []rpc.Object{older}) {
		t.Fatalf("expected inconsistency when the canonical sibling is older")
	}
}

func TestObserveDeltaPCountsEachReadOnce(t *testing.T) {
	collector := NewPBSCollector()
	base := time.Unix(1000, 0)

	oldWrite := NewCompletedWrite("k", base.Add(-20*time.Millisecond), base.Add(-20*time.Millisecond), makeObservedObject("v1", 10, map[string]uint64{"writer": 1}))
	overlapWrite := NewCompletedWrite("k", base.Add(-5*time.Millisecond), base.Add(5*time.Millisecond), makeObservedObject("v2", 20, map[string]uint64{"writer": 2}))
	read := NewCompletedRead("k", base, base.Add(1*time.Millisecond), []rpc.Object{
		makeObservedObject("v2", 20, map[string]uint64{"writer": 2}),
	})

	collector.ObserveCompletedWrite(oldWrite)
	collector.ObserveCompletedWrite(overlapWrite)
	collector.ObserveCompletedRead(read)

	if got := ObserveDeltaP(collector, 10*time.Millisecond); got != 1.0 {
		t.Fatalf("expected one read to count once, got probability %v", got)
	}
}

func TestObserveDeltaPRejectsWriteOlderThanDeltaWindow(t *testing.T) {
	collector := NewPBSCollector()
	base := time.Unix(1500, 0)

	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-20*time.Millisecond), base.Add(-20*time.Millisecond), makeObservedObject("v1", 10, map[string]uint64{"writer": 1})))
	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-10*time.Millisecond), base.Add(-10*time.Millisecond), makeObservedObject("v2", 20, map[string]uint64{"writer": 2})))
	collector.ObserveCompletedRead(NewCompletedRead("k", base, base.Add(1*time.Millisecond), []rpc.Object{
		makeObservedObject("v1", 10, map[string]uint64{"writer": 1}),
	}))

	if got := ObserveDeltaP(collector, 5*time.Millisecond); got != 0.0 {
		t.Fatalf("expected read older than the delta window to be rejected, got %v", got)
	}
}

func TestObserveKPUsesLatestKCompletedWrites(t *testing.T) {
	collector := NewPBSCollector()
	base := time.Unix(2000, 0)

	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-30*time.Millisecond), base.Add(-30*time.Millisecond), makeObservedObject("v1", 10, map[string]uint64{"writer": 1})))
	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-20*time.Millisecond), base.Add(-20*time.Millisecond), makeObservedObject("v2", 20, map[string]uint64{"writer": 2})))
	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-10*time.Millisecond), base.Add(-10*time.Millisecond), makeObservedObject("v3", 30, map[string]uint64{"writer": 3})))
	collector.ObserveCompletedRead(NewCompletedRead("k", base, base.Add(1*time.Millisecond), []rpc.Object{
		makeObservedObject("v2", 20, map[string]uint64{"writer": 2}),
	}))

	if got := ObserveKP(collector, 1); got != 0.0 {
		t.Fatalf("expected K=1 to reject returning the second-latest completed write, got %v", got)
	}
	if got := ObserveKP(collector, 2); got != 1.0 {
		t.Fatalf("expected K=2 to accept returning one of the latest two completed writes, got %v", got)
	}
}

func TestObserveKPAcceptsOverlappingWrite(t *testing.T) {
	collector := NewPBSCollector()
	base := time.Unix(3000, 0)

	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-20*time.Millisecond), base.Add(-20*time.Millisecond), makeObservedObject("v1", 10, map[string]uint64{"writer": 1})))
	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-5*time.Millisecond), base.Add(5*time.Millisecond), makeObservedObject("v2", 20, map[string]uint64{"writer": 2})))
	collector.ObserveCompletedRead(NewCompletedRead("k", base, base.Add(1*time.Millisecond), []rpc.Object{
		makeObservedObject("v2", 20, map[string]uint64{"writer": 2}),
	}))

	if got := ObserveKP(collector, 1); got != 1.0 {
		t.Fatalf("expected overlapping write to satisfy K-regular semantics, got %v", got)
	}
}

func TestObserveKPAcceptsWriteThatCommitsAfterReadReturns(t *testing.T) {
	collector := NewPBSCollector()
	base := time.Unix(3500, 0)

	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-20*time.Millisecond), base.Add(-20*time.Millisecond), makeObservedObject("v1", 10, map[string]uint64{"writer": 1})))
	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(-5*time.Millisecond), base.Add(5*time.Millisecond), makeObservedObject("v2", 20, map[string]uint64{"writer": 2})))
	collector.ObserveCompletedRead(NewCompletedRead("k", base, base.Add(1*time.Millisecond), []rpc.Object{
		makeObservedObject("v2", 20, map[string]uint64{"writer": 2}),
	}))

	if got := ObserveKP(collector, 1); got != 1.0 {
		t.Fatalf("expected overlapping write that commits after the read returns to be accepted, got %v", got)
	}
}

func TestPBSCollectorMaintainsChronologicalOrder(t *testing.T) {
	collector := NewPBSCollector()
	base := time.Unix(4000, 0)

	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(30*time.Millisecond), base.Add(30*time.Millisecond), makeObservedObject("v3", 30, map[string]uint64{"writer": 3})))
	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(10*time.Millisecond), base.Add(10*time.Millisecond), makeObservedObject("v1", 10, map[string]uint64{"writer": 1})))
	collector.ObserveCompletedWrite(NewCompletedWrite("k", base.Add(20*time.Millisecond), base.Add(20*time.Millisecond), makeObservedObject("v2", 20, map[string]uint64{"writer": 2})))

	writes := collector.Writes()
	if len(writes) != 3 {
		t.Fatalf("expected 3 writes, got %d", len(writes))
	}
	if !writes[0].CommittedAt.Before(writes[1].CommittedAt) || !writes[1].CommittedAt.Before(writes[2].CommittedAt) {
		t.Fatalf("expected writes to stay sorted by commit time, got %v, %v, %v", writes[0].CommittedAt, writes[1].CommittedAt, writes[2].CommittedAt)
	}

	collector.ObserveCompletedRead(NewCompletedRead("k", base.Add(30*time.Millisecond), base.Add(30*time.Millisecond), nil))
	collector.ObserveCompletedRead(NewCompletedRead("k", base.Add(10*time.Millisecond), base.Add(10*time.Millisecond), nil))
	collector.ObserveCompletedRead(NewCompletedRead("k", base.Add(20*time.Millisecond), base.Add(20*time.Millisecond), nil))

	reads := collector.Reads()
	if len(reads) != 3 {
		t.Fatalf("expected 3 reads, got %d", len(reads))
	}
	if !reads[0].ReturnedAt.Before(reads[1].ReturnedAt) || !reads[1].ReturnedAt.Before(reads[2].ReturnedAt) {
		t.Fatalf("expected reads to stay sorted by return time, got %v, %v, %v", reads[0].ReturnedAt, reads[1].ReturnedAt, reads[2].ReturnedAt)
	}
}
