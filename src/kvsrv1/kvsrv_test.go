package kvsrv

import (
	//"log"
	"fmt"
	"testing"
	"time"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"6.5840/vclock"
)

const testContextNode = "__test__"

func contextFromCounter(counter uint64) rpc.Context {
	vc := vclock.NewVClock()
	if counter > 0 {
		vc.SetVersion(testContextNode, counter)
	}
	return rpc.NewContextFromVClock(vc)
}

func zeroContext() rpc.Context {
	return rpc.NewContext()
}

func contextCounter(ctx rpc.Context) uint64 {
	var total uint64
	for _, ver := range ctx.VC {
		total += ver
	}
	return total
}

func cloneContext(ctx rpc.Context) rpc.Context {
	clone := ctx
	clone.VC = ctx.VC.Copy()
	return clone
}

// Test Put with a single client and a reliable network
func TestReliablePut(t *testing.T) {
	const Val = "6.5840"
	const Ver = 0

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("One client and reliable Put")

	ck := ts.MakeClerk()
	if err := ck.Put("k", Val, contextFromCounter(Ver)); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}

	if val, ver, err := ck.Get("k"); err != rpc.OK {
		t.Fatalf("Get err %v; expected OK", err)
	} else if val != Val {
		t.Fatalf("Get value err %v; expected %v", val, Val)
	} else if contextCounter(ver) != Ver+1 {
		t.Fatalf("Get wrong version %v; expected %v", contextCounter(ver), Ver+1)
	}

	if err := ck.Put("k", Val, zeroContext()); err != rpc.ErrVersion {
		t.Fatalf("expected Put to fail with ErrVersion; got err=%v", err)
	}

	if err := ck.Put("y", Val, contextFromCounter(1)); err != rpc.ErrNoKey {
		t.Fatalf("expected Put to fail with ErrNoKey; got err=%v", err)
	}

	if _, _, err := ck.Get("y"); err != rpc.ErrNoKey {
		t.Fatalf("expected Get to fail with ErrNoKey; got err=%v", err)
	}
}

// Concurrent writes from the same base context should surface as siblings.
func TestPutConcurrentReliable(t *testing.T) {
	const (
		NCLNT         = 10
	)

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Test: concurrent writes produce siblings")

	ck := ts.MakeClerk()
	tck := ck.(*kvtest.TestClerk)

	const key = "k-siblings"
	if err := ck.Put(key, "base", zeroContext()); err != rpc.OK {
		t.Fatalf("base put failed: %v", err)
	}

	_, baseCtx, err := ck.Get(key)
	if err != rpc.OK {
		t.Fatalf("base get failed: %v", err)
	}

	errCh := make(chan rpc.Err, NCLNT)
	for i := 0; i < NCLNT; i++ {
		ctx := makeConcurrentContext(baseCtx, fmt.Sprintf("writer-%d", i), uint64(i+1))
		value := fmt.Sprintf("value-%d", i)
		go func(ctx rpc.Context, value string) {
			errCh <- ck.Put(key, value, ctx)
		}(ctx, value)
	}

	for i := 0; i < NCLNT; i++ {
		if err := <-errCh; err != rpc.OK {
			t.Fatalf("concurrent put %d failed: %v", i, err)
		}
	}

	raw := rawCoordGet(t, tck, key)
	if raw.Err != rpc.OK {
		t.Fatalf("raw get failed: err=%v", raw.Err)
	}
	if got := len(raw.Objects); got != NCLNT {
		t.Fatalf("expected %d siblings after concurrent writes, got %d", NCLNT, got)
	}
}

// The old memory bound test assumed a single-version linearizable store.
// Dynamo-style sibling retention has a different memory profile.
func TestMemPutManyClientsReliable(t *testing.T) {
	t.Skip("single-version memory bound is not meaningful with sibling-preserving Dynamo semantics")
}

// Test with one client and unreliable network under Dynamo-style semantics:
// if a write doesn't reach W, client sees failure, but partial writes may exist.
func TestUnreliableNet(t *testing.T) {
	const NTRY = 100

	ts := MakeTestKV(t, false)
	defer ts.Cleanup()

	ts.Begin("One client")

	ck := ts.MakeClerk()

	readVersion := func() rpc.Context {
		for i := 0; i < 30; i++ {
			_, ver, err := ck.Get("k")
			if err == rpc.OK {
				return ver
			}
			if err == rpc.ErrNoKey {
				return zeroContext()
			}
			if err == rpc.ErrReadQuorumNotMet || err == rpc.ErrNotCoordinator || err == rpc.ErrRPCFailure {
				continue
			}
			t.Fatalf("Get failed with unexpected err=%v", err)
		}
		t.Fatalf("Get did not reach read quorum after retries")
		return zeroContext()
	}

	sawQuorumFail := false
	for try := 0; try < NTRY; try++ {
		verBefore := readVersion()
		err := ts.PutJson(ck, "k", try, verBefore, 0)
		switch err {
		case rpc.OK, rpc.ErrMaybe, rpc.ErrVersion, rpc.ErrNoKey, rpc.ErrWriteQuorumNotMet:
			// acceptable in unreliable mode
		default:
			t.Fatalf("unexpected Put err %v", err)
		}
		if err == rpc.ErrWriteQuorumNotMet {
			sawQuorumFail = true
		}

		_ = verBefore
		_ = readVersion() // keep exercising reads; no monotonic guarantee under partial writes
	}
	if !sawQuorumFail {
		t.Logf("warning: did not observe ErrWriteQuorumNotMet in this run")
	}
}

// TestConflictSiblingsAndConverge validates the Dynamo-style conflict flow:
// 1) concurrent writes can produce multiple siblings on GET,
// 2) the client merges siblings with a deterministic policy,
// 3) writing merged value/context converges to a single branch.
//
func TestConflictSiblingsAndConverge(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Conflict siblings and converge")

	ck := ts.MakeClerk()
	tck := ck.(*kvtest.TestClerk)

	const key = "k-conflict"
	if err := ck.Put(key, "base", zeroContext()); err != rpc.OK {
		t.Fatalf("base put failed: %v", err)
	}

	// Get a base context and create two concurrent contexts.
	_, baseCtx, err := ck.Get(key)
	if err != rpc.OK {
		t.Fatalf("base get failed: %v", err)
	}

	ctxA := cloneContext(baseCtx)
	ctxA.VC.SetVersion("writer-A", ctxA.VC.GetVersion("writer-A")+1)
	ctxA.Timestamp += 1

	ctxB := cloneContext(baseCtx)
	ctxB.VC.SetVersion("writer-B", ctxB.VC.GetVersion("writer-B")+1)
	ctxB.Timestamp += 2

	// Submit two writes that are concurrent in vector-clock order.
	errCh := make(chan rpc.Err, 2)
	go func() { errCh <- ck.Put(key, "va", ctxA) }()
	go func() { errCh <- ck.Put(key, "vb", ctxB) }()

	e1, e2 := <-errCh, <-errCh
	if !(e1 == rpc.OK || e1 == rpc.ErrMaybe || e1 == rpc.ErrWriteQuorumNotMet) {
		t.Fatalf("put A unexpected err=%v", e1)
	}
	if !(e2 == rpc.OK || e2 == rpc.ErrMaybe || e2 == rpc.ErrWriteQuorumNotMet) {
		t.Fatalf("put B unexpected err=%v", e2)
	}

	// Raw coordinator GET is required to observe siblings.
	raw1 := rawCoordGet(t, tck, key)
	if raw1.Err != rpc.OK {
		t.Fatalf("raw get failed: err=%v", raw1.Err)
	}
	if len(raw1.Objects) < 2 {
		t.Fatalf("expected siblings, got %d object(s)", len(raw1.Objects))
	}

	// Merge sibling contexts and write back a resolved value to converge branches.
	mergedValue := "merged"
	mergedCtx := mergeSiblingContexts(raw1.Objects, mergedValue)
	if err := ck.Put(key, mergedValue, mergedCtx); err != rpc.OK {
		t.Fatalf("merge write failed: err=%v", err)
	}

	// After merge write, GET should converge to one branch.
	raw2 := rawCoordGet(t, tck, key)
	if raw2.Err != rpc.OK {
		t.Fatalf("raw get after merge failed: err=%v", raw2.Err)
	}
	if len(raw2.Objects) != 1 {
		t.Fatalf("expected convergence to 1 sibling, got %d", len(raw2.Objects))
	}
	if raw2.Objects[0].Value != mergedValue {
		t.Fatalf("expected merged value %q, got %q", mergedValue, raw2.Objects[0].Value)
	}
}

// Repeated quorum reads should not invent extra siblings when replicas
// already store the same sibling set.
func TestRepeatedReadsDoNotDuplicateSiblings(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Repeated reads keep sibling count stable")

	ck := ts.MakeClerk()
	tck := ck.(*kvtest.TestClerk)

	const key = "k-stable-siblings"
	if err := ck.Put(key, "base", zeroContext()); err != rpc.OK {
		t.Fatalf("base put failed: %v", err)
	}

	_, baseCtx, err := ck.Get(key)
	if err != rpc.OK {
		t.Fatalf("base get failed: %v", err)
	}

	ctxA := makeConcurrentContext(baseCtx, "writer-A", 1)
	ctxB := makeConcurrentContext(baseCtx, "writer-B", 2)

	if err := ck.Put(key, "va", ctxA); err != rpc.OK {
		t.Fatalf("put A failed: %v", err)
	}
	if err := ck.Put(key, "vb", ctxB); err != rpc.OK {
		t.Fatalf("put B failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		raw := rawCoordGet(t, tck, key)
		if raw.Err != rpc.OK {
			t.Fatalf("raw get %d failed: err=%v", i, raw.Err)
		}
		if got := len(raw.Objects); got != 2 {
			t.Fatalf("expected 2 siblings after read %d, got %d", i, got)
		}
	}
}

// A write based on a merged context should dominate and remove the old siblings.
func TestDominatingWriteRemovesOldSiblings(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Dominating write removes old siblings")

	ck := ts.MakeClerk()
	tck := ck.(*kvtest.TestClerk)

	const key = "k-dominates"
	if err := ck.Put(key, "base", zeroContext()); err != rpc.OK {
		t.Fatalf("base put failed: %v", err)
	}

	_, baseCtx, err := ck.Get(key)
	if err != rpc.OK {
		t.Fatalf("base get failed: %v", err)
	}

	ctxA := makeConcurrentContext(baseCtx, "writer-A", 1)
	ctxB := makeConcurrentContext(baseCtx, "writer-B", 2)
	if err := ck.Put(key, "va", ctxA); err != rpc.OK {
		t.Fatalf("put A failed: %v", err)
	}
	if err := ck.Put(key, "vb", ctxB); err != rpc.OK {
		t.Fatalf("put B failed: %v", err)
	}

	rawBefore := rawCoordGet(t, tck, key)
	if rawBefore.Err != rpc.OK {
		t.Fatalf("raw get before merge failed: err=%v", rawBefore.Err)
	}
	if got := len(rawBefore.Objects); got != 2 {
		t.Fatalf("expected 2 siblings before dominating write, got %d", got)
	}

	mergedValue := "resolved"
	mergedCtx := mergeSiblingContexts(rawBefore.Objects, mergedValue)
	if err := ck.Put(key, mergedValue, mergedCtx); err != rpc.OK {
		t.Fatalf("dominating put failed: %v", err)
	}

	rawAfter := rawCoordGet(t, tck, key)
	if rawAfter.Err != rpc.OK {
		t.Fatalf("raw get after merge failed: err=%v", rawAfter.Err)
	}
	if got := len(rawAfter.Objects); got != 1 {
		t.Fatalf("expected 1 sibling after dominating write, got %d", got)
	}
	if rawAfter.Objects[0].Value != mergedValue {
		t.Fatalf("expected dominating value %q, got %q", mergedValue, rawAfter.Objects[0].Value)
	}
}

// A successful coordinator read should asynchronously repair a replica whose
// sibling set is stale relative to the canonical quorum result.
func TestReadRepairRepairsStaleReplica(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Read repair repairs stale replica")

	ck := ts.MakeClerk()
	tck := ck.(*kvtest.TestClerk)

	const key = "k-read-repair"
	if err := ck.Put(key, "base", zeroContext()); err != rpc.OK {
		t.Fatalf("base put failed: %v", err)
	}

	_, baseCtx, err := ck.Get(key)
	if err != rpc.OK {
		t.Fatalf("base get failed: %v", err)
	}

	ctxA := makeConcurrentContext(baseCtx, "writer-A", 1)
	ctxB := makeConcurrentContext(baseCtx, "writer-B", 2)
	if err := ck.Put(key, "va", ctxA); err != rpc.OK {
		t.Fatalf("put A failed: %v", err)
	}
	if err := ck.Put(key, "vb", ctxB); err != rpc.OK {
		t.Fatalf("put B failed: %v", err)
	}

	prefList := preferenceListForKey(key)
	target := prefList[len(prefList)-1]
	canonicalObjects := make([]rpc.Object, 0)
	for _, server := range prefList {
		got := rawReplicaGet(t, tck, server, key)
		if got.Err != rpc.OK {
			t.Fatalf("replica %s get failed: err=%v", server, got.Err)
		}
		for _, obj := range got.Objects {
			canonicalObjects, _ = rpc.AddObject(canonicalObjects, obj)
		}
	}
	if got := len(canonicalObjects); got != 2 {
		t.Fatalf("expected 2 canonical siblings, got %d", got)
	}
	staleArgs := rpc.RepairArgs{
		Key:     key,
		Objects: []rpc.Object{canonicalObjects[0]},
	}
	staleReply := rpc.RepairReply{}
	ok := tck.Clnt.Call(target, "KVServer.RepairPut", &staleArgs, &staleReply)
	if !ok || staleReply.Err != rpc.OK {
		t.Fatalf("failed to make replica stale: ok=%v err=%v", ok, staleReply.Err)
	}

	before := rawReplicaGet(t, tck, target, key)
	if before.Err != rpc.OK || len(before.Objects) != 1 {
		t.Fatalf("expected stale replica to have 1 sibling, got err=%v len=%d", before.Err, len(before.Objects))
	}

	trigger := rawCoordGet(t, tck, key)
	if trigger.Err != rpc.OK {
		t.Fatalf("trigger get failed: err=%v", trigger.Err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := rawReplicaGet(t, tck, target, key)
		if got.Err == rpc.OK && IsSameSiblings(canonicalObjects, got.Objects) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	final := rawReplicaGet(t, tck, target, key)
	t.Fatalf("read repair did not converge; final err=%v objects=%d", final.Err, len(final.Objects))
}

func makeConcurrentContext(base rpc.Context, writer string, tsBump uint64) rpc.Context {
	ctx := cloneContext(base)
	ctx.VC.SetVersion(writer, ctx.VC.GetVersion(writer)+1)
	ctx.Timestamp += tsBump
	return ctx
}

func mergeSiblingContexts(objects []rpc.Object, mergedValue string) rpc.Context {
	if len(objects) == 0 {
		return zeroContext()
	}

	merged := cloneContext(objects[0].Context)
	for _, obj := range objects[1:] {
		merged = merged.Merge(obj.Context, mergedValue)
	}
	return merged
}

func rawCoordGet(t *testing.T, tck *kvtest.TestClerk, key string) rpc.GetReply {
	t.Helper()

	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	for i := 0; i < numServers; i++ {
		server := tester.ServerName(tester.GRP0, i)
		ok := tck.Clnt.Call(server, "KVServer.CoordGet", &args, &reply)
		if ok && reply.Err != rpc.ErrNotCoordinator {
			return reply
		}
	}
	return reply
}

func rawReplicaGet(t *testing.T, tck *kvtest.TestClerk, server, key string) rpc.GetReply {
	t.Helper()

	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	ok := tck.Clnt.Call(server, "KVServer.ReplicaGet", &args, &reply)
	if !ok {
		t.Fatalf("replica get RPC failed for %s", server)
	}
	return reply
}

func preferenceListForKey(key string) []string {
	nodeIDs := make([]string, 0, numServers)
	for i := 0; i < numServers; i++ {
		nodeIDs = append(nodeIDs, tester.ServerName(tester.GRP0, i))
	}
	ring := chr.MakeConsistentHashRing(numReplicas, numSectors, numServers, nodeIDs)
	return ring.GetPreferenceList(key)
}
