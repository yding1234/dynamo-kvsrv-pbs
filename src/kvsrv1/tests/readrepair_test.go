package kvsrv

import (
	"testing"
	"time"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

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
	canonicalObjects := collectCanonicalReplicaSiblings(t, tck, key, prefList)
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

	triggerReadRepairAndWait(t, tck, key, target, func(got rpc.GetReply) bool {
		return got.Err == rpc.OK && IsSameSiblings(canonicalObjects, got.Objects)
	})
}

// A successful coordinator read should also repair a replica that has lost the
// key entirely and returns ErrNoKey.
func TestReadRepairRepairsErrNoKeyReplica(t *testing.T) {
	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Read repair repairs ErrNoKey replica")

	ck := ts.MakeClerk()
	tck := ck.(*kvtest.TestClerk)

	const key = "k-read-repair-nokey"
	if err := ck.Put(key, "base", zeroContext()); err != rpc.OK {
		t.Fatalf("base put failed: %v", err)
	}

	prefList := preferenceListForKey(key)
	target := prefList[len(prefList)-1]
	canonicalObjects := collectCanonicalReplicaSiblings(t, tck, key, prefList)
	if got := len(canonicalObjects); got != 1 {
		t.Fatalf("expected 1 canonical object, got %d", got)
	}

	clearReply := rpc.RepairReply{}
	ok := tck.Clnt.Call(target, "KVServer.RepairPut", &rpc.RepairArgs{Key: key, Delete: true}, &clearReply)
	if !ok || clearReply.Err != rpc.OK {
		t.Fatalf("failed to clear replica state: ok=%v err=%v", ok, clearReply.Err)
	}

	before := rawReplicaGet(t, tck, target, key)
	if before.Err != rpc.ErrNoKey {
		t.Fatalf("expected target replica to return ErrNoKey before repair, got %v", before.Err)
	}

	triggerReadRepairAndWait(t, tck, key, target, func(got rpc.GetReply) bool {
		return got.Err == rpc.OK && IsSameSiblings(canonicalObjects, got.Objects)
	})
}

func TestReadRepairEventuallyRepairsStaleReplicaUnreliable(t *testing.T) {
	ts := MakeTestKV(t, false)
	defer ts.Cleanup()

	ts.Begin("Read repair eventually repairs stale replica under unreliable network")

	ck := ts.MakeClerk()
	tck := ck.(*kvtest.TestClerk)

	const key = "k-read-repair-unreliable"

	putUntilOK := func(value string, ctx rpc.Context) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			err := ck.Put(key, value, ctx)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrVersion {
				return
			}
			if err == rpc.ErrMaybe || err == rpc.ErrWriteQuorumNotMet || err == rpc.ErrNotCoordinator {
				continue
			}
			t.Fatalf("put %q failed with unexpected err=%v", value, err)
		}
		t.Fatalf("put %q did not succeed before deadline", value)
	}

	getUntilOK := func() rpc.Context {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			_, ctx, err := ck.Get(key)
			if err == rpc.OK {
				return ctx
			}
			if err == rpc.ErrReadQuorumNotMet || err == rpc.ErrNotCoordinator || err == rpc.ErrRPCFailure {
				continue
			}
			t.Fatalf("get failed with unexpected err=%v", err)
		}
		t.Fatalf("get did not succeed before deadline")
		return zeroContext()
	}

	coordGetUntilOK := func() []rpc.Object {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			reply := rawCoordGet(t, tck, key)
			if reply.Err == rpc.OK {
				return reply.Objects
			}
			if reply.Err == rpc.ErrReadQuorumNotMet || reply.Err == rpc.ErrNotCoordinator {
				continue
			}
			t.Fatalf("coord get failed with unexpected err=%v", reply.Err)
		}
		t.Fatalf("coord get did not succeed before deadline")
		return nil
	}

	replicaGetEventually := func(server string) rpc.GetReply {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			args := rpc.GetArgs{Key: key}
			reply := rpc.GetReply{}
			ok := tck.Clnt.Call(server, "KVServer.ReplicaGet", &args, &reply)
			if ok {
				return reply
			}
		}
		return rpc.GetReply{Err: rpc.ErrRPCFailure}
	}

	repairPutUntilOK := func(server string, args *rpc.RepairArgs) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			reply := rpc.RepairReply{}
			ok := tck.Clnt.Call(server, "KVServer.RepairPut", args, &reply)
			if ok && reply.Err == rpc.OK {
				return
			}
		}
		t.Fatalf("RepairPut to %s did not succeed before deadline", server)
	}

	putUntilOK("base", zeroContext())
	baseCtx := getUntilOK()

	putUntilOK("va", makeConcurrentContext(baseCtx, "writer-A", 1))
	putUntilOK("vb", makeConcurrentContext(baseCtx, "writer-B", 2))

	prefList := preferenceListForKey(key)
	target := prefList[len(prefList)-1]
	canonicalObjects := coordGetUntilOK()
	if got := len(canonicalObjects); got != 2 {
		t.Fatalf("expected 2 canonical siblings, got %d", got)
	}

	repairPutUntilOK(target, &rpc.RepairArgs{
		Key:     key,
		Objects: []rpc.Object{canonicalObjects[0]},
	})

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		trigger := rawCoordGet(t, tck, key)
		if trigger.Err != rpc.OK && trigger.Err != rpc.ErrReadQuorumNotMet && trigger.Err != rpc.ErrNotCoordinator {
			t.Fatalf("trigger get failed with unexpected err=%v", trigger.Err)
		}

		got := replicaGetEventually(target)
		if got.Err == rpc.OK && IsSameSiblings(canonicalObjects, got.Objects) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	final := replicaGetEventually(target)
	t.Fatalf("read repair did not converge under unreliable network; final err=%v objects=%d", final.Err, len(final.Objects))
}

func collectCanonicalReplicaSiblings(t *testing.T, tck *kvtest.TestClerk, key string, prefList []string) []rpc.Object {
	t.Helper()

	canonicalObjects := make([]rpc.Object, 0)
	for _, server := range prefList {
		got := rawReplicaGet(t, tck, server, key)
		if got.Err != rpc.OK {
			t.Fatalf("replica %s get failed: err=%v", server, got.Err)
		}
		for _, obj := range got.Objects {
			canonicalObjects = rpc.AddObject(canonicalObjects, obj, nil)
		}
	}
	return canonicalObjects
}

func triggerReadRepairAndWait(t *testing.T, tck *kvtest.TestClerk, key, target string, repaired func(rpc.GetReply) bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		trigger := rawCoordGet(t, tck, key)
		if trigger.Err != rpc.OK {
			t.Fatalf("trigger get failed: err=%v", trigger.Err)
		}

		got := rawReplicaGet(t, tck, target, key)
		if repaired(got) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	final := rawReplicaGet(t, tck, target, key)
	t.Fatalf("read repair did not converge; final err=%v objects=%d", final.Err, len(final.Objects))
}

func preferenceListForKey(key string) []string {
	nodeIDs := make([]string, 0, numServers)
	for i := 0; i < numServers; i++ {
		nodeIDs = append(nodeIDs, tester.ServerName(tester.GRP0, i))
	}
	ring := chr.MakeConsistentHashRing(numReplicas, numSectors, numServers, nodeIDs)
	return ring.GetPreferenceList(key)
}
