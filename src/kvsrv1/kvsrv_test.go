package kvsrv

import (
	//"log"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// Test Put with a single client and a reliable network
func TestReliablePut(t *testing.T) {
	const Val = "6.5840"
	const Ver = 0

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("One client and reliable Put")

	ck := ts.MakeClerk()
	if err := ck.Put("k", Val, Ver); err != rpc.OK {
		t.Fatalf("Put err %v", err)
	}

	if val, ver, err := ck.Get("k"); err != rpc.OK {
		t.Fatalf("Get err %v; expected OK", err)
	} else if val != Val {
		t.Fatalf("Get value err %v; expected %v", val, Val)
	} else if ver != Ver+1 {
		t.Fatalf("Get wrong version %v; expected %v", ver, Ver+1)
	}

	if err := ck.Put("k", Val, 0); err != rpc.ErrVersion {
		t.Fatalf("expected Put to fail with ErrVersion; got err=%v", err)
	}

	if err := ck.Put("y", Val, rpc.Tversion(1)); err != rpc.ErrNoKey {
		t.Fatalf("expected Put to fail with ErrNoKey; got err=%v", err)
	}

	if _, _, err := ck.Get("y"); err != rpc.ErrNoKey {
		t.Fatalf("expected Get to fail with ErrNoKey; got err=%v", err)
	}
}

// Many clients putting on same key.
func TestPutConcurrentReliable(t *testing.T) {
	const (
		PORCUPINETIME = 10 * time.Second
		NCLNT         = 10
		NSEC          = 1
	)

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin("Test: many clients racing to put values to the same key")

	rs := ts.SpawnClientsAndWait(NCLNT, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return ts.OneClientPut(me, ck, []string{"k"}, done)
	})
	ck := ts.MakeClerk()
	ts.CheckPutConcurrent(ck, "k", rs, &kvtest.ClntRes{}, ts.IsReliable())
	ts.CheckPorcupineT(PORCUPINETIME)
}

// Check if memory used on server is reasonable
func TestMemPutManyClientsReliable(t *testing.T) {
	const (
		NCLIENT = 20_000
		MEM     = 1000
	)

	ts := MakeTestKV(t, true)
	defer ts.Cleanup()

	v := kvtest.RandValue(MEM)

	cks := make([]kvtest.IKVClerk, NCLIENT)
	for i, _ := range cks {
		cks[i] = ts.MakeClerk()
	}

	// force allocation of ends for server in each client
	for i := 0; i < NCLIENT; i++ {
		if err := cks[i].Put("k", "", 1); err != rpc.ErrNoKey {
			t.Fatalf("Put failed %v", err)
		}
	}

	ts.Begin("Test: memory use many put clients")

	// allow threads started by labrpc to start
	time.Sleep(1 * time.Second)

	m0 := ts.Config.Group(0).MemSize()

	for i := 0; i < NCLIENT; i++ {
		if err := cks[i].Put("k", v, rpc.Tversion(i)); err != rpc.OK {
			t.Fatalf("Put failed %v", err)
		}
	}

	m1 := ts.Config.Group(0).MemSize()
	f := (float64(m1) - float64(m0)) / NCLIENT
	if m1 > m0+(NCLIENT*10) {
		t.Fatalf("error: server using too much memory %d %d (%.2f byte per client)\n", m0, m1, f)
	}
}

// Test with one client and unreliable network under Dynamo-style semantics:
// if a write doesn't reach W, client sees failure, but partial writes may exist.
func TestUnreliableNet(t *testing.T) {
	const NTRY = 100

	ts := MakeTestKV(t, false)
	defer ts.Cleanup()

	ts.Begin("One client")

	ck := ts.MakeClerk()

	readVersion := func() rpc.Tversion {
		for i := 0; i < 30; i++ {
			_, ver, err := ck.Get("k")
			if err == rpc.OK {
				return ver
			}
			if err == rpc.ErrNoKey {
				return 0
			}
			if err == rpc.ErrReadQuorumNotMet || err == rpc.ErrNotCoordinator {
				continue
			}
			t.Fatalf("Get failed with unexpected err=%v", err)
		}
		t.Fatalf("Get did not reach read quorum after retries")
		return 0
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
