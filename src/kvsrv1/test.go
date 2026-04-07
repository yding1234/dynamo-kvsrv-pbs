package kvsrv

// go test ./kvsrv1

import (
	// "log"
	"testing"

	"6.5840/kvtest1"
	"6.5840/tester1"
	"6.5840/chr"
)

type TestKV struct {
	*kvtest.Test
	t        *testing.T
	reliable bool
}

var (
	numServers = 10
	numSectors = 100
	numReplicas = 3
)

func MakeTestKV(t *testing.T, reliable bool) *TestKV {
	cfg := tester.MakeConfig(t, numServers, reliable, "kvsrv1d", []string{})
	ts := &TestKV{
		t:        t,
		reliable: reliable,
	}
	ts.Test = kvtest.MakeTest(t, cfg, false, ts)
	return ts
}

func (ts *TestKV) MakeClerk() kvtest.IKVClerk {
	clnt := ts.Config.MakeClient()
	// ck := MakeClerk(clnt, tester.ServerName(tester.GRP0, 0))
	nodeIDs := make([]string, 0)
	for i := 0; i < numServers; i++ {
		nodeIDs = append(nodeIDs, tester.ServerName(tester.GRP0, i))
	}
	ring := chr.MakeConsistentHashRing(numReplicas, numSectors, numServers, nodeIDs)
	ck := MakeClerk(clnt, ring)
	return &kvtest.TestClerk{ck, clnt, ts.Test.Config}
}

func (ts *TestKV) DeleteClerk(ck kvtest.IKVClerk) {
	tck := ck.(*kvtest.TestClerk)
	ts.DeleteClient(tck.Clnt)
}
