package kvsrv

// go test ./kvsrv1/...  -v
// make kvsrv1

import (
	// "log"
	"testing"

	"dynamo-kvsrv/kvsrv/internal/testkv"
	"dynamo-kvsrv/kvsrv/chr"
	"dynamo-kvsrv/tester"
)

type TestKV struct {
	*testkv.Test
	t        *testing.T
	reliable bool
}

var (
	numServers = 10
	numSectors = 512
	numReplicas = 3
	readQuorum = 2
	writeQuorum = 2
)

func MakeTestKV(t *testing.T, reliable bool) *TestKV {
	cfg := tester.MakeConfig(t, numServers, reliable, "kvsrv1d", []string{})
	ts := &TestKV{
		t:        t,
		reliable: reliable,
	}
	ts.Test = testkv.MakeTest(t, cfg, false, ts)
	return ts
}

func (ts *TestKV) MakeClerk() testkv.IKVClerk {
	clnt := ts.Config.MakeClient()
	// ck := MakeClerk(clnt, tester.ServerName(tester.GRP0, 0))

	nodeIDs := make([]string, 0)
	for i := 0; i < numServers; i++ {
		nodeIDs = append(nodeIDs, tester.ServerName(tester.GRP0, i))
	}
	ring := chr.MakeConsistentHashRing(numReplicas, numSectors, numServers, nodeIDs)
	ck := MakeClerk(clnt, ring)
	return &testkv.TestClerk{IKVClerk: ck, Clnt: clnt, Cfg: ts.Test.Config}
}

func (ts *TestKV) DeleteClerk(ck testkv.IKVClerk) {
	tck := ck.(*testkv.TestClerk)
	ts.DeleteClient(tck.Clnt)
}


