package kvsrv

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvsrv_eval"
	"6.5840/labrpc"
)

const pbsDemoReplicaFactor = 3

type PBSDemoOptions struct {
	OutputDir          string
	Key                string
	WorkloadIterations int
	SleepBetweenOps    time.Duration
	PlotConfig         kvsrv_eval.SimulationConfig
}

func DefaultPBSDemoOptions() PBSDemoOptions {
	return PBSDemoOptions{
		OutputDir:          ".",
		Key:                "pbs-demo-key",
		WorkloadIterations: 24,
		SleepBetweenOps:    2 * time.Millisecond,
		PlotConfig: kvsrv_eval.SimulationConfig{
			NumReplicas: pbsDemoReplicaFactor,
			ReadQuorum:  2,
			WriteQuorum: 2,
			Delta:       50 * time.Millisecond,
			K:           5,
			Iterations:  1000,
			RNG:         rand.New(rand.NewSource(7)),
		},
	}
}

func RunPBSDemo(opts PBSDemoOptions) (kvsrv_eval.PlotOutput, error) {
	if opts.OutputDir == "" {
		opts.OutputDir = "."
	}
	if opts.Key == "" {
		opts.Key = "pbs-demo-key"
	}
	if opts.WorkloadIterations <= 0 {
		return kvsrv_eval.PlotOutput{}, fmt.Errorf("WorkloadIterations must be > 0")
	}
	if opts.PlotConfig.NumReplicas <= 0 {
		return kvsrv_eval.PlotOutput{}, fmt.Errorf("PlotConfig.NumReplicas must be > 0")
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return kvsrv_eval.PlotOutput{}, err
	}

	ring, _, servers, cleanup := makePBSDemoCluster()
	defer cleanup()

	coordinatorID := ring.GetCoordinator(opts.Key)
	coordinator := servers[coordinatorID]
	if coordinator == nil {
		return kvsrv_eval.PlotOutput{}, fmt.Errorf("missing coordinator server %q", coordinatorID)
	}

	ctx := rpc.NewContext()
	for i := 0; i < opts.WorkloadIterations; i++ {
		value := fmt.Sprintf("value-%02d", i)
		writeCtx := ctx
		if i > 0 {
			writeCtx = nextDemoContext(ctx)
		}

		putArgs := rpc.PutArgs{
			Key:         opts.Key,
			Object:      rpc.NewObject(value, writeCtx),
			BaseContext: writeCtx,
		}
		putReply := rpc.PutReply{}
		coordinator.CoordPut(&putArgs, &putReply)
		if putReply.Err != rpc.OK {
			return kvsrv_eval.PlotOutput{}, fmt.Errorf("CoordPut failed at iteration %d: %v", i, putReply.Err)
		}

		getArgs := rpc.GetArgs{Key: opts.Key}
		getReply := rpc.GetReply{}
		coordinator.CoordGet(&getArgs, &getReply)
		if getReply.Err != rpc.OK {
			return kvsrv_eval.PlotOutput{}, fmt.Errorf("CoordGet failed at iteration %d: %v", i, getReply.Err)
		}
		if len(getReply.Objects) == 0 {
			return kvsrv_eval.PlotOutput{}, fmt.Errorf("CoordGet returned no objects at iteration %d", i)
		}

		ctx = getReply.Objects[0].Context.Copy()
		if opts.SleepBetweenOps > 0 {
			time.Sleep(opts.SleepBetweenOps)
		}
	}

	output, err := kvsrv_eval.PlotToDir(opts.PlotConfig, coordinator.collector, opts.OutputDir)
	if err != nil {
		return kvsrv_eval.PlotOutput{}, err
	}
	if err := assertPBSDemoPlotExists(output.DeltaPPath); err != nil {
		return kvsrv_eval.PlotOutput{}, err
	}
	if err := assertPBSDemoPlotExists(output.KPPath); err != nil {
		return kvsrv_eval.PlotOutput{}, err
	}

	output.DeltaPPath, _ = filepath.Abs(output.DeltaPPath)
	output.KPPath, _ = filepath.Abs(output.KPPath)
	return output, nil
}

func nextDemoContext(ctx rpc.Context) rpc.Context {
	next := ctx.Copy()
	next.VC.SetVersion("pbs-demo-client", next.VC.GetVersion("pbs-demo-client")+1)
	next.Timestamp++
	return next
}

func assertPBSDemoPlotExists(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("expected plot %q to exist: %w", path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("expected plot %q to be a file", path)
	}
	if filepath.Ext(path) != ".png" {
		return fmt.Errorf("expected plot %q to be a PNG file", path)
	}
	return nil
}

func makePBSDemoCluster() (*chr.ConsistentHashRing, []string, map[string]*KVServer, func()) {
	nodeIDs := []string{"s1", "s2", "s3", "s4"}
	ring := chr.MakeConsistentHashRing(pbsDemoReplicaFactor, 8, len(nodeIDs), nodeIDs)
	net := labrpc.MakeNetwork()

	ends := make(map[string]map[string]*labrpc.ClientEnd, len(nodeIDs))
	for _, from := range nodeIDs {
		ends[from] = make(map[string]*labrpc.ClientEnd, len(nodeIDs))
		for _, to := range nodeIDs {
			endName := from + "->" + to
			end := net.MakeEnd(endName)
			net.Connect(endName, to)
			net.Enable(endName, true)
			ends[from][to] = end
		}
	}

	servers := make(map[string]*KVServer, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		servers[nodeID] = MakeKVServer(nodeID, ring, 2, 2, ends[nodeID])
	}
	for _, nodeID := range nodeIDs {
		rs := labrpc.MakeServer()
		rs.AddService(labrpc.MakeService(servers[nodeID]))
		net.AddServer(nodeID, rs)
	}

	cleanup := func() {
		for _, kv := range servers {
			close(kv.stopCh)
		}
		net.Cleanup()
	}
	return ring, nodeIDs, servers, cleanup
}
