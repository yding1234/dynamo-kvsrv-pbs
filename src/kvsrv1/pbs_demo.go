package kvsrv

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/chr"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvsrv_eval"
	"6.5840/labrpc"
)

const pbsDemoNumSectors = 8

type PBSDemoStats struct {
	WriteOK         int64
	WriteErrVersion int64
	WriteOtherErr   int64
	ReadOK          int64
	ReadErr         int64
	ProbeReadOK     int64
	ProbeReadErr    int64
	RefreshOK       int64
	RefreshErr      int64
}

type PBSDemoResult struct {
	Plots        kvsrv_eval.PlotOutput
	Stats        PBSDemoStats
	StatsCSVPath string
}

type PBSDemoOptions struct {
	OutputDir          string
	Key                string
	WorkloadIterations int
	NumWriters         int
	SleepBetweenOps    time.Duration
	NumReaders         int
	ReadSleep          time.Duration
	ProbeReadsPerWrite int
	NumNodes           int
	PlotConfig         kvsrv_eval.SimulationConfig
}

func DefaultPBSDemoOptions() PBSDemoOptions {
	return PBSDemoOptions{
		OutputDir:          ".",
		Key:                "pbs-demo-key",
		WorkloadIterations: 24,
		NumWriters:         4,
		SleepBetweenOps:    2 * time.Millisecond,
		NumReaders:         3,
		ReadSleep:          1 * time.Millisecond,
		ProbeReadsPerWrite: 2,
		NumNodes:           4,
		PlotConfig: kvsrv_eval.SimulationConfig{
			NumReplicas: 3,
			ReadQuorum:  2,
			WriteQuorum: 2,
			Delta:       50 * time.Millisecond,
			K:           5,
			Iterations:  1000,
			RNG:         rand.New(rand.NewSource(7)),
		},
	}
}

func RunPBSDemo(opts PBSDemoOptions) (PBSDemoResult, error) {
	if opts.OutputDir == "" {
		opts.OutputDir = "."
	}
	if opts.Key == "" {
		opts.Key = "pbs-demo-key"
	}
	if opts.WorkloadIterations <= 0 {
		return PBSDemoResult{}, fmt.Errorf("WorkloadIterations must be > 0")
	}
	if opts.NumWriters <= 0 {
		return PBSDemoResult{}, fmt.Errorf("NumWriters must be > 0")
	}
	if opts.NumReaders <= 0 {
		return PBSDemoResult{}, fmt.Errorf("NumReaders must be > 0")
	}
	if opts.ProbeReadsPerWrite < 0 {
		return PBSDemoResult{}, fmt.Errorf("ProbeReadsPerWrite must be >= 0")
	}
	if opts.NumNodes <= 0 {
		return PBSDemoResult{}, fmt.Errorf("NumNodes must be > 0")
	}
	if opts.PlotConfig.NumReplicas <= 0 {
		return PBSDemoResult{}, fmt.Errorf("PlotConfig.NumReplicas must be > 0")
	}
	if opts.PlotConfig.NumReplicas > opts.NumNodes {
		return PBSDemoResult{}, fmt.Errorf("PlotConfig.NumReplicas must be <= NumNodes")
	}
	if opts.PlotConfig.ReadQuorum <= 0 || opts.PlotConfig.ReadQuorum > opts.PlotConfig.NumReplicas {
		return PBSDemoResult{}, fmt.Errorf("PlotConfig.ReadQuorum must be in [1, PlotConfig.NumReplicas]")
	}
	if opts.PlotConfig.WriteQuorum <= 0 || opts.PlotConfig.WriteQuorum > opts.PlotConfig.NumReplicas {
		return PBSDemoResult{}, fmt.Errorf("PlotConfig.WriteQuorum must be in [1, PlotConfig.NumReplicas]")
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return PBSDemoResult{}, err
	}

	ring, _, servers, cleanup := makePBSDemoCluster(opts.NumNodes, opts.PlotConfig.NumReplicas, opts.PlotConfig.ReadQuorum, opts.PlotConfig.WriteQuorum)
	defer cleanup()

	coordinatorID := ring.GetCoordinator(opts.Key)
	coordinator := servers[coordinatorID]
	if coordinator == nil {
		return PBSDemoResult{}, fmt.Errorf("missing coordinator server %q", coordinatorID)
	}

	var writeOK atomic.Int64
	var writeErrVersion atomic.Int64
	var writeOtherErr atomic.Int64
	var readOK atomic.Int64
	var readErr atomic.Int64
	var probeReadOK atomic.Int64
	var probeReadErr atomic.Int64
	var refreshOK atomic.Int64
	var refreshErr atomic.Int64

	_, seedCtx, err := demoPut(coordinator, opts.Key, "seed-00", rpc.NewContext())
	if err != nil {
		return PBSDemoResult{}, fmt.Errorf("initial seed write failed: %w", err)
	}
	writeOK.Add(1)

	var stopWorkers atomic.Bool
	var readersWG sync.WaitGroup
	var writersWG sync.WaitGroup
	workerErrCh := make(chan error, 1)
	reportFatalErr := func(err error) {
		select {
		case workerErrCh <- err:
		default:
		}
		stopWorkers.Store(true)
	}

	for readerID := 0; readerID < opts.NumReaders; readerID++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			for !stopWorkers.Load() {
				if err := demoGet(coordinator, opts.Key); err != nil {
					readErr.Add(1)
					reportFatalErr(fmt.Errorf("reader %d: %w", readerID, err))
					return
				}
				readOK.Add(1)
				if opts.ReadSleep > 0 {
					time.Sleep(opts.ReadSleep)
				}
			}
		}(readerID)
	}

	for writerID := 0; writerID < opts.NumWriters; writerID++ {
		writersWG.Add(1)
		go func(writerID int) {
			defer writersWG.Done()

			writerCtx := seedCtx.Copy()
			writerLabel := fmt.Sprintf("pbs-demo-writer-%d", writerID)
			for i := 0; i < opts.WorkloadIterations && !stopWorkers.Load(); i++ {
				value := fmt.Sprintf("writer-%02d-value-%02d", writerID, i)
				for !stopWorkers.Load() {
					nextCtx := nextDemoContext(writerCtx, writerLabel)
					putErr, committedCtx, err := demoPut(coordinator, opts.Key, value, nextCtx)
					if err != nil {
						reportFatalErr(fmt.Errorf("writer %d iteration %d: %w", writerID, i, err))
						return
					}

					switch putErr {
					case rpc.OK:
						writeOK.Add(1)
						writerCtx = committedCtx
						for probe := 0; probe < opts.ProbeReadsPerWrite; probe++ {
							if err := demoGet(coordinator, opts.Key); err != nil {
								probeReadErr.Add(1)
								reportFatalErr(fmt.Errorf("writer %d iteration %d probe read %d: %w", writerID, i, probe, err))
								return
							}
							probeReadOK.Add(1)
						}
						if opts.SleepBetweenOps > 0 {
							time.Sleep(opts.SleepBetweenOps)
						}
						goto nextWrite
					case rpc.ErrVersion:
						writeErrVersion.Add(1)
						latestCtx, err := demoGetLatestContext(coordinator, opts.Key)
						if err != nil {
							refreshErr.Add(1)
							reportFatalErr(fmt.Errorf("writer %d iteration %d refresh failed: %w", writerID, i, err))
							return
						}
						refreshOK.Add(1)
						writerCtx = latestCtx
						continue
					default:
						writeOtherErr.Add(1)
						reportFatalErr(fmt.Errorf("writer %d iteration %d put failed: %v", writerID, i, putErr))
						return
					}
				}
			nextWrite:
			}
		}(writerID)
	}

	writersWG.Wait()
	stopWorkers.Store(true)
	readersWG.Wait()
	select {
	case err := <-workerErrCh:
		return PBSDemoResult{}, err
	default:
	}

	output, err := kvsrv_eval.PlotToDir(opts.PlotConfig, coordinator.collector, opts.OutputDir)
	if err != nil {
		return PBSDemoResult{}, err
	}
	if err := assertPBSDemoPlotExists(output.DeltaPPath); err != nil {
		return PBSDemoResult{}, err
	}
	if err := assertPBSDemoPlotExists(output.KPPath); err != nil {
		return PBSDemoResult{}, err
	}

	stats := PBSDemoStats{
		WriteOK:         writeOK.Load(),
		WriteErrVersion: writeErrVersion.Load(),
		WriteOtherErr:   writeOtherErr.Load(),
		ReadOK:          readOK.Load(),
		ReadErr:         readErr.Load(),
		ProbeReadOK:     probeReadOK.Load(),
		ProbeReadErr:    probeReadErr.Load(),
		RefreshOK:       refreshOK.Load(),
		RefreshErr:      refreshErr.Load(),
	}

	output.DeltaPPath, _ = filepath.Abs(output.DeltaPPath)
	output.KPPath, _ = filepath.Abs(output.KPPath)
	output.DeltaCSVPath, _ = filepath.Abs(output.DeltaCSVPath)
	output.KPCSVPath, _ = filepath.Abs(output.KPCSVPath)
	statsCSVPath := filepath.Join(opts.OutputDir, "pbs_demo_stats.csv")
	if err := writePBSDemoStatsCSV(statsCSVPath, stats); err != nil {
		return PBSDemoResult{}, err
	}
	statsCSVPath, _ = filepath.Abs(statsCSVPath)

	return PBSDemoResult{
		Plots:        output,
		Stats:        stats,
		StatsCSVPath: statsCSVPath,
	}, nil
}

func writePBSDemoStatsCSV(path string, stats PBSDemoStats) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	rows := [][]string{
		{"metric", "value"},
		{"write_ok", strconv.FormatInt(stats.WriteOK, 10)},
		{"write_err_version", strconv.FormatInt(stats.WriteErrVersion, 10)},
		{"write_other_err", strconv.FormatInt(stats.WriteOtherErr, 10)},
		{"read_ok", strconv.FormatInt(stats.ReadOK, 10)},
		{"read_err", strconv.FormatInt(stats.ReadErr, 10)},
		{"probe_read_ok", strconv.FormatInt(stats.ProbeReadOK, 10)},
		{"probe_read_err", strconv.FormatInt(stats.ProbeReadErr, 10)},
		{"refresh_ok", strconv.FormatInt(stats.RefreshOK, 10)},
		{"refresh_err", strconv.FormatInt(stats.RefreshErr, 10)},
	}

	writer := csv.NewWriter(file)
	if err := writer.WriteAll(rows); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func demoPut(coordinator *KVServer, key string, value string, baseCtx rpc.Context) (rpc.Err, rpc.Context, error) {
	putArgs := rpc.PutArgs{
		Key:         key,
		Object:      rpc.NewObject(value, baseCtx),
		BaseContext: baseCtx,
	}
	putReply := rpc.PutReply{}
	coordinator.CoordPut(&putArgs, &putReply)
	return putReply.Err, putArgs.Object.Context.Copy(), nil
}

func demoGet(coordinator *KVServer, key string) error {
	getArgs := rpc.GetArgs{Key: key}
	getReply := rpc.GetReply{}
	coordinator.CoordGet(&getArgs, &getReply)
	if getReply.Err != rpc.OK {
		return fmt.Errorf("CoordGet failed: %v", getReply.Err)
	}
	if len(getReply.Objects) == 0 {
		return fmt.Errorf("CoordGet returned no objects")
	}
	return nil
}

func demoGetLatestContext(coordinator *KVServer, key string) (rpc.Context, error) {
	getArgs := rpc.GetArgs{Key: key}
	getReply := rpc.GetReply{}
	coordinator.CoordGet(&getArgs, &getReply)
	if getReply.Err != rpc.OK {
		return rpc.Context{}, fmt.Errorf("CoordGet failed: %v", getReply.Err)
	}
	if len(getReply.Objects) == 0 {
		return rpc.Context{}, fmt.Errorf("CoordGet returned no objects")
	}
	return pickLatestObject(getReply.Objects).Context.Copy(), nil
}

func nextDemoContext(ctx rpc.Context, writerLabel string) rpc.Context {
	next := ctx.Copy()
	next.VC.SetVersion(writerLabel, next.VC.GetVersion(writerLabel)+1)
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

func makePBSDemoCluster(numNodes int, numReplicas int, readQuorum int, writeQuorum int) (*chr.ConsistentHashRing, []string, map[string]*KVServer, func()) {
	nodeIDs := make([]string, 0, numNodes)
	for i := 1; i <= numNodes; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("s%d", i))
	}
	ring := chr.MakeConsistentHashRing(numReplicas, pbsDemoNumSectors, len(nodeIDs), nodeIDs)
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
		servers[nodeID] = MakeKVServer(nodeID, ring, writeQuorum, readQuorum, ends[nodeID])
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
