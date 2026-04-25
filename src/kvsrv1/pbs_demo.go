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
	tester "6.5840/tester1"
)

const pbsDemoNumSectors = 512 // TODO: make this a constant in the simulation config

type PBSDemoStats struct {
	WriteOK            int64
	WriteErrVersion    int64
	WriteQuorumRetry   int64 // ErrWriteQuorumNotMet (transient, retried)
	WriteOtherErr      int64
	ReadOK             int64
	ReadNoKey          int64 // ErrNoKey (transient, retried)
	ReadQuorumRetry    int64 // ErrReadQuorumNotMet (transient, retried)
	ReadErr            int64
	// ProbeReadOK        int64
	// ProbeReadErr       int64
	RefreshOK          int64 // number of times the merkle tree is refreshed
	RefreshErr         int64
}

type PBSDemoResult struct {
	Plots        kvsrv_eval.PlotOutput
	Stats        map[string]PBSDemoStats
	StatsCSVPath string
}

type PBSDemoScenario struct {
	Name                string
	Label               string
	EnableReadRepair    bool
	EnableAntiEntropy   bool
	EnableHintedHandoff bool
	FailureMode         string
}

type PBSDemoOptions struct {
	OutputDir          string
	Key                string
	WorkloadIterations int
	NumWriters         int
	SleepBetweenOps    time.Duration
	NumReaders         int
	ReadSleep          time.Duration
	// SleepJitterRatio adds uniform jitter to writer/reader sleep so calls
	// don't land in lock-step. Each sleep is drawn from
	// [base*(1-ratio), base*(1+ratio)]. 0 disables jitter; values >1 are
	// clamped to 1 (so the lower bound never goes negative).
	SleepJitterRatio float64
	// ProbeReadsPerWrite int
	NumNodes           int
	// UnreliableNetwork enables labrpc's reliable=false mode: ~10% request
	// drops, ~10% reply drops, and a small per-message random delay. 
	UnreliableNetwork bool
	// LongReordering enables labrpc's longReordering mode: ~60% of replies
	// are delayed by 200~2000ms. Only meaningful when UnreliableNetwork
	// is also true 
	LongReordering bool
	PlotConfig     kvsrv_eval.SimulationConfig
	Scenarios      []PBSDemoScenario
}

func DefaultPBSDemoOptions() PBSDemoOptions {
	return PBSDemoOptions{
		OutputDir:          ".",
		Key:                "pbs-demo-key",
		WorkloadIterations: 300,
		NumWriters:         1,
		SleepBetweenOps:    1 * time.Millisecond,
		NumReaders:         10,
		ReadSleep:          2 * time.Millisecond,
		SleepJitterRatio:   0.5, // ±50% uniform jitter to break lock-step

		// ProbeReadsPerWrite: 0,
		NumNodes:           5,
		UnreliableNetwork:  false,
		LongReordering:     false,
		PlotConfig: kvsrv_eval.SimulationConfig{
			NumReplicas:  3,
			ReadQuorum:   1,
			WriteQuorum:  1,
			Delta:        10 * time.Millisecond,
			DeltaPoints:  50, // number of sample points along the delta axis
			K:            5,
			Iterations:   5000, // number of Monte Carlo iterations for delta-P prediction
			RNG:          rand.New(rand.NewSource(7)),
			YMin:         0,    // 0 = auto-fit
			YMax:         0,    // 0 = 1.0
			EmitZoomPlot: true, // also emit delta_p_zoom.png / k_p_zoom.png
		},
		Scenarios: DefaultPBSDemoScenarios(),
	}
}

func DefaultPBSDemoScenarios() []PBSDemoScenario {
	return []PBSDemoScenario{
		{
			Name:                "observe_baseline",
			Label:               "observe_baseline",
			EnableReadRepair:    false,
			EnableAntiEntropy:   false,
			EnableHintedHandoff: false,
			FailureMode:         "none",
		},
		{
			Name:                "observe_read_repair",
			Label:               "observe_read_repair",
			EnableReadRepair:    true,
			EnableAntiEntropy:   false,
			EnableHintedHandoff: false,
			FailureMode:         "none",
		},
		{
			Name:                "observe_anti_entropy",
			Label:               "observe_anti_entropy",
			EnableReadRepair:    false,
			EnableAntiEntropy:   true,
			EnableHintedHandoff: false,
			FailureMode:         "none",
		},
		{
			Name:                "observe_hinted_handoff",
			Label:               "observe_hinted_handoff",
			EnableReadRepair:    false,
			EnableAntiEntropy:   false,
			EnableHintedHandoff: true,
			FailureMode:         "single_dead_replica",
		},
	}
}

func RunPBSDemo(opts PBSDemoOptions) (PBSDemoResult, error) {
	if opts.OutputDir == "" {
		opts.OutputDir = "."
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return PBSDemoResult{}, err
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
	if opts.NumNodes <= 0 {
		return PBSDemoResult{}, fmt.Errorf("NumNodes must be > 0")
	}
	if len(opts.Scenarios) == 0 {
		opts.Scenarios = DefaultPBSDemoScenarios()
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

	series := make([]kvsrv_eval.CollectorSeries, 0, len(opts.Scenarios))
	statsByScenario := make(map[string]PBSDemoStats, len(opts.Scenarios))
	var baselineCollector *kvsrv_eval.PBSCollector // TODO: make this a Series

	for _, scenario := range opts.Scenarios {
		collector, stats, err := runPBSDemoScenario(opts, scenario)
		if err != nil {
			return PBSDemoResult{}, fmt.Errorf("%s: %w", scenario.Name, err)
		}
		if baselineCollector == nil {
			baselineCollector = collector
		}
		series = append(series, kvsrv_eval.CollectorSeries{
			Config: kvsrv_eval.SeriesConfig{
				Name:          scenario.Name,
				Label:         scenario.Label,
				Kind:          "observe",
				ReadRepair:    scenario.EnableReadRepair,
				AntiEntropy:   scenario.EnableAntiEntropy,
				HintedHandoff: scenario.EnableHintedHandoff,
				FailureMode:   scenario.FailureMode,
				Notes:         "Observed PBS curve from the corresponding demo scenario.",
			},
			Collector: collector,
		})
		statsByScenario[scenario.Name] = stats
	}
	if baselineCollector == nil {
		return PBSDemoResult{}, fmt.Errorf("no baseline collector generated")
	}

	output, err := kvsrv_eval.PlotComparisonToDir(
		opts.PlotConfig,
		baselineCollector,
		kvsrv_eval.SeriesConfig{
			Name:          "predict_baseline",
			Label:         "predict_baseline",
			Kind:          "predict",
			ReadRepair:    false,
			AntiEntropy:   false,
			HintedHandoff: false,
			FailureMode:   "none",
			Notes:         "PBS baseline predictor without read repair, anti-entropy, or hinted handoff.",
		},
		series,
		opts.OutputDir,
	)
	if err != nil {
		return PBSDemoResult{}, err
	}
	if err := assertPBSDemoPlotExists(output.DeltaPPath); err != nil {
		return PBSDemoResult{}, err
	}
	if err := assertPBSDemoPlotExists(output.KPPath); err != nil {
		return PBSDemoResult{}, err
	}
	if output.DeltaPZoomPath != "" {
		if err := assertPBSDemoPlotExists(output.DeltaPZoomPath); err != nil {
			return PBSDemoResult{}, err
		}
	}
	if output.KPZoomPath != "" {
		if err := assertPBSDemoPlotExists(output.KPZoomPath); err != nil {
			return PBSDemoResult{}, err
		}
	}

	output.DeltaPPath, _ = filepath.Abs(output.DeltaPPath)
	output.KPPath, _ = filepath.Abs(output.KPPath)
	output.DeltaCSVPath, _ = filepath.Abs(output.DeltaCSVPath)
	output.KPCSVPath, _ = filepath.Abs(output.KPCSVPath)
	output.SeriesConfigCSVPath, _ = filepath.Abs(output.SeriesConfigCSVPath)
	if output.DeltaPZoomPath != "" {
		output.DeltaPZoomPath, _ = filepath.Abs(output.DeltaPZoomPath)
	}
	if output.KPZoomPath != "" {
		output.KPZoomPath, _ = filepath.Abs(output.KPZoomPath)
	}
	statsCSVPath := filepath.Join(opts.OutputDir, "pbs_demo_stats.csv")
	if err := writePBSDemoStatsCSV(statsCSVPath, statsByScenario); err != nil {
		return PBSDemoResult{}, err
	}
	statsCSVPath, _ = filepath.Abs(statsCSVPath)

	return PBSDemoResult{
		Plots:        output,
		Stats:        statsByScenario,
		StatsCSVPath: statsCSVPath,
	}, nil
}

func runPBSDemoScenario(opts PBSDemoOptions, scenario PBSDemoScenario) (*kvsrv_eval.PBSCollector, PBSDemoStats, error) {
	ring, _, servers, cleanup := makePBSDemoCluster(opts.NumNodes, opts.PlotConfig.NumReplicas, 
		opts.PlotConfig.ReadQuorum, opts.PlotConfig.WriteQuorum, opts.UnreliableNetwork, opts.LongReordering)
	defer cleanup()

	for _, server := range servers {
		server.readRepairEnabled = scenario.EnableReadRepair
		server.hintedHandoffEnabled = scenario.EnableHintedHandoff
		if scenario.EnableAntiEntropy {
			server.StartAntiEntropy()
		}
		if scenario.EnableHintedHandoff {
			server.StartMembershipFailureDetector()
			server.StartHintedHandoff()
		}
	}

	coordinatorID := ring.GetCoordinator(opts.Key) // TODO: experiment with multiple coordinators and multiple keys
	coordinator := servers[coordinatorID]
	if coordinator == nil {
		return nil, PBSDemoStats{}, fmt.Errorf("missing coordinator server %q", coordinatorID)
	}
	if err := configurePBSDemoScenarioFailure(opts.Key, ring, servers, scenario); err != nil {
		return nil, PBSDemoStats{}, err
	}

	// set atomatic counters for the stats
	var writeOK atomic.Int64
	var writeErrVersion atomic.Int64
	var writeQuorumRetry atomic.Int64
	var writeOtherErr atomic.Int64
	var readOK atomic.Int64
	var readNoKey atomic.Int64
	var readQuorumRetry atomic.Int64
	var readErr atomic.Int64
	// var probeReadOK atomic.Int64
	// var probeReadErr atomic.Int64
	var refreshOK atomic.Int64
	var refreshErr atomic.Int64

	// Write the initial value, always retry on transient errors so the
	// rest of the workload always has a key to operate on
	initialCtx, err := writeInitialValue(coordinator, opts.Key)
	if err != nil {
		return nil, PBSDemoStats{}, fmt.Errorf("initial value write failed: %w", err)
	}

	// set up the workers
	var stopWorkers atomic.Bool
	var readersWG sync.WaitGroup
	var writersWG sync.WaitGroup

	workerErrCh := make(chan error, 1)
	reportFatalErr := func(err error) {
		select {
		case workerErrCh <- err:
		default:
		}
		stopWorkers.Store(true) // stop all workers
	}

	// start the readers
	for readerID := 0; readerID < opts.NumReaders; readerID++ {
		readersWG.Add(1)
		go func(readerID int) {
			defer readersWG.Done()
			// per-goroutine RNG to avoid lock contention on the global rand;
			// seed mixes scenario name + role + id so different scenarios and
			// different worker IDs get independent jitter sequences.
			rng := rand.New(rand.NewSource(workerSeed(scenario.Name, "reader", readerID)))
			for !stopWorkers.Load() {
				softErr, hardErr := demoGet(coordinator, opts.Key)
				if hardErr != nil {
					readErr.Add(1)
					reportFatalErr(fmt.Errorf("reader %d: %w", readerID, hardErr))
					return
				}
				switch softErr {
				case rpc.OK:
					readOK.Add(1)
				case rpc.ErrNoKey:
					readNoKey.Add(1)
				case rpc.ErrReadQuorumNotMet:
					readQuorumRetry.Add(1)
				default:
					readErr.Add(1)
					reportFatalErr(fmt.Errorf("reader %d: unexpected reply %v", readerID, softErr))
					return
				}
				jitteredSleep(rng, opts.ReadSleep, opts.SleepJitterRatio)
			}
		}(readerID)
	}

	// start the writers
	for writerID := 0; writerID < opts.NumWriters; writerID++ {
		writersWG.Add(1)
		go func(writerID int) {
			defer writersWG.Done()

			writerCtx := initialCtx.Copy() // start with the initial context
			writerLabel := fmt.Sprintf("%s-writer-%d", scenario.Name, writerID) // TODO: experiment with multiple writers and multiple keys
			rng := rand.New(rand.NewSource(workerSeed(scenario.Name, "writer", writerID)))

			for i := 0; i < opts.WorkloadIterations && !stopWorkers.Load(); i++ {
				value := fmt.Sprintf("%s-writer-%02d-value-%02d", scenario.Name, writerID, i) // TODO: experiment with multiple values
				for !stopWorkers.Load() {
					nextCtx := writerCtx.Copy()
					nextCtx.Update(writerLabel, value)
					putErr, committedCtx, err := demoPut(coordinator, opts.Key, value, nextCtx)
					if err != nil {
						reportFatalErr(fmt.Errorf("writer %d iteration %d: %w", writerID, i, err))
						return
					}

				switch putErr {
				case rpc.OK:
					writeOK.Add(1)
					writerCtx = committedCtx
					// for probe := 0; probe < opts.ProbeReadsPerWrite; probe++ {
					// 	softErr, hardErr := demoGet(coordinator, opts.Key)
					// 	if hardErr != nil {
					// 		probeReadErr.Add(1)
					// 		reportFatalErr(fmt.Errorf("writer %d iteration %d probe read %d: %w", writerID, i, probe, hardErr))
					// 		return
					// 	}
					// 	if softErr == rpc.OK {
					// 		probeReadOK.Add(1)
					// 	} else {
					// 		probeReadErr.Add(1)
					// 	}
					// }
					jitteredSleep(rng, opts.SleepBetweenOps, opts.SleepJitterRatio)
					goto nextWrite
				case rpc.ErrVersion:
					writeErrVersion.Add(1)
					latestCtx, ok, err := demoGetLatestContext(coordinator, opts.Key) // TODO: experiment with multiple keys
					if err != nil {
						refreshErr.Add(1)
						reportFatalErr(fmt.Errorf("writer %d iteration %d refresh failed: %w", writerID, i, err))
						return
					}
					if !ok {
						// Transient (NoKey/quorum). Try the put again with the
						// same base context; the cluster will eventually heal.
						refreshErr.Add(1)
						continue
					}
					refreshOK.Add(1)
					writerCtx = latestCtx
					continue
				case rpc.ErrWriteQuorumNotMet, rpc.ErrNoKey:
					// Transient under unreliable networks: drops/timeouts can
					// prevent meeting quorum or leave a fresh replica without
					// the key briefly. Just retry the same put.
					writeQuorumRetry.Add(1)
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
		return nil, PBSDemoStats{}, err
	default:
	}

	// wait for the hinted handoff and anti-entropy to complete
	if scenario.EnableHintedHandoff { 
		time.Sleep(2 * defaultHintedHandoffInterval)
	}
	if scenario.EnableAntiEntropy {
		time.Sleep(2 * defaultAntiEntropyInterval)
	}

	stats := PBSDemoStats{
		WriteOK:          writeOK.Load(),
		WriteErrVersion:  writeErrVersion.Load(),
		WriteQuorumRetry: writeQuorumRetry.Load(),
		WriteOtherErr:    writeOtherErr.Load(),
		ReadOK:           readOK.Load(),
		ReadNoKey:        readNoKey.Load(),
		ReadQuorumRetry:  readQuorumRetry.Load(),
		ReadErr:          readErr.Load(),
		// ProbeReadOK:      probeReadOK.Load(),
		// ProbeReadErr:     probeReadErr.Load(),
		RefreshOK:        refreshOK.Load(),
		RefreshErr:       refreshErr.Load(),
	}
	return coordinator.collector, stats, nil
}

func writePBSDemoStatsCSV(path string, statsByScenario map[string]PBSDemoStats) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	rows := [][]string{
		{"scenario", "write_ok", "write_err_version", "write_quorum_retry", "write_other_err", "read_ok", "read_no_key", "read_quorum_retry", "read_err", "refresh_ok", "refresh_err"},
	}
	for _, scenario := range DefaultPBSDemoScenarios() {
		stats, ok := statsByScenario[scenario.Name]
		if !ok {
			continue
		}
		rows = append(rows, []string{
			scenario.Name,
			strconv.FormatInt(stats.WriteOK, 10),
			strconv.FormatInt(stats.WriteErrVersion, 10),
			strconv.FormatInt(stats.WriteQuorumRetry, 10),
			strconv.FormatInt(stats.WriteOtherErr, 10),
			strconv.FormatInt(stats.ReadOK, 10),
			strconv.FormatInt(stats.ReadNoKey, 10),
			strconv.FormatInt(stats.ReadQuorumRetry, 10),
			strconv.FormatInt(stats.ReadErr, 10),
			strconv.FormatInt(stats.RefreshOK, 10),
			strconv.FormatInt(stats.RefreshErr, 10),
		})
	}

	writer := csv.NewWriter(file)
	if err := writer.WriteAll(rows); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func configurePBSDemoScenarioFailure(key string, ring *chr.ConsistentHashRing, servers map[string]*KVServer, scenario PBSDemoScenario) error {
	if scenario.FailureMode == "" || scenario.FailureMode == "none" {
		return nil
	}
	switch scenario.FailureMode {
	case "single_dead_replica":
		prefList := ring.GetPreferenceList(key)
		if len(prefList) < 2 {
			return fmt.Errorf("need at least 2 replicas for hinted handoff scenario")
		}
		deadReplica := prefList[len(prefList)-1]
		for _, server := range servers {
			server.markMemberStatus(deadReplica, rpc.Dead)
		}
		return nil
	default:
		return fmt.Errorf("unsupported failure mode %q", scenario.FailureMode)
	}
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

// demoGet returns (softErr, hardErr).
//   - hardErr != nil  : truly unexpected (panic/protocol violation), demo aborts.
//   - softErr == OK   : reply contained at least one object.
//   - softErr != OK   : transient condition (NoKey, quorum not met, etc.)
//     that the caller should treat as a stat to record but not abort on.
func demoGet(coordinator *KVServer, key string) (rpc.Err, error) {
	getArgs := rpc.GetArgs{Key: key}
	getReply := rpc.GetReply{}
	coordinator.CoordGet(&getArgs, &getReply)
	if getReply.Err == rpc.OK && len(getReply.Objects) == 0 {
		return rpc.OK, fmt.Errorf("CoordGet returned OK with no objects")
	}
	return getReply.Err, nil
}

// demoGetLatestContext returns (ctx, ok, hardErr).
//   - hardErr != nil : truly unexpected; demo aborts.
//   - ok == false    : transient (NoKey / quorum not met); caller should retry.
//   - ok == true     : ctx is the latest object's context.
func demoGetLatestContext(coordinator *KVServer, key string) (rpc.Context, bool, error) {
	getArgs := rpc.GetArgs{Key: key}
	getReply := rpc.GetReply{}
	coordinator.CoordGet(&getArgs, &getReply)
	if getReply.Err != rpc.OK {
		return rpc.Context{}, false, nil
	}
	if len(getReply.Objects) == 0 {
		return rpc.Context{}, false, fmt.Errorf("CoordGet returned OK with no objects")
	}
	return pickLatestObject(getReply.Objects).Context.Copy(), true, nil
}

// write the initial value, always retry on transient errors so the
// rest of the workload always has a key to operate on
func writeInitialValue(coordinator *KVServer, key string) (rpc.Context, error) {
	const maxAttempts = 50
	for i := 0; i < maxAttempts; i++ {
		putErr, ctx, err := demoPut(coordinator, key, "initial-value", rpc.NewContext())
		if err != nil {
			return rpc.Context{}, err
		}
		switch putErr {
		case rpc.OK:
			return ctx, nil
		case rpc.ErrWriteQuorumNotMet, rpc.ErrNoKey, rpc.ErrVersion:
			time.Sleep(20 * time.Millisecond)
			continue
		default:
			return rpc.Context{}, fmt.Errorf("initial value write attempt %d failed: %v", i, putErr)
		}
	}
	return rpc.Context{}, fmt.Errorf("initial value write exhausted %d attempts", maxAttempts)
}

func nextDemoContext(ctx rpc.Context, writerLabel string) rpc.Context {
	next := ctx.Copy()
	next.VC.SetVersion(writerLabel, next.VC.GetVersion(writerLabel)+1)
	next.Timestamp++
	return next
}

// jitteredSleep sleeps for a duration drawn uniformly from
// [base*(1-ratio), base*(1+ratio)]. ratio<=0 falls back to base; ratio>=1 is
// clamped so the lower bound stays non-negative. base<=0 returns immediately.
func jitteredSleep(rng *rand.Rand, base time.Duration, ratio float64) {
	if base <= 0 {
		return
	}
	if ratio <= 0 {
		time.Sleep(base)
		return
	}
	if ratio > 1 {
		ratio = 1
	}
	// scale in [1-ratio, 1+ratio]
	scale := 1 + ratio*(2*rng.Float64()-1)
	d := time.Duration(float64(base) * scale)
	if d > 0 {
		time.Sleep(d)
	}
}

// workerSeed builds a deterministic-but-distinct RNG seed for a given
// (scenario, role, id) tuple so jitter is reproducible across runs while still
// being independent across goroutines and across the four scenarios.
func workerSeed(scenario string, role string, id int) int64 {
	const fnvOffset = 1469598103934665603
	const fnvPrime = 1099511628211
	h := uint64(fnvOffset)
	for _, b := range []byte(scenario) {
		h = (h ^ uint64(b)) * fnvPrime
	}
	h ^= '|'
	h *= fnvPrime
	for _, b := range []byte(role) {
		h = (h ^ uint64(b)) * fnvPrime
	}
	h ^= uint64(id) * fnvPrime
	// shift right one to clear the sign bit before casting to int64
	return int64(h >> 1)
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

func (kv *KVServer) markMemberStatus(serverID string, status rpc.NodeStatus) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	member, ok := kv.members[serverID]
	if !ok {
		return
	}
	member.Status = status
	kv.members[serverID] = member
}

func makePBSDemoCluster(numNodes int, numReplicas int, readQuorum int, writeQuorum int, 
	unreliable bool, longReordering bool) (*chr.ConsistentHashRing, []string, map[string]*KVServer, func()) {
	nodeIDs := make([]string, 0, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeIDs = append(nodeIDs, tester.ServerName(tester.GRP0, i)) // TODO: check if this is correct
	}
	ring := chr.MakeConsistentHashRing(numReplicas, pbsDemoNumSectors, numNodes, nodeIDs)
	net := labrpc.MakeNetwork()
	if unreliable {
		net.Reliable(false)
	}
	if longReordering {
		net.LongReordering(true)
	}

	ends := make(map[string]map[string]*labrpc.ClientEnd, numNodes)
	for _, from := range nodeIDs {
		ends[from] = make(map[string]*labrpc.ClientEnd, numNodes)
		for _, to := range nodeIDs {
			endName := from + "->" + to
			end := net.MakeEnd(endName)
			net.Connect(endName, to)
			net.Enable(endName, true)
			ends[from][to] = end
		}
	}

	servers := make(map[string]*KVServer, numNodes)
	for _, nodeID := range nodeIDs {
		servers[nodeID] = MakeKVServer(nodeID, ring, writeQuorum, readQuorum, ends[nodeID])
	}
	for _, nodeID := range nodeIDs {
		rs := labrpc.MakeServer() // make a server for each node
		rs.AddService(labrpc.MakeService(servers[nodeID])) // add the service to the server
		net.AddServer(nodeID, rs) // add the server to the network
	}

	cleanup := func() {
		for _, kv := range servers {
			close(kv.stopCh)
		}
		net.Cleanup()
	}
	return ring, nodeIDs, servers, cleanup
}
