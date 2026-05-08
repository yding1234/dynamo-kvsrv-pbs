package kvsrv

import (
	"context"
	"encoding/csv"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dynamo-kvsrv/kvsrv/chr"
	"dynamo-kvsrv/kvsrv/rpc"
	"dynamo-kvsrv/kvsrv_eval"
	"dynamo-kvsrv/labrpc"
	tester "dynamo-kvsrv/tester"
)

const pbsDemoNumSectors = 512

type PBSDemoStats struct {
	WriteOK          int64
	WriteErrVersion  int64
	WriteQuorumRetry int64 // ErrWriteQuorumNotMet (transient, retried)
	WriteNoKeyRetry  int64 // ErrNoKey (transient on write path, retried)
	WriteOtherErr    int64
	ReadOK           int64
	ReadNoKey        int64 // ErrNoKey (transient, retried)
	ReadQuorumRetry  int64 // ErrReadQuorumNotMet (transient, retried)
	ReadErr          int64
	// ProbeReadOK        int64
	// ProbeReadErr       int64
	RefreshOK  int64 // number of times the merkle tree is refreshed
	RefreshErr int64
	// ReadAttemptTotal is every demoGet and demoGetLatestContext (reader + writer
	// refresh); used as the E2E delta-P / k-P denominator.
	ReadAttemptTotal int64
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
	OutputDir        string
	Keys             []string
	WorkloadDuration time.Duration
	NumWriters       int
	SleepBetweenOps  time.Duration
	NumReaders       int
	ReadSleep        time.Duration
	SleepJitterRatio float64
	// ProbeReadsPerWrite int
	NumNodes int
	UnreliableNetwork bool
	LongReordering bool
	RandomCoordinator bool
	
	FailureStartAfter   time.Duration
	FailureDownDuration time.Duration
	FailureUpDuration   time.Duration
	FailureRecoverAfter time.Duration
	FailureOverlap      time.Duration
	
	DeadReplicaPickSeed int64
	PlotConfig          kvsrv_eval.SimulationConfig
	Scenarios           []PBSDemoScenario
}

func DefaultPBSDemoOptions() PBSDemoOptions {
	return PBSDemoOptions{
		OutputDir:        ".",
		Keys:             []string{"pbs-demo-key"},
		WorkloadDuration: 3 * time.Second,
		NumWriters:       1,
		SleepBetweenOps:  10 * time.Millisecond,
		NumReaders:       5,
		ReadSleep:        2 * time.Millisecond,
		SleepJitterRatio: 0.5,

		// ProbeReadsPerWrite: 0,
		NumNodes:          5,
		UnreliableNetwork: true,
		LongReordering:    true,
		RandomCoordinator: true,
		PlotConfig: kvsrv_eval.SimulationConfig{
			NumReplicas:  3,
			ReadQuorum:   1,
			WriteQuorum:  1,
			Delta:        2 * time.Millisecond,
			DeltaPoints:  50, // number of sample points along the delta axis
			K:            3,
			Iterations:   3, // number of Monte Carlo iterations for delta-P prediction
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
			// EnableHintedHandoff: false,
			FailureMode:         "single_dead_replica",
			// FailureMode:         "none",
		},
		{
			Name:                "observe_read_repair",
			Label:               "observe_read_repair",
			EnableReadRepair:    true,
			EnableAntiEntropy:   false,
			EnableHintedHandoff: false,
			FailureMode:         "single_dead_replica",
			// FailureMode:         "none",
		},
		{
			Name:                "observe_anti_entropy",
			Label:               "observe_anti_entropy",
			EnableReadRepair:    false,
			EnableAntiEntropy:   true,
			EnableHintedHandoff: false,
			FailureMode:         "single_dead_replica",
			// FailureMode:         "none",
		},
		{
			Name:                "observe_hinted_handoff",
			Label:               "observe_hinted_handoff",
			EnableReadRepair:    false,
			EnableAntiEntropy:   false,
			EnableHintedHandoff: true,
			FailureMode:         "single_dead_replica",
			// FailureMode:         "none",
		},
	}
}


func RunPBSDemoScenario(opts PBSDemoOptions, scenario PBSDemoScenario) (*kvsrv_eval.PBSCollector, PBSDemoStats, error) {
	if len(opts.Keys) == 0 {
		opts.Keys = []string{"pbs-demo-key"}
	}
	if opts.WorkloadDuration <= 0 {
		return nil, PBSDemoStats{}, fmt.Errorf("WorkloadDuration must be > 0")
	}
	if err := validatePBSDemoFailureOptions(&opts); err != nil {
		return nil, PBSDemoStats{}, err
	}

	var fs []string
	if scenariosUseSingleDeadReplica([]PBSDemoScenario{scenario}) {
		n := countPBSDemoScheduledFailurePhases(&opts)
		ring := makePBSDemoHashRing(opts)
		var err error
		fs, err = buildPBSFailureReplicaSequence(opts.Keys[0], ring, n, scenario)
		if err != nil {
			return nil, PBSDemoStats{}, err
		}
	}
	return runPBSDemoScenario(opts, scenario, fs)
}

func RunPBSDemo(opts PBSDemoOptions) (PBSDemoResult, error) {
	if opts.OutputDir == "" {
		opts.OutputDir = "."
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return PBSDemoResult{}, err
	}
	if len(opts.Keys) == 0 {
		opts.Keys = []string{"pbs-demo-key"}
	}
	for i, k := range opts.Keys {
		if k == "" {
			return PBSDemoResult{}, fmt.Errorf("opts.Keys[%d] is empty", i)
		}
	}
	if opts.WorkloadDuration <= 0 {
		return PBSDemoResult{}, fmt.Errorf("WorkloadDuration must be > 0")
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
	if err := validatePBSDemoFailureOptions(&opts); err != nil {
		return PBSDemoResult{}, err
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

	var failureSequence []string
	if scenariosUseSingleDeadReplica(opts.Scenarios) {
		n := countPBSDemoScheduledFailurePhases(&opts)
		ring := makePBSDemoHashRing(opts)
		var err error
		failureSequence, err = buildPBSFailureReplicaSequence(opts.Keys[0], ring, n, PBSDemoScenario{FailureMode: "single_dead_replica"})
		if err != nil {
			return PBSDemoResult{}, err
		}
	}

	for _, scenario := range opts.Scenarios {
		collector, stats, err := runPBSDemoScenario(opts, scenario, failureSequence)
		if err != nil {
			return PBSDemoResult{}, fmt.Errorf("%s: %w", scenario.Name, err)
		}
		if baselineCollector == nil {
			baselineCollector = collector
		}
		readAtt := stats.ReadAttemptTotal
		if readAtt <= 0 {
			readAtt = stats.ReadOK + stats.ReadNoKey + stats.ReadQuorumRetry + stats.ReadErr
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
			Collector:    collector,
			ReadAttempts: readAtt,
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
	if err := assertPBSDemoPlotExists(output.DeltaPE2EPath); err != nil {
		return PBSDemoResult{}, err
	}
	if err := assertPBSDemoPlotExists(output.KPE2EPath); err != nil {
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
	output.DeltaPE2EPath, _ = filepath.Abs(output.DeltaPE2EPath)
	output.KPE2EPath, _ = filepath.Abs(output.KPE2EPath)
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

func runPBSDemoScenario(opts PBSDemoOptions, scenario PBSDemoScenario, failureSequence []string) (*kvsrv_eval.PBSCollector, PBSDemoStats, error) {
	// One collector for the whole cluster: every node writes PBS samples
	// into the same pool, so randomized coordinators don't fragment data.
	sharedCollector := kvsrv_eval.NewPBSCollector()

	ring, nodeIDs, servers, pbsNet, cleanup := makePBSDemoCluster(opts.NumNodes, opts.PlotConfig.NumReplicas,
		opts.PlotConfig.ReadQuorum, opts.PlotConfig.WriteQuorum, opts.UnreliableNetwork, opts.LongReordering, sharedCollector)
	defer cleanup()

	// start the read repair and anti-entropy workers
	for _, server := range servers {
		server.readRepairEnabled = scenario.EnableReadRepair
		if scenario.EnableReadRepair {
			server.StartReadRepairWorkers()
		}
		if scenario.EnableAntiEntropy {
			server.StartMerkleRefresher()
			server.StartAntiEntropy()
		}
		server.hintedHandoffEnabled = scenario.EnableHintedHandoff
		if scenario.EnableHintedHandoff {
			server.StartHintedHandoff()
		}
	}

	candidatesByKey := make(map[string][]*KVServer, len(opts.Keys))
	for _, key := range opts.Keys {
		var cands []*KVServer
		if opts.RandomCoordinator {
			prefList := ring.GetPreferenceList(key)
			cands = make([]*KVServer, 0, len(prefList))
			for _, id := range prefList {
				if s := servers[id]; s != nil {
					cands = append(cands, s)
				}
			}
		} else {
			coordinatorID := ring.GetCoordinator(key)
			if s := servers[coordinatorID]; s != nil {
				cands = []*KVServer{s}
			}
		}
		if len(cands) == 0 {
			return nil, PBSDemoStats{}, fmt.Errorf("no coordinator candidates for key %q", key)
		}
		candidatesByKey[key] = cands
	}

	// pick a coordinator for a key
	var pickMu sync.Mutex
	pickCounts := make(map[string]int64)
	pickCoordinator := func(rng *rand.Rand, key string) *KVServer {
		cands := pbsLiveCoordinatorCandidates(candidatesByKey[key])
		var s *KVServer
		if len(cands) == 1 {
			s = cands[0]
		} else {
			s = cands[rng.Intn(len(cands))]
		}
		pickMu.Lock()
		pickCounts[s.id]++
		pickMu.Unlock()
		return s
	}
	pickKey := func(rng *rand.Rand) string {
		if len(opts.Keys) == 1 {
			return opts.Keys[0]
		}
		return opts.Keys[rng.Intn(len(opts.Keys))]
	}

	failureApplies := scenario.FailureMode == "single_dead_replica" && len(failureSequence) > 0
	legacyAlwaysDead := failureApplies && opts.FailureStartAfter == 0 && opts.FailureDownDuration == 0 && opts.FailureUpDuration == 0 && opts.FailureRecoverAfter == 0
	replicaID := ""
	if len(failureSequence) > 0 {
		replicaID = failureSequence[0]
	}
	var failureCancel context.CancelFunc
	var failureGoroutineWg *sync.WaitGroup
	if failureApplies {
		if legacyAlwaysDead {
			servers[replicaID].SetPBSDemoPauseMembershipGoss(true)
			for _, s := range servers {
				s.markMemberStatus(replicaID, rpc.Dead)
			}
			pbsSetLabRPCVictimIsolated(pbsNet, nodeIDs, replicaID, true)
			tDown := time.Now()
			fmt.Printf("[pbs-demo] scenario=%q failure=1 (legacy) node=%s down_at=%s\n",
				scenario.Name, replicaID, tDown.Format(time.RFC3339Nano))
		} else {
			var cctx context.Context
			cctx, failureCancel = context.WithCancel(context.Background())
			sname := scenario.Name
			var wg sync.WaitGroup
			wg.Add(1)
			failureGoroutineWg = &wg
			go func() {
				defer wg.Done()
				pbsSingleReplicaFailureLoop(cctx, sname, opts, failureSequence, servers, pbsNet, nodeIDs)
			}()
		}
	}

	// set automatic counters for the stats
	var writeOK atomic.Int64
	var writeErrVersion atomic.Int64
	var writeQuorumRetry atomic.Int64
	var writeNoKeyRetry atomic.Int64
	var writeOtherErr atomic.Int64
	var readOK atomic.Int64
	var readNoKey atomic.Int64
	var readQuorumRetry atomic.Int64
	var readErr atomic.Int64
	// var probeReadOK atomic.Int64
	// var probeReadErr atomic.Int64
	var refreshOK atomic.Int64
	var refreshErr atomic.Int64
	var readAttemptTotal atomic.Int64

	// set up the initial context for each key
	initialCtxByKey := make(map[string]rpc.Context, len(opts.Keys))
	for _, key := range opts.Keys {
		bootstrapC := pbsLiveCoordinatorCandidates(candidatesByKey[key])
		ctx, err := writeInitialValue(bootstrapC[0], key)
		if err != nil {
			return nil, PBSDemoStats{}, fmt.Errorf("initial value write for key %q failed: %w", key, err)
		}
		initialCtxByKey[key] = ctx
	}

	// set up the workers
	var stopWorkers atomic.Bool
	var readersWG sync.WaitGroup
	var writersWG sync.WaitGroup
	stopTimer := time.AfterFunc(opts.WorkloadDuration, func() {
		stopWorkers.Store(true)
	})

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
			// randomize the reader ID
			rng := rand.New(rand.NewSource(workerSeed(scenario.Name, "reader", readerID)))
			for !stopWorkers.Load() {
				key := pickKey(rng) // pick a random key
				readAttemptTotal.Add(1)
				softErr, hardErr := demoGet(pickCoordinator(rng, key), key)
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

			writerCtxByKey := make(map[string]rpc.Context, len(opts.Keys))
			for _, key := range opts.Keys {
				writerCtxByKey[key] = initialCtxByKey[key].Copy()
			}
			writerLabel := fmt.Sprintf("%s-writer-%d", scenario.Name, writerID)
			rng := rand.New(rand.NewSource(workerSeed(scenario.Name, "writer", writerID)))

			for i := 0; !stopWorkers.Load(); i++ {
				key := pickKey(rng)
				value := fmt.Sprintf("%s-writer-%02d-key-%s-value-%02d", scenario.Name, writerID, key, i)
				for !stopWorkers.Load() {
					nextCtx := writerCtxByKey[key].Copy()
					nextCtx.Update(writerLabel, value)
					putErr, committedCtx, err := demoPut(pickCoordinator(rng, key), key, value, nextCtx)
					if err != nil {
						reportFatalErr(fmt.Errorf("writer %d iteration %d key %q: %w", writerID, i, key, err))
						return
					}

					switch putErr {
					case rpc.OK:
						writeOK.Add(1)
						writerCtxByKey[key] = committedCtx
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
						readAttemptTotal.Add(1)
						latestCtx, ok, err := demoGetLatestContext(pickCoordinator(rng, key), key)
						if err != nil {
							refreshErr.Add(1)
							reportFatalErr(fmt.Errorf("writer %d iteration %d key %q refresh failed: %w", writerID, i, key, err))
							return
						}
						if !ok {
							// try the put again with the same base context; the cluster will eventually heal.
							refreshErr.Add(1)
							continue
						}
						refreshOK.Add(1)
						writerCtxByKey[key] = latestCtx
						continue
					case rpc.ErrWriteQuorumNotMet, rpc.ErrNoKey:
						switch putErr {
						case rpc.ErrWriteQuorumNotMet:
							writeQuorumRetry.Add(1)
						case rpc.ErrNoKey:
							writeNoKeyRetry.Add(1)
						}
						readAttemptTotal.Add(1)
						latestCtx, ok, err := demoGetLatestContext(pickCoordinator(rng, key), key)
						if err != nil {
							refreshErr.Add(1)
							reportFatalErr(fmt.Errorf("writer %d iteration %d key %q quorum-fail refresh failed: %w", writerID, i, key, err))
							return
						}
						if ok {
							refreshOK.Add(1)
							writerCtxByKey[key] = latestCtx
						}
						continue
					default:
						writeOtherErr.Add(1)
						reportFatalErr(fmt.Errorf("writer %d iteration %d key %q put failed: %v", writerID, i, key, putErr))
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

	// print the coordinator picks
	pickMu.Lock()
	pickIDs := make([]string, 0, len(pickCounts))
	for id := range pickCounts {
		pickIDs = append(pickIDs, id)
	}
	sort.Strings(pickIDs)
	parts := make([]string, 0, len(pickIDs))
	for _, id := range pickIDs {
		parts = append(parts, fmt.Sprintf("%s=%d", id, pickCounts[id]))
	}
	pickMu.Unlock()
	fmt.Printf("[pbs-demo] scenario=%s random=%v num_keys=%d coordinator picks: %s\n",
		scenario.Name, opts.RandomCoordinator, len(opts.Keys), strings.Join(parts, " "))
	if len(opts.Keys) <= 4 {
		// For small key sets, also dump per-key prefLists so it's easy to
		// eyeball whether the picks line up with each key's replicas.
		for _, key := range opts.Keys {
			fmt.Printf("[pbs-demo]   key=%q prefList=%v\n", key, ring.GetPreferenceList(key))
		}
	}

	// wait for the hinted handoff and anti-entropy to complete
	if scenario.EnableHintedHandoff {
		time.Sleep(2 * defaultHintedHandoffInterval)
	}
	if scenario.EnableAntiEntropy {
		time.Sleep(2 * defaultAntiEntropyInterval)
	}

	defer stopTimer.Stop()
	if len(failureSequence) > 0 {
		legacy := legacyAlwaysDead
		sname := scenario.Name
		defer func() {
			tUp := time.Now()
			for _, id := range uniqueStringIDs(failureSequence) {
				pbsSetLabRPCVictimIsolated(pbsNet, nodeIDs, id, false)
			}
			if legacy {
				for _, id := range uniqueStringIDs(failureSequence) {
					fmt.Printf("[pbs-demo] scenario=%q failure=1 (legacy) node=%s up_at=%s (scenario end, labrpc restored)\n",
						sname, id, tUp.Format(time.RFC3339Nano))
				}
			}
		}()
	}
	defer func() {
		if failureCancel != nil {
			failureCancel()
		}
		if failureGoroutineWg != nil {
			failureGoroutineWg.Wait()
		}
	}()

	stats := PBSDemoStats{
		WriteOK:          writeOK.Load(),
		WriteErrVersion:  writeErrVersion.Load(),
		WriteQuorumRetry: writeQuorumRetry.Load(),
		WriteNoKeyRetry:  writeNoKeyRetry.Load(),
		WriteOtherErr:    writeOtherErr.Load(),
		ReadOK:           readOK.Load(),
		ReadNoKey:        readNoKey.Load(),
		ReadQuorumRetry:  readQuorumRetry.Load(),
		ReadErr:          readErr.Load(),
		// ProbeReadOK:      probeReadOK.Load(),
		// ProbeReadErr:     probeReadErr.Load(),
		RefreshOK:        refreshOK.Load(),
		RefreshErr:       refreshErr.Load(),
		ReadAttemptTotal: readAttemptTotal.Load(),
	}
	return sharedCollector, stats, nil
}

func writePBSDemoStatsCSV(path string, statsByScenario map[string]PBSDemoStats) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	rows := [][]string{
		{"scenario", "write_ok", "write_err_version", "write_quorum_retry", "write_no_key_retry", "write_other_err", "read_ok", "read_no_key", "read_quorum_retry", "read_err", "read_attempt_total", "refresh_ok", "refresh_err"},
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
			strconv.FormatInt(stats.WriteNoKeyRetry, 10),
			strconv.FormatInt(stats.WriteOtherErr, 10),
			strconv.FormatInt(stats.ReadOK, 10),
			strconv.FormatInt(stats.ReadNoKey, 10),
			strconv.FormatInt(stats.ReadQuorumRetry, 10),
			strconv.FormatInt(stats.ReadErr, 10),
			strconv.FormatInt(stats.ReadAttemptTotal, 10),
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

func scenariosUseSingleDeadReplica(scenarios []PBSDemoScenario) bool {
	for _, s := range scenarios {
		if s.FailureMode == "single_dead_replica" {
			return true
		}
	}
	return false
}

// makePBSDemoHashRing builds the same hash ring as makePBSDemoCluster (for
// selecting a key's preference list before any labrpc/servers are created).
func makePBSDemoHashRing(o PBSDemoOptions) *chr.ConsistentHashRing {
	nodeIDs := make([]string, 0, o.NumNodes)
	for i := 0; i < o.NumNodes; i++ {
		nodeIDs = append(nodeIDs, tester.ServerName(tester.GRP0, i))
	}
	return chr.MakeConsistentHashRing(o.PlotConfig.NumReplicas, pbsDemoNumSectors, o.NumNodes, nodeIDs)
}

func deadReplicaPickRng(o *PBSDemoOptions) *rand.Rand {
	var seed int64
	if o.DeadReplicaPickSeed != 0 {
		seed = o.DeadReplicaPickSeed
	} else {
		// Deterministic from first key and topology when seed not set (API users).
		h := uint64(crc32.ChecksumIEEE([]byte(o.Keys[0])))
		h ^= uint64(o.NumNodes) * 0x9e37_79b9_7f4a7c15
		h ^= uint64(o.PlotConfig.NumReplicas) << 8
		seed = int64(h)
	}
	return rand.New(rand.NewSource(seed))
}

func selectSingleDeadReplicaID(key string, ring *chr.ConsistentHashRing, scenario PBSDemoScenario, rng *rand.Rand) (string, error) {
	if scenario.FailureMode == "" || scenario.FailureMode == "none" {
		return "", nil
	}
	if scenario.FailureMode != "single_dead_replica" {
		return "", fmt.Errorf("unsupported failure mode %q", scenario.FailureMode)
	}
	prefList := ring.GetPreferenceList(key)
	if len(prefList) < 2 {
		return "", fmt.Errorf("need at least 2 replicas for single_dead_replica")
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(7))
	}
	return prefList[rng.Intn(len(prefList))], nil
}

func validatePBSDemoFailureOptions(opts *PBSDemoOptions) error {
	if opts.FailureUpDuration > 0 && opts.FailureDownDuration <= 0 {
		return fmt.Errorf("FailureUpDuration requires FailureDownDuration > 0 (repeatable dead/healthy loop)")
	}
	if opts.FailureOverlap < 0 {
		return fmt.Errorf("FailureOverlap must be >= 0 (got %v)", opts.FailureOverlap)
	}
	if opts.FailureOverlap > 0 {
		if opts.FailureUpDuration <= 0 {
			return fmt.Errorf("FailureOverlap requires FailureUpDuration > 0 (cycling single_dead_replica mode)")
		}
		if opts.FailureDownDuration <= opts.FailureOverlap {
			return fmt.Errorf("FailureOverlap (%v) must be < FailureDownDuration (%v)", opts.FailureOverlap, opts.FailureDownDuration)
		}
	}
	if opts.FailureStartAfter > 0 && opts.FailureRecoverAfter > 0 && opts.FailureDownDuration == 0 {
		if opts.FailureRecoverAfter <= opts.FailureStartAfter {
			return fmt.Errorf("FailureRecoverAfter (%v) must be after FailureStartAfter (%v)", opts.FailureRecoverAfter, opts.FailureStartAfter)
		}
	}
	return nil
}

// pbsSetLabRPCIncomingEnabled sets net.Enable(from+"->"+target, enabled) for
// every from in nodeIDs.
func pbsSetLabRPCIncomingEnabled(net *labrpc.Network, nodeIDs []string, target string, enabled bool) {
	if net == nil || target == "" {
		return
	}
	for _, from := range nodeIDs {
		endName := from + "->" + target
		net.Enable(endName, enabled)
	}
}


func pbsSetLabRPCVictimIsolated(net *labrpc.Network, nodeIDs []string, target string, isIsolated bool) {
	enabled := !isIsolated
	pbsSetLabRPCIncomingEnabled(net, nodeIDs, target, enabled)
	if net == nil || target == "" {
		return
	}
	for _, to := range nodeIDs {
		endName := target + "->" + to
		net.Enable(endName, enabled)
	}
}


func pbsLiveCoordinatorCandidates(cands []*KVServer) []*KVServer {
	if len(cands) == 0 {
		return cands
	}
	ref := cands[0]
	out := make([]*KVServer, 0, len(cands))
	for _, c := range cands {
		if !ref.isDead(c.id) {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return cands
	}
	return out
}

// uniqueStringIDs returns ids in first-seen order.
func uniqueStringIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}


func countPBSDemoScheduledFailurePhases(o *PBSDemoOptions) int {
	if o == nil {
		return 0
	}
	s := o.FailureStartAfter
	d := o.FailureDownDuration
	u := o.FailureUpDuration
	tRun := o.WorkloadDuration
	if s == 0 && d == 0 && u == 0 && o.FailureRecoverAfter == 0 {
		return 1
	}
	if u > 0 {
		if s >= tRun {
			return 0
		}
		n := 0
		for ts := s; ts < tRun; ts += d + u {
			n++
		}
		return n
	}
	if s >= tRun {
		return 0
	}
	return 1
}


func buildPBSFailureReplicaSequence(key string, ring *chr.ConsistentHashRing, n int, scenario PBSDemoScenario) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	if scenario.FailureMode != "single_dead_replica" {
		return nil, fmt.Errorf("unsupported failure mode %q", scenario.FailureMode)
	}
	prefList := ring.GetPreferenceList(key)
	if len(prefList) < 2 {
		return nil, fmt.Errorf("need at least 2 replicas for single_dead_replica")
	}
	k := len(prefList)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, prefList[i%k])
	}
	return out, nil
}


func pbsSingleReplicaFailureLoop(ctx context.Context, scenarioName string, opts PBSDemoOptions, victimSequence []string, servers map[string]*KVServer, net *labrpc.Network, nodeIDs []string) {
	if len(victimSequence) == 0 {
		return
	}
	mark := func(replicaID string, st rpc.NodeStatus) {
		for _, s := range servers {
			s.markMemberStatus(replicaID, st)
		}
	}
	setVictimIsolated := func(replicaID string, isolated bool) {
		pbsSetLabRPCVictimIsolated(net, nodeIDs, replicaID, isolated)
	}
	sleep := func(d time.Duration) error {
		if d <= 0 {
			return nil
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}

	t0 := time.Now()
	s := opts.FailureStartAfter
	d := opts.FailureDownDuration
	u := opts.FailureUpDuration
	r := opts.FailureRecoverAfter

	defer func() {
		for _, id := range uniqueStringIDs(victimSequence) {
			if srv, ok := servers[id]; ok {
				srv.SetPBSDemoPauseMembershipGoss(false)
			}
			mark(id, rpc.Alive)
			setVictimIsolated(id, false)
		}
	}()

	if err := sleep(s); err != nil {
		return
	}

	if u > 0 {
		ov := opts.FailureOverlap
		if ov < 0 {
			ov = 0
		}
		i := 0
		phase := 0
		for i < len(victimSequence) {
			select {
			case <-ctx.Done():
				return
			default:
			}
			vid := victimSequence[i]
			if vid == "" {
				i++
				continue
			}
			nextJ := i + 1
			for nextJ < len(victimSequence) && victimSequence[nextJ] == "" {
				nextJ++
			}
			hasNext := nextJ < len(victimSequence) && victimSequence[nextJ] != ""
			useOverlap := hasNext && ov > 0 && d > ov
			if !useOverlap {
				if srv, ok := servers[vid]; ok {
					srv.SetPBSDemoPauseMembershipGoss(true)
				}
				mark(vid, rpc.Dead)
				setVictimIsolated(vid, true)
				phase++
				tDown := time.Now()
				fmt.Printf("[pbs-demo] scenario=%q failure=%d node=%s down_at=%s\n",
					scenarioName, phase, vid, tDown.Format(time.RFC3339Nano))
				if err := sleep(d); err != nil {
					return
				}
				mark(vid, rpc.Alive)
				setVictimIsolated(vid, false)
				if srv, ok := servers[vid]; ok {
					srv.SetPBSDemoPauseMembershipGoss(false)
				}
				tUp := time.Now()
				fmt.Printf("[pbs-demo] scenario=%q failure=%d node=%s up_at=%s\n",
					scenarioName, phase, vid, tUp.Format(time.RFC3339Nano))
				i++
			} else {
				if srv, ok := servers[vid]; ok {
					srv.SetPBSDemoPauseMembershipGoss(true)
				}
				mark(vid, rpc.Dead)
				setVictimIsolated(vid, true)
				phase++
				tDown1 := time.Now()
				fmt.Printf("[pbs-demo] scenario=%q failure=%d node=%s down_at=%s\n",
					scenarioName, phase, vid, tDown1.Format(time.RFC3339Nano))
				nextVid := victimSequence[nextJ]
				if err := sleep(d - ov); err != nil {
					return
				}
				if srv, ok := servers[nextVid]; ok {
					srv.SetPBSDemoPauseMembershipGoss(true)
				}
				mark(nextVid, rpc.Dead)
				setVictimIsolated(nextVid, true)
				phase++
				tDown2 := time.Now()
				fmt.Printf("[pbs-demo] scenario=%q failure=%d node=%s down_at=%s (overlap %v with %q)\n",
					scenarioName, phase, nextVid, tDown2.Format(time.RFC3339Nano), ov, vid)
				if err := sleep(ov); err != nil {
					return
				}
				mark(vid, rpc.Alive)
				setVictimIsolated(vid, false)
				if srv, ok := servers[vid]; ok {
					srv.SetPBSDemoPauseMembershipGoss(false)
				}
				tUp1 := time.Now()
				fmt.Printf("[pbs-demo] scenario=%q failure=%d node=%s up_at=%s\n",
					scenarioName, phase-1, vid, tUp1.Format(time.RFC3339Nano))
				if err := sleep(d - ov); err != nil {
					return
				}
				mark(nextVid, rpc.Alive)
				setVictimIsolated(nextVid, false)
				if srv, ok := servers[nextVid]; ok {
					srv.SetPBSDemoPauseMembershipGoss(false)
				}
				tUp2 := time.Now()
				fmt.Printf("[pbs-demo] scenario=%q failure=%d node=%s up_at=%s\n",
					scenarioName, phase, nextVid, tUp2.Format(time.RFC3339Nano))
				i = nextJ + 1
			}
			if i < len(victimSequence) {
				if err := sleep(u); err != nil {
					return
				}
			}
		}
		return
	}

	// One-shot: single victim (first in sequence) for non-repeating timing.
	vid := victimSequence[0]
	if vid == "" {
		return
	}
	if srv, ok := servers[vid]; ok {
		srv.SetPBSDemoPauseMembershipGoss(true)
	}
	mark(vid, rpc.Dead)
	setVictimIsolated(vid, true)
	tDown1 := time.Now()
	fmt.Printf("[pbs-demo] scenario=%q failure=1 node=%s down_at=%s\n",
		scenarioName, vid, tDown1.Format(time.RFC3339Nano))
	if d > 0 {
		if err := sleep(d); err != nil {
			return
		}
	} else if r > 0 {
		rem := time.Until(t0.Add(r))
		if rem < 0 {
			rem = 0
		}
		if err := sleep(rem); err != nil {
			return
		}
	} else {
		<-ctx.Done()
		return
	}
	tUp1 := time.Now()
	mark(vid, rpc.Alive)
	setVictimIsolated(vid, false)
	if srv, ok := servers[vid]; ok {
		srv.SetPBSDemoPauseMembershipGoss(false)
	}
	fmt.Printf("[pbs-demo] scenario=%q failure=1 node=%s up_at=%s\n",
		scenarioName, vid, tUp1.Format(time.RFC3339Nano))
}

// put the value for a key
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

// get the value for a key
func demoGet(coordinator *KVServer, key string) (rpc.Err, error) {
	getArgs := rpc.GetArgs{Key: key}
	getReply := rpc.GetReply{}
	coordinator.CoordGet(&getArgs, &getReply)
	if getReply.Err == rpc.OK && len(getReply.Objects) == 0 {
		return rpc.OK, fmt.Errorf("CoordGet returned OK with no objects")
	}
	return getReply.Err, nil
}


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
	// Keep failure detector and gossip from treating PBS-injected state as
	// stale and overriding Status (e.g. marking Alive back to Suspect/Dead).
	kv.memberLastUpdated[serverID] = time.Now()
}

func makePBSDemoCluster(numNodes int, numReplicas int, readQuorum int, writeQuorum int,
	unreliable bool, longReordering bool, sharedCollector *kvsrv_eval.PBSCollector,
) (*chr.ConsistentHashRing, []string, map[string]*KVServer, *labrpc.Network, func()) {
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
		s := MakeKVServer(nodeID, ring, writeQuorum, readQuorum, ends[nodeID])
		// Share one collector across the whole cluster so PBS samples land in
		// a single pool regardless of which node served as coordinator.
		if sharedCollector != nil {
			s.collector = sharedCollector
		}
		servers[nodeID] = s
	}
	for _, nodeID := range nodeIDs {
		rs := labrpc.MakeServer()                          // make a server for each node
		rs.AddService(labrpc.MakeService(servers[nodeID])) // add the service to the server
		net.AddServer(nodeID, rs)                          // add the server to the network
	}

	cleanup := func() {
		for _, kv := range servers {
			close(kv.stopCh)
		}
		net.Cleanup()
	}
	return ring, nodeIDs, servers, net, cleanup
}
