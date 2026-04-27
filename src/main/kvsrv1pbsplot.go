package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	kvsrv "6.5840/kvsrv1"
)

func main() {
	opts := kvsrv.DefaultPBSDemoOptions()

	outputDir := flag.String("out", "exps", "base directory for per-run experiment outputs")
	numNodes := flag.Int("num-nodes", opts.NumNodes, "total number of nodes in the demo cluster")
	numReplicas := flag.Int("n", opts.PlotConfig.NumReplicas, "replication factor N used by both the real cluster and prediction")
	readQuorum := flag.Int("r", opts.PlotConfig.ReadQuorum, "read quorum R used by both the real cluster and prediction")
	writeQuorum := flag.Int("w", opts.PlotConfig.WriteQuorum, "write quorum W used by both the real cluster and prediction")
	workloadDuration := flag.Duration("duration", opts.WorkloadDuration, "duration to run each demo scenario")
	numWriters := flag.Int("writers", opts.NumWriters, "number of writer goroutines")
	sleepBetweenOps := flag.Duration("sleep", opts.SleepBetweenOps, "delay between operations")
	numReaders := flag.Int("readers", opts.NumReaders, "number of reader goroutines")
	readSleep := flag.Duration("read-sleep", opts.ReadSleep, "delay between reader get operations")
	sleepJitter := flag.Float64("sleep-jitter", opts.SleepJitterRatio, "uniform jitter added to writer/reader sleeps; 0 disables, 1 = ±100% (clamped at 1)")
	delta := flag.Duration("delta", opts.PlotConfig.Delta, "max delta value for the delta-P sweep")
	deltaPoints := flag.Int("delta-points", opts.PlotConfig.DeltaPoints, "number of sample points along the delta axis")
	maxK := flag.Int("k", opts.PlotConfig.K, "max K value for the K-P sweep")
	unreliable := flag.Bool("unreliable", opts.UnreliableNetwork, "enable labrpc unreliable network (~10% drop, small per-message delay) to widen the PBS transition window")
	longReordering := flag.Bool("long-reordering", opts.LongReordering, "enable labrpc long-reorder long-tail delay on some replies; works with or without -unreliable")
	simIterations := flag.Int("sim-iters", opts.PlotConfig.Iterations, "number of Monte Carlo iterations for delta-P prediction")
	yMin := flag.Float64("ymin", opts.PlotConfig.YMin, "y-axis lower bound for delta_p.png and k_p.png; <=0 means auto-fit")
	yMax := flag.Float64("ymax", opts.PlotConfig.YMax, "y-axis upper bound for delta_p.png and k_p.png; <=0 means 1.0")
	noZoomPlot := flag.Bool("no-zoom-plot", !opts.PlotConfig.EmitZoomPlot, "disable the auto delta_p_zoom.png / k_p_zoom.png output (zoomed to observed series only)")
	randomCoordinator := flag.Bool("random-coordinator", opts.RandomCoordinator, "pick a fresh coordinator uniformly from the key's preference list per request (PBS-paper style); set false to always send to ring.GetCoordinator(key)")
	failureStartAfter := flag.Duration("failure-start-after", 0, "with single_dead_replica: all replicas stay alive for this long before the first down (0 = at scenario start, unless only legacy -failure-recover-after is set with no other timing flags)")
	failureDownDuration := flag.Duration("failure-down-duration", 0, "length of each dead interval; with -failure-up-duration>0, cycles dead then alive; if 0, use -failure-recover-after for a single absolute recover time")
	failureUpDuration := flag.Duration("failure-up-duration", 0, "healthy time between a recover and the next down; >0 requires -failure-down-duration>0, repeats until -duration")
	failureRecoverAfter := flag.Duration("failure-recover-after", 0, "if -failure-down-duration is 0, mark alive at this time from scenario start (must be > -failure-start-after if that is set); 0 and no other timing flags = original stay-dead for full -duration")
	numKeys := flag.Int("num-keys", 1, "number of keys to drive workload against; each request picks one uniformly at random. Auto-generated as pbs-demo-key-0..N-1; with N>1 this spreads load across multiple preference lists")
	keyPrefix := flag.String("key-prefix", "pbs-demo-key", "prefix for auto-generated keys when -num-keys > 1")
	keysList := flag.String("keys", "", "comma-separated explicit key list; overrides -num-keys/-key-prefix when set (e.g. \"alpha,beta,gamma\")")
	seed := flag.Int64("seed", 7, "seed for the prediction RNG")
	flag.Parse()

	opts.Keys = resolveKeys(*keysList, *keyPrefix, *numKeys)
	opts.OutputDir = filepath.Join(*outputDir, experimentDirName(
		*numReplicas, *writeQuorum, *readQuorum, *numWriters, *numReaders,
		*workloadDuration, *unreliable, *longReordering, len(opts.Keys), failureModeDirTag(opts.Scenarios)))
	opts.NumNodes = *numNodes
	opts.PlotConfig.NumReplicas = *numReplicas
	opts.PlotConfig.ReadQuorum = *readQuorum
	opts.PlotConfig.WriteQuorum = *writeQuorum
	opts.WorkloadDuration = *workloadDuration
	opts.NumWriters = *numWriters
	opts.SleepBetweenOps = *sleepBetweenOps
	opts.NumReaders = *numReaders
	opts.ReadSleep = *readSleep
	opts.SleepJitterRatio = *sleepJitter
	opts.PlotConfig.Delta = *delta
	opts.PlotConfig.DeltaPoints = *deltaPoints
	opts.PlotConfig.K = *maxK
	opts.UnreliableNetwork = *unreliable
	opts.LongReordering = *longReordering
	opts.PlotConfig.Iterations = *simIterations
	opts.PlotConfig.YMin = *yMin
	opts.PlotConfig.YMax = *yMax
	opts.PlotConfig.EmitZoomPlot = !*noZoomPlot
	opts.RandomCoordinator = *randomCoordinator
	opts.FailureStartAfter = *failureStartAfter
	opts.FailureDownDuration = *failureDownDuration
	opts.FailureUpDuration = *failureUpDuration
	opts.FailureRecoverAfter = *failureRecoverAfter
	opts.DeadReplicaPickSeed = *seed
	opts.PlotConfig.RNG = rand.New(rand.NewSource(*seed))

	startedAt := time.Now()
	result, err := kvsrv.RunPBSDemo(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kvsrv1pbsplot: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("generated Delta-P plot: %s\n", result.Plots.DeltaPPath)
	fmt.Printf("generated K-P plot: %s\n", result.Plots.KPPath)
	fmt.Printf("generated Delta-P E2E plot: %s\n", result.Plots.DeltaPE2EPath)
	fmt.Printf("generated K-P E2E plot: %s\n", result.Plots.KPE2EPath)
	if result.Plots.DeltaPZoomPath != "" {
		fmt.Printf("generated Delta-P zoom plot: %s\n", result.Plots.DeltaPZoomPath)
	}
	if result.Plots.KPZoomPath != "" {
		fmt.Printf("generated K-P zoom plot: %s\n", result.Plots.KPZoomPath)
	}
	fmt.Printf("generated Delta-P CSV: %s\n", result.Plots.DeltaCSVPath)
	fmt.Printf("generated K-P CSV: %s\n", result.Plots.KPCSVPath)
	fmt.Printf("generated series config CSV: %s\n", result.Plots.SeriesConfigCSVPath)
	fmt.Printf("generated demo stats CSV: %s\n", result.StatsCSVPath)
	for _, scenario := range kvsrv.DefaultPBSDemoScenarios() {
		stats, ok := result.Stats[scenario.Name]
		if !ok {
			continue
		}
		fmt.Printf("stats[%s]: write_ok=%d write_err_version=%d write_quorum_retry=%d write_other_err=%d read_ok=%d read_no_key=%d read_quorum_retry=%d read_err=%d refresh_ok=%d refresh_err=%d\n",
			scenario.Name,
			stats.WriteOK,
			stats.WriteErrVersion,
			stats.WriteQuorumRetry,
			stats.WriteOtherErr,
			stats.ReadOK,
			stats.ReadNoKey,
			stats.ReadQuorumRetry,
			stats.ReadErr,
			stats.RefreshOK,
			stats.RefreshErr,
		)
	}
	fmt.Printf("completed in %s\n", time.Since(startedAt).Round(time.Millisecond))
}

func failureModeDirTag(scenarios []kvsrv.PBSDemoScenario) string {
	if len(scenarios) == 0 {
		return "none"
	}
	seen := make(map[string]struct{}, len(scenarios))
	for _, s := range scenarios {
		m := strings.TrimSpace(s.FailureMode)
		if m == "" {
			m = "none"
		}
		seen[m] = struct{}{}
	}
	modes := make([]string, 0, len(seen))
	for m := range seen {
		modes = append(modes, m)
	}
	sort.Strings(modes)
	// e.g. two modes -> "none_x_single_dead_replica" (x separates distinct values)
	if len(modes) == 1 {
		return sanitizeExperimentToken(modes[0])
	}
	for i := range modes {
		modes[i] = sanitizeExperimentToken(modes[i])
	}
	return strings.Join(modes, "x")
}

func sanitizeExperimentToken(s string) string {
	return strings.NewReplacer("/", "-", "\\", "-", " ", "-", ":", "-").Replace(s)
}

func experimentDirName(
	numReplicas int, writeQuorum int, readQuorum int, numWriters int, numReaders int,
	duration time.Duration, unreliable bool, longReordering bool, numKeys int, failureModeTag string,
) string {
	networkMode := "reliable"
	if unreliable {
		networkMode = "unreliable"
	}
	reorderingMode := "no-long-reordering"
	if longReordering {
		reorderingMode = "long-reordering"
	}

	// Include the key-set size in the dir name so multi-key sweeps don't
	// silently overwrite single-key runs (and vice versa). 1 key keeps the
	// historical "_keys1" tag rather than the legacy unsuffixed name; if
	// you want to compare against pre-multikey artefacts move them aside.
	// w/r are write/read quorum; writersN/readersM are worker goroutine counts.
	// fm_ carries failure modes (see failureModeDirTag) so e.g. single_dead
	// runs don't mix with no-failure renames in the same folder.
	name := fmt.Sprintf("n%d_w%d_r%d_writers%d_readers%d_duration%s_%s_%s_fm_%s_keys%d",
		numReplicas, writeQuorum, readQuorum, numWriters, numReaders, duration, networkMode, reorderingMode, failureModeTag, numKeys)
	return strings.NewReplacer("/", "-", "\\", "-", " ", "").Replace(name)
}

func resolveKeys(keysList string, keyPrefix string, numKeys int) []string {
	if keysList != "" {
		raw := strings.Split(keysList, ",")
		keys := make([]string, 0, len(raw))
		for _, k := range raw {
			k = strings.TrimSpace(k)
			if k != "" {
				keys = append(keys, k)
			}
		}
		if len(keys) > 0 {
			return keys
		}
	}
	if numKeys < 1 {
		numKeys = 1
	}
	if numKeys == 1 {
		return []string{keyPrefix}
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("%s-%d", keyPrefix, i)
	}
	return keys
}
