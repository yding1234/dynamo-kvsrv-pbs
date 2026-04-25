package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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
	unreliable := flag.Bool("unreliable", true, "enable labrpc unreliable network (~10% drop, small per-message delay) to widen the PBS transition window")
	longReordering := flag.Bool("long-reordering", true, "enable labrpc long-reordering (~60% of replies delayed 200~2000ms); only meaningful with -unreliable")
	simIterations := flag.Int("sim-iters", opts.PlotConfig.Iterations, "number of Monte Carlo iterations for delta-P prediction")
	yMin := flag.Float64("ymin", opts.PlotConfig.YMin, "y-axis lower bound for delta_p.png and k_p.png; <=0 means auto-fit")
	yMax := flag.Float64("ymax", opts.PlotConfig.YMax, "y-axis upper bound for delta_p.png and k_p.png; <=0 means 1.0")
	noZoomPlot := flag.Bool("no-zoom-plot", !opts.PlotConfig.EmitZoomPlot, "disable the auto delta_p_zoom.png / k_p_zoom.png output (zoomed to observed series only)")
	randomCoordinator := flag.Bool("random-coordinator", opts.RandomCoordinator, "pick a fresh coordinator uniformly from the key's preference list per request (PBS-paper style); set false to always send to ring.GetCoordinator(key)")
	numKeys := flag.Int("num-keys", 1, "number of keys to drive workload against; each request picks one uniformly at random. Auto-generated as pbs-demo-key-0..N-1; with N>1 this spreads load across multiple preference lists")
	keyPrefix := flag.String("key-prefix", "pbs-demo-key", "prefix for auto-generated keys when -num-keys > 1")
	keysList := flag.String("keys", "", "comma-separated explicit key list; overrides -num-keys/-key-prefix when set (e.g. \"alpha,beta,gamma\")")
	seed := flag.Int64("seed", 7, "seed for the prediction RNG")
	flag.Parse()

	opts.Keys = resolveKeys(*keysList, *keyPrefix, *numKeys)
	opts.OutputDir = filepath.Join(*outputDir, experimentDirName(*numReplicas, *writeQuorum, *readQuorum, *workloadDuration, *unreliable, *longReordering, len(opts.Keys)))
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
	opts.PlotConfig.RNG = rand.New(rand.NewSource(*seed))

	startedAt := time.Now()
	result, err := kvsrv.RunPBSDemo(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kvsrv1pbsplot: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("generated Delta-P plot: %s\n", result.Plots.DeltaPPath)
	fmt.Printf("generated K-P plot: %s\n", result.Plots.KPPath)
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

func experimentDirName(numReplicas int, writeQuorum int, readQuorum int, duration time.Duration, unreliable bool, longReordering bool, numKeys int) string {
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
	name := fmt.Sprintf("n%d_w%d_r%d_duration%s_%s_%s_keys%d",
		numReplicas, writeQuorum, readQuorum, duration, networkMode, reorderingMode, numKeys)
	return strings.NewReplacer("/", "-", "\\", "-", " ", "").Replace(name)
}

// resolveKeys turns the three CLI flags (-keys, -key-prefix, -num-keys) into
// the working-set list passed to the demo. Precedence:
//  1. Explicit -keys wins (comma-separated list).
//  2. -num-keys == 1 returns the bare keyPrefix (e.g. "pbs-demo-key"),
//     so single-key runs hash to the same preference list as before the
//     multi-key refactor and stay comparable to historical experiments.
//  3. -num-keys >= 2 generates "<prefix>-0".."<prefix>-(N-1)".
//
// numKeys < 1 is clamped to 1 so the demo always has at least one key.
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
