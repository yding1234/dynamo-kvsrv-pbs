package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	kvsrv "6.5840/kvsrv1"
)

func main() {
	opts := kvsrv.DefaultPBSDemoOptions()

	outputDir := flag.String("out", ".", "directory to write delta_p.png and k_p.png")
	numNodes := flag.Int("num-nodes", opts.NumNodes, "total number of nodes in the demo cluster")
	numReplicas := flag.Int("n", opts.PlotConfig.NumReplicas, "replication factor N used by both the real cluster and prediction")
	readQuorum := flag.Int("r", opts.PlotConfig.ReadQuorum, "read quorum R used by both the real cluster and prediction")
	writeQuorum := flag.Int("w", opts.PlotConfig.WriteQuorum, "write quorum W used by both the real cluster and prediction")
	workloadIterations := flag.Int("ops", opts.WorkloadIterations, "number of put/get iterations to run against the demo cluster")
	numWriters := flag.Int("writers", opts.NumWriters, "number of writer goroutines")
	sleepBetweenOps := flag.Duration("sleep", opts.SleepBetweenOps, "delay between operations")
	numReaders := flag.Int("readers", opts.NumReaders, "number of reader goroutines")
	readSleep := flag.Duration("read-sleep", opts.ReadSleep, "delay between reader get operations")
	sleepJitter := flag.Float64("sleep-jitter", opts.SleepJitterRatio, "uniform jitter added to writer/reader sleeps; 0 disables, 1 = ±100% (clamped at 1)")
	delta := flag.Duration("delta", opts.PlotConfig.Delta, "max delta value for the delta-P sweep")
	deltaPoints := flag.Int("delta-points", opts.PlotConfig.DeltaPoints, "number of sample points along the delta axis")
	maxK := flag.Int("k", opts.PlotConfig.K, "max K value for the K-P sweep")
	unreliable := flag.Bool("unreliable", opts.UnreliableNetwork, "enable labrpc unreliable network (~10% drop, small per-message delay) to widen the PBS transition window")
	longReordering := flag.Bool("long-reordering", opts.LongReordering, "enable labrpc long-reordering (~60% of replies delayed 200~2000ms); only meaningful with -unreliable")
	simIterations := flag.Int("sim-iters", opts.PlotConfig.Iterations, "number of Monte Carlo iterations for delta-P prediction")
	yMin := flag.Float64("ymin", opts.PlotConfig.YMin, "y-axis lower bound for delta_p.png and k_p.png; <=0 means auto-fit")
	yMax := flag.Float64("ymax", opts.PlotConfig.YMax, "y-axis upper bound for delta_p.png and k_p.png; <=0 means 1.0")
	noZoomPlot := flag.Bool("no-zoom-plot", !opts.PlotConfig.EmitZoomPlot, "disable the auto delta_p_zoom.png / k_p_zoom.png output (zoomed to observed series only)")
	seed := flag.Int64("seed", 7, "seed for the prediction RNG")
	flag.Parse()

	opts.OutputDir = *outputDir
	opts.NumNodes = *numNodes
	opts.PlotConfig.NumReplicas = *numReplicas
	opts.PlotConfig.ReadQuorum = *readQuorum
	opts.PlotConfig.WriteQuorum = *writeQuorum
	opts.WorkloadIterations = *workloadIterations
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
