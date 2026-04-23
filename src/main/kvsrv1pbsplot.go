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
	probeReads := flag.Int("probe-reads", opts.ProbeReadsPerWrite, "number of immediate probe reads after each successful write")
	delta := flag.Duration("delta", opts.PlotConfig.Delta, "max delta value for the delta-P sweep")
	maxK := flag.Int("k", opts.PlotConfig.K, "max K value for the K-P sweep")
	simIterations := flag.Int("sim-iters", opts.PlotConfig.Iterations, "number of Monte Carlo iterations for delta-P prediction")
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
	opts.ProbeReadsPerWrite = *probeReads
	opts.PlotConfig.Delta = *delta
	opts.PlotConfig.K = *maxK
	opts.PlotConfig.Iterations = *simIterations
	opts.PlotConfig.RNG = rand.New(rand.NewSource(*seed))

	startedAt := time.Now()
	result, err := kvsrv.RunPBSDemo(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kvsrv1pbsplot: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("generated Delta-P plot: %s\n", result.Plots.DeltaPPath)
	fmt.Printf("generated K-P plot: %s\n", result.Plots.KPPath)
	fmt.Printf("generated Delta-P CSV: %s\n", result.Plots.DeltaCSVPath)
	fmt.Printf("generated K-P CSV: %s\n", result.Plots.KPCSVPath)
	fmt.Printf("generated demo stats CSV: %s\n", result.StatsCSVPath)
	fmt.Printf("stats: write_ok=%d write_err_version=%d write_other_err=%d read_ok=%d read_err=%d probe_read_ok=%d probe_read_err=%d refresh_ok=%d refresh_err=%d\n",
		result.Stats.WriteOK,
		result.Stats.WriteErrVersion,
		result.Stats.WriteOtherErr,
		result.Stats.ReadOK,
		result.Stats.ReadErr,
		result.Stats.ProbeReadOK,
		result.Stats.ProbeReadErr,
		result.Stats.RefreshOK,
		result.Stats.RefreshErr,
	)
	fmt.Printf("completed in %s\n", time.Since(startedAt).Round(time.Millisecond))
}
