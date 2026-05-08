package kvsrv_eval

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"time"
)

type SimulationConfig struct {
	NumReplicas int
	ReadQuorum  int
	WriteQuorum int
	Delta       time.Duration
	DeltaPoints int // number of sample points along the delta axis
	K           int
	Iterations  int        // number of trials to run
	RNG         *rand.Rand // random number generator

	YMin float64
	YMax float64

	EmitZoomPlot bool
}

type SimulationResult struct {
	ConsistentTrials int
	Iterations       int
	Probability      float64 // for k,p-regular semantics, only need probability
}

func validateDeltaPConfig(config SimulationConfig) error {
	switch {
	case config.NumReplicas <= 0:
		return fmt.Errorf("NumReplicas must be > 0")
	case config.ReadQuorum <= 0 || config.ReadQuorum > config.NumReplicas:
		return fmt.Errorf("ReadQuorum must be in [1, NumReplicas]")
	case config.WriteQuorum <= 0 || config.WriteQuorum > config.NumReplicas:
		return fmt.Errorf("WriteQuorum must be in [1, NumReplicas]")
	case config.Delta < 0:
		return fmt.Errorf("Delta must be >= 0")
	case config.DeltaPoints < 0:
		return fmt.Errorf("DeltaPoints must be >= 0")
	case config.Iterations <= 0:
		return fmt.Errorf("Iterations must be > 0")
	case config.K <= 0:
		return fmt.Errorf("K must be > 0")
	}
	return nil
}

// run the WARS Monte Carlo simulation
// and returns the estimated probability of a consistent read after delta time after write commits
func SimulateDeltaP(config SimulationConfig, samplers WARSSamplers) SimulationResult {
	if err := validateDeltaPConfig(config); err != nil {
		panic(err)
	}

	consistentTrials := 0
	for iter := 0; iter < config.Iterations; iter++ {
		if simulateTrial(config, samplers) {
			consistentTrials++
		}
	}

	return SimulationResult{
		ConsistentTrials: consistentTrials,
		Iterations:       config.Iterations,
		Probability:      float64(consistentTrials) / float64(config.Iterations),
	}
}

// algorithm 1 in the PBS paper
func simulateTrial(config SimulationConfig, samplers WARSSamplers) bool {
	writeRequests := make([]time.Duration, config.NumReplicas)
	writeLatencies := make([]time.Duration, config.NumReplicas)
	readRequests := make([]time.Duration, config.NumReplicas)
	readLatencies := make([]time.Duration, config.NumReplicas)

	for i := 0; i < config.NumReplicas; i++ {
		writeRequests[i] = samplers.WriteRequest.GetSample(config.RNG)
		writeAck := samplers.WriteAck.GetSample(config.RNG)
		writeLatencies[i] = writeRequests[i] + writeAck

		readRequests[i] = samplers.ReadRequest.GetSample(config.RNG)
		readResponse := samplers.ReadResponse.GetSample(config.RNG)
		readLatencies[i] = readRequests[i] + readResponse
	}

	// finish time of the write quorum
	writeFinish := nthSmallest(writeLatencies, config.WriteQuorum)
	// indices of the first R replies
	firstReplies := firstKIndices(readLatencies, config.ReadQuorum)

	for _, i := range firstReplies {
		if writeFinish+readRequests[i]+config.Delta >= writeRequests[i] {
			return true
		}
	}

	return false
}

func nthSmallest(values []time.Duration, n int) time.Duration {
	copied := slices.Clone(values)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i] < copied[j]
	})
	return copied[n-1]
}

func firstKIndices(values []time.Duration, k int) []int {
	indices := make([]int, len(values))
	for i := range values {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return values[indices[i]] < values[indices[j]]
	})

	selected := make([]int, k)
	copy(selected, indices[:k])
	return selected
}

func EvaluateDeltaP(trace WARSTrace, config SimulationConfig) SimulationResult {
	samplers, err := NewWARSSamplers(trace)
	if err != nil {
		panic(err)
	}
	return SimulateDeltaP(config, samplers)
}

// // collector -> trace -> samplers -> simulator
// func EvaluateDeltaPFromCollector(collector *PBSCollector, config SimulationConfig) SimulationResult {
// 	if collector == nil {
// 		panic("collector is nil")
// 	}
// 	return EvaluateDeltaP(collector.Trace(), config)
// }

func PredictDeltaPSweep(trace WARSTrace, baseConfig SimulationConfig, deltas []time.Duration) []SimulationResult {
	results := make([]SimulationResult, len(deltas))
	for i, delta := range deltas {
		config := baseConfig
		config.Delta = delta
		config.RNG = rand.New(rand.NewSource(seedForDeltaSweep(config, delta)))

		results[i] = EvaluateDeltaP(trace, config)
	}
	return results
}

func EvaluateKP(config SimulationConfig) SimulationResult {
	missSingleWrite := combination(config.NumReplicas-config.WriteQuorum, config.ReadQuorum) / combination(config.NumReplicas, config.ReadQuorum)
	probability := 1.0 - math.Pow(missSingleWrite, float64(config.K))

	return SimulationResult{
		Probability: probability,
	}
}

func PredictKPSweep(baseConfig SimulationConfig, ks []int) []SimulationResult {
	results := make([]SimulationResult, len(ks))
	for i, k := range ks {
		config := baseConfig
		config.K = k

		results[i] = EvaluateKP(config)
	}
	return results
}

func combination(n, k int) float64 {
	if k < 0 || k > n {
		return 0.0
	}
	if k == 0 || k == n {
		return 1.0
	}
	if k > n-k {
		k = n - k
	}
	result := 1.0
	for i := 1; i <= k; i++ {
		result *= float64(n-k+i) / float64(i)
	}
	return result
}

func seedForDeltaSweep(config SimulationConfig, delta time.Duration) int64 {
	var seed uint64 = 1469598103934665603
	mix := func(v uint64) {
		seed ^= v + 0x9e3779b97f4a7c15 + (seed << 6) + (seed >> 2)
	}

	mix(uint64(config.NumReplicas))
	mix(uint64(config.ReadQuorum))
	mix(uint64(config.WriteQuorum))
	mix(uint64(config.Iterations))
	mix(uint64(config.K))
	mix(uint64(delta.Nanoseconds()))

	return int64(seed)
}
