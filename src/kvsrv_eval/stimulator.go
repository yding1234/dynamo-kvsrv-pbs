package kvsrv_eval

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

type SimulationConfig struct {
	NumReplicas int
	ReadQuorum  int
	WriteQuorum int
	Delta       time.Duration
	K           int
	Iterations  int        // number of trials to run
	RNG         *rand.Rand // random number generator

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
	case config.Iterations <= 0:
		return fmt.Errorf("Iterations must be > 0")
	}
	return nil
}

func validateSamplers(samplers WARSSamplers) error {
	switch {
	case samplers.WriteRequest.Len() == 0:
		return fmt.Errorf("WriteRequest sampler is empty")
	case samplers.WriteAck.Len() == 0:
		return fmt.Errorf("WriteAck sampler is empty")
	case samplers.ReadRequest.Len() == 0:
		return fmt.Errorf("ReadRequest sampler is empty")
	case samplers.ReadResponse.Len() == 0:
		return fmt.Errorf("ReadResponse sampler is empty")
	}
	return nil
}

// SimulateDeltaP runs the WARS Monte Carlo simulation described in the PBS
// paper and returns the estimated probability of a read being consistent
// delta time after a write commits.
func SimulateDeltaP(config SimulationConfig, samplers WARSSamplers) (SimulationResult, error) {
	if err := validateDeltaPConfig(config); err != nil {
		return SimulationResult{}, err
	}
	if err := validateSamplers(samplers); err != nil {
		return SimulationResult{}, err
	}
	if config.RNG == nil {
		config.RNG = rand.New(rand.NewSource(time.Now().UnixNano()))
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
	}, nil
}

func simulateTrial(config SimulationConfig, samplers WARSSamplers) bool {
	writeRequests := make([]time.Duration, config.NumReplicas)
	writeLatencies := make([]time.Duration, config.NumReplicas)
	readRequests := make([]time.Duration, config.NumReplicas)
	readLatencies := make([]time.Duration, config.NumReplicas)

	for replica := 0; replica < config.NumReplicas; replica++ {
		writeRequests[replica] = samplers.WriteRequest.GetSample(config.RNG)
		writeAck := samplers.WriteAck.GetSample(config.RNG)
		writeLatencies[replica] = writeRequests[replica] + writeAck

		readRequests[replica] = samplers.ReadRequest.GetSample(config.RNG)
		readResponse := samplers.ReadResponse.GetSample(config.RNG)
		readLatencies[replica] = readRequests[replica] + readResponse
	}

	writeFinish := nthSmallest(writeLatencies, config.WriteQuorum)
	firstReplies := firstKIndices(readLatencies, config.ReadQuorum)
	for _, replica := range firstReplies {
		if writeFinish+readRequests[replica]+config.Delta >= writeRequests[replica] {
			return true
		}
	}

	return false
}

func nthSmallest(values []time.Duration, n int) time.Duration {
	copied := make([]time.Duration, len(values))
	copy(copied, values)
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

// EvaluateDeltaP builds empirical samplers from a collected WARS trace and then
// runs the Monte Carlo (delta, p) estimator.
func EvaluateDeltaP(trace WARSTrace, config SimulationConfig) (SimulationResult, error) {
	samplers, err := NewWARSSamplers(trace)
	if err != nil {
		return SimulationResult{}, err
	}
	return SimulateDeltaP(config, samplers)
}

// tracer -> trace -> samplers -> simulator
func EvaluateDeltaPFromTracer(tracer *Tracer, config SimulationConfig) (SimulationResult, error) {
	if tracer == nil {
		return SimulationResult{}, fmt.Errorf("tracer is nil")
	}
	return EvaluateDeltaP(tracer.Trace(), config)
}

// EvaluateDeltaPSweep evaluates multiple delta values against the same observed
// WARS trace.
func EvaluateDeltaPSweep(trace WARSTrace, baseConfig SimulationConfig, deltas []time.Duration) ([]SimulationResult, error) {
	results := make([]SimulationResult, 0, len(deltas))
	for _, delta := range deltas {
		config := configForDeltaSweep(baseConfig, delta)
		config.Delta = delta

		result, err := EvaluateDeltaP(trace, config)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

// EvaluateKP computes the paper's closed-form probability that a read returns
// one of the latest K completed versions under quorum overlap assumptions.
func PredictKP(config SimulationConfig) (SimulationResult, error) {
	if err := validateKPConfig(config); err != nil {
		return SimulationResult{}, err
	}

	missSingleWrite := combination(config.NumReplicas-config.WriteQuorum, config.ReadQuorum) / combination(config.NumReplicas, config.ReadQuorum)
	probability := 1.0 - math.Pow(missSingleWrite, float64(config.K))

	return SimulationResult{
		Probability: probability,
	}, nil
}

func EvaluateKP(config SimulationConfig) (SimulationResult, error) {
	return PredictKP(config)
}

// EvaluateKPSweep evaluates multiple K values for the same quorum setup.
func PredictKPSweep(baseConfig SimulationConfig, ks []int) ([]SimulationResult, error) {
	results := make([]SimulationResult, 0, len(ks))
	for _, k := range ks {
		config := baseConfig
		config.K = k

		result, err := EvaluateKP(config)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func EvaluateKPSweep(baseConfig SimulationConfig, ks []int) ([]SimulationResult, error) {
	return PredictKPSweep(baseConfig, ks)
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

func validateKPConfig(config SimulationConfig) error {
	switch {
	case config.NumReplicas <= 0:
		return fmt.Errorf("NumReplicas must be > 0")
	case config.ReadQuorum <= 0 || config.ReadQuorum > config.NumReplicas:
		return fmt.Errorf("ReadQuorum must be in [1, NumReplicas]")
	case config.WriteQuorum <= 0 || config.WriteQuorum > config.NumReplicas:
		return fmt.Errorf("WriteQuorum must be in [1, NumReplicas]")
	case config.K <= 0:
		return fmt.Errorf("K must be > 0")
	}
	return nil
}

func configForDeltaSweep(baseConfig SimulationConfig, delta time.Duration) SimulationConfig {
	config := baseConfig
	if baseConfig.RNG != nil {
		config.RNG = rand.New(rand.NewSource(seedForDeltaSweep(baseConfig, delta)))
	}
	return config
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
