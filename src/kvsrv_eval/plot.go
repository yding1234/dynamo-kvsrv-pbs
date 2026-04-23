package kvsrv_eval

import (
	"encoding/csv"
	"fmt"
	"image/color"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

type PlotOutput struct {
	DeltaPPath string
	KPPath     string
	DeltaCSVPath string
	KPCSVPath    string
}

// Plot renders two figures side by side in separate files:
// - delta_p.png: dashed prediction vs. solid observation for delta-P
// - k_p.png: dashed prediction vs. solid observation for K-P
func Plot(config SimulationConfig, collector *PBSCollector) (PlotOutput, error) {
	return PlotToDir(config, collector, ".")
}

func PlotToDir(config SimulationConfig, collector *PBSCollector, outputDir string) (PlotOutput, error) {
	if collector == nil {
		return PlotOutput{}, fmt.Errorf("collector is nil")
	}

	deltas := deltaSweep(config.Delta)
	ks := kSweep(config.K)

	trace := collector.Trace()
	predictedDelta, err := EvaluateDeltaPSweep(trace, config, deltas)
	if err != nil {
		return PlotOutput{}, err
	}
	observedDelta := ObserveDeltaPSweep(collector, deltas)

	predictedKP, err := PredictKPSweep(config, ks)
	if err != nil {
		return PlotOutput{}, err
	}
	observedKP := ObserveKPSweep(collector, ks)

	deltaPlot := filepath.Join(outputDir, "delta_p.png")
	if err := saveDeltaPlot(deltaPlot, deltas, predictedDelta, observedDelta); err != nil {
		return PlotOutput{}, err
	}
	deltaCSV := filepath.Join(outputDir, "delta_p.csv")
	if err := saveDeltaCSV(deltaCSV, deltas, predictedDelta, observedDelta); err != nil {
		return PlotOutput{}, err
	}

	kpPlot := filepath.Join(outputDir, "k_p.png")
	if err := saveKPPlot(kpPlot, ks, predictedKP, observedKP); err != nil {
		return PlotOutput{}, err
	}
	kpCSV := filepath.Join(outputDir, "k_p.csv")
	if err := saveKPCSV(kpCSV, ks, predictedKP, observedKP); err != nil {
		return PlotOutput{}, err
	}

	return PlotOutput{
		DeltaPPath:   deltaPlot,
		KPPath:       kpPlot,
		DeltaCSVPath: deltaCSV,
		KPCSVPath:    kpCSV,
	}, nil
}

func saveDeltaPlot(path string, deltas []time.Duration, predicted []SimulationResult, observed []float64) error {
	p := plot.New()
	p.Title.Text = "Delta-P"
	p.X.Label.Text = "Delta (ms)"
	p.Y.Label.Text = "Probability"
	p.Y.Min = 0
	p.Y.Max = 1

	predictedLine, err := plotter.NewLine(durationResultsToXYs(deltas, predicted))
	if err != nil {
		return err
	}
	stylePredictedLine(predictedLine)

	observedLine, err := plotter.NewLine(durationProbabilitiesToXYs(deltas, observed))
	if err != nil {
		return err
	}
	styleObservedLine(observedLine)

	p.Add(predictedLine, observedLine)
	p.Legend.Add("predict", predictedLine)
	p.Legend.Add("observe", observedLine)
	return p.Save(6*vg.Inch, 4*vg.Inch, path)
}

func saveKPPlot(path string, ks []int, predicted []SimulationResult, observed []float64) error {
	p := plot.New()
	p.Title.Text = "K-P"
	p.X.Label.Text = "K"
	p.Y.Label.Text = "Probability"
	p.Y.Min = 0
	p.Y.Max = 1

	predictedLine, err := plotter.NewLine(intResultsToXYs(ks, predicted))
	if err != nil {
		return err
	}
	stylePredictedLine(predictedLine)

	observedLine, err := plotter.NewLine(intProbabilitiesToXYs(ks, observed))
	if err != nil {
		return err
	}
	styleObservedLine(observedLine)

	p.Add(predictedLine, observedLine)
	p.Legend.Add("predict", predictedLine)
	p.Legend.Add("observe", observedLine)
	return p.Save(6*vg.Inch, 4*vg.Inch, path)
}

func saveDeltaCSV(path string, deltas []time.Duration, predicted []SimulationResult, observed []float64) error {
	rows := make([][]string, 0, len(deltas)+1)
	rows = append(rows, []string{"delta_ms", "predict", "observe"})
	for i, delta := range deltas {
		rows = append(rows, []string{
			formatFloat(durationToMilliseconds(delta)),
			formatFloat(predicted[i].Probability),
			formatFloat(observed[i]),
		})
	}
	return writeCSV(path, rows)
}

func saveKPCSV(path string, ks []int, predicted []SimulationResult, observed []float64) error {
	rows := make([][]string, 0, len(ks)+1)
	rows = append(rows, []string{"k", "predict", "observe"})
	for i, k := range ks {
		rows = append(rows, []string{
			strconv.Itoa(k),
			formatFloat(predicted[i].Probability),
			formatFloat(observed[i]),
		})
	}
	return writeCSV(path, rows)
}

func writeCSV(path string, rows [][]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.WriteAll(rows); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}

func durationResultsToXYs(xs []time.Duration, results []SimulationResult) plotter.XYs {
	points := make(plotter.XYs, len(xs))
	for i, x := range xs {
		points[i].X = durationToMilliseconds(x)
		points[i].Y = results[i].Probability
	}
	return points
}

func durationProbabilitiesToXYs(xs []time.Duration, ys []float64) plotter.XYs {
	points := make(plotter.XYs, len(xs))
	for i, x := range xs {
		points[i].X = durationToMilliseconds(x)
		points[i].Y = ys[i]
	}
	return points
}

func intResultsToXYs(xs []int, results []SimulationResult) plotter.XYs {
	points := make(plotter.XYs, len(xs))
	for i, x := range xs {
		points[i].X = float64(x)
		points[i].Y = results[i].Probability
	}
	return points
}

func intProbabilitiesToXYs(xs []int, ys []float64) plotter.XYs {
	points := make(plotter.XYs, len(xs))
	for i, x := range xs {
		points[i].X = float64(x)
		points[i].Y = ys[i]
	}
	return points
}

func stylePredictedLine(line *plotter.Line) {
	line.Color = color.RGBA{R: 214, G: 39, B: 40, A: 255}
	line.Width = vg.Points(1.5)
	line.Dashes = []vg.Length{vg.Points(5), vg.Points(3)}
}

func styleObservedLine(line *plotter.Line) {
	line.Color = color.RGBA{R: 31, G: 119, B: 180, A: 255}
	line.Width = vg.Points(1.5)
}

func durationToMilliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func deltaSweep(maxDelta time.Duration) []time.Duration {
	if maxDelta <= 0 {
		return []time.Duration{0}
	}

	const maxPoints = 25
	step := maxDelta / maxPoints
	if step <= 0 {
		step = time.Nanosecond
	}

	deltas := []time.Duration{0}
	for delta := step; delta < maxDelta; delta += step {
		if delta != deltas[len(deltas)-1] {
			deltas = append(deltas, delta)
		}
	}
	if deltas[len(deltas)-1] != maxDelta {
		deltas = append(deltas, maxDelta)
	}
	return deltas
}

func kSweep(maxK int) []int {
	if maxK <= 0 {
		return []int{1}
	}

	ks := make([]int, maxK)
	for i := 1; i <= maxK; i++ {
		ks[i-1] = i
	}
	return ks
}