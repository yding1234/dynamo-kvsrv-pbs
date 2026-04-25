package kvsrv_eval

import (
	"encoding/csv"
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
	DeltaPPath          string
	KPPath              string
	DeltaCSVPath        string
	KPCSVPath           string
	SeriesConfigCSVPath string
}


// configuration for a series of observations or predictions.
type SeriesConfig struct {
	Name          string
	Label         string
	Kind          string // "predict" or "observe"
	ReadRepair    bool
	AntiEntropy   bool
	HintedHandoff bool
	FailureMode   string
	Notes         string
}

// observation or prediction and its collector
type CollectorSeries struct {
	Config    SeriesConfig
	Collector *PBSCollector
}

// a series of probability values for a given delta or k value
type NamedProbabilitySeries struct {
	Name   string
	Label  string
	Values []float64
}


func Plot(config SimulationConfig, collector *PBSCollector) (PlotOutput, error) {
	return PlotToDir(config, collector, ".")
}

func PlotToDir(config SimulationConfig, collector *PBSCollector, outputDir string) (PlotOutput, error) {
	return PlotComparisonToDir(
		config,
		collector,
		SeriesConfig{
			Name:  "predict_baseline",
			Label: "predict_baseline",
			Kind:  "predict",
			Notes: "PBS baseline predictor without read repair, anti-entropy, or hinted handoff.",
		},
		[]CollectorSeries{
			{
				Config: SeriesConfig{
					Name:  "observe",
					Label: "observe",
					Kind:  "observe",
				},
				Collector: collector,
			},
		},
		outputDir,
	)
}

func PlotComparisonToDir(
	config SimulationConfig,
	baselineCollector *PBSCollector,
	predictedConfig SeriesConfig,
	observedSeries []CollectorSeries,
	outputDir string,
) (PlotOutput, error) {

	deltas := deltaSweep(config.Delta, config.DeltaPoints)
	ks := kSweep(config.K)

	// predicted probability values for the delta and k sweeps
	predictedDeltaP := probabilities(PredictDeltaPSweep(baselineCollector.Trace(), config, deltas))
	predictedKP := probabilities(PredictKPSweep(config, ks))

	predictedDeltaSeries := NamedProbabilitySeries{
		Name:   predictedConfig.Name,
		Label:  predictedConfig.Label,
		Values: predictedDeltaP,
	}
	predictedKPSeries := NamedProbabilitySeries{
		Name:   predictedConfig.Name,
		Label:  predictedConfig.Label,
		Values: predictedKP,
	}

	observedDeltaPs := make([]NamedProbabilitySeries, 0, len(observedSeries))
	observedKPs := make([]NamedProbabilitySeries, 0, len(observedSeries))
	configRows := make([]SeriesConfig, 0, len(observedSeries)+1) // predicted and observed series configs
	configRows = append(configRows, predictedConfig)

	// observed probability values for the delta and k sweeps
	for _, observed := range observedSeries {
		cfg := observed.Config
		observedDeltaPs = append(observedDeltaPs, NamedProbabilitySeries{
			Name:   cfg.Name,
			Label:  cfg.Label,
			Values: ObserveDeltaPSweep(observed.Collector, deltas),
		})
		observedKPs = append(observedKPs, NamedProbabilitySeries{
			Name:   cfg.Name,
			Label:  cfg.Label,
			Values: ObserveKPSweep(observed.Collector, ks),
		})
		configRows = append(configRows, cfg)
	}

	// save the plots and CSVs
	deltaPlot := filepath.Join(outputDir, "delta_p.png")
	if err := saveDeltaPlot(deltaPlot, deltas, predictedDeltaSeries, observedDeltaPs); err != nil {
		return PlotOutput{}, err
	}
	deltaCSV := filepath.Join(outputDir, "delta_p.csv")
	if err := saveDeltaCSV(deltaCSV, deltas, predictedDeltaSeries, observedDeltaPs); err != nil {
		return PlotOutput{}, err
	}

	kpPlot := filepath.Join(outputDir, "k_p.png")
	if err := saveKPPlot(kpPlot, ks, predictedKPSeries, observedKPs); err != nil {
		return PlotOutput{}, err
	}
	kpCSV := filepath.Join(outputDir, "k_p.csv")
	if err := saveKPCSV(kpCSV, ks, predictedKPSeries, observedKPs); err != nil {
		return PlotOutput{}, err
	}

	configCSV := filepath.Join(outputDir, "pbs_series_config.csv")
	if err := saveSeriesConfigCSV(configCSV, configRows); err != nil {
		return PlotOutput{}, err
	}

	return PlotOutput{
		DeltaPPath:          deltaPlot,
		KPPath:              kpPlot,
		DeltaCSVPath:        deltaCSV,
		KPCSVPath:           kpCSV,
		SeriesConfigCSVPath: configCSV,
	}, nil
}

// probabilities extracts the Probability field from each SimulationResult.
func probabilities(results []SimulationResult) []float64 {
	out := make([]float64, len(results))
	for i, r := range results {
		out[i] = r.Probability
	}
	return out
}

// minSeriesLeft returns the smallest "leftmost" probability across the predicted
// curve and all observed curves. Used to set y-axis lower bound; assumes the
// curves are non-decreasing in delta/k so the minimum lives at index 0.
func minSeriesLeft(predicted NamedProbabilitySeries, observed []NamedProbabilitySeries) float64 {
	m := 1.0
	if len(predicted.Values) > 0 && predicted.Values[0] < m {
		m = predicted.Values[0]
	}
	for _, s := range observed {
		if len(s.Values) > 0 && s.Values[0] < m {
			m = s.Values[0]
		}
	}
	return m
}


func saveDeltaPlot(path string, deltas []time.Duration, predicted NamedProbabilitySeries, observed []NamedProbabilitySeries) error {
	p := plot.New()
	p.Title.Text = "Delta-P"
	p.X.Label.Text = "Delta (ms)"
	p.Y.Label.Text = "Probability"
	// don't use predicted to see observed lines more clearly
	p.Y.Min = minSeriesLeft(NamedProbabilitySeries{Name: predicted.Name, Label: predicted.Label, Values: make([]float64, 0)}, observed)
	p.Y.Max = 1

	// predicted line
	predictedLine, err := plotter.NewLine(DelatPsToXYs(deltas, predicted.Values))
	if err != nil {
		return err
	}
	stylePredictedLine(predictedLine)
	p.Add(predictedLine)
	p.Legend.Add(predicted.Label, predictedLine)

	// observed lines
	for i, series := range observed {
		observedLine, err := plotter.NewLine(DelatPsToXYs(deltas, series.Values))
		if err != nil {
			return err
		}
		styleObservedLine(observedLine, i)
		p.Add(observedLine)
		p.Legend.Add(series.Label, observedLine)
	}

	return p.Save(7*vg.Inch, 4.5*vg.Inch, path)
}

func saveKPPlot(path string, ks []int, predicted NamedProbabilitySeries, observed []NamedProbabilitySeries) error {
	p := plot.New()
	p.Title.Text = "K-P"
	p.X.Label.Text = "K"
	p.Y.Label.Text = "Probability"
	p.Y.Min = minSeriesLeft(predicted, observed)
	p.Y.Max = 1

	// predicted line
	predictedLine, err := plotter.NewLine(KPsToXYs(ks, predicted.Values))
	if err != nil {
		return err
	}
	stylePredictedLine(predictedLine)
	p.Add(predictedLine)
	p.Legend.Add(predicted.Label, predictedLine)

	// observed lines
	for i, series := range observed {
		observedLine, err := plotter.NewLine(KPsToXYs(ks, series.Values))
		if err != nil {
			return err
		}
		styleObservedLine(observedLine, i)
		p.Add(observedLine)
		p.Legend.Add(series.Label, observedLine)
	}

	return p.Save(7*vg.Inch, 4.5*vg.Inch, path)
}

func saveDeltaCSV(path string, deltas []time.Duration, predicted NamedProbabilitySeries, observed []NamedProbabilitySeries) error {
	header := []string{"delta_ms", predicted.Name}
	for _, series := range observed {
		header = append(header, series.Name)
	}

	rows := make([][]string, 0, len(deltas)+1)
	rows = append(rows, header)
	for i, delta := range deltas {
		row := []string{
			formatFloat(durationToMilliseconds(delta)),
			formatFloat(predicted.Values[i]),
		}
		for _, series := range observed {
			row = append(row, formatFloat(series.Values[i]))
		}
		rows = append(rows, row)
	}
	return writeCSV(path, rows)
}

func saveKPCSV(path string, ks []int, predicted NamedProbabilitySeries, observed []NamedProbabilitySeries) error {
	header := []string{"k", predicted.Name}
	for _, series := range observed {
		header = append(header, series.Name)
	}

	rows := make([][]string, 0, len(ks)+1)
	rows = append(rows, header)
	for i, k := range ks {
		row := []string{
			strconv.Itoa(k),
			formatFloat(predicted.Values[i]),
		}
		for _, series := range observed {
			row = append(row, formatFloat(series.Values[i]))
		}
		rows = append(rows, row)
	}
	return writeCSV(path, rows)
}

func saveSeriesConfigCSV(path string, configs []SeriesConfig) error {
	rows := [][]string{
		{"name", "label", "kind", "read_repair", "anti_entropy", "hinted_handoff", "failure_mode", "notes"},
	}
	for _, cfg := range configs {
		rows = append(rows, []string{
			cfg.Name,
			cfg.Label,
			cfg.Kind,
			strconv.FormatBool(cfg.ReadRepair),
			strconv.FormatBool(cfg.AntiEntropy),
			strconv.FormatBool(cfg.HintedHandoff),
			cfg.FailureMode,
			cfg.Notes,
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

func DelatPsToXYs(xs []time.Duration, probabilities []float64) plotter.XYs {
	points := make(plotter.XYs, len(xs))
	for i, x := range xs {
		points[i].X = durationToMilliseconds(x)
		points[i].Y = probabilities[i]
	}
	return points
}

func KPsToXYs(xs []int, probabilities []float64) plotter.XYs {
	points := make(plotter.XYs, len(xs))
	for i, x := range xs {
		points[i].X = float64(x)
		points[i].Y = probabilities[i]
	}
	return points
}

func stylePredictedLine(line *plotter.Line) {
	line.Color = color.RGBA{R: 214, G: 39, B: 40, A: 255}
	line.Width = vg.Points(1.5)
	line.Dashes = []vg.Length{vg.Points(5), vg.Points(3)}
}

func styleObservedLine(line *plotter.Line, idx int) {
	palette := []color.RGBA{
		{R: 31, G: 119, B: 180, A: 255},
		{R: 44, G: 160, B: 44, A: 255},
		{R: 255, G: 127, B: 14, A: 255},
		{R: 148, G: 103, B: 189, A: 255},
		{R: 140, G: 86, B: 75, A: 255},
	}
	line.Color = palette[idx%len(palette)]
	line.Width = vg.Points(1.5)
}

func durationToMilliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func deltaSweep(maxDelta time.Duration, points int) []time.Duration {
    if maxDelta <= 0 {
        return []time.Duration{0}
    }
    if points <= 1 {
        return []time.Duration{0, maxDelta}
    }

    deltas := make([]time.Duration, 0, points+1)
    for i := 0; i <= points; i++ {
        d := time.Duration(int64(i) * int64(maxDelta) / int64(points))
        
        if len(deltas) == 0 || d != deltas[len(deltas)-1] {
            deltas = append(deltas, d)
        }
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
