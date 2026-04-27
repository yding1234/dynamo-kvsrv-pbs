package kvsrv_eval

import (
	"encoding/csv"
	"fmt"
	"image/color"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
)

// PBSThreshold99_99 is the probability level we annotate on zoom plots so
// readers can see at-a-glance which Δ (or K) each consistency mechanism
// requires to reach 99.99% staleness probability.
const PBSThreshold99_99 = 0.9999

// probabilityYMax is the fixed upper bound for all P(staleness) y-axes; values
// are in [0,1] and we do not autoscale the top of the panel below 1.0.
const probabilityYMax = 1.0

// ensureProbabilityYSpan fixes gonum's axis sanitizer: when Y.Min==Y.Max it does
// Min--/Max++ on floats, turning (1,1) into (0,2) and rescaling the panel to
// 0..2. We keep a strict upper bound of 1.0 but nudge Min slightly below Max
// when the data floor sits at the ceiling.
func ensureProbabilityYSpan(yMin float64) (min, max float64) {
	max = probabilityYMax
	min = yMin
	if min >= max {
		min = math.Nextafter(max, 0)
	}
	if min < 0 {
		min = 0
	}
	return min, max
}

type PlotOutput struct {
	DeltaPPath          string
	KPPath              string
	DeltaPE2EPath       string
	KPE2EPath           string
	DeltaPE2ECSVPath    string
	KPE2ECSVPath        string
	DeltaPZoomPath      string // empty when zoom plots are disabled
	KPZoomPath          string // empty when zoom plots are disabled
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
	// ReadAttempts is total read attempts for end-to-end probability
	// (non-stale reads / all attempts). When <=0, observed curves fall back
	// to successful-read denominator.
	ReadAttempts int64
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
	observedDeltaPE2E := make([]NamedProbabilitySeries, 0, len(observedSeries))
	observedKPE2E := make([]NamedProbabilitySeries, 0, len(observedSeries))
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
		observedDeltaPE2E = append(observedDeltaPE2E, NamedProbabilitySeries{
			Name:   cfg.Name,
			Label:  cfg.Label,
			Values: ObserveDeltaPSweepE2E(observed.Collector, deltas, observed.ReadAttempts),
		})
		observedKPE2E = append(observedKPE2E, NamedProbabilitySeries{
			Name:   cfg.Name,
			Label:  cfg.Label,
			Values: ObserveKPSweepE2E(observed.Collector, ks, observed.ReadAttempts),
		})
		configRows = append(configRows, cfg)
	}

	// y-axis bounds for the main plots.
	// <=0 means "auto-fit to predicted + observed" (delta) / observed-only (k).
	// We keep the original behavior: delta_p.png used to ignore predicted in
	// auto-fit so the observed cluster wasn't squashed; we preserve that.
	mainDeltaYMin, mainDeltaYMax := resolveYRange(config.YMin, config.YMax,
		minSeriesLeft(NamedProbabilitySeries{}, observedDeltaPs))
	mainKPYMin, mainKPYMax := resolveYRange(config.YMin, config.YMax,
		minSeriesLeft(predictedKPSeries, observedKPs))

	// save the plots and CSVs (main plots: no threshold marker)
	deltaPlot := filepath.Join(outputDir, "delta_p.png")
	if err := saveDeltaPlot(deltaPlot, deltas, predictedDeltaSeries, observedDeltaPs, mainDeltaYMin, mainDeltaYMax, 0); err != nil {
		return PlotOutput{}, err
	}
	deltaCSV := filepath.Join(outputDir, "delta_p.csv")
	if err := saveDeltaCSV(deltaCSV, deltas, predictedDeltaSeries, observedDeltaPs); err != nil {
		return PlotOutput{}, err
	}

	kpPlot := filepath.Join(outputDir, "k_p.png")
	if err := saveKPPlot(kpPlot, ks, predictedKPSeries, observedKPs, mainKPYMin, mainKPYMax, 0); err != nil {
		return PlotOutput{}, err
	}
	kpCSV := filepath.Join(outputDir, "k_p.csv")
	if err := saveKPCSV(kpCSV, ks, predictedKPSeries, observedKPs); err != nil {
		return PlotOutput{}, err
	}
	deltaPE2EPlot := filepath.Join(outputDir, "delta_p_e2e.png")
	e2eDeltaYMin := minSeriesLeft(NamedProbabilitySeries{}, observedDeltaPE2E)
	if e2eDeltaYMin < 0 {
		e2eDeltaYMin = 0
	}
	if err := saveDeltaPlot(deltaPE2EPlot, deltas, NamedProbabilitySeries{}, observedDeltaPE2E, e2eDeltaYMin, probabilityYMax, PBSThreshold99_99); err != nil {
		return PlotOutput{}, err
	}
	kpE2EPlot := filepath.Join(outputDir, "k_p_e2e.png")
	e2eKPYMin := minSeriesLeft(NamedProbabilitySeries{}, observedKPE2E)
	if e2eKPYMin < 0 {
		e2eKPYMin = 0
	}
	if err := saveKPPlot(kpE2EPlot, ks, NamedProbabilitySeries{}, observedKPE2E, e2eKPYMin, probabilityYMax, PBSThreshold99_99); err != nil {
		return PlotOutput{}, err
	}
	deltaPE2ECSV := filepath.Join(outputDir, "delta_p_e2e.csv")
	if err := saveDeltaObservedCSV(deltaPE2ECSV, deltas, observedDeltaPE2E); err != nil {
		return PlotOutput{}, err
	}
	kpE2ECSV := filepath.Join(outputDir, "k_p_e2e.csv")
	if err := saveKPObservedCSV(kpE2ECSV, ks, observedKPE2E); err != nil {
		return PlotOutput{}, err
	}

	configCSV := filepath.Join(outputDir, "pbs_series_config.csv")
	if err := saveSeriesConfigCSV(configCSV, configRows); err != nil {
		return PlotOutput{}, err
	}

	out := PlotOutput{
		DeltaPPath:          deltaPlot,
		KPPath:              kpPlot,
		DeltaPE2EPath:       deltaPE2EPlot,
		KPE2EPath:           kpE2EPlot,
		DeltaPE2ECSVPath:    deltaPE2ECSV,
		KPE2ECSVPath:        kpE2ECSV,
		DeltaCSVPath:        deltaCSV,
		KPCSVPath:           kpCSV,
		SeriesConfigCSVPath: configCSV,
	}

	// Optional zoom plots: y-axis fit to observed series only so that the
	// near-1.0 differences between baseline / read_repair / anti_entropy /
	// hinted_handoff are visually distinguishable.
	if config.EmitZoomPlot {
		// y-axis: start at the minimum y across all observed points.
		zoomDeltaYMin := minObservedYMin(observedDeltaPs)
		zoomKPYMin := minObservedYMin(observedKPs)
		if zoomDeltaYMin < 0 {
			zoomDeltaYMin = 0
		}
		if zoomKPYMin < 0 {
			zoomKPYMin = 0
		}

		// Pass an empty predicted series so the zoom plot only shows observed
		// curves; otherwise the predicted line would be partially clipped at
		// the bottom of the zoomed y-range and add visual noise.
		// Pass threshold so each observed series gets a "first crosses 99.99%"
		// annotation (vertical drop-line + label) for at-a-glance comparison.
		deltaZoom := filepath.Join(outputDir, "delta_p_zoom.png")
		if err := saveDeltaPlot(deltaZoom, deltas, NamedProbabilitySeries{}, observedDeltaPs, zoomDeltaYMin, probabilityYMax, PBSThreshold99_99); err != nil {
			return PlotOutput{}, err
		}
		kpZoom := filepath.Join(outputDir, "k_p_zoom.png")
		if err := saveKPPlot(kpZoom, ks, NamedProbabilitySeries{}, observedKPs, zoomKPYMin, probabilityYMax, PBSThreshold99_99); err != nil {
			return PlotOutput{}, err
		}
		out.DeltaPZoomPath = deltaZoom
		out.KPZoomPath = kpZoom
	}

	return out, nil
}

// resolveYRange returns the (min, max) to use for a plot's y-axis given the
// user override (<=0 means "auto") and the auto-fit floor. The max is always
// probabilityYMax (1.0); yMax is ignored so P plots stay on [0,1].
func resolveYRange(yMin, yMax, autoMin float64) (float64, float64) {
	_ = yMax
	resolvedMin := autoMin
	if yMin > 0 {
		resolvedMin = yMin
	}
	if resolvedMin >= probabilityYMax {
		// guard against a degenerate range
		resolvedMin = autoMin
	}
	if resolvedMin < 0 {
		resolvedMin = 0
	}
	return resolvedMin, probabilityYMax
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

// minObservedYMin returns the minimum probability value across all points of all
// observed series (e.g. for zoom plots where we want the y floor at the data's
// global minimum, not only the first delta or K sample).
func minObservedYMin(observed []NamedProbabilitySeries) float64 {
	m := 1.0
	for _, s := range observed {
		for _, v := range s.Values {
			if v < m {
				m = v
			}
		}
	}
	return m
}

func saveDeltaPlot(path string, deltas []time.Duration, predicted NamedProbabilitySeries, observed []NamedProbabilitySeries, yMin, yMax, threshold float64) error {
	_ = yMax
	p := plot.New()
	p.Title.Text = "Delta-P"
	p.X.Label.Text = "Delta (ms)"
	p.Y.Label.Text = "Probability"
	p.Y.Min, p.Y.Max = ensureProbabilityYSpan(yMin)

	// predicted line (skip when caller passed an empty series, e.g. zoom plot
	// that intentionally omits the predicted curve to declutter the view).
	if len(predicted.Values) > 0 {
		predictedLine, err := plotter.NewLine(DelatPsToXYs(deltas, predicted.Values))
		if err != nil {
			return err
		}
		stylePredictedLine(predictedLine)
		p.Add(predictedLine)
		p.Legend.Add(predicted.Label, predictedLine)
	}

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

	// Threshold annotation: horizontal dashed line at y=threshold plus a
	// vertical drop-line + text label at the first delta where each observed
	// series crosses the threshold.
	if threshold > 0 && yMin < threshold && threshold <= probabilityYMax {
		xMin := durationToMilliseconds(deltas[0])
		xMax := durationToMilliseconds(deltas[len(deltas)-1])
		if err := addThresholdLine(p, xMin, xMax, threshold); err != nil {
			return err
		}
		xs := make([]float64, len(deltas))
		for i, d := range deltas {
			xs[i] = durationToMilliseconds(d)
		}
		for i, series := range observed {
			x, ok := firstCrossingX(xs, series.Values, threshold)
			if !ok {
				continue
			}
			if err := addCrossingMarker(p, x, threshold, yMin,
				fmt.Sprintf("%s: %.2fms", series.Label, x), i); err != nil {
				return err
			}
		}
	}

	return p.Save(7*vg.Inch, 4.5*vg.Inch, path)
}

func saveKPPlot(path string, ks []int, predicted NamedProbabilitySeries, observed []NamedProbabilitySeries, yMin, yMax, threshold float64) error {
	_ = yMax
	p := plot.New()
	p.Title.Text = "K-P"
	p.X.Label.Text = "K"
	p.Y.Label.Text = "Probability"
	p.Y.Min, p.Y.Max = ensureProbabilityYSpan(yMin)

	// predicted line (skip when caller passed an empty series, e.g. zoom plot
	// that intentionally omits the predicted curve to declutter the view).
	if len(predicted.Values) > 0 {
		predictedLine, err := plotter.NewLine(KPsToXYs(ks, predicted.Values))
		if err != nil {
			return err
		}
		stylePredictedLine(predictedLine)
		p.Add(predictedLine)
		p.Legend.Add(predicted.Label, predictedLine)
	}

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

	// Threshold annotation; same idea as saveDeltaPlot but K is discrete
	// so we report the smallest *integer* K at which the series crosses
	// the threshold (no interpolation makes sense for K-regularity).
	if threshold > 0 && yMin < threshold && threshold <= probabilityYMax {
		xMin := float64(ks[0])
		xMax := float64(ks[len(ks)-1])
		if err := addThresholdLine(p, xMin, xMax, threshold); err != nil {
			return err
		}
		for i, series := range observed {
			k, ok := firstCrossingK(ks, series.Values, threshold)
			if !ok {
				continue
			}
			label := fmt.Sprintf("%s: K=%d", series.Label, k)
			if err := addCrossingMarker(p, float64(k), threshold, yMin, label, i); err != nil {
				return err
			}
		}
	}

	return p.Save(7*vg.Inch, 4.5*vg.Inch, path)
}

// firstCrossingK returns the smallest integer K at which the curve first
// reaches or exceeds threshold. K-regularity is defined over discrete K, so
// no interpolation is performed. Returns (0, false) when no swept K crosses.
func firstCrossingK(ks []int, ys []float64, threshold float64) (int, bool) {
	if len(ks) == 0 || len(ks) != len(ys) {
		return 0, false
	}
	for i, y := range ys {
		if y >= threshold {
			return ks[i], true
		}
	}
	return 0, false
}

// firstCrossingX returns the smallest x where the (xs, ys) curve first reaches
// or exceeds threshold. Linear interpolation is used between the two adjacent
// points that bracket the crossing. Returns (0, false) if the curve never
// crosses (e.g. the observed series tops out below threshold within the swept
// range).
func firstCrossingX(xs []float64, ys []float64, threshold float64) (float64, bool) {
	if len(xs) == 0 || len(xs) != len(ys) {
		return 0, false
	}
	if ys[0] >= threshold {
		return xs[0], true
	}
	for i := 1; i < len(ys); i++ {
		if ys[i] >= threshold {
			y0, y1 := ys[i-1], ys[i]
			x0, x1 := xs[i-1], xs[i]
			if y1 == y0 {
				return x1, true
			}
			t := (threshold - y0) / (y1 - y0)
			return x0 + t*(x1-x0), true
		}
	}
	return 0, false
}

// addThresholdLine draws a horizontal dashed line at y=threshold over [xMin, xMax].
func addThresholdLine(p *plot.Plot, xMin, xMax, threshold float64) error {
	line, err := plotter.NewLine(plotter.XYs{{X: xMin, Y: threshold}, {X: xMax, Y: threshold}})
	if err != nil {
		return err
	}
	line.Color = color.RGBA{R: 100, G: 100, B: 100, A: 200}
	line.Width = vg.Points(0.8)
	line.Dashes = []vg.Length{vg.Points(2), vg.Points(2)}
	p.Add(line)
	p.Legend.Add(fmt.Sprintf("P=%.4f", threshold), line)
	return nil
}

// addCrossingMarker draws a short vertical line from yMin to threshold at x,
// plus a text label placed near the top. The label color matches the i-th
// observed series so it lines up visually with the curve.
func addCrossingMarker(p *plot.Plot, x, threshold, yMin float64, label string, seriesIdx int) error {
	col := observedPalette[seriesIdx%len(observedPalette)]

	drop, err := plotter.NewLine(plotter.XYs{{X: x, Y: yMin}, {X: x, Y: threshold}})
	if err != nil {
		return err
	}
	drop.Color = col
	drop.Width = vg.Points(0.8)
	drop.Dashes = []vg.Length{vg.Points(3), vg.Points(2)}
	p.Add(drop)

	// Stagger labels along Y so they don't overlap when several series cross
	// at nearly the same x. seriesIdx 0 sits highest, subsequent ones step
	// down by ~6% of the (threshold - yMin) span.
	span := threshold - yMin
	if span <= 0 {
		span = math.Max(threshold*0.001, 1e-6)
	}
	yLabel := threshold - span*(0.05+0.06*float64(seriesIdx))
	if yLabel < yMin {
		yLabel = yMin + span*0.02
	}

	labels, err := plotter.NewLabels(plotter.XYLabels{
		XYs:    plotter.XYs{{X: x, Y: yLabel}},
		Labels: []string{" " + label},
	})
	if err != nil {
		return err
	}
	for li := range labels.TextStyle {
		labels.TextStyle[li].Color = col
		labels.TextStyle[li].XAlign = draw.XLeft
		labels.TextStyle[li].YAlign = draw.YCenter
	}
	p.Add(labels)
	return nil
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

func saveDeltaObservedCSV(path string, deltas []time.Duration, observed []NamedProbabilitySeries) error {
	header := []string{"delta_ms"}
	for _, series := range observed {
		header = append(header, series.Name)
	}
	rows := make([][]string, 0, len(deltas)+1)
	rows = append(rows, header)
	for i, delta := range deltas {
		row := []string{formatFloat(durationToMilliseconds(delta))}
		for _, series := range observed {
			row = append(row, formatFloat(series.Values[i]))
		}
		rows = append(rows, row)
	}
	return writeCSV(path, rows)
}

func saveKPObservedCSV(path string, ks []int, observed []NamedProbabilitySeries) error {
	header := []string{"k"}
	for _, series := range observed {
		header = append(header, series.Name)
	}
	rows := make([][]string, 0, len(ks)+1)
	rows = append(rows, header)
	for i, k := range ks {
		row := []string{strconv.Itoa(k)}
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

// observedPalette is shared by styleObservedLine and addCrossingMarker so
// crossing-line / label colors visually match their parent observed curve.
var observedPalette = []color.RGBA{
	{R: 31, G: 119, B: 180, A: 255},
	{R: 44, G: 160, B: 44, A: 255},
	{R: 255, G: 127, B: 14, A: 255},
	{R: 148, G: 103, B: 189, A: 255},
	{R: 140, G: 86, B: 75, A: 255},
}

func styleObservedLine(line *plotter.Line, idx int) {
	line.Color = observedPalette[idx%len(observedPalette)]
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
