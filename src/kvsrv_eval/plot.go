package kvsrv_eval

import (
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func Plot(writes []CompletedWrite, reads []CompletedRead) {
	plot := plot.New()
	plot.Add(plotter.NewScatter(writes))
	plot.Add(plotter.NewScatter(reads))
	plot.Save(4*vg.Inch, 4*vg.Inch, "plot.png")
}