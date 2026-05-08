// Command posterdeltakp reads delta_p.csv and k_p.csv from a directory and writes a wide PNG:
// either two columns (Δ–P | K–P) or four (adds trapezoid cumulative ∫P·dx panels, **not** normalized —
// proportional P curves used to collapse when divided by total ∫).
//
// CSV columns whose header starts with "predict" are omitted (observed curves only).
package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"dynamo-kvsrv/kvsrv_eval"

	"gonum.org/v1/plot/vg"
)

func main() {
	dir := flag.String("dir", ".", "folder containing delta_p.csv and k_p.csv (series labels = each file's header row, after delta_ms / k)")
	out := flag.String("o", "", "output PNG path (default depends on -cols)")
	cols := flag.Int("cols", 4, "number of subplot columns: 2 (Δ-P | K-P) or 4 (+ trapezoid ∫ P·dx, unnormalized)")
	widthIn := flag.Float64("width-in", 0, "output width in inches (0 = auto: 44 for -cols 2, 88 for -cols 4)")
	heightIn := flag.Float64("height-in", 0, "output height in inches (0 = use poster default ~15)")

	flag.Parse()

	if *cols != 2 && *cols != 4 {
		log.Fatal("-cols must be 2 or 4")
	}

	outPath := *out
	if outPath == "" {
		if *cols == 4 {
			outPath = filepath.Join(*dir, "poster_delta_k_p_4cdf.png")
		} else {
			outPath = filepath.Join(*dir, "poster_delta_k_p_zoom.png")
		}
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	sty := kvsrv_eval.DefaultPosterStyle()
	imgW := *widthIn
	if imgW <= 0 {
		if *cols == 4 {
			imgW = 88
		} else {
			imgW = 44
		}
	}
	sty.ImageWidth = vg.Length(imgW) * vg.Inch

	imgH := *heightIn
	if imgH > 0 {
		sty.ImageHeight = vg.Length(imgH) * vg.Inch
	}

	var err error
	switch *cols {
	case 2:
		err = kvsrv_eval.RenderPosterDeltaKPCombined(*dir, outPath, sty)
	case 4:
		err = kvsrv_eval.RenderPosterDeltaKPFourColumn(*dir, outPath, sty)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s (%d cols)", outPath, *cols)
}
