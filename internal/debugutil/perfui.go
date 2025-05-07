package debugutil

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"time"

	"github.com/vearch/vearch/v3/internal/pkg/log"
)

func CPUProfile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err := GetPerfSVGHtml(w, "profile"); err != nil {
		log.Error(err.Error())
	}
}

func HeapProfile(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := GetPerfSVGHtml(w, "heap"); err != nil {
		log.Error(err.Error())
	}
}

func GetPerfSVGHtml(w io.Writer, name string) error {
	suffix := time.Now().UnixNano()
	perfDataFile := fmt.Sprintf("perf-%v.data", suffix)
	perfUnfoldFile := fmt.Sprintf("perf-%v.unfold", suffix)
	perfFoldedFile := fmt.Sprintf("perf-%v.folded", suffix)
	flameGraphSVGFile := fmt.Sprintf("flamegraph-%v.svg", suffix)

	pid := os.Getpid()
	var cmdPerf *exec.Cmd
	if name == "profile" {
		cmdPerf = exec.Command("perf", "record", "-g",
			"-p", fmt.Sprintf("%d", pid),
			"-o", perfDataFile, "sleep", "30")
	} else {
		cmdPerf = exec.Command("perf", "record", "-g", "-e",
			"\"kmem:*\"", "-p", fmt.Sprintf("%d", pid),
			"-o", perfDataFile, "sleep", "30")
	}
	err := cmdPerf.Run()
	if err != nil {
		return err
	}

	defer func() {
		os.Remove(perfDataFile)
	}()
	cmdScript := exec.Command("perf", "script", "-i", perfDataFile)
	UnfoldFile, err := os.Create(perfUnfoldFile)
	if err != nil {
		return err
	}

	defer func() {
		UnfoldFile.Close()
		os.Remove(perfUnfoldFile)
	}()
	cmdScript.Stdout = UnfoldFile
	err = cmdScript.Run()
	if err != nil {
		return err
	}

	cmdStackCollapse := exec.Command("stackcollapse-perf.pl", perfUnfoldFile)
	FoldedFile, err := os.Create(perfFoldedFile)
	if err != nil {
		return err
	}

	defer func() {
		FoldedFile.Close()
		os.Remove(perfFoldedFile)
	}()
	cmdStackCollapse.Stdout = FoldedFile
	err = cmdStackCollapse.Run()
	if err != nil {
		return err
	}

	cmdFlameGraph := exec.Command("flamegraph.pl", perfFoldedFile)
	flameGraphSVG, err := os.Create(flameGraphSVGFile)
	if err != nil {
		return err
	}

	defer func() {
		flameGraphSVG.Close()
		os.Remove(flameGraphSVGFile)
	}()
	cmdFlameGraph.Stdout = flameGraphSVG
	err = cmdFlameGraph.Run()
	if err != nil {
		return err
	}

	flameGraphByte, err := os.ReadFile(flameGraphSVGFile)
	if err != nil {
		return err
	}

	var b bytes.Buffer
	b.WriteString(`<html>
<head>
<title>/perf/FlameGraph/</title>
<style>
.profile-name{
	display:inline-block;
	width:6rem;
}
</style>
</head>
<body>
   <h1>Flame Graph</h1>
   <div>` + string(flameGraphByte) + `</div>
</body>
</html>
`)

	_, err = w.Write(b.Bytes())
	return err
}
