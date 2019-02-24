package main

import (
	"flag"
	"fmt"

	"github.com/cyberdelia/go-metrics-graphite"
	ui "github.com/gizak/termui"
	"github.com/gizak/termui/widgets"
	. "github.com/jackdoe/back-to-back/broker"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	var pbindServerProducer = flag.String("bindProducer", ":9000", "bind to addr for producers")
	var pbindServerConsumer = flag.String("bindConsumer", ":9001", "bind to addr for consumers")
	var psendStatsToGraphite = flag.String("graphite", "", "send stats to graphite host:port (e.g. 127.0.0.1:2003)")
	var pgraphitePrefix = flag.String("graphitePrefix", "btb", "send stats to graphite")
	flag.Parse()
	log.SetLevel(log.FatalLevel)
	if err := ui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v", err)
	}
	defer ui.Close()

	sockConsumer, err := net.Listen("tcp", *pbindServerConsumer)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	sockProducer, err := net.Listen("tcp", *pbindServerProducer)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	registry := metrics.DefaultRegistry
	btb := NewBackToBack(registry)
	go btb.Listen(sockConsumer, btb.ClientWorkerConsumer)
	go btb.Listen(sockProducer, btb.ClientWorkerProducer)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Info("closing..")
		sockConsumer.Close()
		sockProducer.Close()
		os.Exit(0)
	}()

	if *psendStatsToGraphite != "" {
		addr, err := net.ResolveTCPAddr("tcp", *psendStatsToGraphite)
		if err != nil {
			log.Fatal("error resolving", err)
		}
		go graphite.Graphite(registry, 10e9, *pgraphitePrefix, addr)
	}

	drawables := map[string]*Graph{}
	uiEvents := ui.PollEvents()
	ticker := time.NewTicker(time.Second).C
	for {
		select {
		case e := <-uiEvents:
			switch e.ID {
			case "q", "<C-c>": // press 'q' or 'C-c' to quit
				return
			}
		case <-ticker:
			draw(drawables, registry)
		}
	}

	os.Exit(0)
}

type Graph struct {
	history map[string][]float64
	title   string
	index   int
	color   ui.Color
}

func (h *Graph) draw() []ui.Drawable {
	out := []ui.Drawable{}
	keys := []string{}
	for name, _ := range h.history {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	for _, name := range keys {
		data := h.history[name]
		p1 := widgets.NewPlot()
		p1.Title = fmt.Sprintf("%s: %s", h.title, name)
		p1.Marker = widgets.MarkerDot
		p1.Data = [][]float64{data}
		p1.DotRune = '+'
		p1.AxesColor = ui.ColorWhite
		p1.LineColors[0] = h.color
		p1.DrawDirection = widgets.DrawLeft
		out = append(out, p1)
	}
	return out
}
func (h *Graph) add(s string, n float64) {
	if len(h.history[s]) > 50 {
		h.history[s] = append(h.history[s][1:], n)
	} else {
		h.history[s] = append(h.history[s], n)
	}
}

func NewGraph(title string) *Graph {
	return &Graph{
		title:   title,
		history: map[string][]float64{},
	}
}

func draw(drawables map[string]*Graph, registry metrics.Registry) {
	percentiles := []float64{0.75, 0.99}
	du := float64(time.Second)
	registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			h, ok := drawables[name]
			if !ok {
				h = NewGraph(name)
				drawables[name] = h
				h.color = ui.ColorBlue
			}
			h.add("count", float64(metric.Count()))
		case metrics.Gauge:
			h, ok := drawables[name]
			if !ok {
				h = NewGraph(name)
				h.color = ui.ColorYellow
				drawables[name] = h
			}
			h.add("value", float64(metric.Value()))
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles(percentiles)
			h, ok := drawables[name]
			if !ok {
				h = NewGraph(name)
				drawables[name] = h
				h.color = ui.ColorMagenta
			}
			h.add("one-minute-k", float64(t.Rate1())/1000)
			h.add("five-minute-k", float64(t.Rate5())/1000)
			for psIdx, psKey := range percentiles {
				key := strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)
				h.add(key, float64(ps[psIdx]/du))
			}
		default:
		}
	})
	out := []ui.Drawable{}
	offx := 0
	offy := 0
	keys := []string{}
	for name, _ := range drawables {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	j := 0
	for _, n := range keys {
		d := drawables[n]
		charts := d.draw()
		for _, p := range charts {
			p.SetRect(offx, offy, offx+60, offy+15)
			offx += 60
			j++
			if j%3 == 0 {
				j = 0
				offy += 15
				offx = 0
			}

		}

		out = append(out, charts...)
	}
	ui.Render(out...)

}
