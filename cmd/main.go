package main

import (
	"flag"
	"github.com/cyberdelia/go-metrics-graphite"
	. "github.com/jackdoe/back-to-back/broker"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var pbindServerProducer = flag.String("bindProducer", ":9000", "bind to addr for producers")
	var pstatsEvery = flag.Int("logStatsInterval", 5, "print stats ever N seconds")

	var pbindServerConsumer = flag.String("bindConsumer", ":9001", "bind to addr for consumers")
	var psendStatsToGraphite = flag.String("graphite", "", "send stats to graphite host:port (e.g. 127.0.0.1:2003)")
	var pgraphitePrefix = flag.String("graphitePrefix", "btb", "send stats to graphite")
	flag.Parse()

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
	btb.PrintStatsEvery(*pstatsEvery, log.New())

	os.Exit(0)
}
