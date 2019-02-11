package main

import (
	"flag"
	. "github.com/jackdoe/back-to-back/broker"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var pbindServerProducer = flag.String("bindProducer", ":9000", "bind to addr for producers")
	var pbindServerConsumer = flag.String("bindConsumer", ":9001", "bind to addr for consumers")
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

	btb := NewBackToBack()
	go btb.Listen(sockConsumer, btb.ClientWorkerConsumer)
	go btb.Listen(sockProducer, btb.ClientWorkerProducer)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Info("closing..")
		sockConsumer.Close()
		sockProducer.Close()
	}()

	for {
		log.Infof("%s", btb.String())
		time.Sleep(1 * time.Second)
	}
	os.Exit(0)
}
