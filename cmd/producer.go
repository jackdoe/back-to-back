package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func main() {
	var pserver = flag.String("server", "127.0.0.1:9000", "connect to addr")
	var pmessage = flag.String("message", "hello producer", "send this message")
	var pn = flag.Int("n", 1000, "number of messages")
	var pworkers = flag.Int("workers", 10, "number of workers")
	var ptopic = flag.String("topic", "abc", "topic")
	flag.Parse()
	done := make(chan int, *pworkers)
	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	brokers := []string{}
	for i := 0; i < *pworkers; i++ {
		brokers = append(brokers, *pserver)
	}

	p := client.NewProducer(brokers)

	work := func() {
		for i := 0; i < *pn; i++ {
			_, err := p.Request(&Message{
				Topic:     *ptopic,
				Data:      []byte(*pmessage),
				TimeoutMs: 0,
			})
			if err != nil {
				log.Printf("%s", err.Error())
			}
			//			log.Printf("%s", res.String())
		}

		done <- *pn
	}

	t0 := time.Now().UnixNano()
	for i := 0; i < *pworkers; i++ {
		go work()
	}

	sum := 0
	for i := 0; i < *pworkers; i++ {
		sum += <-done

	}
	took := float64(time.Now().UnixNano()-t0) / float64(1000000000)
	log.Printf("... %d messages, took: %.2fs, speed: %.2f per second", sum, took, float64(sum)/took)
}
