package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
	"time"
)

func main() {
	var pserver = flag.String("server", "127.0.0.1:8000", "connect to addr")
	var pmessage = flag.String("message", "hello producer", "send this message")
	var pn = flag.Int("n", 1000, "number of messages")
	var pworkers = flag.Int("workers", 10, "number of workers")
	var pconnections = flag.Int("connections", 2, "number of connections")
	var ptopic = flag.String("topic", "abc", "topic")
	flag.Parse()
	done := make(chan int, *pworkers)
	work := func() {
		c := client.NewProducer(*pconnections, *pserver, *ptopic)
		for i := 0; i < *pn; i++ {
			c.ProduceIO(&Message{
				Data:      []byte(*pmessage),
				TimeoutMs: 10000,
			})
			//log.Printf("%s", res.String())
		}
		c.Close()
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
