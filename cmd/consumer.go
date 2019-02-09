package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
	"sync/atomic"
	"time"
)

func main() {
	var pserver = flag.String("server", "127.0.0.1:8000", "connect to addr")
	var preply = flag.String("reply", "hello world", "reply with this string")
	var ptopic = flag.String("topic", "abc", "topic")
	var pworkers = flag.Int("workers", 10, "number of workers")
	flag.Parse()

	c := client.NewConsumer(*pserver, *ptopic)
	i := uint64(0)
	go func() {
		for {
			log.Printf("received so far: %d", i)
			time.Sleep(1 * time.Second)
		}
	}()

	c.Consume(*pworkers, func(m *Message) *Message {
		//	log.Printf("received %s", m)

		i = atomic.AddUint64(&i, 1)

		return &Message{
			Data: []byte(*preply),
		}
	})

}
