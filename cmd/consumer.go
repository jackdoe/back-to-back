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
	var pserver = flag.String("server", "127.0.0.1:9001", "connect to addr")
	var preply = flag.String("reply", "hello world", "reply with this string")
	var ptopic = flag.String("topic", "abc", "topic")
	var pworkers = flag.Int("workers", 10, "number of workers")
	flag.Parse()

	i := uint64(0)
	go func() {
		for {
			log.Printf("received so far: %d", i)
			time.Sleep(1 * time.Second)
		}
	}()

	client.Consume([]string{*pserver}, []string{*ptopic}, *pworkers, func(m *Message) *Message {
		atomic.AddUint64(&i, 1)

		return &Message{
			Data: []byte(*preply),
		}
	})

}
