package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
	"net/http"
	_ "net/http/pprof"
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
		log.Println(http.ListenAndServe("localhost:6062", nil))
	}()

	dispatch := map[string]func(*Message) *Message{}
	dispatch[*ptopic] = func(*Message) *Message {
		atomic.AddUint64(&i, 1)
		//time.Sleep(1 * time.Second)
		return &Message{
			Topic: *ptopic,
			Data:  []byte(*preply),
		}
	}

	addrs := []string{}
	for i := 0; i < *pworkers; i++ {
		addrs = append(addrs, *pserver)
	}
	client.NewConsumer(addrs, dispatch)

	for {
		log.Printf("received so far: %d", i)
		time.Sleep(1 * time.Second)
	}

}
