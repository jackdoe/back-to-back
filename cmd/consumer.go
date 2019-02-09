package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
)

func main() {
	var pserver = flag.String("server", "127.0.0.1:8000", "connect to addr")
	var preply = flag.String("reply", "hello world", "reply with this string")
	var ptopic = flag.String("topic", "abc", "topic")
	var pworkers = flag.Int("workers", 10, "number of workers")
	flag.Parse()

	c := client.NewConsumer(*pserver, *ptopic)
	i := 0
	c.CloseOnExit()
	c.Consume(*pworkers, func(m *Message) *Message {
		//	log.Printf("received %s", m)
		if i%10000 == 0 {
			log.Printf("received %d", i)
		}
		i++
		return &Message{
			Data: []byte(*preply),
		}

	})
}
