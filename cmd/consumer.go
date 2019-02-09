package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	//	"log"
)

func main() {
	var pserver = flag.String("server", "127.0.0.1:8000", "connect to addr")
	var preply = flag.String("reply", "hello world", "reply with this string")
	var ptopic = flag.String("topic", "abc", "topic")
	flag.Parse()

	c := client.NewClient(*pserver, *ptopic, 2)

	c.Consume(func(m *Message) *Message {
		//	log.Printf("received %s", m)
		return &Message{
			Data: []byte(*preply),
		}
	})
}
