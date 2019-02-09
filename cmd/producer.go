package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
)

func main() {
	var pserver = flag.String("server", "127.0.0.1:8000", "connect to addr")
	var pmessage = flag.String("message", "hello producer", "send this message")
	var pn = flag.Int("n", 1000, "number of messages")
	var ptopic = flag.String("topic", "abc", "topic")
	flag.Parse()

	c := client.NewClient(*pserver, *ptopic, 2)
	for i := 0; i < *pn; i++ {
		_, err := c.ProduceIO(&Message{
			Data:      []byte(*pmessage),
			TimeoutMs: 10000,
		})
		//	log.Printf("%s", res.String())
		if err != nil {
			log.Fatal(err)
		}
	}
}
