package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
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

	work := func() {
		conn := Connect(*pserver)
		for i := 0; i < *pn; i++ {
			res, err := client.ProduceIO(conn, &Message{
				Topic:     *ptopic,
				Data:      []byte(*pmessage),
				TimeoutMs: 100000,
			})
			if err != nil {
				log.Printf("failed to produce: %s", err.Error())
				conn.Close()
				conn = Connect(*pserver)
			} else if res.Type == MessageType_ERROR {
				log.Printf("error: %s", string(res.Data))
			}

			//			log.Printf("%s", res.String())
		}

		conn.Close()
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
