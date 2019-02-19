package main

import (
	"flag"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"time"
)

func handleRequest() {
	// assume time takes 50 + rand(200)
	time.Sleep(time.Duration(50) + time.Duration(rand.Intn(200)))
}

func main() {
	var pbroker = flag.String("broker", "127.0.0.1:9001", "broker")
	var pbindhttp = flag.String("bindhttp", ":12312", "http bind")
	var pmaxproc = flag.Int("gomaxproc", 2, "gomaxproc")
	flag.Parse()

	runtime.GOMAXPROCS(*pmaxproc)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleRequest()
	})

	dispatch := map[string]func(*Message) *Message{}
	dispatch["sim"] = func(*Message) *Message {
		handleRequest()
		return &Message{
			Topic: "sim",
			Data:  []byte{},
		}
	}
	addrs := []string{}
	for i := 0; i < *pmaxproc; i++ {
		addrs = append(addrs, *pbroker)
	}

	client.NewConsumer(addrs, dispatch)
	log.Fatal(http.ListenAndServe(*pbindhttp, nil))
}
