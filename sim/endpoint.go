package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	//	"time"
)

func handleRequest() int {
	// assume time takes 50 + rand(200)
	//time.Sleep(50 * time.Millisecond)
	k := 0
	for i := 0; i < rand.Intn(100); i++ {
		sha256.Sum256(make([]byte, 100000))
		k++
	}
	return k
	//	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
}

func main() {
	var pbroker = flag.String("broker", "127.0.0.1:9001", "broker")
	var pbindhttp = flag.String("bindhttp", ":12312", "http bind")
	var pmaxproc = flag.Int("gomaxproc", 2, "gomaxproc")
	flag.Parse()

	runtime.GOMAXPROCS(*pmaxproc)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n := handleRequest()
		w.Write([]byte(fmt.Sprintf("%d", n)))
	})

	dispatch := map[string]func(*Message) *Message{}
	dispatch["sim"] = func(*Message) *Message {
		n := handleRequest()
		return &Message{
			Topic: "sim",
			Data:  []byte(fmt.Sprintf("%d", n)),
		}
	}
	addrs := []string{}
	for i := 0; i < *pmaxproc; i++ {
		addrs = append(addrs, *pbroker)
	}

	client.NewConsumer(addrs, dispatch)
	log.Fatal(http.ListenAndServe(*pbindhttp, nil))
}
