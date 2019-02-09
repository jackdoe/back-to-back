package main

import (
	"flag"
	. "github.com/jackdoe/back-to-back/broker"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
)

func main() {
	var pbindServer = flag.String("bind", ":8000", "bind to addr")
	flag.Parse()

	sock, err := net.Listen("tcp", *pbindServer)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	btb := NewBackToBack(sock)
	btb.Listen()
	os.Exit(0)
}
