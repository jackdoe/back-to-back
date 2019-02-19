package main

import (
	"flag"
	"fmt"
	"github.com/couchbaselabs/ghistogram"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func getHist() *ghistogram.Histogram {
	return ghistogram.NewHistogram(10, 10, 1.5)
}

func main() {
	var pserver = flag.String("server", "127.0.0.1:9000", "connect to addr")
	var pserverHttp = flag.String("serverHttp", "http://127.0.0.1:12312/", "connect to addr")
	var pn = flag.Int("n", 1000, "number of messages")
	var pworkers = flag.Int("workers", 10, "number of workers")
	flag.Parse()
	done := make(chan int, *pworkers)

	brokers := []string{}
	for i := 0; i < *pworkers; i++ {
		brokers = append(brokers, *pserver)
	}

	producer := client.NewProducer(brokers)
	histogramBTB := getHist()
	histogramHTTP := getHist()
	work := func() {
		hist := getHist()
		topics := []string{"sim", "sim-fast"}
		idx := 0
		for i := 0; i < *pn; i++ {
			t0 := time.Now().UnixNano()
			_, err := producer.Request(&Message{
				Topic:          topics[idx%len(topics)],
				Data:           []byte{},
				TimeoutAfterMs: 0,
			})

			if err != nil {
				panic(err)
			}
			took := (time.Now().UnixNano() - t0) / 1000000
			hist.Add(uint64(took), 1)
			idx++
		}
		histogramBTB.AddAll(hist)
		done <- *pn
	}

	workHttp := func() {
		hist := getHist()
		urls := []string{*pserverHttp, fmt.Sprintf("%sfast", *pserverHttp)}
		idx := 0

		for i := 0; i < *pn; i++ {
			t0 := time.Now().UnixNano()
			resp, err := http.Get(urls[idx%len(urls)])
			if err != nil {
				panic(err)
			}
			defer resp.Body.Close()
			_, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			took := (time.Now().UnixNano() - t0) / 1000000
			hist.Add(uint64(took), 1)
			idx++
		}
		histogramHTTP.AddAll(hist)
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
	log.Printf("btb ... %d messages, took: %.2fs, speed: %.2f per second", sum, took, float64(sum)/took)
	fmt.Println(histogramBTB.EmitGraph([]byte("- "), nil).String())

	t0 = time.Now().UnixNano()
	for i := 0; i < *pworkers; i++ {
		go workHttp()
	}

	sum = 0
	for i := 0; i < *pworkers; i++ {
		sum += <-done

	}
	took = float64(time.Now().UnixNano()-t0) / float64(1000000000)
	log.Printf("http ... %d messages, took: %.2fs, speed: %.2f per second", sum, took, float64(sum)/took)
	fmt.Println(histogramHTTP.EmitGraph([]byte("- "), nil).String())

}
