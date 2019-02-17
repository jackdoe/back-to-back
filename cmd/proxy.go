package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	client "github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"sync/atomic"
)

type Config struct {
	Topics []struct {
		Name string   `json:"name"`
		Dest []string `json:"dest"`
	} `json:"topics"`
	ConsumeBrokers []string `json:"consumeBrokers"`
	ProduceBrokers []string `json:"produceBrokers"`
	Bind           string   `json:"bind"`
}

func parse(fn string) *Config {
	config := &Config{}

	data, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Fatal(err.Error())
	}
	err = json.Unmarshal(data, config)
	if err != nil {
		log.Fatal(err.Error())
	}

	return config
}

const (
	BACK_TO_BACK_TOPIC  = "back-to-back-topic"
	BACK_TO_BACK_PATH   = "back-to-back-path"
	BACK_TO_BACK_METHOD = "back-to-back-method"
	BACK_TO_BACK_STATUS = "back-to-back-status"
)

func main() {
	var pconfig = flag.String("c", "config.json", "config")
	flag.Parse()
	conf := parse(*pconfig)
	go func() {
		log.Println(http.ListenAndServe("localhost:6063", nil))
	}()

	dispatch := map[string]func(*Message) *Message{}
	httpClient := &http.Client{}
	counter := uint64(0)

	for _, topic := range conf.Topics {
		dispatch[topic.Name] = func(req *Message) *Message {
			method, ok := req.Header[BACK_TO_BACK_METHOD]
			if !ok {
				// just ignore
				return &Message{
					Topic: topic.Name,
					Type:  MessageType_ERROR,
					Data:  []byte("expected back-to-back-method header"),
				}
			}
			// rotate through the destinations
			cnt := atomic.AddUint64(&counter, 1)
			dest := topic.Dest[int(cnt%uint64(len(topic.Dest)))]

			httpReq, err := http.NewRequest(method, dest, bytes.NewReader(req.Data))
			for h, v := range req.Header {
				httpReq.Header.Add(h, v)
			}

			if err != nil {
				return &Message{
					Topic: topic.Name,
					Type:  MessageType_ERROR,
					Data:  []byte(err.Error()),
				}
			}
			// FIXME: timeout

			resp, err := httpClient.Do(httpReq)
			if err != nil {
				return &Message{
					Topic: topic.Name,
					Type:  MessageType_ERROR,
					Data:  []byte(err.Error()),
				}
			}

			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return &Message{
					Topic: topic.Name,
					Type:  MessageType_ERROR,
					Data:  []byte(err.Error()),
				}
			}

			message := &Message{
				Topic:  topic.Name,
				Header: map[string]string{},
				Data:   body,
			}
			for h, hv := range resp.Header {
				for _, v := range hv {
					message.Header[h] = v
				}
			}
			message.Header[BACK_TO_BACK_STATUS] = fmt.Sprintf("%d", resp.Status)
			return message
		}
	}

	consumer := client.NewConsumer(conf.ConsumeBrokers, dispatch)
	producer := client.NewProducer(conf.ProduceBrokers)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		topic := r.Header.Get(BACK_TO_BACK_TOPIC)
		if topic == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("back-to-back-topic header is required"))
			return
		}

		reqPath := r.Header.Get(BACK_TO_BACK_PATH)
		if reqPath == "" {
			reqPath = r.URL.Path
		}

		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("back-to-back body is needed: "))
			w.Write([]byte(err.Error()))
			return
		}
		r.Body.Close()

		message := &Message{
			Topic:  topic,
			Data:   b,
			Header: map[string]string{},
		}

		for h, hv := range r.Header {
			for _, v := range hv {
				message.Header[h] = v
			}
		}
		message.Header[BACK_TO_BACK_METHOD] = r.Method
		message.Header[BACK_TO_BACK_PATH] = reqPath

		reply, err := producer.Request(message)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		if reply.Header != nil {
			for h, v := range reply.Header {
				w.Header().Set(h, v)
			}
		}
		status, ok := reply.Header[BACK_TO_BACK_STATUS]
		statusCode := 200
		if !ok {
			code, err := strconv.ParseInt(status, 10, 32)
			if err != nil {
				statusCode = int(code)
			}

		}
		w.WriteHeader(statusCode)
		w.Write(reply.Data)
	})
	log.Printf("binding to %s", conf.Bind)
	log.Fatal(http.ListenAndServe(conf.Bind, nil))
	consumer.Close()
}
