package broker

import (
	"github.com/jackdoe/back-to-back/client"
	. "github.com/jackdoe/back-to-back/spec"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntegration(t *testing.T) {
	consumerAddr := ":12312"
	producerAddr := ":12313"
	sockConsumer, err := net.Listen("tcp", consumerAddr)
	if err != nil {
		t.Fatal("Listen error: ", err)
	}

	sockProducer, err := net.Listen("tcp", producerAddr)
	if err != nil {
		t.Fatal("Listen error: ", err)
	}

	btb := NewBackToBack()
	go btb.Listen(sockConsumer, btb.ClientWorkerConsumer)
	go btb.Listen(sockProducer, btb.ClientWorkerProducer)

	acount := uint64(0)
	bcount := uint64(0)

	dispatch := map[string]func(*Message) *Message{}
	dispatch["a"] = func(m *Message) *Message {
		atomic.AddUint64(&acount, 1)
		if m.Data[0] != byte('a') {
			t.Fatal("expected a")
		}

		return &Message{
			Data: []byte("a"),
		}
	}

	dispatch["b"] = func(m *Message) *Message {
		atomic.AddUint64(&bcount, 1)
		if m.Data[0] != byte('b') {
			t.Fatal("expected b")
		}
		return &Message{
			Data: []byte("b"),
		}
	}

	dispatch["c"] = func(m *Message) *Message {
		if string(m.Data) == "pause" {
			time.Sleep(200 * time.Millisecond)
		}

		return &Message{
			Data: []byte("c"),
		}
	}

	go client.Consume(10, []string{consumerAddr, consumerAddr}, dispatch)

	producer := client.NewProducer([]string{producerAddr, producerAddr})
	count := uint64(10000)
	// first test with timeout
	for i := uint64(0); i < count; i++ {
		topic := "a"
		if i&1 == 0 {
			topic = "b"
		}
		reply, err := producer.Request(&Message{
			Topic:          topic,
			Data:           []byte(topic),
			TimeoutAfterMs: 1000,
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		if reply.Data[0] != byte(topic[0]) {
			t.Fatalf("expected %s got %s", topic, string(reply.Data))
		}
	}

	if acount != count/2 {
		t.Fatalf("acount = %d", acount)
	}

	if bcount != count/2 {
		t.Fatalf("bcount = %d", acount)
	}

	acount = 0
	bcount = 0

	// first test with timeout
	for i := uint64(0); i < count; i++ {
		topic := "a"
		if i&1 == 0 {
			topic = "b"
		}
		reply, err := producer.Request(&Message{
			Topic:          topic,
			Data:           []byte(topic),
			TimeoutAfterMs: 0,
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		if reply.Data[0] != byte(topic[0]) {
			t.Fatalf("expected %s got %s", topic, string(reply.Data))
		}
	}

	if acount != count/2 {
		t.Fatalf("acount = %d", acount)
	}

	if bcount != count/2 {
		t.Fatalf("bcount = %d", acount)
	}

	errors := 0
	attempts := 100
	// first test with timeout
	for i := 0; i < attempts; i++ {
		topic := "c"
		data := []byte("hello")
		if i&1 == 0 {
			data = []byte("pause")
		}

		reply, err := producer.Request(&Message{
			Topic:          topic,
			Data:           data,
			TimeoutAfterMs: 120,
		})
		if err != nil {
			if err.Error() != "ERROR_CONSUMER_TIMEOUT" {
				t.Fatalf("%s", err.Error())
			}
			errors++
		} else {
			if reply.Data[0] != byte(topic[0]) {
				t.Fatalf("expected %s got %s", topic, string(reply.Data))
			}
		}

	}
	if errors != attempts/2 {
		t.Fatalf("expected %d errors got %d", attempts/2, errors)
	}

	btb.DumpStats()

	sockConsumer.Close()
	sockProducer.Close()
}
