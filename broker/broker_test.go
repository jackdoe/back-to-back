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

	btb := NewBackToBack(nil)
	go btb.Listen(sockConsumer, btb.ClientWorkerConsumer)
	go btb.Listen(sockProducer, btb.ClientWorkerProducer)

	producer := client.NewProducer([]string{producerAddr, producerAddr})

	testRetries(t, consumerAddr, producer)
	testMultipleTopics(t, consumerAddr, producer)
	testTimeouts(t, consumerAddr, producer)

	sockConsumer.Close()
	sockProducer.Close()
}

func testMultipleTopics(t *testing.T, consumerAddr string, producer *client.Producer) {
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

	consumerA := client.NewConsumer([]string{consumerAddr, consumerAddr}, dispatch)

	count := uint64(10000)
	// first test multiple topics with timeout
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

	// first test multiple topics
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

	consumerA.Close()
}
func testTimeouts(t *testing.T, consumerAddr string, producer *client.Producer) {
	dispatch := map[string]func(*Message) *Message{}
	dispatch["c"] = func(m *Message) *Message {
		if string(m.Data) == "pause" {
			time.Sleep(200 * time.Millisecond)
		}

		return &Message{
			Data: []byte("c"),
		}
	}

	consumerA := client.NewConsumer([]string{consumerAddr, consumerAddr}, dispatch)

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
			if err.Error() != "ERROR_CONSUMER_TIMEOUT: consumer timed out" {
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

	consumerA.Close()
}

func testRetries(t *testing.T, consumerAddr string, producer *client.Producer) {

	topic := "retry"

	dispatchSleeping := map[string]func(*Message) *Message{}
	consumerA := client.NewConsumer([]string{consumerAddr, consumerAddr}, dispatchSleeping)
	dispatchSleeping[topic] = func(m *Message) *Message {
		consumerA.Close()
		return &Message{
			Data: []byte("c"),
		}
	}

	dispatchWorking := map[string]func(*Message) *Message{}
	dispatchWorking[topic] = func(m *Message) *Message {
		return &Message{
			Data: []byte("c"),
		}
	}

	consumerB := client.NewConsumer([]string{consumerAddr, consumerAddr}, dispatchWorking)
	attempts := 100000
	// at least 1 error
	retries := 0
	for i := 0; i < attempts; i++ {

		data := []byte("pause")
		reply, err := producer.Request(&Message{
			Topic:          topic,
			Data:           data,
			TimeoutAfterMs: 0,
			RetryTTL:       10,
		})
		if reply.RetryTTL != 10 {
			retries++
		}

		if err != nil {
			t.Fatalf("%s", err.Error())
		}
	}

	if retries == 0 {
		t.Fatalf("expected to fail at least once got %d", retries)
	}
	consumerA.Close()
	consumerB.Close()
}
