package broker

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Topic struct {
	waiting              chan net.Conn
	requests             chan *Message
	name                 string
	producedCount        uint64
	consumedCount        uint64
	consumedTimeoutCount uint64

	sync.RWMutex
}
type Producer struct {
	replyChannel chan *Message
	sync.RWMutex
}

type BackToBack struct {
	topics     map[string]*Topic
	pollCount  uint64
	uuid       uint64
	producerID uint32
	producers  map[uint32]*Producer
	sync.RWMutex
}

func NewBackToBack() *BackToBack {
	return &BackToBack{
		topics:    map[string]*Topic{},
		producers: map[uint32]*Producer{},
	}
}

func (btb *BackToBack) getTopic(topic string) *Topic {
	btb.RLock()
	t, ok := btb.topics[topic]
	btb.RUnlock()
	if !ok {
		btb.Lock()
		t = &Topic{
			requests: make(chan *Message, 1000),
			name:     topic,
		}
		btb.topics[topic] = t
		btb.Unlock()
	}

	return t
}

func (btb *BackToBack) Listen(listener net.Listener, worker func(net.Conn)) {
	log.Infof("listening at %s", listener.Addr())

	for {
		fd, err := listener.Accept()
		if err != nil {
			log.Warnf("accept error: %s", err.Error())
			break
		}
		if tc, ok := fd.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go worker(fd)
	}

	listener.Close()
}
func (btb *BackToBack) DumpStats() {
	producedCount := uint64(0)
	consumedCount := uint64(0)
	btb.RLock()
	for name, t := range btb.topics {
		producedCount += t.producedCount
		consumedCount += t.consumedCount
		log.Infof("%s: producedCount: %d consumedCount: %d, consumer timeout: %d, queue len: %d", name, t.producedCount, t.consumedCount, t.consumedTimeoutCount, len(t.requests))
	}
	btb.RUnlock()
	log.Infof("total: producedCount: %d consumedCount: %d, pollCount: %d", producedCount, consumedCount, btb.pollCount)
}

func (btb *BackToBack) ClientWorkerProducer(c net.Conn) {
	// must have buffer of 1
	// in case there is *no* consumer we should timeout in the client
	// otherwise we will get unordered messages
	// meaning that new request comes, and we consumer from the reply channel
	// but it could contain old request that timed out
	// also makes it easier to close the channel in case of error
	replyChannel := make(chan *Message, 1)
	topics := map[string]*Topic{}
	last_message_id := uint32(0)

	pid := atomic.AddUint32(&btb.producerID, 1)

	producer := &Producer{replyChannel: replyChannel}

	btb.Lock()
	btb.producers[pid] = producer
	btb.Unlock()

	waitForMessageWithTimeout := func(topic string, id uint64, timeout <-chan time.Time) *Message {
		for {
			select {
			case reply := <-replyChannel:
				//				log.Printf("received %d want %d", reply.Uuid, id)
				if reply.Uuid == id {
					return reply
				}
			case <-timeout:
				return &Message{Uuid: id, Topic: topic, Type: MessageType_ERROR, Data: []byte("consumer timed out")}
			}
		}
	}

	waitForMessage := func(id uint64) *Message {
		for {
			reply := <-replyChannel
			if reply.Uuid == id {
				return reply
			}
		}
	}

	for {
		last_message_id++
		message, err := ReceiveRequest(c)
		if err != nil {
			//			log.Warnf("err receive: %s", err.Error())
			break
		}

		topic, ok := topics[message.Topic]
		if !ok {
			topic = btb.getTopic(message.Topic)
			topics[message.Topic] = topic
		}

		message.Uuid = uint64(pid)<<uint64(32) | uint64(last_message_id)

		var reply *Message
		topic.requests <- message
		if message.TimeoutAfterMs == 0 {
			reply = waitForMessage(message.Uuid)
		} else {
			message.TimeoutAtNanosecond = uint64(time.Now().UnixNano()) + (uint64(message.TimeoutAfterMs) * uint64(1000000))
			after := time.After(time.Duration(message.TimeoutAfterMs) * time.Millisecond)
			reply = waitForMessageWithTimeout(message.Topic, message.Uuid, after)

			if reply.Type == MessageType_ERROR {
				atomic.AddUint64(&topic.consumedTimeoutCount, 1)
			}
		}

		err = Send(c, Marshallable(reply))
		if err != nil {
			log.Warnf("err reply: %s", err.Error())
			break
		}

		atomic.AddUint64(&topic.producedCount, 1)
	}

	c.Close()

	btb.Lock()

	delete(btb.producers, pid)

	for {
		select {
		case <-replyChannel:
		default:
		}
	}

	btb.Unlock()

	producer.Lock()

	close(replyChannel)

	producer.Unlock()

}

func (btb *BackToBack) ClientWorkerConsumer(c net.Conn) {
	empty, _ := Pack(&Message{Type: MessageType_EMPTY})
	topics := map[string]*Topic{}

LOOP:
	for {
		poll, err := ReceivePoll(c)
		atomic.AddUint64(&btb.pollCount, 1)
		if err != nil {
			log.Printf("error waiting for poll: %s", err.Error())
			break LOOP
		}
	PICK:
		for _, t := range poll.Topic {
			topic, ok := topics[t]
			if !ok {
				topic = btb.getTopic(t)
				topics[t] = topic
			}

			select {
			case request := <-topic.requests:
				atomic.AddUint64(&topic.consumedCount, 1)

				if request.TimeoutAfterMs > 0 {
					now := uint64(time.Now().UnixNano())
					if now > request.TimeoutAtNanosecond {
						// assume the consumer already timed out, dont say anything
						continue PICK
					}
				}

				err := Send(c, Marshallable(request))
				var reply *Message
				hasError := false

				if err != nil {
					reply = &Message{Uuid: request.Uuid, Type: MessageType_ERROR, Data: []byte(err.Error()), Topic: t}
					hasError = true
				}

				if !hasError {
					reply, err = ReceiveRequest(c)
					if err != nil {
						reply = &Message{Uuid: request.Uuid, Type: MessageType_ERROR, Data: []byte(err.Error()), Topic: t}
						hasError = true
					}
				}
				if !hasError {
					if reply.Type != MessageType_REPLY {
						reply = &Message{Uuid: request.Uuid, Type: MessageType_ERROR, Data: []byte("broken consumer"), Topic: t}
						hasError = true
					}
				}

				// this is where it gets interesting
				// we received a message, but by this time the producer could be disconnected

				btb.RLock()
				producer, ok := btb.producers[uint32(request.Uuid>>32)]
				if !ok {
					btb.RUnlock()
					continue PICK
				}
				btb.RUnlock()

				producer.RLock()
				producer.replyChannel <- reply
				producer.RLock()
				if hasError {
					break LOOP
				}

				continue LOOP
			default:
			}
		}

		_, err = c.Write(empty)

		if err != nil {
			log.Warnf("failed to reply empty: %s", err.Error())
			break LOOP
		}
	}
	c.Close()
}
