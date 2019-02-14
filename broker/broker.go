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
	requests             chan MessageAndReply
	name                 string
	producedCount        uint64
	consumedCount        uint64
	consumedTimeoutCount uint64

	sync.RWMutex
}
type MessageAndReply struct {
	message *Message
	reply   chan *Message
}

type BackToBack struct {
	topics     map[string]*Topic
	pollCount  uint64
	uuid       uint64
	producerID uint32
	sync.RWMutex
}

func NewBackToBack() *BackToBack {
	return &BackToBack{
		topics: map[string]*Topic{},
	}
}

func (btb *BackToBack) getTopic(topic string) *Topic {
	btb.RLock()
	t, ok := btb.topics[topic]
	btb.RUnlock()
	if !ok {
		btb.Lock()
		t = &Topic{
			requests: make(chan MessageAndReply, 1000),
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
	replyChannel := make(chan *Message, 1000)
	topics := map[string]*Topic{}
	last_message_id := uint32(0)

	pid := atomic.AddUint32(&btb.producerID, 1)

	waitForMessageWithTimeout := func(topic string, id uint64, timeout <-chan time.Time) *Message {
		for {
			select {
			case reply := <-replyChannel:
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

		topic.requests <- MessageAndReply{message, replyChannel}

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

	// dont close it, should be collected
	// close(replyChannel)
}

func (btb *BackToBack) ClientWorkerConsumer(c net.Conn) {
	empty, _ := Pack(&Message{Type: MessageType_EMPTY})
	topics := map[string]*Topic{}

POLL:
	for {
		poll, err := ReceivePoll(c)
		atomic.AddUint64(&btb.pollCount, 1)
		if err != nil {
			log.Printf("error waiting for poll: %s", err.Error())
			break POLL
		}
	AGAIN:
		for {
			for _, t := range poll.Topic {
				topic, ok := topics[t]
				if !ok {
					topic = btb.getTopic(t)
					topics[t] = topic
				}

				select {
				case r := <-topic.requests:
					request := r.message
					atomic.AddUint64(&topic.consumedCount, 1)

					hasError := false
					var reply *Message

					if request.TimeoutAfterMs > 0 {
						now := uint64(time.Now().UnixNano())
						if now > request.TimeoutAtNanosecond {
							continue AGAIN
						}
					}

					err := Send(c, Marshallable(request))
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

					// we received a message, but by this time the producer could be disconnected
					r.reply <- reply

					if hasError {
						break POLL
					}

					continue POLL
				default:
				}
			}
			break AGAIN
		}

		_, err = c.Write(empty)

		if err != nil {
			log.Warnf("failed to reply empty: %s", err.Error())
			break POLL
		}
	}
	c.Close()
}
