package broker

import (
	"fmt"
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type MessageAndOrigin struct {
	message *Message
	origin  net.Conn
}

type Topic struct {
	waiting  chan net.Conn
	requests chan MessageAndOrigin
	name     string
	sync.RWMutex
}

type BackToBack struct {
	topics        map[string]*Topic
	producedCount uint64
	consumedCount uint64
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
			requests: make(chan MessageAndOrigin, 10000),
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

		go worker(fd)
	}

	listener.Close()
}
func (btb *BackToBack) String() string {
	return fmt.Sprintf("producedCount: %d consumedCount: %d", btb.producedCount, btb.consumedCount)
}
func (btb *BackToBack) ClientWorkerProducer(c net.Conn) {
	for {
		message, err := ReceiveRequest(c)

		if err != nil {
			log.Warnf("err receive: %s", err.Error())
			c.Close()
			return
		}

		r := MessageAndOrigin{message, c}
		topic := btb.getTopic(message.Topic)

		topic.requests <- r
		atomic.AddUint64(&btb.producedCount, 1)
	}
}

func (btb *BackToBack) ClientWorkerConsumer(c net.Conn) {
	empty, _ := Pack(&Message{Type: MessageType_EMPTY})
	topics := map[string]*Topic{}
LOOP:
	for {
		poll, err := ReceivePoll(c)
		if err != nil {
			log.Printf("error waiting for poll: %s", err.Error())
			break LOOP
		}

		//		log.Printf("received poll: %s", poll)
		// XXX: SHUFFLE
		for _, t := range poll.Topic {
			topic, ok := topics[t]
			if !ok {
				topic = btb.getTopic(t)
				topics[t] = topic
			}

			select {
			case request := <-topic.requests:
				atomic.AddUint64(&btb.consumedCount, 1)
				deadline := time.Now().Add(time.Duration(request.message.TimeoutMs) * time.Millisecond)
				//				c.SetDeadline(deadline)
				err := Send(c, Marshallable(request.message))

				if err != nil {
					log.Printf("error sending: %s deadline: %s timeout ms: %d", err.Error(), deadline, request.message.TimeoutMs)
					break LOOP
				}

				reply, err := ReceiveRequest(c)

				if err != nil {
					log.Printf("error waiting for reply: %s", err.Error())
					break LOOP
				}

				remote := request.origin
				//				remote.SetWriteDeadline(deadline)
				//				_, err = remote.Write(reply)
				err = Send(remote, Marshallable(reply))
				if err != nil {
					remote.Close()
					log.Warnf("failed to reply: %s", err.Error())
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
