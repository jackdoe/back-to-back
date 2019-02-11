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
	message      *Message
	replyChannel chan *Message
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
	uuid          uint64
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
			requests: make(chan MessageAndOrigin, 1000),
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
func (btb *BackToBack) String() string {
	return fmt.Sprintf("producedCount: %d consumedCount: %d", btb.producedCount, btb.consumedCount)
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

	for {
		message, err := ReceiveRequest(c)
		if err != nil {
			log.Warnf("err receive: %s", err.Error())
			break
		}
		r := MessageAndOrigin{message, replyChannel}

		topic, ok := topics[message.Topic]
		if !ok {
			topic = btb.getTopic(message.Topic)
			topics[message.Topic] = topic
		}

		topic.requests <- r
		reply := <-replyChannel

		err = Send(c, Marshallable(reply))
		if err != nil {
			log.Warnf("err reply: %s", err.Error())
			break
		}

		atomic.AddUint64(&btb.producedCount, 1)
	}

	c.Close()
	log.Warnf("closing producer %s, done", c.RemoteAddr())
	close(replyChannel)
}

func (btb *BackToBack) ClientWorkerConsumer(c net.Conn) {
	empty, _ := Pack(&Message{Type: MessageType_EMPTY})
	topics := map[string]*Topic{}
	never := time.Time{}
LOOP:
	for {
		c.SetDeadline(never)
		poll, err := ReceivePoll(c)
		if err != nil {
			log.Printf("error waiting for poll: %s", err.Error())
			break LOOP
		}

		for _, t := range poll.Topic {
			topic, ok := topics[t]
			if !ok {
				topic = btb.getTopic(t)
				topics[t] = topic
			}

			select {
			case request := <-topic.requests:
				atomic.AddUint64(&btb.consumedCount, 1)

				remote := request.replyChannel
				deadline := time.Now().Add(time.Duration(request.message.TimeoutMs) * time.Millisecond)
				c.SetDeadline(deadline)

				err := Send(c, Marshallable(request.message))
				if err != nil {
					remote <- &Message{Type: MessageType_ERROR, Data: []byte(err.Error()), Topic: t}
					break LOOP
				}

				reply, err := ReceiveRequest(c)
				if err != nil {
					remote <- &Message{Type: MessageType_ERROR, Data: []byte(err.Error()), Topic: t}
					continue LOOP
				}

				remote <- reply
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
