package broker

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
)

type MessageAndOrigin struct {
	message      *Message
	replyChannel chan *Message
}

type Topic struct {
	waiting       chan net.Conn
	requests      chan MessageAndOrigin
	name          string
	producedCount uint64
	consumedCount uint64
	sync.RWMutex
}

type BackToBack struct {
	topics map[string]*Topic
	uuid   uint64
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
func (btb *BackToBack) DumpStats() {
	producedCount := uint64(0)
	consumedCount := uint64(0)
	btb.RLock()
	for name, t := range btb.topics {
		producedCount += t.producedCount
		consumedCount += t.consumedCount
		log.Infof("%s: producedCount: %d consumedCount: %d", name, t.producedCount, t.consumedCount)
	}
	btb.RUnlock()
	log.Infof("total: producedCount: %d consumedCount: %d", producedCount, consumedCount)
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
			//			log.Warnf("err receive: %s", err.Error())
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

		atomic.AddUint64(&topic.producedCount, 1)
	}

	c.Close()
	close(replyChannel)
}

func (btb *BackToBack) ClientWorkerConsumer(c net.Conn) {
	empty, _ := Pack(&Message{Type: MessageType_EMPTY})
	topics := map[string]*Topic{}
	// dont do any deadlines here, they will be handled by the producer
	// just let nature take its course, otherwise everything gets 10 times more
	// complicated
LOOP:
	for {
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
				atomic.AddUint64(&topic.consumedCount, 1)

				remote := request.replyChannel

				err := Send(c, Marshallable(request.message))
				if err != nil {
					remote <- &Message{Type: MessageType_ERROR, Data: []byte(err.Error()), Topic: t}
					break LOOP
				}

				reply, err := ReceiveRequest(c)
				if err != nil {
					remote <- &Message{Type: MessageType_ERROR, Data: []byte(err.Error()), Topic: t}
					break LOOP
				}

				if reply.Type != MessageType_REPLY {
					remote <- &Message{Type: MessageType_ERROR, Data: []byte("broken consumer"), Topic: t}
					break LOOP
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
