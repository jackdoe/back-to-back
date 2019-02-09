package broker

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Topic struct {
	channel  chan *Message
	requests map[uint64]chan *Message
	sync.Mutex
}

type BackToBack struct {
	topics   map[string]*Topic
	uuid     uint64
	listener net.Listener
	sync.RWMutex
}

func NewBackToBack(listener net.Listener) *BackToBack {
	return &BackToBack{
		topics:   map[string]*Topic{},
		uuid:     0, //uint64(time.Now().UnixNano()),
		listener: listener,
	}
}

func (btb *BackToBack) getTopic(topic string) *Topic {
	btb.RLock()
	t, ok := btb.topics[topic]
	btb.RUnlock()
	if !ok {
		btb.Lock()
		t = &Topic{
			channel:  make(chan *Message, 1),
			requests: map[uint64]chan *Message{},
		}
		btb.topics[topic] = t
		btb.Unlock()
	}

	return t
}

func (btb *BackToBack) Listen() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	log.Infof("listening at %s", btb.listener.Addr())

	go func() {
		<-sigs
		btb.listener.Close()
	}()

	for {
		fd, err := btb.listener.Accept()
		if err != nil {
			log.Warnf("accept error: %s", err.Error())
			break
		}

		go btb.clientWorker(fd)
	}

	btb.listener.Close()
}

func (btb *BackToBack) processMessage(localReplyChannel chan *Message, c net.Conn, message *Message) error {
	topic := btb.getTopic(message.Topic)
	if message.Type == MessageType_REQUEST {
		message.RequestID = atomic.AddUint64(&btb.uuid, 1)
		//log.Infof("requiest: %s", message.String())

		topic.Lock()
		topic.requests[message.RequestID] = localReplyChannel
		topic.Unlock()

		topic.channel <- message

		var reply *Message

		select {
		case reply = <-localReplyChannel:
		case <-time.After(time.Duration(message.TimeoutMs) * time.Millisecond):
			reply = &Message{
				Type:      MessageType_TIMEOUT,
				RequestID: message.RequestID,
				Topic:     message.Topic,
			}
			log.Infof("timeout reply: %s", reply.String())
		}

		//log.Infof("reply: %s", reply.String())

		topic.Lock()
		delete(topic.requests, message.RequestID)
		topic.Unlock()

		return Send(c, reply)
	} else if message.Type == MessageType_REPLY {
		//log.Infof("reply: %s", message.String())
		topic.Lock()

		to, ok := topic.requests[message.RequestID]
		topic.Unlock()

		if !ok {
			log.Warnf("ignoring reply for missing message id %d, topic: %s", message.RequestID, message.Topic)
		} else {
			to <- message
		}
	} else if message.Type == MessageType_PING {
		pong := &Message{
			Type:  MessageType_PONG,
			Topic: message.Topic,
		}
		return Send(c, pong)
	} else {
		log.Infof("POLL: %s", message.String())
	}

	request := <-topic.channel

	err := Send(c, request)
	if err != nil {
		return err
	}

	return nil
}

func (btb *BackToBack) clientWorker(c net.Conn) {
	localReplyChanel := make(chan *Message, 1)

	// you are either producer or consumer
	log.Infof("worker for %s started", c.RemoteAddr())
	for {
		message, err := Receive(c)
		if err != nil {
			log.Warnf("err receive: %s", err.Error())
			c.Close()
			break
		}

		err = btb.processMessage(localReplyChanel, c, message)
		if err != nil {
			log.Warnf("err process: %s", err.Error())
			c.Close()
			break
		}
	}

	log.Warnf("disconnecting %s", c.RemoteAddr())
	close(localReplyChanel)

	// FIXME delete the client from everywhere
	btb.RLock()
	for _, topic := range btb.topics {
		topic.Lock()
		for id, ch := range topic.requests {
			if ch == localReplyChanel {
				log.Warnf("dropping %d on the floor", id)
				delete(topic.requests, id)
			}
		}
		topic.Unlock()

	}
	btb.RUnlock()

}
