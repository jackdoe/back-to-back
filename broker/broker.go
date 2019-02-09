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

type BackToBack struct {
	topics   map[string]chan *Message
	requests map[uint64]chan *Message
	uuid     uint64
	listener net.Listener
	sync.RWMutex
}

func NewBackToBack(listener net.Listener) *BackToBack {
	return &BackToBack{
		topics:   map[string]chan *Message{},
		requests: map[uint64]chan *Message{},
		uuid:     uint64(time.Now().UnixNano()),
		listener: listener,
	}
}

func (btb *BackToBack) getTopic(topic string) chan *Message {
	t, ok := btb.topics[topic]
	if !ok {
		t = make(chan *Message)
		btb.topics[topic] = t
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
	if message.Type == MessageType_REQUEST {
		message.RequestID = atomic.AddUint64(&btb.uuid, 1)
		//log.Infof("requiest: %s", message.String())

		btb.Lock()
		t := btb.getTopic(message.Topic)
		btb.requests[message.RequestID] = localReplyChannel
		btb.Unlock()

		t <- message

		var reply *Message

		select {
		case reply = <-localReplyChannel:
		case <-time.After(time.Duration(message.TimeoutMs) * time.Millisecond):
			reply = &Message{
				Type:      MessageType_TIMEOUT,
				RequestID: message.RequestID,
				Topic:     message.Topic,
			}
		}

		//log.Infof("reply: %s", reply.String())

		btb.Lock()
		delete(btb.requests, message.RequestID)
		btb.Unlock()

		return Send(c, reply)
	} else if message.Type == MessageType_REPLY {
		//log.Infof("reply: %s", message.String())
		btb.Lock()
		to, ok := btb.requests[message.RequestID]
		btb.Unlock()

		if !ok {
			log.Warnf("ignoring reply for missing message id %d, topic: %s", message.RequestID, message.Topic)
		} else {
			to <- message
		}
	} else {
		log.Infof("POLL: %s", message.String())
	}

	btb.Lock()
	t := btb.getTopic(message.Topic)
	btb.Unlock()

	request := <-t
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
			log.Warnf("err: %s", err.Error())
			c.Close()
			break
		}

		err = btb.processMessage(localReplyChanel, c, message)
		if err != nil {
			log.Warnf("err: %s", err.Error())
			c.Close()
			break
		}
	}
	log.Warnf("disconnecting %s", c.RemoteAddr())
	close(localReplyChanel)
	// FIXME delete the client from everywhere
	btb.Lock()
	for id, ch := range btb.requests {
		if ch == localReplyChanel {
			log.Warnf("dropping %d on the floor", id)
			delete(btb.requests, id)
		}

	}
	btb.Unlock()

}
