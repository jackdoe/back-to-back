package broker

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type MessageAndOrigin struct {
	message *Message
	origin  net.Conn
}

type Topic struct {
	waiting  chan net.Conn
	requests chan MessageAndOrigin
	ping     chan bool
	name     string
	sync.RWMutex
}

type BackToBack struct {
	topics   map[string]*Topic
	listener net.Listener
	sync.RWMutex
}

func NewBackToBack(listener net.Listener) *BackToBack {
	return &BackToBack{
		topics:   map[string]*Topic{},
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
			ping:     make(chan bool, 10), // XXX: poc
			requests: make(chan MessageAndOrigin, 10000),
			name:     topic,
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
		log.Info("closing..")
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

func (btb *BackToBack) clientWorkerProducer(topic *Topic, c net.Conn) {
	for {
		message, err := Receive(c)
		if err != nil {
			log.Warnf("err receive: %s", err.Error())
			c.Close()
			return
		}
		r := MessageAndOrigin{message, c}

		topic.requests <- r
		topic.ping <- true
	}
}

func (btb *BackToBack) clientWorkerConsumer(topic *Topic, c net.Conn) {

LOOP:
	for {
		select {
		case <-topic.ping:
			err := Send(c, &Message{Topic: topic.name, Type: MessageType_POLL})
			if err != nil {
				log.Warnf("failed to request poll: %s", err.Error())
				break LOOP
			}
			Receive(c)

			select {
			case request := <-topic.requests:
				deadline := time.Now().Add(time.Duration(request.message.TimeoutMs) * time.Millisecond)
				//				c.SetDeadline(deadline)
				err := Send(c, request.message)
				if err != nil {
					log.Printf("error sending: %s deadline: %s timeout ms: %d", err.Error(), deadline, request.message.TimeoutMs)
					break LOOP
				}
				reply, err := Receive(c)
				if err != nil {
					log.Printf("error waiting for reply: %s", err.Error())
					break LOOP
				}

				remote := request.origin
				//				remote.SetWriteDeadline(deadline)
				err = Send(remote, reply)
				if err != nil {
					remote.Close()
					log.Warnf("failed to reply: %s", err.Error())
				}
			default:
				request := &Message{Topic: topic.name, Type: MessageType_EMPTY}
				err := Send(c, request)
				if err != nil {
					log.Warnf("failed to reply empty: %s", err.Error())
					break LOOP
				}
			}
		}
	}
	c.Close()
}

func (btb *BackToBack) clientWorker(c net.Conn) {
	//log.Infof("worker for %s started", c.RemoteAddr())
	// make sure we can at least do PINGPONG
	message, err := Receive(c)
	topic := btb.getTopic(message.Topic)
	if err != nil {
		log.Warnf("err receive: %s", err.Error())
		c.Close()
		return
	}

	if message.Type == MessageType_I_AM_PRODUCER || message.Type == MessageType_I_AM_CONSUMER {
		pong := &Message{
			Type:  MessageType_PONG,
			Topic: message.Topic,
		}
		err = Send(c, pong)
		if err != nil {
			log.Warnf("err pong: %s", err.Error())
			c.Close()
			return
		}
		if message.Type == MessageType_I_AM_PRODUCER {
			btb.clientWorkerProducer(topic, c)
		} else {
			btb.clientWorkerConsumer(topic, c)
		}
	} else {
		log.Warnf("did not receive PING, %s", message)
		c.Close()
		return
	}
}
