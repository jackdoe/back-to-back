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

type Topic struct {
	waiting chan net.Conn
	sync.RWMutex
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
		uuid:     uint64(time.Now().UnixNano()),
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
			waiting: make(chan net.Conn, 1000),
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
		//log.Infof("request %s", message)
		if err != nil {
			log.Warnf("err receive: %s", err.Error())
			c.Close()
			return
		}

		var reply *Message
	REMOTE:
		for {
			remote := <-topic.waiting
			remote.SetDeadline(time.Now().Add(time.Duration(message.TimeoutMs) * time.Millisecond))
			err = Send(remote, message)
			if err != nil {
				log.Printf("error sending: %s", err.Error())
				remote.Close()
				continue REMOTE
			}
			reply, err = Receive(remote)
			if err != nil {
				log.Printf("error waiting for reply: %s", err.Error())
				remote.Close()
				continue REMOTE
			}
			//log.Infof("reply %s", reply)
			topic.waiting <- remote
			break
		}

		c.SetDeadline(time.Now().Add(time.Duration(message.TimeoutMs) * time.Millisecond))
		err = Send(c, reply)
		if err != nil {
			log.Warnf("err process: %s", err.Error())
			c.Close()
			break
		}
	}
}

func (btb *BackToBack) clientWorkerConsumer(topic *Topic, c net.Conn) {
	topic.waiting <- c
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
