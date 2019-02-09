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

func (btb *BackToBack) processMessage(localReplyChannel chan *Message, c net.Conn, message *Message) error {
	topic := btb.getTopic(message.Topic)

	//Exaple request/reply:
	//	producer A -> sends message { topic: "abc". data "xyz" }
	//
	//                          broker X receives the message of type REQUEST
	//                                   adds message ID
	//                                   creates map from RequestID to replyTo channel
	//                                   pushes to the topic channel
	//                                   waits for reply on the replyTo channel
	//
	//      consumer A -> sends message {topic: "abc", POLL }
	//                    waits for requests
	//                    when request comes in
	//                    writes reply copying the RequestID to the reply
	//
	//                          broker X receives a message of type REPLY
	//                                   looks up if has a channel for message.requestID
	//                                   writes the replyTo the channel

	if message.Type == MessageType_REQUEST {
		message.RequestID = atomic.AddUint64(&btb.uuid, 1)

		topic.Lock()
		topic.requests[message.RequestID] = localReplyChannel
		topic.Unlock()

		// anyone listening on the topic can pick it up
		topic.channel <- message

		var reply *Message

		/*
			the issue here is that we might get a
			timeout, and then for some reason
			close the connection, and so the
			localReplyChannel will be closed, the
			other side can try to write to closed
			channel so in some time if a reply
			comes,
		*/
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
			// XXX if we write to closed channel we will panic
			// so we just rlock the whole btb and on async close we close the channel with write lock
			// this has to be rewritten using proper channel patterns
			// but since the whole project is proof of concept, not sure it is worth it

			// having this rlock allows us to make sure nobody is closing any channels while we are trying to write to them
			// we also drain the channel on disconnect, so we could always write to it, it will just go in the void
			btb.RLock()
			to <- message
			btb.RUnlock()
		}
	} else if message.Type == MessageType_PING {
		pong := &Message{
			Type:  MessageType_PONG,
			Topic: message.Topic,
		}
		return Send(c, pong)
	} else if message.Type == MessageType_POLL {
		// after the poll we can wait for another
		log.Infof("POLL: %s", message.String())
	} else {
		log.Infof("UNKNOWN: %s", message.String())
	}
	// by default assume it is a consumer
	// producers only use REQUEST message
	// maybe its better to split producers and consumers in the broker code
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

	// should be rare
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

	btb.Lock()

	// drain
L:
	for {
		select {
		case <-localReplyChanel:
		default:
			break L
		}
	}

	close(localReplyChanel)
	btb.Unlock()
}
