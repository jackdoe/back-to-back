package client

import (
	"errors"
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"time"
)

type broker struct {
	c    net.Conn
	addr string
}

func newBroker(addr string) *broker {
	return &broker{
		addr: addr,
		c:    Connect(addr),
	}
}

func (b *broker) produceIO(request *Message) (*Message, error) {
	request.Type = MessageType_REQUEST
	if request.TimeoutMs > 0 {
		deadline := time.Now().Add(time.Duration(request.TimeoutMs) * time.Millisecond)
		b.c.SetDeadline(deadline)
	}

	err := Send(b.c, Marshallable(request))
	if err != nil {
		return nil, err
	}
	m, err := ReceiveRequest(b.c)
	if err != nil {
		return nil, err
	}

	if m.Type == MessageType_ERROR {
		return nil, errors.New(string(m.Data))
	}

	if request.TimeoutMs > 0 {
		b.c.SetDeadline(time.Time{})
	}

	return m, nil
}

func (b *broker) reconnect() {
	b.c.Close()
	b.c = Connect(b.addr)
}

type Producer struct {
	brokers         chan *broker
	reconnectPlease chan *broker
}

func NewProducer(addrs []string) *Producer {
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})

	log.Infof("connecting to %v", addrs)

	p := &Producer{
		brokers:         make(chan *broker, len(addrs)),
		reconnectPlease: make(chan *broker, len(addrs)),
	}

	for _, b := range addrs {
		p.brokers <- newBroker(b)
	}

	go p.reconnector()

	return p
}

func (p *Producer) reconnector() {
	for {
		b := <-p.reconnectPlease
		log.Infof("reconnecting %s", b.addr)
		go func(b *broker) {
			b.reconnect()
			p.brokers <- b
		}(b)
	}
}

func (p *Producer) Request(request *Message) (*Message, error) {
PICK:
	for {
		b := <-p.brokers
		reply, err := b.produceIO(request)
		if err != nil {
			p.reconnectPlease <- b
			continue PICK
		}

		p.brokers <- b
		return reply, nil
	}
}
