package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	log "github.com/sirupsen/logrus"
	"math/rand"
)

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
		if reply.Type == MessageType_ERROR {
			continue
		}

		return reply, nil
	}
}
