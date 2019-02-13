package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	log "github.com/sirupsen/logrus"
	//"net"
)

type Consumer struct {
	reconnectPlease chan *broker
	brokers         []*broker
	sem             chan bool
	dispatch        map[string]func(*Message) *Message
}

func NewConsumer(addrs []string, dispatch map[string]func(*Message) *Message) *Consumer {
	log.Infof("connecting to %v", addrs)

	c := &Consumer{
		brokers:         []*broker{},
		reconnectPlease: make(chan *broker, len(addrs)),
		dispatch:        dispatch,
		sem:             make(chan bool, len(addrs)),
	}

	for _, b := range addrs {
		c.sem <- true
		broker := newBroker(b)
		c.brokers = append(c.brokers, broker)
		go c.consume(broker)
	}

	go c.reconnector()

	return c
}

func (c *Consumer) consume(b *broker) {
	err := b.consume(c.sem, c.dispatch) // has to exit in order to be reconnected
	if err != nil {
		log.Info("error consuming[%s]: %s", b.addr, err.Error())
		c.reconnectPlease <- b
	}

}
func (c *Consumer) reconnector() {
	for {
		b := <-c.reconnectPlease
		log.Infof("reconnecting %s", b.addr)
		go func(b *broker) {
			b.reconnect()
			go c.consume(b)
		}(b)
	}
}

func Consume(n int, addr []string, dispatch map[string]func(*Message) *Message) {
	for i := 0; i < n; i++ {
		go NewConsumer(addr, dispatch)
	}
}
