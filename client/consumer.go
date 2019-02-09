package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

func NewConsumer(addr string, topic string) *Client {
	return NewClient(addr, topic)
}

func (c *Client) consumeConnection(idx int, cb func(*Message) *Message) error {
	conn := c.connectAndPoll(idx)

	for {
		m, err := Receive(conn)
		if err != nil {
			log.Warnf("error on conn addr: %s, %s", c.addr, err)
			conn.Close()
			conn = c.connectAndPoll(idx)

			continue
		}

		reply := cb(m)
		reply.Topic = c.topic
		reply.Type = MessageType_REPLY
		reply.RequestID = m.RequestID

		err = Send(conn, reply)
		if err != nil {
			log.Warnf("error replying %s", err)
		}
	}
}

func (c *Client) connectAndPoll(idx int) net.Conn {
	for {
		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}
		err = Send(conn, &Message{Topic: c.topic, Type: MessageType_POLL})
		if err != nil {
			log.Warn(err)
			conn.Close()
			continue
		}

		c.connections[idx] = conn

		return conn
	}
}

func (c *Client) Consume(n int, cb func(*Message) *Message) {
	c.connections = make([]net.Conn, n)
	for i := 1; i < n; i++ {
		go c.consumeConnection(i, cb)
	}
	c.consumeConnection(0, cb)
}
