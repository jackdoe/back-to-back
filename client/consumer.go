package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"time"
)

func NewConsumer(addr string, topic string) *Client {
	return NewClient(addr, topic)
}

func (c *Client) consumeConnection(cb func(*Message) *Message) {
	c.Lock()
	conn := c.connect(MessageType_I_AM_CONSUMER)
	c.Unlock()
	for {
		m, err := Receive(conn)
		if err != nil {
			log.Warnf("error on conn addr: %s, %s", c.addr, err)
			conn.Close()

			c.Lock()
			conn = c.connect(MessageType_I_AM_CONSUMER)
			c.Unlock()

			continue
		}

		reply := cb(m)

		reply.Topic = c.topic
		reply.Type = MessageType_REPLY
		reply.RequestID = m.RequestID
		reply.TimeoutMs = m.TimeoutMs

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		err = Send(conn, reply)

		if err != nil {
			log.Warnf("error replying %s", err)
		}
	}
}

func (c *Client) Consume(n int, cb func(*Message) *Message) {
	for i := 1; i < n; i++ {
		go c.consumeConnection(cb)
	}
	c.consumeConnection(cb)
}
