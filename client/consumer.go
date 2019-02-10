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

func (c *Client) consumeConnection(cb func(*Message) *Message) {
	conn := c.connectAndPoll()

	for {
		m, err := Receive(conn)
		if err != nil {
			log.Warnf("error on conn addr: %s, %s", c.addr, err)
			conn.Close()
			conn = c.connectAndPoll()

			continue
		}

		reply := cb(m)
		// FIXME: in case of broadcast we should not reply
		if m.Type == MessageType_REQUEST {
			reply.Topic = c.topic
			reply.Type = MessageType_REPLY
			reply.RequestID = m.RequestID

			conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			err = Send(conn, reply)
			if err != nil {
				log.Warnf("error replying %s", err)
			}
		}
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		err = Send(conn, &Message{Topic: c.topic, Type: MessageType_POLL})
		if err != nil {
			log.Warn(err)
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	}
}

func (c *Client) connectAndPoll() net.Conn {
	c.Lock()
	defer c.Unlock()
	for {
		conn := c.connect()

		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		err := Send(conn, &Message{Topic: c.topic, Type: MessageType_POLL})
		if err != nil {
			log.Warn(err)
			conn.Close()
			continue
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return conn
	}
}

func (c *Client) Consume(n int, cb func(*Message) *Message) {
	for i := 1; i < n; i++ {
		go c.consumeConnection(cb)
	}
	c.consumeConnection(cb)
}
