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
	reconnect := func() net.Conn {
		for {
			c.Lock()
			conn := c.connect(MessageType_I_AM_CONSUMER)
			conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			conn.SetReadDeadline(time.Time{})
			c.Unlock()
			return conn
		}

	}
	conn := reconnect()

	for {
		m, err := Receive(conn)
		if err != nil {
			log.Warnf("error on conn addr: %s, %s", c.addr, err)
			conn.Close()

			conn = reconnect()
			continue
		}
		if m.Type == MessageType_POLL {
			err := Send(conn, &Message{Topic: c.topic, Type: MessageType_POLL})
			if err != nil {
				log.Warnf("failed to request poll: %s", err.Error())
				break
			}
		} else if m.Type == MessageType_EMPTY {

		} else if m.Type == MessageType_REQUEST {
			request := m
			reply := cb(request)

			reply.Topic = c.topic
			reply.Type = MessageType_REPLY

			err = Send(conn, reply)

			if err != nil {
				log.Warnf("error replying %s", err)
				conn.Close()
				conn = reconnect()
				continue
			}
		}
	}
}

func (c *Client) Consume(n int, cb func(*Message) *Message) {
	for i := 1; i < n; i++ {
		go c.consumeConnection(cb)
	}
	c.consumeConnection(cb)
}
