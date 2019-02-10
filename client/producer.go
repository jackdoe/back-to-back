package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"time"
)

func NewProducer(n int, addr string, topic string) *Client {
	c := NewClient(addr, topic)
	c.connection = c.connect(MessageType_I_AM_PRODUCER)
	return c
}

func (c *Client) ProduceIO(request *Message) *Message {
	request.Topic = c.topic
	request.Type = MessageType_REQUEST

	for {
		conn := c.connection
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		err := Send(conn, request)
		if err != nil {
			log.Warnf("error sending, trying again: %s", err.Error())

			c.Lock()
			c.connection = c.connect(MessageType_I_AM_PRODUCER)
			c.Unlock()

			continue
		}

		conn.SetReadDeadline(time.Now().Add(c.readTimeout))
		m, err := Receive(conn)
		if err != nil {
			log.Warnf("error receiving, trying again: %s", err.Error())
			continue
		}
		if m.Type == MessageType_TIMEOUT {
			log.Warnf("timeout, retrying")
			continue
		}
		return m
	}
}
