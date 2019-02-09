package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

func NewProducer(n int, addr string, topic string) *Client {
	c := NewClient(addr, topic)
	c.connections = make([]net.Conn, n)
	for i := 0; i < n; i++ {
		c.reconnect(i)
	}
	return c
}

func (c *Client) reconnect(idx int) net.Conn {
	for {
		conn, err := c.dial()
		if err != nil {
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}

		err = Send(conn, &Message{Topic: c.topic, Type: MessageType_PING})
		if err != nil {
			log.Warn(err)
			conn.Close()
			continue
		}

		m, err := Receive(conn)
		if err != nil {
			log.Warnf("error waiting for pong", err)
			conn.Close()
			continue
		}

		if m.Type != MessageType_PONG {
			log.Warnf("received %s, expected PONG", m.String())
			conn.Close()
			continue
		}

		c.connections[idx] = conn
		log.Infof("reconnected")
		return conn
	}
}

func (c *Client) ProduceIO(request *Message) *Message {
	// just try forever
	request.Topic = c.topic
	request.Type = MessageType_REQUEST
	idx := c.random.Intn(len(c.connections))
	for {
		conn := c.connections[idx]
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		err := Send(conn, request)

		if err != nil {
			log.Warnf("error sending, trying again: %s", err.Error())

			c.Lock()
			conn = c.reconnect(idx)
			c.Unlock()

			continue
		}
		conn.SetReadDeadline(time.Now().Add(c.readTimeout))
		m, err := Receive(conn)
		if err != nil {
			log.Warnf("error receiving, trying again: %s", err.Error())
			continue
		}
		return m
	}
}
