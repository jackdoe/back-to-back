package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"time"
)

type Client struct {
	topic       string
	connections []net.Conn
	random      *rand.Rand
	addr        string
	consumer    chan *Message
}

func NewClient(addr string, topic string, nconnections int) *Client {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s) // initialize local pseudorandom generator

	c := &Client{
		connections: []net.Conn{},
		topic:       topic,
		random:      r,
		addr:        addr,
	}

	for i := 0; i < nconnections; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		log.Warnf("connection %d to %s", i, conn.RemoteAddr())
		c.connections = append(c.connections, conn)
	}

	return c
}

func (c *Client) ProduceIO(request *Message) (*Message, error) {
	request.Topic = c.topic
	request.Type = MessageType_REQUEST

	conn := c.connections[c.random.Intn(len(c.connections))]
	err := Send(conn, request)
	if err != nil {
		return nil, err
	}
	return Receive(conn)
}

func (c *Client) consumeConnection(idx int, cb func(*Message) *Message) error {
	reconnect := func(conn net.Conn, err error) {
		log.Warnf("error on conn idx: %d, addr: %s, %s", idx, c.addr, err)
		conn.Close()
		// sleep 1 second between retries
		time.Sleep(1 * time.Second)
		conn, err = net.Dial("tcp", c.addr)
		if err != nil {
			log.Fatal(err)
		}

		c.connections[idx] = conn
	}

	log.Infof("sending POLL to connection %d", idx)
	err := Send(c.connections[idx], &Message{Topic: c.topic, Type: MessageType_POLL})
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn := c.connections[idx]
		m, err := Receive(conn)
		if err != nil {
			reconnect(conn, err)
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

func (c *Client) Consume(cb func(*Message) *Message) {
	for i := 1; i < len(c.connections); i++ {
		go c.consumeConnection(i, cb)
	}
	c.consumeConnection(0, cb)
}
