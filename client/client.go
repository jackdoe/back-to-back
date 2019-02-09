package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Client struct {
	connection     net.Conn
	topic          string
	random         *rand.Rand
	addr           string
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
	onExit         []func()
	sync.Mutex
}

func NewClient(addr string, topic string) *Client {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s) // initialize local pseudorandom generator

	// XXX config timeouts
	c := &Client{
		connectTimeout: 5 * time.Second,
		readTimeout:    5 * time.Second,
		writeTimeout:   5 * time.Second,
		topic:          topic,
		random:         r,
		addr:           addr,
	}

	return c
}

func (c *Client) connect() net.Conn {
	for {
		conn, err := c.dial()
		if err != nil {
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}
		conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		conn.SetReadDeadline(time.Now().Add(c.readTimeout))

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

		return conn
	}
}

func (c *Client) Close() {
	c.connection.Close()
}

func (c *Client) dial() (net.Conn, error) {
	d := net.Dialer{Timeout: c.connectTimeout}
	return d.Dial("tcp", c.addr)
}
