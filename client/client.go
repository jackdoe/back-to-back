package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	//. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Client struct {
	connections []net.Conn
	topic       string
	random      *rand.Rand
	addr        string
	consumer    chan *Message
	sync.Mutex
}

func NewClient(addr string, topic string) *Client {
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s) // initialize local pseudorandom generator

	c := &Client{
		connections: nil,
		topic:       topic,
		random:      r,
		addr:        addr,
	}

	return c
}

func (c *Client) CloseOnExit() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		c.Close()
		os.Exit(0)
	}()
}

func (c *Client) Close() {
	c.Lock()
	for _, conn := range c.connections {
		conn.Close()
	}
	c.Unlock()
}

func (c *Client) connect() net.Conn {
	for {
		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}
		return conn
	}
}
