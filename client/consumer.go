package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	//"net"
	"time"
)

func ConsumeConnection(addr string, topics []string, cb func(*Message) *Message) {
	poll, _ := Pack(&Poll{Topic: topics})
CONNECT:
	for {
		conn := Connect(addr)
	POLL:
		for {
			<-time.After(100 * time.Millisecond)
			// consume while messages are available
			for {
				_, err := conn.Write(poll)
				if err != nil {
					log.Warnf("error on poll addr: %s, %s", addr, err)
					conn.Close()
					continue CONNECT
				}

				m, err := ReceiveRequest(conn)
				if err != nil {
					log.Warnf("error on conn addr: %s, %s", addr, err)
					conn.Close()
					continue CONNECT
				}

				if m.Type == MessageType_EMPTY {
					continue POLL
				}

				reply := cb(m)

				reply.Topic = m.Topic
				reply.Type = MessageType_REPLY

				err = Send(conn, Marshallable(reply))
				if err != nil {
					log.Warnf("error replying %s", err)
					conn.Close()
					continue CONNECT
				}
			}
		}
	}
}

func Consume(addr []string, topics []string, n int, cb func(*Message) *Message) {
	for _, a := range addr {
		for i := 1; i < n; i++ {
			go ConsumeConnection(a, topics, cb)
		}
		ConsumeConnection(a, topics, cb)
	}
}
