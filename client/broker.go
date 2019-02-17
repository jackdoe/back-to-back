package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	"net"
	"time"
)

type broker struct {
	c    net.Conn
	addr string
}

func newBroker(addr string) *broker {
	return &broker{
		addr: addr,
		c:    Connect(addr),
	}
}
func (b *broker) close() {
	b.c.Close()
}

func (b *broker) consume(sem chan bool, dispatch map[string]func(*Message) *Message) error {
	topics := []string{}
	for k, _ := range dispatch {
		topics = append(topics, k)
	}
	poll, _ := Pack(&Poll{Topic: topics})
	maxSleep := 100
	sleep := maxSleep

POLL:
	for {
		if sleep == maxSleep {
			<-time.After(time.Duration(sleep) * time.Millisecond)
		}

		// consume while messages are available
		for {
			<-sem
			_, err := b.c.Write(poll)
			if err != nil {
				sem <- true
				return err
			}

			m, err := ReceiveRequest(b.c)
			if err != nil {
				sem <- true
				return err
			}

			if m.Type == MessageType_EMPTY {
				if sleep < maxSleep {
					sleep++
				}
				sem <- true
				continue POLL
			}

			cb := dispatch[m.Topic] // should always be ok
			reply := cb(m)

			reply.Topic = m.Topic
			if reply.Type == MessageType_UNKNOWN {
				reply.Type = MessageType_REPLY
			}
			reply.Uuid = m.Uuid
			err = Send(b.c, Marshallable(reply))
			if err != nil {
				sem <- true
				return err
			}

			sem <- true
			sleep = 0
		}
	}

	return nil
}

func (b *broker) produceIO(request *Message) (*Message, error) {
	request.Type = MessageType_REQUEST
	if request.TimeoutAfterMs > 0 {
		deadline := time.Now().Add(time.Duration(request.TimeoutAfterMs+1000) * time.Millisecond)
		b.c.SetDeadline(deadline)
	}

	err := Send(b.c, Marshallable(request))
	if err != nil {
		return nil, err
	}
	m, err := ReceiveRequest(b.c)
	if err != nil {
		return nil, err
	}

	if request.TimeoutAfterMs > 0 {
		b.c.SetDeadline(time.Time{})
	}

	return m, nil
}

func (b *broker) reconnect() {
	b.c.Close()
	b.c = Connect(b.addr)
}
