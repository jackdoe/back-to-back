package broker

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Topic struct {
	waiting                 chan net.Conn
	requests                chan MessageAndReply
	name                    string
	producedCount           uint64
	consumedCount           uint64
	consumedTimeoutCount    uint64
	consumedConnectionError uint64
	consumedRetries         uint64

	sync.RWMutex
}
type MessageAndReply struct {
	message *Message
	reply   chan *Message
}

type BackToBack struct {
	topics              map[string]*Topic
	pollCount           uint64
	activeConsumerCount int64
	activeProducerCount int64
	uuid                uint64
	producerID          uint32
	sync.RWMutex
}

func NewBackToBack() *BackToBack {
	return &BackToBack{
		topics: map[string]*Topic{},
	}
}

func (btb *BackToBack) getTopic(topic string) *Topic {
	btb.RLock()
	t, ok := btb.topics[topic]
	btb.RUnlock()
	if !ok {
		btb.Lock()
		t = &Topic{
			requests: make(chan MessageAndReply, 10000),
			name:     topic,
		}
		btb.topics[topic] = t
		btb.Unlock()
	}

	return t
}

func (btb *BackToBack) Listen(listener net.Listener, worker func(net.Conn)) {
	log.Infof("listening at %s", listener.Addr())

	for {
		fd, err := listener.Accept()
		if err != nil {
			log.Warnf("accept error: %s", err.Error())
			break
		}

		if tc, ok := fd.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}

		go worker(fd)
	}

	listener.Close()
}
func (btb *BackToBack) DumpStats() {
	producedCount := uint64(0)
	consumedCount := uint64(0)
	btb.RLock()
	for name, t := range btb.topics {
		producedCount += t.producedCount
		consumedCount += t.consumedCount
		log.Infof("%s: p/c: %d/%d, consumer [ timeout/err/retries: %d/%d/%d ], queue len: %d",
			name,
			t.producedCount,
			t.consumedCount,
			t.consumedTimeoutCount,
			t.consumedConnectionError,
			t.consumedRetries,
			len(t.requests))
	}
	btb.RUnlock()

	log.Infof("total: p/c: %d/%d, poll: %d, consumer conn: %d, producer conn: %d", producedCount, consumedCount, btb.pollCount, btb.activeConsumerCount, btb.activeProducerCount)
}

func makeError(m *Message, errorType MessageType, e string) *Message {
	return &Message{Uuid: m.Uuid, Topic: m.Topic, Type: errorType, Data: []byte(e)}
}

func waitForMessageWithTimeout(r MessageAndReply, mar chan MessageAndReply, replyChannel chan *Message, timeout <-chan time.Time) *Message {
	select {
	case mar <- r:
	default:
		return makeError(r.message, MessageType_ERROR_BROKER_FULL, "full")
	}

	for {
		select {
		case reply := <-replyChannel:
			if reply.Uuid == r.message.Uuid {
				return reply
			}
		case <-timeout:
			return makeError(r.message, MessageType_ERROR_CONSUMER_TIMEOUT, "consumer timed out")
		}
	}
}

func waitForMessage(replyChannel chan *Message, id uint64) *Message {
	for {
		reply := <-replyChannel
		if reply.Uuid == id {
			return reply
		}
	}
}

func (btb *BackToBack) ClientWorkerProducer(c net.Conn) {
	atomic.AddInt64(&btb.activeProducerCount, 1)
	replyChannel := make(chan *Message, 10000)
	topics := map[string]*Topic{}
	last_message_id := uint32(0)
	pid := atomic.AddUint32(&btb.producerID, 1)

	for {
		last_message_id++
		message, err := ReceiveRequest(c)
		if err != nil {
			//			log.Warnf("err receive: %s", err.Error())
			break
		}

		topic, ok := topics[message.Topic]
		if !ok {
			topic = btb.getTopic(message.Topic)
			topics[message.Topic] = topic
		}

		message.Uuid = uint64(pid)<<uint64(32) | uint64(last_message_id)

		var reply *Message
		request := MessageAndReply{message, replyChannel}
		if message.TimeoutAfterMs == 0 {
			topic.requests <- request
			reply = waitForMessage(replyChannel, message.Uuid)
		} else {
			message.TimeoutAtNanosecond = uint64(time.Now().UnixNano()) + (uint64(message.TimeoutAfterMs) * uint64(1000000))

			timer := time.NewTimer(time.Duration(message.TimeoutAfterMs) * time.Millisecond)

			reply = waitForMessageWithTimeout(request, topic.requests, replyChannel, timer.C)

			timer.Stop()

			if reply.Type == MessageType_ERROR {
				atomic.AddUint64(&topic.consumedTimeoutCount, 1)
			}
		}

		err = Send(c, Marshallable(reply))
		if err != nil {
			log.Warnf("err reply: %s", err.Error())
			break
		}

		atomic.AddUint64(&topic.producedCount, 1)
	}

	c.Close()
	atomic.AddInt64(&btb.activeProducerCount, -1)
	// dont close it, should be collected, otherwise we might end up writing
	// in closed channel (panic) from the consumer if it gets delayed and replies
	// to timed out message
	// close(replyChannel)
}

func (btb *BackToBack) ClientWorkerConsumer(c net.Conn) {
	atomic.AddInt64(&btb.activeConsumerCount, 1)
	empty, _ := Pack(&Message{Type: MessageType_EMPTY})
	topics := map[string]*Topic{}

POLL:
	for {
		poll, err := ReceivePoll(c)
		atomic.AddUint64(&btb.pollCount, 1)
		if err != nil {
			log.Printf("error waiting for poll: %s", err.Error())
			break POLL
		}
	AGAIN:
		for {
			// shuffle the topics so there is less bias towards specific order
			// e.g. if consumer allways asks "a,b" and "a" has more messages than b
			// it will never drain b
			rand.Shuffle(len(poll.Topic), func(i, j int) {
				poll.Topic[i], poll.Topic[j] = poll.Topic[j], poll.Topic[i]
			})
			for _, t := range poll.Topic {
				topic, ok := topics[t]
				if !ok {
					topic = btb.getTopic(t)
					topics[t] = topic
				}

				select {
				case r := <-topic.requests:
					request := r.message
					atomic.AddUint64(&topic.consumedCount, 1)

					hasError := false
					var reply *Message

					if request.TimeoutAfterMs > 0 {
						now := uint64(time.Now().UnixNano())
						if now > request.TimeoutAtNanosecond {
							continue AGAIN
						}
					}

					err := Send(c, Marshallable(request))
					if err != nil {
						reply = makeError(request, MessageType_ERROR_CONSUMER_SEND, err.Error())
						hasError = true
					}

					if !hasError {
						reply, err = ReceiveRequest(c)

						if err != nil {
							reply = makeError(request, MessageType_ERROR_CONSUMER_RECEIVE, err.Error())
							hasError = true
						}
					}

					if hasError {
						atomic.AddUint64(&topic.consumedConnectionError, 1)
						now := uint64(time.Now().UnixNano())

						if request.RetryTTL > 0 && (request.TimeoutAtNanosecond == 0 || now < request.TimeoutAtNanosecond) {
							atomic.AddUint64(&topic.consumedRetries, 1)

							request.RetryTTL--
							topic.requests <- r
						} else {
							reply.RetryTTL = request.RetryTTL
							r.reply <- reply
						}

						break POLL
					} else {
						reply.RetryTTL = request.RetryTTL
						r.reply <- reply
					}
					continue POLL
				default:
				}
			}
			break AGAIN
		}

		_, err = c.Write(empty)

		if err != nil {
			log.Warnf("failed to reply empty: %s", err.Error())
			break POLL
		}
	}
	c.Close()
	atomic.AddInt64(&btb.activeConsumerCount, -1)
}
