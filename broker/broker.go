package broker

import (
	"fmt"
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	"github.com/rcrowley/go-metrics"
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
	requestsTimer           metrics.Timer
	pollCount               metrics.Meter
	producedBytes           metrics.Counter
	consumedBytes           metrics.Counter
	consumedTimeoutCount    metrics.Meter
	consumedConnectionError metrics.Meter
	consumedRetries         metrics.Meter

	sync.RWMutex
}

type MessageAndReply struct {
	message *Message
	reply   chan *Message
}

type BackToBack struct {
	topics              map[string]*Topic
	activeConsumerCount int64
	activeProducerCount int64
	uuid                uint64
	producerID          uint32
	registry            metrics.Registry
	sync.RWMutex
}

func NewBackToBack(registry metrics.Registry) *BackToBack {
	if registry == nil {
		registry = metrics.DefaultRegistry
	}
	btb := &BackToBack{
		topics:   map[string]*Topic{},
		registry: registry,
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			btb.updateCounters()
		}
	}()
	return btb
}

func (btb *BackToBack) GetRegistry() metrics.Registry {
	return btb.registry
}

func (btb *BackToBack) PrintStatsEvery(n int, l metrics.Logger) {
	metrics.LogScaled(metrics.DefaultRegistry, time.Duration(n)*time.Second, time.Millisecond, l)
}

func (btb *BackToBack) getTopic(topic string) *Topic {
	btb.RLock()
	t, ok := btb.topics[topic]
	btb.RUnlock()
	if !ok {
		btb.Lock()
		t = &Topic{
			requests:                make(chan MessageAndReply, 10000),
			name:                    topic,
			requestsTimer:           metrics.GetOrRegisterTimer(fmt.Sprintf("topic.%s.requests", topic), btb.registry),
			pollCount:               metrics.GetOrRegisterMeter(fmt.Sprintf("topic.%s.pollCount", topic), btb.registry),
			producedBytes:           metrics.GetOrRegisterCounter(fmt.Sprintf("topic.%s.producedBytes", topic), btb.registry),
			consumedBytes:           metrics.GetOrRegisterCounter(fmt.Sprintf("topic.%s.consumedBytes", topic), btb.registry),
			consumedTimeoutCount:    metrics.GetOrRegisterMeter(fmt.Sprintf("topic.%s.consumedTimeoutCount", topic), btb.registry),
			consumedConnectionError: metrics.GetOrRegisterMeter(fmt.Sprintf("topic.%s.consumedConnectionError", topic), btb.registry),
			consumedRetries:         metrics.GetOrRegisterMeter(fmt.Sprintf("topic.%s.consumedRetries", topic), btb.registry),
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

func (btb *BackToBack) updateCounters() {
	btb.RLock()
	for name, t := range btb.topics {
		queueLen := metrics.GetOrRegisterGauge(fmt.Sprintf("topic.%s.queueLen", name), btb.registry)
		queueLen.Update(int64(len(t.requests)))

	}
	btb.RUnlock()

	consumersCount := metrics.GetOrRegisterGauge("connections.consumers", btb.registry)
	producersCount := metrics.GetOrRegisterGauge("connections.producers", btb.registry)
	producersCount.Update(btb.activeProducerCount)
	consumersCount.Update(btb.activeConsumerCount)
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

	// timers := map[string]map[string]metrics.Timer{}
	// prefix := strings.Replace(strings.Replace(c.RemoteAddr().String(), ".", "_", -1), ":", "_", -1)
	// getMetricKey := func(t string, name string) string {

	// 	key := fmt.Sprintf("perProducer.%s.%s.%s", prefix, t, name)
	// 	return key
	// }
	// getOrCreateTimer := func(kind string, name string) metrics.Timer {
	// 	k, ok := timers[kind]
	// 	if !ok {
	// 		k = map[string]metrics.Timer{}
	// 		timers[kind] = k
	// 	}
	// 	t, ok := k[name]
	// 	if !ok {
	// 		key := getMetricKey(kind, name)
	// 		t = metrics.GetOrRegisterTimer(key, btb.registry)
	// 		k[name] = t
	// 	}
	// 	return t
	// }

	for {
		last_message_id++
		message, err := ReceiveRequest(c)
		t0 := time.Now().UnixNano()
		if err != nil {
			//			log.Warnf("err receive: %s", err.Error())
			break
		}

		topic, ok := topics[message.Topic]
		if !ok {
			topic = btb.getTopic(message.Topic)
			topics[message.Topic] = topic
		}
		//		timerWaitForConsumer := getOrCreateTimer("waitForConsuer", message.Topic)
		//		timerWaitForProducer := getOrCreateTimer("waitForProducer", message.Topic)
		topic.producedBytes.Inc(int64(len(message.Data)))

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
				topic.consumedTimeoutCount.Mark(1)
			}
		}

		//		timerWaitForConsumer.Update(took)
		err = Send(c, Marshallable(reply))

		//		timerWaitForProducer.Update(took)
		if err != nil {
			log.Warnf("err reply: %s", err.Error())
			break
		}
		took := time.Nanosecond * time.Duration(time.Now().UnixNano()-t0)
		topic.requestsTimer.Update(took)

	}

	c.Close()
	atomic.AddInt64(&btb.activeProducerCount, -1)
	// dont close it, should be collected, otherwise we might end up writing
	// in closed channel (panic) from the consumer if it gets delayed and replies
	// to timed out message
	// close(replyChannel)
	// for kind, v := range timers {
	// 	for key, _ := range v {
	// 		btb.registry.Unregister(getMetricKey(kind, key))
	// 	}
	// }
}

func (btb *BackToBack) ClientWorkerConsumer(c net.Conn) {
	atomic.AddInt64(&btb.activeConsumerCount, 1)
	empty, _ := Pack(&Message{Type: MessageType_EMPTY})
	topics := map[string]*Topic{}

POLL:
	for {
		poll, err := ReceivePoll(c)
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
				topic.pollCount.Mark(1)
				select {
				case r := <-topic.requests:
					request := r.message

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
						topic.consumedConnectionError.Mark(1)
						now := uint64(time.Now().UnixNano())

						if request.RetryTTL > 0 && (request.TimeoutAtNanosecond == 0 || now < request.TimeoutAtNanosecond) {
							topic.consumedRetries.Mark(1)

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
