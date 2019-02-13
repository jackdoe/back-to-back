package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	//log "github.com/sirupsen/logrus"
	"errors"
	"net"
	"time"
)

func ProduceIO(c net.Conn, request *Message) (*Message, error) {
	request.Type = MessageType_REQUEST
	if request.TimeoutMs > 0 {
		deadline := time.Now().Add(time.Duration(request.TimeoutMs) * time.Millisecond)
		c.SetDeadline(deadline)
	}
	err := Send(c, Marshallable(request))
	if err != nil {
		return nil, err
	}
	m, err := ReceiveRequest(c)
	if err != nil {
		return nil, err
	}
	if m.Type == MessageType_ERROR {
		return nil, errors.New(string(m.Data))
	}
	return m, nil
}
