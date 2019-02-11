package client

import (
	. "github.com/jackdoe/back-to-back/spec"
	. "github.com/jackdoe/back-to-back/util"
	//log "github.com/sirupsen/logrus"
	//"time"
	"net"
)

func ProduceIO(c net.Conn, request *Message) (*Message, error) {
	request.Type = MessageType_REQUEST
	err := Send(c, Marshallable(request))
	if err != nil {
		return nil, err
	}
	m, err := ReceiveRequest(c)
	if err != nil {
		return nil, err
	}
	return m, nil
}
