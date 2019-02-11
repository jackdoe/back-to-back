package util

import (
	"encoding/binary"
	. "github.com/jackdoe/back-to-back/spec"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"time"
)

// XXX NO COPY PASTA
type Marshallable interface {
	MarshalTo(dAtA []byte) (int, error)
	Size() int
	String() string
}

func Pack(message Marshallable) ([]byte, error) {
	size := message.Size()
	data := make([]byte, 4+size)
	_, err := message.MarshalTo(data[4:])
	if err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(data[0:], uint32(size))
	return data, nil
}

func Send(c net.Conn, message Marshallable) error {
	data, err := Pack(message)
	//log.Printf("sending %s", message.String())
	if err != nil {
		return err
	}
	_, err = c.Write(data)
	return err
}

func Receive(c net.Conn) ([]byte, error) {
	hdr := make([]byte, 4)
	_, err := io.ReadFull(c, hdr)
	if err != nil {
		return nil, err
	}

	dataLen := binary.LittleEndian.Uint32(hdr)
	data := make([]byte, dataLen)

	_, err = io.ReadFull(c, data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func ReceiveRequest(c net.Conn) (*Message, error) {
	data, err := Receive(c)
	if err != nil {
		return nil, err
	}

	message := &Message{}
	err = message.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	//log.Printf("received message %s", message)
	return message, nil
}

func ReceivePoll(c net.Conn) (*Poll, error) {
	data, err := Receive(c)
	if err != nil {
		return nil, err
	}

	message := &Poll{}
	err = message.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return message, nil
}

func Connect(addr string) net.Conn {
	d := net.Dialer{Timeout: 1 * time.Second}

	for {
		conn, err := d.Dial("tcp", addr)
		if err != nil {
			log.Warn(err)
			time.Sleep(1 * time.Second)
			continue
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		return conn
	}
}
