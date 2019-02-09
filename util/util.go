package util

import (
	"encoding/binary"
	. "github.com/jackdoe/back-to-back/spec"
	"io"
	"net"
)

func Send(c net.Conn, message *Message) error {
	size := message.Size()
	data := make([]byte, 4+size)
	_, err := message.MarshalTo(data[4:])
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(data[0:], uint32(size))
	_, err = c.Write(data)
	return err
}

func Receive(c net.Conn) (*Message, error) {
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

	message := &Message{}
	err = message.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return message, nil
}
