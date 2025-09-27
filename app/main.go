package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

type KafkaResponseMessage struct {
	MessageSize int32
	Header      Header
	Body        Body
}

// Request Header V2
type Header struct {
	RequestApiKey     int16
	RequestApiVersion int16
	CorrelationId     int32
	ClientId          *string
	TAG_BUFFER        []byte
}
type Body struct {
}

func (krm *KafkaResponseMessage) Serialize() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, int32(0))
	binary.Write(&buf, binary.BigEndian, krm.Header.CorrelationId)

	b := buf.Bytes()
	size := int32(len(b)) - 4
	binary.BigEndian.PutUint32(b[0:4], uint32(size))
	return b
}

func NewKafkaResponseMessage(correlationId int32) KafkaResponseMessage {
	return KafkaResponseMessage{

		Header: Header{
			CorrelationId: correlationId,
		},
	}
}

func Read(r io.Reader) (*KafkaResponseMessage, error) {
	var messageSize int32
	if err := binary.Read(r, binary.BigEndian, &messageSize); err != nil {
		return nil, err
	}
	fmt.Printf("MessageSize read: %x", messageSize)

	reqApiKeyBuf := make([]byte, 2)
	_, err := io.ReadFull(r, reqApiKeyBuf)
	if err != nil {
		return nil, err
	}
	reqApiKey := int16(binary.BigEndian.Uint16(reqApiKeyBuf))

	reqApiVersionBuf := make([]byte, 2)
	_, err = io.ReadFull(r, reqApiVersionBuf)
	if err != nil {
		return nil, err
	}
	reqApiVersion := int16(binary.BigEndian.Uint16(reqApiVersionBuf))

	var correlationId int32
	if err := binary.Read(r, binary.BigEndian, &correlationId); err != nil {
		return nil, err
	}

	// Ora leggo la client_id, che è una NULLABLE_STRING, leggo prima size(INT16) bytes (dimensione stringa)

	clientIdLenBuf := make([]byte, 2) // INT16
	_, err = io.ReadFull(r, clientIdLenBuf)
	if err != nil {
		return nil, err
	}
	clientIdLen := int16(binary.BigEndian.Uint16(clientIdLenBuf))
	var clientId *string
	switch clientIdLen {
	case -1:
		clientId = nil
	case 0:
		empty := ""
		clientId = &empty
	default:
		clientIdBuf := make([]byte, clientIdLen) // INT16
		_, err = io.ReadFull(r, clientIdBuf)
		if err != nil {
			return nil, err
		}
		s := string(clientIdBuf)
		clientId = &s
	}

	return &KafkaResponseMessage{
		MessageSize: messageSize,
		Header: Header{
			RequestApiKey:     reqApiKey,
			RequestApiVersion: reqApiVersion,
			CorrelationId:     correlationId,
			ClientId:          clientId,
		},
	}, nil

}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	kReqHeader, err := Read(conn)
	if err != nil {
		fmt.Println("Error reading kafka request header v2 message: ", err.Error())
		os.Exit(1)
	}

	fmt.Printf("%+v", kReqHeader)

	// KReqHeader.Header.CorrelationId

	kResponse := NewKafkaResponseMessage(kReqHeader.Header.CorrelationId)

	_, err = conn.Write(kResponse.Serialize())
	if err != nil {
		fmt.Println("Error sending kafka response message: ", err.Error())
		os.Exit(1)
	}

}
