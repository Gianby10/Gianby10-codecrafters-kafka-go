package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

type KafkaMessage struct {
	MessageSize int32
	Header      Header
	Body        Body
}

type Header interface {
	Serialize(io.Writer) error
	Deserialize(io.Reader) error
}

type Body interface {
	Serialize(io.Writer) error
	Deserialize(io.Reader) error
}

type ResponseHeaderV0 struct {
	CorrelationId int32
}

type RequestHeaderV2 struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
	TAG_BUFFER    []byte
}

type ApiVersionsResponseV4 struct {
	ErrorCode int16
}

func (rh *ResponseHeaderV0) Serialize(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, rh.CorrelationId)
}

func (rh *ResponseHeaderV0) Deserialize(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &rh.CorrelationId)
}

func (rh *RequestHeaderV2) Deserialize(r io.Reader) error {
	if err := binary.Read(r, binary.BigEndian, &rh.ApiKey); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &rh.ApiVersion); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &rh.CorrelationId); err != nil {
		return err
	}

	var clientIdLen int16
	if err := binary.Read(r, binary.BigEndian, &clientIdLen); err != nil {
		return err
	}

	switch clientIdLen {
	case -1:
		rh.ClientId = nil
	case 0:
		empty := ""
		rh.ClientId = &empty
	default:
		clientIdBuf := make([]byte, clientIdLen)
		if _, err := io.ReadFull(r, clientIdBuf); err != nil {
			return err
		}
		s := string(clientIdBuf)
		rh.ClientId = &s
	}

	return nil
}

func (rh *RequestHeaderV2) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, rh.ApiKey); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, rh.ApiVersion); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, rh.CorrelationId); err != nil {
		return err
	}

	if rh.ClientId == nil {
		if err := binary.Write(w, binary.BigEndian, int16(-1)); err != nil {
			return err
		}
	}

	if rh.ClientId != nil {
		if err := binary.Write(w, binary.BigEndian, int16(len(*rh.ClientId))); err != nil {
			return err
		}

		if _, err := w.Write([]byte(*rh.ClientId)); err != nil {
			return err
		}
	}
	return nil
}

func (api *ApiVersionsResponseV4) Serialize(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, api.ErrorCode)
}

func (api *ApiVersionsResponseV4) Deserialize(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &api.ErrorCode)
}

func (km *KafkaMessage) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	// Message size placeholder
	if err := binary.Write(&buf, binary.BigEndian, int32(0)); err != nil {
		return nil, err
	}

	if err := km.Header.Serialize(&buf); err != nil {
		return nil, err
	}

	// Serialize body

	b := buf.Bytes()
	messageSize := int32(len(b) - 4) // Escludo i primi 4 byte temporanei di message size
	binary.BigEndian.PutUint32(b[0:4], uint32(messageSize))
	return b, nil
}

func (km *KafkaMessage) Deserialize(r io.Reader, header Header) error {
	if err := binary.Read(r, binary.BigEndian, &km.MessageSize); err != nil {
		return err
	}

	if err := header.Deserialize(r); err != nil {
		return err
	}
	km.Header = header

	return nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reqMsg := &KafkaMessage{}
	err := reqMsg.Deserialize(conn, &RequestHeaderV2{})
	if err != nil {
		log.Printf("error deserializing kafka message: %s", err.Error())
	}

	log.Printf("Received message: %+v", reqMsg)

	responseHeader := &ResponseHeaderV0{
		CorrelationId: reqMsg.Header.(*RequestHeaderV2).CorrelationId,
	}

	responseMsg := &KafkaMessage{
		Header: responseHeader,
		Body:   &ApiVersionsResponseV4{ErrorCode: 35},
	}

	conn.Read(make([]byte, 1024)) // Leggo il resto del messaggio (che non mi interessa)

	responseBytes, err := responseMsg.Serialize()
	if err != nil {
		log.Printf("error serializing kafka response message: %s", err.Error())
	}
	conn.Write(responseBytes)
	if err != nil {
		log.Printf("error writing kafka response message: %s", err.Error())
	}

}

func main() {

	fmt.Println("Logs from your program will appear here!")

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

	handleConnection(conn)

}
