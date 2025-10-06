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

	if err := km.Body.Serialize(&buf); err != nil {
		return nil, err
	}

	b := buf.Bytes()
	messageSize := int32(len(b) - 4) // Escludo i primi 4 byte temporanei di message size
	binary.BigEndian.PutUint32(b[0:4], uint32(messageSize))
	return b, nil
}

func (km *KafkaMessage) Deserialize(r io.Reader, header Header, body Body) error {
	if err := binary.Read(r, binary.BigEndian, &km.MessageSize); err != nil {
		return err
	}

	if err := header.Deserialize(r); err != nil {
		return err
	}
	km.Header = header

	if err := body.Deserialize(r); err != nil {
		return err
	}
	km.Body = body

	return nil
}

func ReadKafkaRequestMessage(r io.Reader) (*KafkaMessage, error) {
	var msgSize int32
	if err := binary.Read(r, binary.BigEndian, &msgSize); err != nil {
		return nil, err
	}

	// Leggo header per sapere la APIKEY e quindi il body
	header := &RequestHeaderV2{}
	if err := header.Deserialize(r); err != nil {
		return nil, err
	}

	var body Body
	switch header.ApiKey {
	case 18: // ApiVersion
		body = &ApiVersionsRequestV4{}
	case 75: // DescribeTopicsPartitions
		body = &DescribeTopicsPartitionsRequestV0{}
	default:
		return nil, fmt.Errorf("unsupported ApiKey: %d", header.ApiKey)
	}

	if err := body.Deserialize(r); err != nil {
		return nil, err
	}

	return &KafkaMessage{
		MessageSize: msgSize,
		Header:      header,
		Body:        body,
	}, nil
}

func WriteKafkaResponseMessage(w io.Writer, msg *KafkaMessage) error {
	responseBytes, err := msg.Serialize()
	if err != nil {
		return fmt.Errorf("error serializing kafka response message: %s", err.Error())
	}
	_, err = w.Write(responseBytes)
	if err != nil {
		return fmt.Errorf("error writing kafka response message: %s", err.Error())
	}

	log.Printf("Sent response (size=%d) CorrelationId=%d", len(responseBytes), msg.Header.(*ResponseHeaderV0).CorrelationId)
	return nil
}

func makeApiVersionsResponse(requestHeader *RequestHeaderV2) *KafkaMessage {
	var apiVersionsBody *ApiVersionsResponseV4
	correlationId := getCorrelationIdFromHeader(requestHeader)
	if requestHeader.ApiVersion < 0 || requestHeader.ApiVersion > 4 {
		apiVersionsBody = &ApiVersionsResponseV4{
			ErrorCode:      35, // UNSUPPORTED_VERSION
			ThrottleTimeMs: 0,
		}
		log.Printf("Unsupported ApiVersion: %d", requestHeader.ApiVersion)
	} else {
		apiVersionsBody = &ApiVersionsResponseV4{ErrorCode: 0, ApiKeys: []ApiVersion{{ApiKey: 18, MinVersion: 0, MaxVersion: 4}, {ApiKey: 75, MinVersion: 0, MaxVersion: 0}}, ThrottleTimeMs: 0}
	}

	return &KafkaMessage{
		Header: &ResponseHeaderV0{
			CorrelationId: correlationId,
		},
		Body: apiVersionsBody,
	}
}

func handleConnection(conn net.Conn) {

	defer conn.Close()

	for {

		kafkaReqMsg, err := ReadKafkaRequestMessage(conn)
		if err != nil {
			if err == io.EOF {
				log.Print("Connection closed by client.")
				return
			}
			log.Printf("error reading kafka message: %s", err.Error())
			return
		}

		var responseMsg *KafkaMessage
		var apiKey int16 = kafkaReqMsg.Header.(*RequestHeaderV2).ApiKey
		switch apiKey {
		case 18: // ApiVersion
			responseMsg = makeApiVersionsResponse(kafkaReqMsg.Header.(*RequestHeaderV2))
		case 75: // DescribeTopicsPartitions
			// TODO
			responseMsg = &KafkaMessage{
				Header: &ResponseHeaderV1{
					CorrelationId: getCorrelationIdFromHeader(kafkaReqMsg.Header.(*RequestHeaderV2)),
				},
				Body: &DescribeTopicsPartitionsResponseV0{
					Topics: []DescribeTopicsPartitionsResponseTopic{{
						ErrorCode:                 3,
						TopicName:                 StringToPtr(*kafkaReqMsg.Body.(*DescribeTopicsPartitionsRequestV0).Topics[0].TopicName),
						TopicAuthorizedOperations: 3576, // Bitmap da sistemare TODO
					}},
					NextCursor: 0xff, // -1
				},
			}
		default:
			log.Printf("Unsupported ApiKey: %d", apiKey)
			return
		}
		WriteKafkaResponseMessage(conn, responseMsg)
	}

}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}

}
