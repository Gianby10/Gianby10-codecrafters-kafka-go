package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	case 1:
		// Fetch

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

	return nil
}
