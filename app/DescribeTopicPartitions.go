package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type DescribeTopicsPartitionsRequestV0 struct {
	Topics                 []Topic
	ResponsePartitionLimit int32
	Cursor                 byte
	TAG_BUFFER             []byte
}

type Topic struct {
	TopicName  *string
	TAG_BUFFER []byte
}

func (api *DescribeTopicsPartitionsRequestV0) Serialize(w io.Writer) error {

	// Scrivo la lunghezza del COMPACT_ARRAY TopicsArray come UVARINT
	topicsArrayLen := len(api.Topics) + 1 // +1 per encoding COMPACT_ARRAY
	topicsArrayLenBytes := make([]byte, binary.MaxVarintLen64)
	bytesRead := binary.PutUvarint(topicsArrayLenBytes, uint64(topicsArrayLen))
	if _, err := w.Write(topicsArrayLenBytes[:bytesRead]); err != nil {
		return err
	}

	fmt.Printf("Topics array len: %d", topicsArrayLenBytes[:bytesRead])
	// Scrivo ora ogni ApiKey nell'array ApiKeys
	for _, topic := range api.Topics {
		// Scrivo la COMPACT_STRING TopicName (con tanto di byte di lunghezza)

		writeCompactString(w, topic.TopicName)

		// TAG_BUFFER
		if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
			return err
		}
	}

	// Scrivo il campo ResponsePartitionLimit (int32)
	if err := binary.Write(w, binary.BigEndian, api.ResponsePartitionLimit); err != nil {
		return err
	}

	// Scrivo il campo Cursor (byte)
	if err := binary.Write(w, binary.BigEndian, api.Cursor); err != nil {
		return err
	}

	// Scrivo un byte vuoto di TAG_BUFFER
	if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
		return err
	}

	return nil
}

func (api *DescribeTopicsPartitionsRequestV0) Deserialize(r io.Reader) error {
	// Per leggere un UVARINT ho bisogno di un io.ByteReader
	// Se r non lo implementa, lo wrappo in un bufio.Reader che lo implementa
	// (e che fa buffering, quindi è più efficiente)

	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}

	// Leggo la lunghezza della COMPACT_STRING ClientId come UVARINT con un byte reader
	arrayLen, err := binary.ReadUvarint(br)
	if err != nil {
		return err
	}
	if arrayLen == 0 {
		return fmt.Errorf("invalid array length 0 for Topics")
	}
	// Se la lunghezza è 1, significa che l'array è vuoto (1 byte per il terminatore)
	if arrayLen == 1 {
		api.Topics = []Topic{}
	}

	// Altrimenti leggo gli elementi dell'array
	topicsArray := make([]Topic, arrayLen-1) // Tolgo 1 per il byte di terminazione (COMPACT_STRING)
	for i := uint64(0); i < arrayLen-1; i++ {
		topicName, err := readCompactString(br.(io.Reader))
		if err != nil {
			return err
		}
		topicsArray[i].TopicName = topicName
		// Leggo TAG_BUFFER Byte
		if err := binary.Read(br.(io.Reader), binary.BigEndian, &topicsArray[i].TAG_BUFFER); err != nil {
			return err
		}
	}
	api.Topics = topicsArray

	// Leggo il campo ResponsePartitionLimit (int32)
	if err := binary.Read(br.(io.Reader), binary.BigEndian, &api.ResponsePartitionLimit); err != nil {
		return err
	}

	// Leggo il campo Cursor (byte)
	if err := binary.Read(br.(io.Reader), binary.BigEndian, &api.Cursor); err != nil {
		return err
	}

	// Leggo TAG_BUFFER Byte
	if err := binary.Read(br.(io.Reader), binary.BigEndian, byte(0)); err != nil {
		return err
	}
	return nil
}
