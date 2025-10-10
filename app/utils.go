package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

func ReadCompactString(r io.Reader) (*string, error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}

	// Leggo la lunghezza della COMPACT_STRING ClientId come UVARINT con un byte reader
	strLen, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	switch strLen {
	case 0:
		return nil, nil
	case 1:
		empty := ""
		return &empty, nil
	default:

		strBuf := make([]byte, strLen-1) // Tolgo 1 byte per il terminatore
		if _, err := io.ReadFull(br.(io.Reader), strBuf); err != nil {
			return nil, err
		}
		s := string(strBuf)
		return &s, nil

	}
}

func WriteCompactString(w io.Writer, s *string) error {
	// s è una COMPACT_STRING quindi prima la lunghezza come UVARINT e poi la stringa
	// Una COMPACT_STRING può essere null, in quel caso la lunghezza è 0
	// Può essere vuota, in quel caso la lunghezza è 1 (1 byte per il terminatore)
	// Altrimenti la lunghezza è len(stringa)+1 (1 byte per il terminatore)
	if s == nil {
		strLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(strLen, uint64(0))
		if _, err := w.Write(strLen[:bytesRead]); err != nil {
			return err
		}
	} else if *s == "" {
		strLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(strLen, uint64(1))
		if _, err := w.Write(strLen[:bytesRead]); err != nil {
			return err
		}
	} else {
		strLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(strLen, uint64(len(*s)+1))
		if _, err := w.Write(strLen[:bytesRead]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(*s)); err != nil {
			return err
		}
	}
	return nil
}

func GetCorrelationIdFromHeader(header Header) int32 {
	switch h := header.(type) {
	case *ResponseHeaderV0:
		return h.CorrelationId
	case *RequestHeaderV2:
		return h.CorrelationId
	default:
		return -1
	}
}

func StringToPtr(s string) *string {
	return &s
}

func WriteInt32CompactArray(w io.Writer, values []int32) error {
	var arrayLenBytes [binary.MaxVarintLen64]byte
	// Scrivo lunghezza del COMPACT_ARRAY come UVARINT

	arrayLen := len(values) + 1 // +1 per encoding COMPACT_ARRAY
	bytesRead := binary.PutUvarint(arrayLenBytes[:], uint64(arrayLen))
	if _, err := w.Write(arrayLenBytes[:bytesRead]); err != nil {
		return fmt.Errorf("write compact array length: %w", err)
	}

	// Scrivo ora ogni int32 nell'array
	for _, value := range values {
		if err := binary.Write(w, binary.BigEndian, value); err != nil {
			return fmt.Errorf("write int32 element: %w", err)
		}
	}
	return nil
}

func ReadInt32CompactArray(r io.Reader) ([]int32, error) {
	arrayLen, err := ReadUVarInt(r)
	if err != nil {
		return nil, err
	}

	if arrayLen == 0 {
		return nil, nil // 0 = array nil
	}

	if arrayLen == 1 {
		return []int32{}, nil // 1 = array vuoto
	}

	arrayLen-- // Rimuoviamo 1 per il byte di terminazione // https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions

	array := make([]int32, arrayLen)
	for i := uint64(0); i < arrayLen; i++ {
		if err := binary.Read(r, binary.BigEndian, &array[i]); err != nil {
			return nil, err
		}
	}

	return array, nil
}

func ReadVarInt(r io.Reader) (int64, error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return binary.ReadVarint(br)
}

func ReadUVarInt(r io.Reader) (uint64, error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return binary.ReadUvarint(br)
}

func ReadUUID(r io.Reader) ([16]byte, error) {
	var uuid [16]byte
	if _, err := io.ReadFull(r, uuid[:]); err != nil {
		return [16]byte{}, err
	}
	return uuid, nil
}

func ReadGenericCompactArray[T any](r io.Reader) ([]T, error) {
	arrayLen, err := ReadUVarInt(r)
	if err != nil {
		return nil, err
	}

	if arrayLen == 0 {
		return nil, nil // 0 = array nil
	}

	if arrayLen == 1 {
		return []T{}, nil // 1 = array vuoto
	}

	arrayLen-- // Rimuoviamo 1 per il byte di terminazione // https://kafka.apache.org/protocol.html#The_Messages_DescribeTopicPartitions

	array := make([]T, arrayLen)
	for i := range array {
		if err := binary.Read(r, binary.BigEndian, &array[i]); err != nil {
			return nil, err
		}
	}

	return array, nil
}
