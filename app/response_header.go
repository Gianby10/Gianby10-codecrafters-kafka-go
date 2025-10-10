package main

import (
	"encoding/binary"
	"io"
)

type ResponseHeaderV0 struct {
	CorrelationId int32
}

type ResponseHeaderV1 struct {
	CorrelationId int32
	TAG_BUFFER    []byte
}

func (rh *ResponseHeaderV0) Serialize(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, rh.CorrelationId)
}

func (rh *ResponseHeaderV0) Deserialize(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &rh.CorrelationId)
}

func (rh *ResponseHeaderV1) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, rh.CorrelationId); err != nil {
		return err
	}

	// TAG_BUFFER
	if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
		return err
	}
	return nil
}

func (rh *ResponseHeaderV1) Deserialize(r io.Reader) error {
	// TODO: LEGGERE TAG_BUFFER
	return binary.Read(r, binary.BigEndian, &rh.CorrelationId)
}
