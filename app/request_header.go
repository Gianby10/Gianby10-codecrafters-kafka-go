package main

import (
	"encoding/binary"
	"io"
)

type RequestHeaderV2 struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
	TAG_BUFFER    []byte
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

	// Leggo un byte di TAG_BUFFER
	var tagBuffer byte
	if err := binary.Read(r, binary.BigEndian, &tagBuffer); err != nil {
		return err
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

	// Scrivi un byte di TAG_BUFFER
	if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
		return err
	}
	return nil
}
