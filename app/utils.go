package main

import (
	"bufio"
	"encoding/binary"
	"io"
)

func readCompactString(r io.Reader) (*string, error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}

	// Leggo la lunghezza della COMPACT_STRING ClientId come UVARINT con un byte reader
	cIdLen, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}
	switch cIdLen {
	case 0:
		return nil, nil
	case 1:
		empty := ""
		return &empty, nil
	default:
		clientIdBuf := make([]byte, cIdLen-1) // Tolgo 1 byte per il terminatore
		if _, err := io.ReadFull(br.(io.Reader), clientIdBuf); err != nil {
			return nil, err
		}
		s := string(clientIdBuf)
		return &s, nil
	}
}
