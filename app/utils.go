package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

func readCompactString(r io.Reader) (*string, error) {
	fmt.Printf("reading COMPACT_STRING...\n")
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
		fmt.Printf("Reading compact_string with len = %d\n", cIdLen)
		clientIdBuf := make([]byte, cIdLen-1) // Tolgo 1 byte per il terminatore
		if _, err := io.ReadFull(br.(io.Reader), clientIdBuf); err != nil {
			return nil, err
		}
		s := string(clientIdBuf)
		fmt.Printf("Read compact_string %s\n", s)
		return &s, nil

	}
}
