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

func writeCompactString(w io.Writer, s *string) error {
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
