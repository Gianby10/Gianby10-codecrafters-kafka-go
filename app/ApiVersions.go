package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	TAG_BUFFER []byte
}

type ApiVersionsResponseV4 struct {
	ErrorCode      int16
	ApiKeys        []ApiVersion
	ThrottleTimeMs int32
	TAG_BUFFER     []byte
}

type ApiVersionsRequestV4 struct {
	ClientId              *string // Client_software_name forse? COMPACT_STRING
	ClientSoftwareVersion *string // COMPACT_STRING
	TAG_BUFFER            []byte
}

func (api *ApiVersionsResponseV4) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, api.ErrorCode); err != nil {
		return err
	}

	// Scrivo e mando la lunghezza dell'array di ApiKeys come UVARINT
	apiKeysArrayLen := len(api.ApiKeys) + 1
	apiKeysArrayLenBytes := make([]byte, binary.MaxVarintLen64)
	bytesRead := binary.PutUvarint(apiKeysArrayLenBytes, uint64(apiKeysArrayLen))
	if _, err := w.Write(apiKeysArrayLenBytes[:bytesRead]); err != nil {
		return err
	}

	fmt.Printf("ApiKeys array len: %d", apiKeysArrayLenBytes[:bytesRead])
	// Scrivo ora ogni ApiKey nell'array ApiKeys
	for _, apiKey := range api.ApiKeys {
		if err := binary.Write(w, binary.BigEndian, apiKey.ApiKey); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, apiKey.MinVersion); err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, apiKey.MaxVersion); err != nil {
			return err
		}
		// TAG_BUFFER
		if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
			return err
		}
	}
	if err := binary.Write(w, binary.BigEndian, int32(api.ThrottleTimeMs)); err != nil {
		return err
	}

	/// TAG_BUFFER
	if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
		return err
	}

	return nil
}

func (api *ApiVersionsResponseV4) Deserialize(r io.Reader) error {
	// TODO
	return binary.Read(r, binary.BigEndian, &api.ErrorCode)
}

func (api *ApiVersionsRequestV4) Serialize(w io.Writer) error {

	writeCompactString(w, api.ClientId)

	writeCompactString(w, api.ClientSoftwareVersion)

	// Scrivo un byte vuoto di TAG_BUFFER
	if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
		return err
	}

	return nil
}

func (api *ApiVersionsRequestV4) Deserialize(r io.Reader) error {
	// Per leggere un UVARINT ho bisogno di un io.ByteReader
	// Se r non lo implementa, lo wrappo in un bufio.Reader che lo implementa
	// (e che fa buffering, quindi è più efficiente)

	br := bufio.NewReader(r)

	// Leggo la lunghezza della COMPACT_STRING ClientId come UVARINT con un byte reader
	clientId, err := readCompactString(br)
	if err != nil {
		return err
	}
	api.ClientId = clientId

	// Leggo la lunghezza della COMPACT_STRING ClientSoftwareVersion come UVARINT con un byte reader
	clientSwVer, err := readCompactString(br)
	if err != nil {
		return err
	}
	api.ClientSoftwareVersion = clientSwVer

	// Leggo un byte di TAG_BUFFER
	var tagBuffer byte
	if err := binary.Read(br, binary.BigEndian, &tagBuffer); err != nil {
		return err
	}
	return nil
}

func NewApiVersionsResponse(requestHeader *RequestHeaderV2) *KafkaMessage {
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
