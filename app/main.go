package main

import (
	"bufio"
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

type ResponseHeaderV0 struct {
	CorrelationId int32
}

type RequestHeaderV2 struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
	TAG_BUFFER    []byte
}

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

func (rh *ResponseHeaderV0) Serialize(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, rh.CorrelationId)
}

func (rh *ResponseHeaderV0) Deserialize(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &rh.CorrelationId)
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

	// ClientID è una COMPACT_STRING quindi prima la lunghezza come UVARINT e poi la stringa
	// Una COMPACT_STRING può essere null, in quel caso la lunghezza è 0
	// Può essere vuota, in quel caso la lunghezza è 1 (1 byte per il terminatore)
	// Altrimenti la lunghezza è len(stringa)+1 (1 byte per il terminatore)
	if api.ClientId == nil {
		cIdLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(cIdLen, uint64(0))
		if _, err := w.Write(cIdLen[:bytesRead]); err != nil {
			return err
		}
	} else if *api.ClientId == "" {
		cIdLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(cIdLen, uint64(1))
		if _, err := w.Write(cIdLen[:bytesRead]); err != nil {
			return err
		}
	} else {
		cIdLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(cIdLen, uint64(len(*api.ClientId)+1))
		if _, err := w.Write(cIdLen[:bytesRead]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(*api.ClientId)); err != nil {
			return err
		}
	}

	// ClientSoftwareVersion è una COMPACT_STRING quindi prima la lunghezza come UVARINT e poi la stringa
	// Una COMPACT_STRING può essere null, in quel caso la lunghezza è 0
	// Può essere vuota, in quel caso la lunghezza è 1 (1 byte per il terminatore)
	// Altrimenti la lunghezza è len(stringa)+1 (1 byte per il terminatore)
	if api.ClientSoftwareVersion == nil {
		cSwVerLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(cSwVerLen, uint64(0))
		if _, err := w.Write(cSwVerLen[:bytesRead]); err != nil {
			return err
		}
	} else if *api.ClientSoftwareVersion == "" {
		cSwVerLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(cSwVerLen, uint64(1))
		if _, err := w.Write(cSwVerLen[:bytesRead]); err != nil {
			return err
		}
	} else {
		cSwVerLen := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(cSwVerLen, uint64(len(*api.ClientSoftwareVersion)+1))
		if _, err := w.Write(cSwVerLen[:bytesRead]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(*api.ClientSoftwareVersion)); err != nil {
			return err
		}
	}

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

func ReadKafkaMessage(r io.Reader) (*KafkaMessage, error) {
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

func makeApiVersionsResponse(correlationId int32, errorCode int16) *KafkaMessage {
	var apiVersionsBody *ApiVersionsResponseV4
	if errorCode == 0 {
		apiVersionsBody = &ApiVersionsResponseV4{ErrorCode: 0, ApiKeys: []ApiVersion{{ApiKey: 18, MinVersion: 0, MaxVersion: 4}, {ApiKey: 75, MinVersion: 0, MaxVersion: 0}}, ThrottleTimeMs: 0}

	} else {
		apiVersionsBody = &ApiVersionsResponseV4{
			ErrorCode:      errorCode,
			ThrottleTimeMs: 0,
		}
	}

	return &KafkaMessage{
		MessageSize: 0, // Impostata durante la serializzazione
		Header: &ResponseHeaderV0{
			CorrelationId: correlationId,
		},
		Body: apiVersionsBody,
	}
}

func handleConnection(conn net.Conn) {

	defer conn.Close()

	for {

		kafkaReqMsg, err := ReadKafkaMessage(conn)
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
			if kafkaReqMsg.Header.(*RequestHeaderV2).ApiVersion > 4 {
				log.Printf("Unsupported ApiVersion: %d", kafkaReqMsg.Header.(*RequestHeaderV2).ApiVersion)
				responseMsg = makeApiVersionsResponse(kafkaReqMsg.Header.(*RequestHeaderV2).CorrelationId, 35) // 35 = UnsupportedVersion
			} else {
				responseMsg = makeApiVersionsResponse(kafkaReqMsg.Header.(*RequestHeaderV2).CorrelationId, 0)
			}
		case 75: // DescribeTopicsPartitions
			// TODO
			log.Printf("Received DescribeTopicsPartitions request, but not implemented yet. Bytes: %+v", kafkaReqMsg)
		default:
			log.Printf("Unsupported ApiKey: %d", apiKey)
			return
		}

		responseBytes, err := responseMsg.Serialize()
		if err != nil {
			log.Printf("error serializing kafka response message: %s", err.Error())
		}
		conn.Write(responseBytes)
		if err != nil {
			log.Printf("error writing kafka response message: %s", err.Error())
		}
	}

}

func main() {

	fmt.Println("Logs from your program will appear here!")

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
