package main

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"slices"
)

type DescribeTopicsPartitionsRequestV0 struct {
	Topics                 []DescribeTopicsPartitionsRequestTopic
	ResponsePartitionLimit int32
	Cursor                 byte
	TAG_BUFFER             byte // TODO: byte -> []byte
}
type DescribeTopicsPartitionsRequestTopic struct {
	TopicName  *string
	TAG_BUFFER byte // TODO: byte -> []byte
}

func (api *DescribeTopicsPartitionsRequestV0) Serialize(w io.Writer) error {

	// Scrivo la lunghezza del COMPACT_ARRAY TopicsArray come UVARINT
	topicsArrayLen := len(api.Topics) + 1 // +1 per encoding COMPACT_ARRAY
	topicsArrayLenBytes := make([]byte, binary.MaxVarintLen64)
	bytesRead := binary.PutUvarint(topicsArrayLenBytes, uint64(topicsArrayLen))
	if _, err := w.Write(topicsArrayLenBytes[:bytesRead]); err != nil {
		return err
	}

	// Scrivo ora ogni ApiKey nell'array ApiKeys
	for _, topic := range api.Topics {
		// Scrivo la COMPACT_STRING TopicName (con tanto di byte di lunghezza)

		WriteCompactString(w, topic.TopicName)

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
		api.Topics = []DescribeTopicsPartitionsRequestTopic{}
	}

	// Altrimenti leggo gli elementi dell'array
	topicsArray := make([]DescribeTopicsPartitionsRequestTopic, arrayLen-1) // Tolgo 1 per il byte di terminazione (COMPACT_STRING)
	for i := uint64(0); i < arrayLen-1; i++ {
		topicName, err := ReadCompactString(br.(io.Reader))
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
	// if err := binary.Read(br.(io.Reader), binary.BigEndian, byte(0)); err != nil {
	// 	return err
	// }
	return nil
}

type DescribeTopicsPartitionsResponseV0 struct {
	ThrottleTimeMs int32
	Topics         []DescribeTopicsPartitionsResponseTopic
	NextCursor     byte
	TAG_BUFFER     []byte
}
type Partition struct {
	ErrorCode              int16
	ParitionIndex          int32
	LeaderId               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	IsrNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownELR           []int32
	OfflineReplicas        []int32
	TaggedFields           byte // TODO: byte -> []byte
}

type DescribeTopicsPartitionsResponseTopic struct {
	ErrorCode                 int16
	TopicName                 *string
	TopicId                   [16]byte
	IsInternal                bool
	PartitionsArray           []Partition
	TopicAuthorizedOperations int32 // Bitfield
	TAG_BUFFER                []byte
}

func (api *DescribeTopicsPartitionsResponseV0) Serialize(w io.Writer) error {

	// Scrivo il campo ThrottleTimeMs (int32)
	if err := binary.Write(w, binary.BigEndian, api.ThrottleTimeMs); err != nil {
		return err
	}

	// Scrivo la lunghezza del COMPACT_ARRAY TopicsArray come UVARINT
	topicsArrayLen := len(api.Topics) + 1 // +1 per encoding COMPACT_ARRAY
	topicsArrayLenBytes := make([]byte, binary.MaxVarintLen64)
	bytesRead := binary.PutUvarint(topicsArrayLenBytes, uint64(topicsArrayLen))
	if _, err := w.Write(topicsArrayLenBytes[:bytesRead]); err != nil {
		return err
	}

	// Scrivo ora ogni ApiKey nell'array ApiKeys
	for _, topic := range api.Topics {

		// Scrivo il campo ErrorCode (int16)
		if err := binary.Write(w, binary.BigEndian, topic.ErrorCode); err != nil {
			return err
		}

		// Scrivo la COMPACT_STRING TopicName (con tanto di byte di lunghezza)
		WriteCompactString(w, topic.TopicName)

		// Scrivo il campo TopicId ([16]byte)
		if _, err := w.Write(topic.TopicId[:]); err != nil {
			return err
		}

		// Scrivo il campo IsInternal (bool come byte)
		var isInternalByte byte
		if topic.IsInternal {
			isInternalByte = 1
		} else {
			isInternalByte = 0
		}
		if err := binary.Write(w, binary.BigEndian, isInternalByte); err != nil {
			return err
		}

		// Scrivo la lunghezza del COMPACT_ARRAY PartitionsArray come UVARINT
		partitionsArrayLen := len(topic.PartitionsArray) + 1 // +1 per encoding COMPACT_ARRAY
		partitionsArrayLenBytes := make([]byte, binary.MaxVarintLen64)
		bytesRead := binary.PutUvarint(partitionsArrayLenBytes, uint64(partitionsArrayLen))
		if _, err := w.Write(partitionsArrayLenBytes[:bytesRead]); err != nil {
			return err
		}

		// Scrivo ora ogni Partition nell'array PartitionsArray
		for _, partition := range topic.PartitionsArray {
			// Scrivo il campo ErrorCode della Partition(int16)
			if err := binary.Write(w, binary.BigEndian, partition.ErrorCode); err != nil {
				return err
			}

			// Scrivo il campo PartitionIndex della Partition(int32)
			if err := binary.Write(w, binary.BigEndian, partition.ParitionIndex); err != nil {
				return err
			}

			// Scrivo il campo LeaderId della Partition(int32)
			if err := binary.Write(w, binary.BigEndian, partition.LeaderId); err != nil {
				return err
			}

			// Scrivo il campo LeaderEpoch della Partition(int32)
			if err := binary.Write(w, binary.BigEndian, partition.LeaderEpoch); err != nil {
				return err
			}

			// Scrivo la COMPACT_ARRAY ReplicaNodes (array di int32)
			if err := WriteInt32CompactArray(w, partition.ReplicaNodes); err != nil {
				return err
			}

			// Scrivo COMPACT_ARRAY IsrNodes come UVARINT
			if err := WriteInt32CompactArray(w, partition.IsrNodes); err != nil {
				return err
			}

			// Scrivo COMPACT_ARRAY EligibleLeaderReplicas come UVARINT
			if err := WriteInt32CompactArray(w, partition.EligibleLeaderReplicas); err != nil {
				return err
			}

			// Scrivo COMPACT_ARRAY LastKnownELR come UVARINT
			if err := WriteInt32CompactArray(w, partition.LastKnownELR); err != nil {
				return err
			}
			// Scrivo COMPACT_ARRAY OfflineReplicas come UVARINT
			if err := WriteInt32CompactArray(w, partition.OfflineReplicas); err != nil {
				return err
			}

			// TAG_BUFFER
			if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
				return err
			}
		}

		// Scrivo il campo TopicAuthorizedOperations (int32)
		if err := binary.Write(w, binary.BigEndian, topic.TopicAuthorizedOperations); err != nil {
			return err
		}

		// TAG_BUFFER
		if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
			return err
		}
	}

	// Scrivo il campo NextCursor (int32)
	if err := binary.Write(w, binary.BigEndian, api.NextCursor); err != nil {
		return err
	}

	// Scrivo un byte vuoto di TAG_BUFFER
	if err := binary.Write(w, binary.BigEndian, byte(0)); err != nil {
		return err
	}

	return nil
}

func (api *DescribeTopicsPartitionsResponseV0) Deserialize(r io.Reader) error {
	// TODO
	return nil
}

func NewDescribeTopicsPartitionsResponse(requestHeader *RequestHeaderV2, body *DescribeTopicsPartitionsRequestV0) *KafkaMessage {

	responseBody := &DescribeTopicsPartitionsResponseV0{}
	responseBody.NextCursor = 0xff // -1
	var responseTopics []DescribeTopicsPartitionsResponseTopic
	for _, topic := range body.Topics {
		// fmt.Printf("Inside req body topics loop: %+v", topic)
		if topic.TopicName == nil {
			log.Println("Request contains nil topic name")
			continue
		}

		uuid, ok := ClusterMetadataCache.TopicInfo[*topic.TopicName]
		if !ok {
			log.Printf("Unknown topic: %s", *topic.TopicName)
			responseTopics = append(responseTopics, DescribeTopicsPartitionsResponseTopic{
				ErrorCode:                 3, // UNKNOWN_TOPIC_OR_PARTITION
				TopicName:                 topic.TopicName,
				TopicAuthorizedOperations: 3576,
			})
			continue
		}

		partitions := ClusterMetadataCache.PartitionInfo[uuid]
		if partitions == nil {
			partitions = []Partition{}
		}
		responseTopics = append(responseTopics, DescribeTopicsPartitionsResponseTopic{
			ErrorCode:                 0,
			TopicName:                 topic.TopicName,
			TopicId:                   uuid,
			PartitionsArray:           partitions,
			TopicAuthorizedOperations: 3576,
		})
	}

	responseBody.Topics = responseTopics
	// I topics sono ordinati in ordine alfabetico
	slices.SortFunc(responseBody.Topics, func(a, b DescribeTopicsPartitionsResponseTopic) int {
		if a.TopicName == nil && b.TopicName == nil {
			return 0
		}
		if a.TopicName == nil {
			return -1
		}
		if b.TopicName == nil {
			return 1
		}
		return cmp.Compare(*a.TopicName, *b.TopicName)
	})
	return &KafkaMessage{
		Header: &ResponseHeaderV1{
			CorrelationId: GetCorrelationIdFromHeader(requestHeader),
		},
		Body: responseBody,
	}
}
