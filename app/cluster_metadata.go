package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

// https://binspec.org/kafka-cluster-metadata
type ClusterMetadataRecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	MagicByte            byte
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsLength        int32
	Records              []ClusterMetadataRecord
}

type ClusterMetadataRecord struct {
	Length            int64 // VARINT
	Attributes        byte
	TimestampDelta    int64 // VARINT
	OffsetDelta       int64 // VARINT
	KeyLength         int64 // VARINT
	Key               []byte
	ValueLength       int64 // VARINT
	Value             any
	HeadersArrayCount uint64 // UVARINT
}

type FeatureLevelRecordValue struct {
	Name         *string // COMPACT_STRING
	FeatureLevel int16
	TAG_BUFFER   byte // TODO: byte -> []byte
}

type TopicRecordValue struct {
	TopicName  *string
	TopicId    [16]byte
	TAG_BUFFER byte // TODO: byte -> []byte
}

type PartitionRecordValue struct {
	PartitionId     int32
	TopicUUID       [16]byte
	Replicas        []int32
	Isr             []int32
	RemovingReplics []int32
	AddingReplicas  []int32
	Leader          int32
	LeaderEpoch     int32
	PartitionEpoch  int32
	Directories     [][16]byte // COMPACT_ARRAY di UUIDs
	TAG_BUFFER      byte       // TODO: byte -> []byte
}

func ReadFeatureLevelValue(r io.Reader) (*FeatureLevelRecordValue, error) {
	// Inizializzo un featureLevelRecord vuoto
	featureLevelRecord := FeatureLevelRecordValue{}
	// Non Leggo FrameVersion, Type e Version che abbiamo già letto arrivati a questo punto
	// Leggo COMPACT_STRING Name
	name, err := ReadCompactString(r)
	if err != nil {
		return nil, err
	}
	featureLevelRecord.Name = name

	// Leggo FeatureLevel (int16)
	if err := binary.Read(r, binary.BigEndian, &featureLevelRecord.FeatureLevel); err != nil {
		return nil, err
	}

	// TAGGED_FIELDS
	if err := binary.Read(r, binary.BigEndian, &featureLevelRecord.TAG_BUFFER); err != nil {
		return nil, err
	}

	return &featureLevelRecord, nil
}

func ReadTopicRecordValue(r io.Reader) (*TopicRecordValue, error) {
	// Inizializzo un TopicrecordValue vuoto
	topicRecord := TopicRecordValue{}
	// Non Leggo FrameVersion, Type e Version che abbiamo già letto arrivati a questo punto
	// Leggo COMPACT_STRING TopicName
	topicName, err := ReadCompactString(r)
	if err != nil {
		return nil, err
	}
	topicRecord.TopicName = topicName

	// Leggo TopicID che è un array di 16 byte rappresentante un UUID
	topicId, err := ReadUUID(r)
	if err != nil {
		return nil, err
	}

	topicRecord.TopicId = topicId

	// TAGGED_FIELDS
	if err := binary.Read(r, binary.BigEndian, &topicRecord.TAG_BUFFER); err != nil {
		return nil, err
	}
	return &topicRecord, nil
}

func ReadPartitionRecordValue(r io.Reader) (*PartitionRecordValue, error) {
	// Inizializzo un partitionRecordValue vuoto
	partitionRecordValue := PartitionRecordValue{}
	// Non Leggo FrameVersion, Type e Version che abbiamo già letto arrivati a questo punto

	// Leggo PartitionId (int32)
	if err := binary.Read(r, binary.BigEndian, &partitionRecordValue.PartitionId); err != nil {
		return nil, err
	}

	// Leggo TopicID che è un array di 16 byte rappresentante un UUID
	topicId, err := ReadUUID(r)
	if err != nil {
		return nil, err
	}
	partitionRecordValue.TopicUUID = topicId

	// Leggo Replicas array
	replicaArray, err := ReadInt32CompactArray(r)
	if err != nil {
		return nil, err
	}
	partitionRecordValue.Replicas = replicaArray

	// Leggo ISR array
	isrArray, err := ReadInt32CompactArray(r)
	if err != nil {
		return nil, err
	}
	partitionRecordValue.Isr = isrArray

	// Leggo RemovingReplicas array
	removingReplicasArray, err := ReadInt32CompactArray(r)
	if err != nil {
		return nil, err
	}
	partitionRecordValue.RemovingReplics = removingReplicasArray

	// Leggo AddingReplicas array
	addingReplicasArray, err := ReadInt32CompactArray(r)
	if err != nil {
		return nil, err
	}
	partitionRecordValue.AddingReplicas = addingReplicasArray

	// Leggo Leader (int32) sarebbe il replica ID del leader
	if err := binary.Read(r, binary.BigEndian, &partitionRecordValue.Leader); err != nil {
		return nil, err
	}

	// Leggo LeaderEpoch (int32)
	if err := binary.Read(r, binary.BigEndian, &partitionRecordValue.LeaderEpoch); err != nil {
		return nil, err
	}

	// Leggo PartitionEpoch (int32)
	if err := binary.Read(r, binary.BigEndian, &partitionRecordValue.PartitionEpoch); err != nil {
		return nil, err
	}

	// // Leggo la lunghezza dell'array Directories ([]UUID)
	// directoriesArrayLen, err := ReadUVarInt(f)
	// if err != nil {
	//   return nil, err
	// }

	// Leggo l'array Directories ([]UUID)
	dirArray, err := ReadGenericCompactArray[[16]byte](r)
	if err != nil {
		return nil, err
	}

	partitionRecordValue.Directories = dirArray

	// TAGGED_FIELDS
	if err := binary.Read(r, binary.BigEndian, &partitionRecordValue.TAG_BUFFER); err != nil {
		return nil, err
	}

	return &partitionRecordValue, nil
}

func ReadClusterMetadataRecordValue(r io.Reader) (any, error) {
	var valueFrameVersion, valueType, valueVersion int8

	// Leggo FrameVersion (int8)
	if err := binary.Read(r, binary.BigEndian, &valueFrameVersion); err != nil {
		return nil, err
	}

	// Leggo Type (int8)
	if err := binary.Read(r, binary.BigEndian, &valueType); err != nil {
		return nil, err
	}

	// Leggo Version (int8)
	if err := binary.Read(r, binary.BigEndian, &valueVersion); err != nil {
		return nil, err
	}

	// Ora in base al campo type capisco di che tipo di record si tratta e di conseguenza lo leggo e deserializzo in maniera appropriata
	switch valueType {
	case 2:
		// TopicRecord
		return ReadTopicRecordValue(r)
	case 3:
		// PartitionRecord
		return ReadPartitionRecordValue(r)
	case 12:
		// FeatureLevelRecord
		return ReadFeatureLevelValue(r)
	default:
		return nil, fmt.Errorf("invalid record type: %d", valueType)
	}

}

func ReadClusterMetadataRecordBatch(r io.Reader) (*ClusterMetadataRecordBatch, error) {
	// Inizializzo ClusterMetadataRecordBatch vuota
	batch := ClusterMetadataRecordBatch{}
	// Leggo BaseOffset (int64)
	if err := binary.Read(r, binary.BigEndian, &batch.BaseOffset); err != nil {
		return nil, err
	}

	// Leggo BatchLength (int32) che mi dirà quanti byte leggere
	if err := binary.Read(r, binary.BigEndian, &batch.BatchLength); err != nil {
		return nil, err
	}

	// reader limitato al contenuto del batch

	// Leggo PartitionLeaderEpoch (int32)
	if err := binary.Read(r, binary.BigEndian, &batch.PartitionLeaderEpoch); err != nil {
		return nil, err
	}

	// Leggo MagicByte (byte)
	if err := binary.Read(r, binary.BigEndian, &batch.MagicByte); err != nil {
		return nil, err
	}

	// Leggo CRC (int32)
	if err := binary.Read(r, binary.BigEndian, &batch.CRC); err != nil {
		return nil, err
	}

	// Leggo Attributes (int16)
	if err := binary.Read(r, binary.BigEndian, &batch.Attributes); err != nil {
		return nil, err
	}

	// Leggo LastOffsetDelta (int32)
	if err := binary.Read(r, binary.BigEndian, &batch.LastOffsetDelta); err != nil {
		return nil, err
	}

	// Leggo BaseTimestamp (int64)
	if err := binary.Read(r, binary.BigEndian, &batch.BaseTimestamp); err != nil {
		return nil, err
	}

	// Leggo MaxTimestamp (int64)
	if err := binary.Read(r, binary.BigEndian, &batch.MaxTimestamp); err != nil {
		return nil, err
	}

	// Leggo ProducerId (int64)
	if err := binary.Read(r, binary.BigEndian, &batch.ProducerId); err != nil {
		return nil, err
	}

	// Leggo ProducerEpoch (int16)
	if err := binary.Read(r, binary.BigEndian, &batch.ProducerEpoch); err != nil {
		return nil, err
	}

	// Leggo BaseSequence (int32)
	if err := binary.Read(r, binary.BigEndian, &batch.BaseSequence); err != nil {
		return nil, err
	}

	// Leggo RecordsLength: quanti record ci sono (int32)
	if err := binary.Read(r, binary.BigEndian, &batch.RecordsLength); err != nil {
		return nil, err
	}
	// Leggo ora i record (che non sono un COMPACT_ARRAY)
	records := make([]ClusterMetadataRecord, 0)
	for i := 0; i < int(batch.RecordsLength); i++ {
		record := ClusterMetadataRecord{}

		// Leggo Length (del record) (VARINT)
		length, err := ReadVarInt(r)
		if err != nil {
			return nil, err
		}
		record.Length = length

		// Leggo Attributes (del record) (byte)
		if err := binary.Read(r, binary.BigEndian, &record.Attributes); err != nil {
			return nil, err
		}

		// Leggo TimestampDelta (del record) (VARINT)
		timestampDelta, err := ReadVarInt(r)
		if err != nil {
			return nil, err
		}
		record.TimestampDelta = timestampDelta

		// Leggo OffsetDelta (del record) (VARINT)
		offsetDelta, err := ReadVarInt(r)
		if err != nil {
			return nil, err
		}
		record.OffsetDelta = offsetDelta

		// Leggo keyLength (del record) (VARINT)
		keyLength, err := ReadVarInt(r)
		if err != nil {
			return nil, err
		}
		record.KeyLength = keyLength

		// Se record len è -1 significa che array è nil, altrimenti lo instanziamo
		if record.KeyLength == -1 {
			record.Key = nil
		} else {
			key := make([]byte, record.KeyLength)
			if _, err := io.ReadFull(r, key); err != nil {
				return nil, err
			}
			record.Key = key
		}

		// Leggo ValueLength (VARINT)
		valueLength, err := ReadVarInt(r)
		if err != nil {
			return nil, err
		}
		record.ValueLength = valueLength

		// Leggo Value
		recordValue, err := ReadClusterMetadataRecordValue(r)
		if err != nil {
			return nil, err
		}

		record.Value = recordValue

		headerArrayCount, err := ReadUVarInt(r)
		if err != nil {
			return nil, err
		}
		record.HeadersArrayCount = headerArrayCount

		records = append(records, record)
	}

	batch.Records = records
	return &batch, nil
}

func LoadClusterMetadata(path string) ([]ClusterMetadataRecordBatch, error) {
	f, err := os.Open(path)

	if err != nil {
		return nil, err
	}
	defer f.Close()
	batches := make([]ClusterMetadataRecordBatch, 0, 16)
	br := bufio.NewReader(f)
	for {
		batch, err := ReadClusterMetadataRecordBatch(br)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		batches = append(batches, *batch)

	}
	return batches, nil
}

type ClusterMetadata struct {
	TopicInfo     map[string][16]byte
	PartitionInfo map[[16]byte][]Partition
}

var ClusterMetadataCache ClusterMetadata

func init() {
	var err error
	recordBatches, err := LoadClusterMetadata("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		log.Printf("Cannot load cluster metadata: %v", err)
		return
	}
	// Parsing
	ClusterMetadataCache.TopicInfo = ParseMetadataTopicInfo(recordBatches)
	ClusterMetadataCache.PartitionInfo = ParseMetadataTopicPartitions(recordBatches)
	//

	log.Printf("Loaded %d records from cluster metadata", len(recordBatches))
}

func ParseMetadataTopicInfo(batches []ClusterMetadataRecordBatch) map[string][16]byte {
	// Returns a map topicName[topicUUID]

	topicInfo := make(map[string][16]byte)

	for _, batch := range batches {
		for _, record := range batch.Records {
			if topicRecord, ok := record.Value.(*TopicRecordValue); ok {
				if topicRecord.TopicName != nil {
					topicInfo[*topicRecord.TopicName] = topicRecord.TopicId
				}
			}
		}
	}

	return topicInfo
}

func ParseMetadataTopicPartitions(batches []ClusterMetadataRecordBatch) map[[16]byte][]Partition {
	// Returns a map topicUUID[]Partition

	partitionInfo := make(map[[16]byte][]Partition)

	for _, batch := range batches {
		for _, record := range batch.Records {
			if partitionRecord, ok := record.Value.(*PartitionRecordValue); ok {
				partition := Partition{
					ParitionIndex: partitionRecord.PartitionId,
					LeaderId:      partitionRecord.Leader,
					LeaderEpoch:   partitionRecord.LeaderEpoch,
					ReplicaNodes:  partitionRecord.Replicas,
					IsrNodes:      partitionRecord.Isr,
					// EligibleLeaderReplicas: partitionRecord,
					// LastKnownELR: ,
					// OfflineReplicas: ,
					ErrorCode: 0,
				}

				partitionInfo[partitionRecord.TopicUUID] = append(partitionInfo[partitionRecord.TopicUUID], partition)
			}
		}
	}

	return partitionInfo
}
