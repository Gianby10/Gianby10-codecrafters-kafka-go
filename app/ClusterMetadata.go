package main

import (
	"encoding/binary"
	"fmt"
	"io"
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
	TAG_BUFFER   []byte // boh al momento metto solo un byte 0
}

type TopicRecordValue struct {
	TopicName  *string
	TopicId    [16]byte
	TAG_BUFFER []byte
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
	TAG_BUFFER      []byte
}

func ReadFeatureLevelValue(r io.Reader) (*FeatureLevelRecordValue, error) {
	// Inizializzo un featureLevelRecord vuoto
	featureLevelRecord := FeatureLevelRecordValue{}
	// Non Leggo FrameVersion, Type e Version che abbiamo già letto arrivati a questo punto
	// Leggo COMPACT_STRING Name
	name, err := readCompactString(r)
	if err != nil {
		return nil, err
	}
	featureLevelRecord.Name = name

	// Leggo FeatureLevel (int16)
	if err := binary.Read(r, binary.BigEndian, &featureLevelRecord.FeatureLevel); err != nil {
		return nil, err
	}

	return &featureLevelRecord, nil
}

func ReadTopicRecordValue(r io.Reader) (*TopicRecordValue, error) {
	// Inizializzo un TopicrecordValue vuoto
	topicRecord := TopicRecordValue{}
	// Non Leggo FrameVersion, Type e Version che abbiamo già letto arrivati a questo punto
	// Leggo COMPACT_STRING TopicName
	topicName, err := readCompactString(r)
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

	// TAG_FIELDS

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
	batchReader := io.LimitReader(r, int64(batch.BatchLength))

	// Leggo PartitionLeaderEpoch (int32)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.PartitionLeaderEpoch); err != nil {
		return nil, err
	}

	// Leggo MagicByte (byte)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.MagicByte); err != nil {
		return nil, err
	}

	// Leggo CRC (int32)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.CRC); err != nil {
		return nil, err
	}

	// Leggo Attributes (int16)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.Attributes); err != nil {
		return nil, err
	}

	// Leggo LastOffsetDelta (int32)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.LastOffsetDelta); err != nil {
		return nil, err
	}

	// Leggo BaseTimestamp (int64)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.BaseTimestamp); err != nil {
		return nil, err
	}

	// Leggo MaxTimestamp (int64)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.MaxTimestamp); err != nil {
		return nil, err
	}

	// Leggo ProducerId (int64)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.ProducerId); err != nil {
		return nil, err
	}

	// Leggo ProducerEpoch (int16)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.ProducerEpoch); err != nil {
		return nil, err
	}

	// Leggo BaseSequence (int32)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.BaseSequence); err != nil {
		return nil, err
	}

	// Leggo RecordsLength: quanti record ci sono (int32)
	if err := binary.Read(batchReader, binary.BigEndian, &batch.RecordsLength); err != nil {
		return nil, err
	}

	// Leggo ora i record (che non sono un COMPACT_ARRAY)
	records := make([]ClusterMetadataRecord, 0, batch.RecordsLength)
	for i := 0; i < int(batch.RecordsLength); i++ {
		record := ClusterMetadataRecord{}

		// Leggo Length (del record) (VARINT)
		length, err := ReadVarInt(batchReader)
		if err != nil {
			return nil, err
		}
		record.Length = length

		// LimitReader per non uscire dai confini del record
		recordReader := io.LimitReader(batchReader, length)

		// Leggo Attributes (del record) (byte)
		if err := binary.Read(recordReader, binary.BigEndian, &record.Attributes); err != nil {
			return nil, err
		}

		fmt.Printf("Batch fin ora: %+v", batch)

		// Leggo TimestampDelta (del record) (VARINT)
		timestampDelta, err := ReadVarInt(recordReader)
		if err != nil {
			return nil, err
		}
		record.TimestampDelta = timestampDelta

		// Leggo OffsetDelta (del record) (VARINT)
		offsetDelta, err := ReadVarInt(recordReader)
		if err != nil {
			return nil, err
		}
		record.OffsetDelta = offsetDelta

		// Leggo keyLength (del record) (VARINT)
		keyLength, err := ReadVarInt(recordReader)
		if err != nil {
			return nil, err
		}
		record.KeyLength = keyLength

		// Se record len è -1 significa che array è nil, altrimenti lo instanziamo
		if record.KeyLength == -1 {
			record.Key = nil
		} else {
			key := make([]byte, record.KeyLength)
			if _, err := io.ReadFull(recordReader, key); err != nil {
				return nil, err
			}
			record.Key = key
		}

		// Leggo ValueLength (VARINT)
		valueLength, err := ReadVarInt(recordReader)
		if err != nil {
			return nil, err
		}
		record.ValueLength = valueLength

		var valueReader io.Reader
		if record.ValueLength > 0 {
			valueReader = io.LimitReader(recordReader, record.ValueLength)
		} else {
			// valueReader = recordReader // fallback (Kafka usa -1 per null)
			valueReader = io.LimitReader(recordReader, 0)
		}

		// Leggo Value
		recordValue, err := ReadClusterMetadataRecordValue(valueReader)
		if err != nil {
			return nil, err
		}

		record.Value = recordValue

		headerArrayCount, err := ReadUVarInt(recordReader)
		if err != nil {
			return nil, err
		}
		record.HeadersArrayCount = headerArrayCount

		records = append(records, record)
	}

	batch.Records = records

	return &batch, nil
}

func ReadClusterMetadata() error {
	f, err := os.Open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return err
	}
	defer f.Close()
	batches := make([]ClusterMetadataRecordBatch, 0)
	for {
		batch, err := ReadClusterMetadataRecordBatch(f)
		if err == io.EOF {
			fmt.Println("Hit an EOF")

			break
		}
		if err != nil {
			return err
		}

		batches = append(batches, *batch)

	}

	return nil
}
