package main

import (
	"bufio"
	"encoding/binary"
	"io"
)

type FetchRequestTopic struct {
	TopicId    [16]byte // UUID
	Partitions []FetchRequestPartition
	TAG_BUFFER byte // TODO: byte -> []byte
}

type FetchRequestPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
	TAG_BUFFER         byte // TODO: byte -> []byte
}

type FetchRequestForgottenTopicsData struct {
	TopicId    [16]byte
	Partitions []int32
}

type FetchRequestV16 struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionId           int32
	SessionEpoch        int32
	Topics              []FetchRequestTopic
	ForgottenTopicsData []FetchRequestForgottenTopicsData
	RackId              *string // COMPACT_STRING
}

func (api *FetchRequestV16) Deserialize(r io.Reader) error {
	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}

	if err := binary.Read(r, binary.BigEndian, &api.MaxWaitMs); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &api.MinBytes); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &api.MaxBytes); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &api.IsolationLevel); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &api.SessionId); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &api.SessionEpoch); err != nil {
		return err
	}

	reqTopics, err := ReadGenericCompactArray[FetchRequestTopic](r)
	if err != nil {
		return err
	}

	api.Topics = reqTopics

	return nil
}
