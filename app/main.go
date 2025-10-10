package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func handleConnection(conn net.Conn) {

	defer conn.Close()
	for {

		kafkaReqMsg, err := ReadKafkaRequestMessage(conn)
		if err != nil {
			if err == io.EOF {
				log.Print("Connection closed by client.")
				return
			}
			log.Printf("error reading kafka message: %s", err.Error())
			return
		}

		var responseMsg *KafkaMessage
		requestHeader := kafkaReqMsg.Header.(*RequestHeaderV2)

		var apiKey int16 = kafkaReqMsg.Header.(*RequestHeaderV2).ApiKey
		switch apiKey {
		case 18: // ApiVersion
			responseMsg = NewApiVersionsResponse(requestHeader)
		case 75: // DescribeTopicsPartitions
			requestBody := kafkaReqMsg.Body.(*DescribeTopicsPartitionsRequestV0)
			responseMsg = NewDescribeTopicsPartitionsResponse(requestHeader, requestBody)
		default:
			log.Printf("Unsupported ApiKey: %d", apiKey)
			return
		}
		WriteKafkaResponseMessage(conn, responseMsg)
	}

}

func main() {
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
