package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {
	//producer
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := kafka.NewProducer(config)

	if err != nil {
		fmt.Println("Failed to create producer: %v\n", err)
		return
	}

	defer producer.Close()

	deliveryChan := make(chan kafka.Event)

	value := os.Args[2]

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &os.Args[1], Partition: selectPartition(value)},
		Value:          []byte(value),
	}, deliveryChan)

	if err != nil {
		fmt.Println("Failed to produce message: %v\n", err)
	}
	fmt.Println("Message Produced Successfully to topic: ", os.Args[1], " with value: ", value+" and partition: ", selectPartition(value))
	<-deliveryChan
}

func selectPartition(value string) int32 {
	if value[0] < 'M' {
		return 0
	}
	return 1
}
