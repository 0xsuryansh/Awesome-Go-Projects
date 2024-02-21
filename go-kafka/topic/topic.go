package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"strconv"
)

func main() {
	//get topic name from CLI arguments
	topicName := os.Args[1]
	numPartitions, err := strconv.Atoi(os.Args[2])

	if err != nil {
		fmt.Println("Invalid number of partitions: %v\n", err)
		return
	}

	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	adminClient, err := kafka.NewAdminClient(config)

	if err != nil {
		fmt.Print("Failed to create AdminClient %v \n", err)
		return
	}

	topic := kafka.TopicSpecification{
		Topic:         topicName,
		NumPartitions: numPartitions,
	}

	_, err = adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{topic})

	if err != nil {
		fmt.Println("Failed to create topic: %v\n", err)
		return
	}

	fmt.Println("Topic Created Successfully with name: ", topicName, " and partitions:", numPartitions, "\n")

	adminClient.Close()
}
