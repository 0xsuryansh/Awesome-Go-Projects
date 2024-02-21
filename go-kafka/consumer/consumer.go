package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {
	topic := os.Args[1]
	//consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		fmt.Println("Failed to create consumer: %v\n", err)
		return
	}

	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		fmt.Println("Failed to subscribe to topic: %v\n", err)
		return
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}
