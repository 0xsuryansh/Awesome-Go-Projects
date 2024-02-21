package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	adminClient, err := kafka.NewAdminClient(config)

	if err != nil {
		fmt.Print("Failed to create AdminClient %v \n", err)
		return
	}

	topic := kafka.TopicSpecification{
		Topic:         "users",
		NumPartitions: 2,
	}

	_, err = adminClient.CreateTopics(context.Background(), []kafka.TopicSpecification{topic})

	if err != nil {
		fmt.Println("Failed to create topic: %v\n", err)
		return
	}

	fmt.Println("Topic Created Successfully")

	adminClient.Close()

	//producer
	producer, err := kafka.NewProducer(config)

	if err != nil {
		fmt.Println("Failed to create producer: %v\n", err)
		return
	}

	defer producer.Close()

	deliveryChan := make(chan kafka.Event)

	value := "Sello Go!"

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic.Topic, Partition: selectPartition(value)},
		Value:          []byte(value),
	}, deliveryChan)

	if err != nil {
		fmt.Println("Failed to produce message: %v\n", err)
	}
	fmt.Println("Message Produced Successfully")
	<-deliveryChan

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

	err = consumer.SubscribeTopics([]string{"users"}, nil)

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

func selectPartition(value string) int32 {
	if value[0] < 'M' {
		return 0
	}
	return 1
}
