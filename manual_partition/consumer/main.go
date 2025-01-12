package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	topic := "manual_partition_topic"
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "manual_partition_group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}

	err = consumer.Assign([]kafka.TopicPartition{{Topic: &topic, Partition: 0}})
	if err != nil {
		log.Fatalf("Failed to assign partition: %s\n", err)
	}

	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("Processing order %s:\n%s\n", e.TopicPartition, string(e.Value))
			response := fmt.Sprintf("Processed order %s", string(e.Value))
			producer, err := kafka.NewProducer(&kafka.ConfigMap{
				"bootstrap.servers": "localhost:9092",
				"client.id":         "manual_partition_consumer",
				"acks":              "all",
			})
			if err != nil {
				fmt.Printf("Failed to create producer: %s\n", err)
			}
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: e.TopicPartition.Partition},
				Value:          []byte(response),
			}, nil)
		case kafka.Error:
			fmt.Printf("Error: %v\n", e)
		}
	}
}
