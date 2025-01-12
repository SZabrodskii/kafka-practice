package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type OrderPlacer struct {
	producer   *kafka.Producer
	topic      string
	deliveryCh chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		p,
		topic,
		make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("Placing %s order of size %d", orderType, size)
		payload = []byte(format)
	)
	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value:          payload,
	},
		op.deliveryCh,
	)

	if err != nil {
		fmt.Printf("Failed to produce message: %s\n", err)
	}

	<-op.deliveryCh
	fmt.Printf("Successfully placed order: %s\n", format)
	return nil
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "auto_partition_producer",
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	op := NewOrderPlacer(p, "auto_partition_topic")
	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("market order", i+1); err != nil {
			fmt.Printf("Failed to place order: %s\n", err)
		}
		time.Sleep(time.Second * 1)
	}

}
