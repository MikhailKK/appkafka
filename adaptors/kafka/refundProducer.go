package kafka

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	"github.com/MikhailKK/appkafka/domain"
)

func StartRefundProducer(brokers []string, config *sarama.Config) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start refund producer: %s", err)
	}
	return producer
}

func ProduceRefundMessage(producer sarama.SyncProducer, msg domain.RefundMessage) {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Failed to marshal refund message: %s", err)
		return
	}

	producerMsg := &sarama.ProducerMessage{
		Topic: "refund",
		Value: sarama.ByteEncoder(msgBytes),
	}

	partition, offset, err := producer.SendMessage(producerMsg)
	if err != nil {
		log.Printf("Failed to send refund message: %s", err)
	} else {
		log.Printf("Refund message sent to partition %d with offset %d", partition, offset)
	}
}
