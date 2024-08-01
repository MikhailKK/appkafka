package kafka

import (
	"encoding/json"
	"log"

	"github.com/MikhailKK/appkafka/domain"

	"github.com/IBM/sarama"
)

func NewKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	return config
}

func StartProducer(brokers []string, config *sarama.Config) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start producer: %s", err)
	}
	return producer
}

func ProduceMessage(producer sarama.SyncProducer, msg domain.Message) {
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Failed to marshal message: %s", err)
		return
	}

	producerMsg := &sarama.ProducerMessage{
		Topic: "test",
		Key:   sarama.StringEncoder(msg.Key),
		Value: sarama.ByteEncoder(msgBytes),
	}

	partition, offset, err := producer.SendMessage(producerMsg)
	if err != nil {
		log.Printf("Failed to send message: %s", err)
	} else {
		log.Printf("Message sent to partition %d with offset %d", partition, offset)
	}
}
