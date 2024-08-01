package producer

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	ID     int    `json:"id"`
	Type   string `json:"type"`
	Amount int    `json:"amount"`
	Key    string `json:"key"`
}

func StartProducer(brokers []string, config *sarama.Config) {
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start producer: %s", err)
	}
	defer producer.Close()

	id := 1
	for {
		msg := Message{
			ID:     id,
			Type:   "bet",
			Amount: 20,
			Key:    "key-" + strconv.Itoa(id%2), // Простое распределение ключей
		}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Failed to marshal message: %s", err)
			continue
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

		id++
		time.Sleep(2 * time.Second)
	}
}
