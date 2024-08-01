package consumer

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

type Message struct {
	ID     int    `json:"id"`
	Type   string `json:"type"`
	Amount int    `json:"amount"`
}

type Consumer struct {
	Partition int32
}

func (consumer *Consumer) StartConsumer(brokers []string, topic string, partition int32) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}
	defer master.Close()

	consumer.Partition = partition

	// Создаем consumer для определенной партиции
	partitionConsumer, err := master.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %s", err)
	}
	defer partitionConsumer.Close()

	// Обрабатываем сообщения
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var receivedMsg Message
			err := json.Unmarshal(msg.Value, &receivedMsg)
			if err != nil {
				log.Printf("Failed to unmarshal message: %s", err)
				continue
			}

			log.Printf("Message claimed: id = %d, type = %s, amount = %d, offset = %d, timestamp = %v, topic = %s, partition = %d",
				receivedMsg.ID, receivedMsg.Type, receivedMsg.Amount, msg.Offset, msg.Timestamp, msg.Topic, msg.Partition)
		case err := <-partitionConsumer.Errors():
			log.Printf("Error: %s", err)
		}
	}
}
