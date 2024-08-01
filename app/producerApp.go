package app

import (
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/MikhailKK/appkafka/adaptors/kafka"
	"github.com/MikhailKK/appkafka/domain"
)

func StartProducer(brokers []string, config *sarama.Config) {
	producer := kafka.StartProducer(brokers, config)
	defer producer.Close()

	id := 1
	for {
		msg := domain.Message{
			ID:     id,
			Type:   "bet",
			Amount: 20,
			Key:    "key-" + strconv.Itoa(id%2), // Простое распределение ключей
		}
		kafka.ProduceMessage(producer, msg)
		id++
		time.Sleep(2 * time.Second)
	}
}
