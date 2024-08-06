package app

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/MikhailKK/appkafka/adaptors/kafka"
	"github.com/MikhailKK/appkafka/domain"
)

func StartRefundProducer(brokers []string, config *sarama.Config) {
	producer := kafka.StartRefundProducer(brokers, config)
	defer producer.Close()

	id := 1
	for {
		msg := domain.RefundMessage{
			KafkaID: id,
			Reason:  1,
		}
		kafka.ProduceRefundMessage(producer, msg)
		id++
		time.Sleep(8 * time.Second)
	}
}
