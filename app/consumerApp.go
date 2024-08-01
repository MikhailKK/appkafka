package app

import "github.com/MikhailKK/appkafka/adaptors/kafka"

func StartConsumer(brokers []string, topic string, partition int32) {
	consumer := kafka.StartConsumer(brokers, topic, partition)
	defer consumer.Close()

	kafka.ConsumeMessages(consumer)
}
