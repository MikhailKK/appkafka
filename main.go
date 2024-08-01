package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/MikhailKK/appkafka/consumer"
	"github.com/MikhailKK/appkafka/producer"
)

func main() {
	// Создаем конфигурацию Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Адрес брокера Kafka
	brokers := []string{"localhost:9092"}

	// Запускаем производителя
	go producer.StartProducer(brokers, config)

	// Запускаем потребителя
	consumerGroup, err := sarama.NewConsumerGroup(brokers, "test-group", config)
	if err != nil {
		log.Fatalf("Failed to start consumer group: %s", err)
	}
	defer consumerGroup.Close()

	consumer := consumer.Consumer{}

	// Захват сигнала для корректного завершения работы
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			err := consumerGroup.Consume(ctx, []string{"test"}, &consumer)
			if err != nil {
				log.Fatalf("Error from consumer: %s", err)
			}
		}
	}()

	<-sigterm
	log.Println("Terminating: via signal")
}
