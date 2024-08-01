package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Создаем конфигурацию Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Адрес брокера Kafka
	brokers := []string{"localhost:9092"}

	// Запускаем производитель и отправляем сообщения
	go func() {
		producer, err := sarama.NewSyncProducer(brokers, config)
		if err != nil {
			log.Fatalf("Failed to start producer: %s", err)
		}
		defer producer.Close()

		for {
			msg := &sarama.ProducerMessage{
				Topic: "test",
				Value: sarama.StringEncoder("Hello Kafka!"),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("Failed to send message: %s", err)
			} else {
				log.Printf("Message sent to partition %d with offset %d", partition, offset)
			}

			time.Sleep(2 * time.Second)
		}
	}()

	// Запускаем потребителя и читаем сообщения
	consumerGroup, err := sarama.NewConsumerGroup(brokers, "test-group", config)
	if err != nil {
		log.Fatalf("Failed to start consumer group: %s", err)
	}
	defer consumerGroup.Close()

	consumer := Consumer{}

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

type Consumer struct{}

func (consumer *Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumer *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(msg.Value), msg.Timestamp, msg.Topic)
		session.MarkMessage(msg, "")
	}
	return nil
}
