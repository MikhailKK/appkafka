package main

import (
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

	// Адрес брокера Kafka
	brokers := []string{"localhost:9092"}

	// Запускаем производителя
	go producer.StartProducer(brokers, config)

	// Запускаем потребителя для первой партиции
	go func() {
		cons := consumer.Consumer{}
		cons.StartConsumer(brokers, "test", 0)
	}()

	// Захват сигнала для корректного завершения работы
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	log.Println("Terminating: via signal")
}
