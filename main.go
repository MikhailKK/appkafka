package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/MikhailKK/appkafka/adaptors/kafka"
	"github.com/MikhailKK/appkafka/app"
	"github.com/MikhailKK/appkafka/port/http"
)

func main() {
	// Создаем конфигурацию Kafka
	config := kafka.NewKafkaConfig()

	// Адрес брокера Kafka
	brokers := []string{"localhost:9092"}

	// Запускаем производителя
	go app.StartProducer(brokers, config)

	// Запускаем потребителя для первой партиции
	go func() {
		app.StartConsumer(brokers, "test", 0)
	}()

	// Запускаем HTTP сервер
	go http.StartHTTPServer()

	// Захват сигнала для корректного завершения работы
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	<-sigterm
	log.Println("Terminating: via signal")
}
