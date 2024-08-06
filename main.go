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

	// Подключение к базе данных
	dbConnStr := "postgres://postgres:admin@localhost:5432/openmind?sslmode=disable"

	app.InitDB(dbConnStr)

	// Запускаем производителя для топика "test"
	go app.StartProducer(brokers, config)

	// Запускаем производителя для топика "refund"
	go app.StartRefundProducer(brokers, config)

	// Запускаем потребителя для первой партиции топика "test"
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
