package app

import (
	"log"
	"sync"

	"github.com/MikhailKK/appkafka/adaptors/kafka"
	"github.com/MikhailKK/appkafka/domain"
)

var (
	lastMessage domain.Message
	mu          sync.RWMutex
)

func StartConsumer(brokers []string, topic string, partition int32) {
	consumer := kafka.StartConsumer(brokers, topic, partition)
	defer consumer.Close()

	kafka.ConsumeMessages(consumer, processMessage)
}

func processMessage(msg domain.Message) {
	storeMessage(msg)
	err := InsertMessage(msg)
	if err != nil {
		log.Printf("Failed to insert message into database: %s", err)
	}
}

func storeMessage(msg domain.Message) {
	mu.Lock()
	defer mu.Unlock()
	lastMessage = msg
}

func GetLastMessage() domain.Message {
	mu.RLock()
	defer mu.RUnlock()
	return lastMessage
}
