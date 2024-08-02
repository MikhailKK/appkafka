package kafka

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	"github.com/MikhailKK/appkafka/domain"
)

func StartConsumer(brokers []string, topic string, partition int32) sarama.PartitionConsumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}

	partitionConsumer, err := master.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %s", err)
	}
	return partitionConsumer
}

func ConsumeMessages(consumer sarama.PartitionConsumer, callback func(domain.Message)) {
	for {
		select {
		case msg := <-consumer.Messages():
			var receivedMsg domain.Message
			err := json.Unmarshal(msg.Value, &receivedMsg)
			if err != nil {
				log.Printf("Failed to unmarshal message: %s", err)
				continue
			}

			log.Printf("Message claimed: id = %d, type = %s, amount = %d, offset = %d, timestamp = %v, topic = %s, partition = %d",
				receivedMsg.ID, receivedMsg.Type, receivedMsg.Amount, msg.Offset, msg.Timestamp, msg.Topic, msg.Partition)

			callback(receivedMsg)

		case err := <-consumer.Errors():
			log.Printf("Error: %s", err)
		}
	}
}
