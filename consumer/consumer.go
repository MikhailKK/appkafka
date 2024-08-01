package consumer

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

type Message struct {
	ID     int    `json:"id"`
	Type   string `json:"type"`
	Amount int    `json:"amount"`
}

type Consumer struct{}

func (consumer *Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumer *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var receivedMsg Message
		err := json.Unmarshal(msg.Value, &receivedMsg)
		if err != nil {
			log.Printf("Failed to unmarshal message: %s", err)
			continue
		}

		log.Printf("Message claimed: id = %d, type = %s, amount = %d, offset = %d, timestamp = %v, topic = %s",
			receivedMsg.ID, receivedMsg.Type, receivedMsg.Amount, msg.Offset, msg.Timestamp, msg.Topic)
		session.MarkMessage(msg, "")
	}
	return nil
}
