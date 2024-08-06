package domain

import "gorm.io/gorm"

// Message represents the structure of the message to be sent and received.
type Message struct {
	gorm.Model
	KafkaID int    `gorm:"index"`
	Type    string `gorm:"type:varchar(200)"`
	Amount  int
	Key     string `gorm:"type:varchar(50)"`
}

type RefundMessage struct {
	gorm.Model
	KafkaID int `gorm:"index"`
	Reason  int
}
