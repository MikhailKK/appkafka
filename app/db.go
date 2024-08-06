package app

import (
	"log"

	"github.com/MikhailKK/appkafka/domain"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB

func InitDB(connStr string) {
	var err error
	db, err = gorm.Open(postgres.Open(connStr), &gorm.Config{})
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	// Автоматическое создание таблиц
	err = db.AutoMigrate(&domain.Message{}, &domain.RefundMessage{})
	if err != nil {
		log.Fatalf("AutoMigrate failed: %v\n", err)
	}
}

func InsertMessage(msg domain.Message) error {
	return db.Create(&msg).Error
}
