package models

import (
	"time"

	"github.com/go-playground/validator/v10"
)

type Transaction struct {
	ID        string    `json:"id" validate:"required"`
	UserID    string    `json:"user_id" validate:"required"`
	Amount    int64     `json:"amount" validate:"required,ne=0"`
	Type      string    `json:"type" validate:"required,oneof=purchase refund bonus"`
	CreatedAt time.Time `json:"created_at" validate:"required"`
}

var validate = validator.New()

func (t *Transaction) Validate() error {
	return validate.Struct(t)
}
