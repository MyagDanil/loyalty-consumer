package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"loyalty-consumer/internal/cache"
	"loyalty-consumer/internal/models"
	"loyalty-consumer/internal/store"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type HandlerFunc func(ctx context.Context, msg *sarama.ConsumerMessage) error

type TransactionHandler struct {
	cache  cache.Cache
	store  store.Store
	logger *logrus.Logger
}

func NewTransactionHandler(logger *logrus.Logger, store store.Store, cache cache.Cache) *TransactionHandler {
	return &TransactionHandler{
		logger: logger,
		store:  store,
		cache:  cache,
	}
}

func (h *TransactionHandler) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {

	var tx models.Transaction
	if err := json.Unmarshal(msg.Value, &tx); err != nil {

		h.logger.Errorf("Invalid JSON format: %v", err)
		return fmt.Errorf("unmarshal message: %w", err)

	}
	if err := tx.Validate(); err != nil {
		h.logger.Errorf("Validation failed: %v", err)
		return fmt.Errorf("validation error: %w", err)
	}

	pgxTx, err := h.store.BeginTx(ctx)
	if err != nil {
		h.logger.Errorf("Failed to begin transaction: %v", err)
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer pgxTx.Rollback(ctx)

	if err := h.store.InsertTransactionTx(ctx, pgxTx, tx); err != nil {
		h.logger.Infof("Failed to insert transaction")
		return fmt.Errorf("fail %v,%v,%v", ctx, pgxTx, tx)
	}

	if err := h.store.UpdateBalanceTx(ctx, pgxTx, tx.UserID, int(tx.Amount)); err != nil {
		h.logger.Infof("Failed to update balance")
		return fmt.Errorf("fail %v,%v,%s,%d", ctx, pgxTx, tx.UserID, int(tx.Amount))
	}

	if err := pgxTx.Commit(ctx); err != nil {
		h.logger.Errorf("Failed to commit transaction: %v", err)
		return fmt.Errorf("commit transaction: %w", err)
	}

	go func(userID string) {
		bgCtx := context.Background()
		bal, err := h.store.GetBalance(bgCtx, userID)
		if err != nil {
			h.logger.Warnf("Cache update: fetch balance failed for user %s: %v", userID, err)
			return
		}
		if err := h.cache.SetBalance(bgCtx, userID, bal); err != nil {
			h.logger.Warnf("Cache update: set balance failed for user %s: %v", userID, err)
		} else {
			h.logger.Debugf("Cache update: balance for user %s set to %d", userID, bal)
		}
	}(tx.UserID)

	h.logger.Infof("Transaction %s processed for user %s", tx.ID, tx.UserID)
	return nil

}
