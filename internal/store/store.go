package store

import (
	"context"
	"fmt"
	"loyalty-consumer/internal/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Queryable interface {
	Exec(ctx context.Context, query string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...any) pgx.Row
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) InsertTransaction(ctx context.Context, tx models.Transaction) error {
	return s.insertTransaction(ctx, s.pool, tx)
}

// InsertTransactionTx сохраняет транзакцию в рамках существующей транзакции
func (s *Store) InsertTransactionTx(ctx context.Context, pgxTx pgx.Tx, tx models.Transaction) error {
	return s.insertTransaction(ctx, pgxTx, tx)
}

// UpdateBalance обновляет баланс пользователя без транзакции
func (s *Store) UpdateBalance(ctx context.Context, userID string, amount int) error {
	return s.updateBalance(ctx, s.pool, userID, amount)
}

// UpdateBalanceTx обновляет баланс в рамках транзакции
func (s *Store) UpdateBalanceTx(ctx context.Context, pgxTx pgx.Tx, userID string, amount int) error {
	return s.updateBalance(ctx, pgxTx, userID, amount)
}

// GetBalance возвращает баланс пользователя без транзакции
func (s *Store) GetBalance(ctx context.Context, userID string) (int64, error) {
	return s.getBalance(ctx, s.pool, userID)
}

// GetBalanceTx возвращает баланс в рамках транзакции
func (s *Store) GetBalanceTx(ctx context.Context, pgxTx pgx.Tx, userID string) (int64, error) {
	return s.getBalance(ctx, pgxTx, userID)
}

func (s *Store) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *Store) insertTransaction(ctx context.Context, q Queryable, tx models.Transaction) error {
	const sql = `
	INSERT INTO transactions (id user_id,amount,time)
	VALUES ($1, $2, $3, $4)
	`
	if _, err := q.Exec(ctx, sql, tx.ID, tx.UserID, tx.Amount, tx.CreatedAt); err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}
	return nil
}

func (s *Store) updateBalance(ctx context.Context, q Queryable, userID string, amount int) error {
	const sql = `
        UPDATE users
        SET balance = balance + $1
        WHERE id = $2
    `
	if _, err := q.Exec(ctx, sql, amount, userID); err != nil {
		return fmt.Errorf("update balance for user %s: %w", userID, err)
	}
	return nil
}

func (s *Store) getBalance(ctx context.Context, q Queryable, userID string) (int64, error) {
	const sql = `SELECT balance FROM users WHERE id = $1`
	row := q.QueryRow(ctx, sql, userID)
	var balance int64
	if err := row.Scan(&balance); err != nil {
		if err == pgx.ErrNoRows {
			return 0, fmt.Errorf("get balance: user %s not found", userID)
		}
		return 0, fmt.Errorf("get balance for user %s: %w", userID, err)
	}
	return balance, nil
}
