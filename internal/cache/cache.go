package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Cache интерфейс для кэширования балансов
type Cache interface {
	SetBalance(ctx context.Context, userID string, balance int64) error
	GetBalance(ctx context.Context, userID string) (int64, bool, error)
	DeleteBalance(ctx context.Context, userID string) error
	Close() error
}

// RedisCache реализация кеша на Redis
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	log    *logrus.Entry
}

// NewRedisCache создаёт новый Redis кеш
func NewRedisCache(client *redis.Client, ttl time.Duration, entry *logrus.Entry) *RedisCache {
	return &RedisCache{
		client: client,
		ttl:    ttl,
		log:    entry,
	}
}

// SetBalance сохраняет баланс пользователя в кеш
func (c *RedisCache) SetBalance(ctx context.Context, userID string, balance int64) error {
	key := c.balanceKey(userID)
	if err := c.client.Set(ctx, key, balance, c.ttl).Err(); err != nil {
		return fmt.Errorf("set balance in cache for user %s: %w", userID, err)
	}
	return nil
}

// GetBalance возвращает баланс из кеша
// Возвращает (balance, found, error)
func (c *RedisCache) GetBalance(ctx context.Context, userID string) (int64, bool, error) {
	key := c.balanceKey(userID)
	val, err := c.client.Get(ctx, key).Result()

	if err == redis.Nil {
		// Ключ не найден — это норма
		return 0, false, nil
	}

	if err != nil {
		return 0, false, fmt.Errorf("get balance from cache for user %s: %w", userID, err)
	}

	balance, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse balance from cache for user %s: %w", userID, err)
	}

	return balance, true, nil
}

// DeleteBalance удаляет баланс из кеша
func (c *RedisCache) DeleteBalance(ctx context.Context, userID string) error {
	key := c.balanceKey(userID)
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("delete balance from cache for user %s: %w", userID, err)
	}
	return nil
}

func (c *RedisCache) Close() error {

	if err := c.client.Close(); err != nil {
		c.log.Errorf("Failed to close Redis client: %v", err)
		return err
	}

	c.log.Infof("Redis is closed")
	return nil
}

// balanceKey формирует ключ для баланса пользователя
func (c *RedisCache) balanceKey(userID string) string {
	return fmt.Sprintf("balance:%s", userID)
}
