package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Config хранит настройки подключения к Redis.
type Config struct {
	Addr         string        `yaml:"addr"`
	Password     string        `yaml:"password"`
	User         string        `yaml:"user"`
	DB           int           `yaml:"db"`
	MaxRetries   int           `yaml:"max_retries"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// NewClient создаёт и проверяет подключение к Redis.
func NewClient(ctx context.Context, cfg Config, logger *logrus.Entry) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		Username:     cfg.User,
		DB:           cfg.DB,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		logger.Errorf("Redis connection failed: %v", err)
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	logger.Info("Connected to Redis")
	return client, nil
}
