package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"

	cachepkg "loyalty-consumer/internal/cache"
	cfgpkg "loyalty-consumer/internal/config"
	handler "loyalty-consumer/internal/handler"
	kafkapkg "loyalty-consumer/internal/kafka"
	redispkg "loyalty-consumer/internal/redis"
	storepkg "loyalty-consumer/internal/store"
)

func main() {
	// Загрузка конфигурации
	cfg, err := cfgpkg.NewConfig()
	if err != nil {
		logrus.Fatalf("Config load error: %v", err)
	}

	// Инициализация логгера
	logger := cfgpkg.NewLogger(cfg.LogLevel)
	logger.Info("Starting loyalty-consumer service")

	// Подключение к PostgreSQL
	pgPool, err := pgxpool.New(context.Background(), cfg.PostgresURL)
	if err != nil {
		logger.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgPool.Close()
	if err := pgPool.Ping(context.Background()); err != nil {
		logger.Fatalf("PostgreSQL ping failed: %v", err)
	}
	logger.Info("Connected to PostgreSQL")

	// Создание слоя хранения
	st := storepkg.NewStore(pgPool)

	// Подготовка и подключение к Redis
	redisCfg := redispkg.Config{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		User:         cfg.RedisUser,
		DB:           cfg.RedisDB,
		MaxRetries:   cfg.RedisMaxRetries,
		DialTimeout:  cfg.RedisDialTimeout,
		ReadTimeout:  cfg.RedisReadTimeout,
		WriteTimeout: cfg.RedisWriteTimeout,
	}
	redisEntry := logger.WithField("component", "redis_cache")

	redisClient, err := redispkg.NewClient(context.Background(), redisCfg, redisEntry)
	if err != nil {
		logger.Fatalf("Cannot initialize Redis: %v", err)
	}
	defer redisClient.Close()

	cacheStore := cachepkg.NewRedisCache(redisClient, cfg.CacheTTL, redisEntry)

	// Создание обработчика транзакций
	txHandler := handler.NewTransactionHandler(logger, *st, cacheStore)

	// Создание Kafka consumer
	consumer, err := kafkapkg.NewConsumer(cfg, txHandler.Handle, logger)
	if err != nil {
		logger.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// Настройка graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Received shutdown signal")
		cancel()
	}()

	// Запуск consumer
	logger.Info("Starting Kafka consumer...")
	if err := consumer.Start(ctx); err != nil {
		logger.Fatalf("Consumer start error: %v", err)
	}

	// Ожидание сигнала и корректная остановка
	<-ctx.Done()
	logger.Info("Shutting down consumer...")
	consumer.Stop()
	logger.Info("loyalty-consumer stopped")
}
