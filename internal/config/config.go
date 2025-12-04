package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type Config struct {
	// Kafka
	KafkaBrokers []string
	KafkaTopics  string

	// Postgres
	PostgresURL string
	// Redis
	RedisAddr         string        `env:"REDIS_ADDR" yaml:"redis.addr"`
	RedisPassword     string        `env:"REDIS_PASSWORD" yaml:"redis.password"`
	RedisUser         string        `env:"REDIS_USER" yaml:"redis.user"`
	RedisDB           int           `env:"REDIS_DB" yaml:"redis.db"`
	RedisMaxRetries   int           `env:"REDIS_MAX_RETRIES" yaml:"redis.max_retries"`
	RedisDialTimeout  time.Duration `env:"REDIS_DIAL_TIMEOUT" yaml:"redis.dial_timeout"`
	RedisReadTimeout  time.Duration `env:"REDIS_READ_TIMEOUT" yaml:"redis.read_timeout"`
	RedisWriteTimeout time.Duration `env:"REDIS_WRITE_TIMEOUT" yaml:"redis.write_timeout"`

	// Cache TTL
	CacheTTL time.Duration `env:"CACHE_TTL"      yaml:"cache.ttl"`

	LogLevel    string
	MetricsPort int
}

func NewConfig() (*Config, error) {
	cfg := &Config{}

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	cfg.KafkaBrokers = strings.Split(brokers, ",")

	topic := os.Getenv("KAFKA_TOPICS")
	if topic == "" {
		topic = "transactions"
	}
	cfg.KafkaTopics = topic

	pgURL := os.Getenv("POSTGRES_SQL")
	if pgURL == "" {
		pgURL = "postgres://user:password@localhost:5432/loyalty_db?sslmode=disable"
	}
	cfg.PostgresURL = pgURL

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	cfg.RedisAddr = redisAddr

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	cfg.LogLevel = logLevel

	metricsPortStr := os.Getenv("METRICS_PORT")
	if metricsPortStr == "" {
		metricsPortStr = "8080"
	}
	port, err := strconv.Atoi(metricsPortStr)
	if err != nil {
		return nil, err
	}
	cfg.MetricsPort = port

	return cfg, nil
}

func NewLogger(level string) *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(os.Stdout) // os.Stdout - указание, куда выводить сообщения, то есть в консоль, иначе бы они выводилсь в Stderr или вообще никуда
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2025-10-11 11:46:05",
	})

	logLevel := parseLogLevel(level)
	logger.SetLevel(logLevel)

	return logger
}

func parseLogLevel(level string) logrus.Level {
	switch strings.ToLower(level) {
	case "debug":
		return logrus.DebugLevel
	case "info":
		return logrus.InfoLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	default:
		return logrus.InfoLevel
	}
}
