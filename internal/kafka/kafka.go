package kafka

import (
	"context"
	"fmt"
	"loyalty-consumer/internal/config"
	"loyalty-consumer/internal/handler"
	"sync"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	wg      sync.WaitGroup
	group   sarama.ConsumerGroup
	Topic   string
	handler handler.HandlerFunc
	logger  *logrus.Logger
	ready   chan bool
}

func NewConsumer(cfg *config.Config, handlerFunc handler.HandlerFunc, logger *logrus.Logger) (*Consumer, error) {

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0

	saramaConfig.Consumer.Offsets.AutoCommit.Enable = false
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaConfig.Consumer.Return.Errors = true

	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	group, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, "loyalty-consumer-group", saramaConfig)
	if err != nil {

		return nil, fmt.Errorf("cfg kafka broker not exist")
	}
	logger.Info("kafka consmer create succsesfuly")

	return &Consumer{
		group:   group,
		Topic:   cfg.KafkaTopics,
		handler: handlerFunc,
		logger:  logger,
		ready:   make(chan bool),
	}, nil

}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	c.logger.Info("Consumer group session started")
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	c.logger.Info("Consumer group session ended")
	return nil
}

func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Consumer is starting")

	// 1) Ошибки группы
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for err := range c.group.Errors() {
			c.logger.Errorf("Consumer group error: %v", err)
		}
	}()

	// 2) Основной луп обработки
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if err := c.group.Consume(ctx, []string{c.Topic}, c); err != nil {
				c.logger.Errorf("Error consuming messages: %v", err)
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Ждём, пока Setup не закроет ready
	<-c.ready
	c.logger.Info("Consumer ready, consuming messages...")

	return nil
}
func (c *Consumer) Stop() {
	c.logger.Info("Shutting down consumer group")
	if err := c.group.Close(); err != nil {
		c.logger.Errorf("Error closing consumer group: %v", err)
	}
	c.wg.Wait()
	c.logger.Info("Consumer stopped gracefully")
}

func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.Infof("Started consuming partition %d", claim.Partition())

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			c.logger.WithFields(logrus.Fields{
				"topic":     message.Topic,
				"partition": message.Partition,
				"offset":    message.Offset,
				"key":       string(message.Key),
				"timestamp": message.Timestamp,
			}).Info("Received message")

			err := c.handler(sess.Context(), message)
			if err != nil {
				c.logger.WithFields(logrus.Fields{
					"topic":     message.Topic,
					"partition": message.Partition,
					"offset":    message.Offset,
					"error":     err.Error(),
				}).Error("Failed to process message")

				continue
			}

			sess.MarkMessage(message, "")

			c.logger.WithFields(logrus.Fields{
				"topic":     message.Topic,
				"partition": message.Partition,
				"offset":    message.Offset,
			}).Debug("Message processed and offset marked")

		case <-sess.Context().Done():
			c.logger.Info("Session context cancelled, stopping partition consumer")
			return nil
		}
	}
}
