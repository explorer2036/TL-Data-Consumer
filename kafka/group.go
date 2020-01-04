package kafka

import (
	"TL-Data-Consumer/config"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	totalConsumedCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "total_consumed_count",
		Help: "The total count of messages consumed from kafka",
	})
)

func init() {
	prometheus.MustRegister(totalConsumedCount)
}

const (
	defaultStreamBuffer = 100
	defaultRoutinesNum  = 2
)

// Message a message with specific topic
type Message struct {
	Data  []byte
	Topic string
}

// Consumer represents the kafka consumers
type Consumer struct {
	settings *config.Config

	brokers []string      // the kafka brokers
	topics  []string      // consumer topics
	group   string        // consumer group
	ready   chan struct{} // mark the consumer is ready

	// message stream
	stream chan *Message
}

// NewConsumer create consumers with number of goroutines
func NewConsumer(settings *config.Config) *Consumer {
	if settings.Server.ConsumeBuffer == 0 {
		settings.Server.ConsumeBuffer = defaultStreamBuffer
	}
	if settings.Server.ConsumeRoutines == 0 {
		settings.Server.ConsumeRoutines = defaultRoutinesNum
	}

	return &Consumer{
		settings: settings,

		ready:  make(chan struct{}, settings.Server.ConsumeRoutines),
		stream: make(chan *Message, settings.Server.ConsumeBuffer),
	}
}

// groupHandler represents a Sarama consumer group consumer
type groupHandler struct {
	consumer *Consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (g *groupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (g *groupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (g *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for {
		select {
		case m, ok := <-claim.Messages():
			// if the messages channel is closed, exit the goroutine
			if !ok {
				return nil
			}

			// send the message to channel
			g.consumer.stream <- &Message{Data: m.Value, Topic: m.Topic}

			// mark the message consumed
			session.MarkMessage(m, "")

			// update the total count of messages consumed
			totalConsumedCount.Add(1)
		}
	}
}

// ReadMessages retrives the messages from the channel
func (c *Consumer) ReadMessages() <-chan *Message {
	return c.stream
}

// Start create goroutines to do the consumer group jobs
func (c *Consumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < c.settings.Server.ConsumeRoutines; i++ {
		wg.Add(1)
		go c.handle(ctx, wg)
	}
}

// handle begins to consume the messages from kafka with topics and group
func (c *Consumer) handle(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// new a sarama config for consumer group
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := c.settings.Server.Brokers
	group := c.settings.Server.Group
	topics := c.settings.Server.Topics

	// setup a new sarama consumer group
	handler := groupHandler{consumer: c}

	// new a consumer group client for consuming messages with group
	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// mark the goroutine started
	c.ready <- struct{}{}

	// loop for consuming messages with topics
	for {
		// try to fetch messages from kafka
		if err := client.Consume(ctx, topics, &handler); err != nil {
			fmt.Printf("restart consumer topics(%v), group(%s): %v\n", topics, group, err)
			time.Sleep(time.Second)
			continue
		}

		select {
		// receive a canceled signal
		case <-ctx.Done():
			fmt.Println("consumer goroutine exit")
			return
		default:
		}
	}
}

// IsReady check if the consumer is ready
func (c *Consumer) IsReady() {
	for i := 0; i < c.settings.Server.ConsumeRoutines; i++ {
		<-c.ready
	}
	fmt.Println("consumer goroutines are ready")
}
