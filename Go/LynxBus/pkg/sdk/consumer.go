package sdk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ConsumerInterface interface {
	Subscribe(topics []string) error
	Unsubscribe() error
	Consume(ctx context.Context, handler RecordHandler) error
	CommitOffset(topic string, partition int32, offset int64) error
	CommitOffsets(offsets map[string]map[int32]int64) error
	Pause(partitions []PartitionMetadata) error
	Resume(partitions []PartitionMetadata) error
	Close() error
}

type ConsumerConfig struct {
	GroupID                string
	AutoOffsetReset        string
	FetchMinBytes          int
	FetchMaxBytes          int
	FetchMaxWait           time.Duration
	MaxPollRecords         int
	EnableAutoCommit       bool
	AutoCommitInterval     time.Duration
	SessionTimeout         time.Duration
	RebalanceTimeout       time.Duration
	IsolationLevel         string
	MaxPartitionFetchBytes int
}

type Consumer struct {
	client  *Client
	config  *ConsumerConfig
	mu      sync.RWMutex
	closed  bool
	topics  []string
	handler RecordHandler
	ctx     context.Context
	cancel  context.CancelFunc
}

type RecordHandler func(*ConsumerRecord) error

func NewDefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		AutoOffsetReset:        "latest",
		FetchMinBytes:          1,
		FetchMaxBytes:          52428800, // 50MB
		FetchMaxWait:           500 * time.Millisecond,
		MaxPollRecords:         500,
		EnableAutoCommit:       true,
		AutoCommitInterval:     5 * time.Second,
		SessionTimeout:         10 * time.Second,
		RebalanceTimeout:       60 * time.Second,
		IsolationLevel:         "read_uncommitted",
		MaxPartitionFetchBytes: 1048576, // 1MB
	}
}

func newConsumer(client *Client, groupID string, opts ...ConsumerOption) (*Consumer, error) {
	if groupID == "" {
		return nil, fmt.Errorf("group ID cannot be empty")
	}

	config := NewDefaultConsumerConfig()
	config.GroupID = groupID

	for _, opt := range opts {
		opt(config)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		client: client,
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (c *Consumer) Subscribe(topics []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}

	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}

	c.topics = topics
	return nil
}

func (c *Consumer) Unsubscribe() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}

	c.topics = nil
	return nil
}

func (c *Consumer) Consume(ctx context.Context, handler RecordHandler) error {
	if handler == nil {
		return fmt.Errorf("record handler cannot be nil")
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("consumer is closed")
	}

	if len(c.topics) == 0 {
		c.mu.Unlock()
		return fmt.Errorf("no topics subscribed")
	}

	c.handler = handler
	c.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return nil
		default:
			record := &ConsumerRecord{
				Record: &Record{
					Metadata: RecordMetadata{
						ConsumedAt: time.Now(),
					},
				},
			}

			if err := handler(record); err != nil {
				record.Metadata.RetryCount++
			}
		}
	}
}

func (c *Consumer) CommitOffset(topic string, partition int32, offset int64) error {
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	return nil
}

func (c *Consumer) CommitOffsets(offsets map[string]map[int32]int64) error {
	if len(offsets) == 0 {
		return fmt.Errorf("no offsets to commit")
	}
	return nil
}

func (c *Consumer) Pause(partitions []PartitionMetadata) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}
	return nil
}

func (c *Consumer) Resume(partitions []PartitionMetadata) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("consumer is closed")
	}
	return nil
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}

	return nil
}

type ConsumerOption func(*ConsumerConfig)

func WithAutoOffsetReset(reset string) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.AutoOffsetReset = reset
	}
}

func WithFetchMinBytes(bytes int) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.FetchMinBytes = bytes
	}
}

func WithFetchMaxBytes(bytes int) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.FetchMaxBytes = bytes
	}
}

func WithFetchMaxWait(wait time.Duration) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.FetchMaxWait = wait
	}
}

func WithMaxPollRecords(max int) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.MaxPollRecords = max
	}
}

func WithAutoCommit(enable bool) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.EnableAutoCommit = enable
	}
}

func WithAutoCommitInterval(interval time.Duration) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.AutoCommitInterval = interval
	}
}

func WithSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.SessionTimeout = timeout
	}
}

func WithRebalanceTimeout(timeout time.Duration) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.RebalanceTimeout = timeout
	}
}

func WithIsolationLevel(level string) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.IsolationLevel = level
	}
}

func WithMaxPartitionFetchBytes(bytes int) ConsumerOption {
	return func(config *ConsumerConfig) {
		config.MaxPartitionFetchBytes = bytes
	}
}
