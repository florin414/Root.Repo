package sdk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
	CompressionLz4    CompressionType = "lz4"
	CompressionZstd   CompressionType = "zstd"
)

func (c CompressionType) IsValid() bool {
	switch c {
	case CompressionNone, CompressionGzip, CompressionSnappy, CompressionLz4, CompressionZstd:
		return true
	}
	return false
}

func (c CompressionType) String() string {
	return string(c)
}

type ProducerInterface interface {
	Send(ctx context.Context, record *Record) (*ProducerResult, error)
	SendAsync(ctx context.Context, record *Record, callback func(*ProducerResult, error))
	SendBatch(ctx context.Context, records []*Record) ([]*ProducerResult, error)
	Flush(timeout time.Duration) error
	Close() error
}

type ProducerConfig struct {
	Acks              int
	Compression       CompressionType
	BatchSize         int
	LingerMS          time.Duration
	RetryBackoff      time.Duration
	MaxRetries        int
	MaxInFlight       int
	DeliveryTimeout   time.Duration
	EnableIdempotence bool
	PartitionStrategy string
}

type ProducerResult struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

type Producer struct {
	client *Client
	config *ProducerConfig
	mu     sync.RWMutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
}

func NewDefaultProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Acks:              1,
		Compression:       CompressionNone,
		BatchSize:         16384,
		LingerMS:          100 * time.Millisecond,
		RetryBackoff:      100 * time.Millisecond,
		MaxRetries:        3,
		MaxInFlight:       5,
		DeliveryTimeout:   30 * time.Second,
		EnableIdempotence: false,
		PartitionStrategy: "round-robin",
	}
}

func newProducer(client *Client, opts ...ProducerOption) (*Producer, error) {
	config := NewDefaultProducerConfig()

	for _, opt := range opts {
		opt(config)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Producer{
		client: client,
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (p *Producer) Send(ctx context.Context, record *Record) (*ProducerResult, error) {
	if err := p.validateRecord(record); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, fmt.Errorf("producer closed")
	default:
		record.Metadata.ProducedAt = time.Now()

		result := &ProducerResult{
			Topic:     record.Topic,
			Partition: record.Partition,
			Offset:    0,
			Timestamp: record.Metadata.ProducedAt,
		}

		return result, nil
	}
}

func (p *Producer) SendAsync(ctx context.Context, record *Record, callback func(*ProducerResult, error)) {
	go func() {
		result, err := p.Send(ctx, record)
		if callback != nil {
			callback(result, err)
		}
	}()
}

func (p *Producer) SendBatch(ctx context.Context, records []*Record) ([]*ProducerResult, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("empty record batch")
	}

	results := make([]*ProducerResult, 0, len(records))
	for _, record := range records {
		result, err := p.Send(ctx, record)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (p *Producer) validateRecord(record *Record) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}

	if record.Topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	if len(record.Value) == 0 {
		return fmt.Errorf("record value cannot be empty")
	}

	return nil
}

func (p *Producer) Flush(timeout time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("producer closed")
	}

	// Implementation for flushing pending messages
	return nil
}

func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	if p.cancel != nil {
		p.cancel()
	}

	return p.Flush(p.config.DeliveryTimeout)
}

type ProducerOption func(*ProducerConfig)

func WithAcks(acks int) ProducerOption {
	return func(config *ProducerConfig) {
		config.Acks = acks
	}
}

func WithCompression(compression CompressionType) ProducerOption {
	return func(config *ProducerConfig) {
		config.Compression = compression
	}
}

func WithBatchSize(size int) ProducerOption {
	return func(config *ProducerConfig) {
		config.BatchSize = size
	}
}

func WithLingerMS(duration time.Duration) ProducerOption {
	return func(config *ProducerConfig) {
		config.LingerMS = duration
	}
}

func WithMaxRetries(retries int) ProducerOption {
	return func(config *ProducerConfig) {
		config.MaxRetries = retries
	}
}

func WithIdempotence(enable bool) ProducerOption {
	return func(config *ProducerConfig) {
		config.EnableIdempotence = enable
	}
}
