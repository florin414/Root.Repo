package sdk

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ClientInterface interface {
	NewProducer(opts ...ProducerOption) (*Producer, error)
	NewConsumer(groupID string, opts ...ConsumerOption) (*Consumer, error)
	GetTopicMetadata(ctx context.Context, topic string) ([]PartitionMetadata, error)
	Close() error
}

type ClientConfig struct {
	Brokers          []string
	ClientID         string
	ConnectTimeout   time.Duration
	RequestTimeout   time.Duration
	RetryMax         int
	RetryBackoff     time.Duration
	RequiredAcks     int
	MaxInflight      int
	SecurityProtocol string
	SASL             *SASLConfig
	TLS              *TLSConfig
}

type SASLConfig struct {
	Enable    bool
	Mechanism string
	Username  string
	Password  string
}

type TLSConfig struct {
	Enable             bool
	CertFile           string
	KeyFile            string
	CAFile             string
	VerifySSL          bool
	InsecureSkipVerify bool
}

func NewDefaultConfig() *ClientConfig {
	return &ClientConfig{
		ConnectTimeout:   10 * time.Second,
		RequestTimeout:   30 * time.Second,
		RetryMax:         3,
		RetryBackoff:     100 * time.Millisecond,
		RequiredAcks:     1,
		MaxInflight:      5,
		SecurityProtocol: "plaintext",
	}
}

type Client struct {
	config *ClientConfig
	mu     sync.RWMutex
	closed bool
}

func NewClient(config *ClientConfig) (*Client, error) {
	if config == nil {
		config = NewDefaultConfig()
	}

	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 10 * time.Second
	}

	return &Client{
		config: config,
	}, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	return nil
}

func (c *Client) NewProducer(opts ...ProducerOption) (*Producer, error) {
	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}
	return newProducer(c, opts...)
}

func (c *Client) NewConsumer(groupID string, opts ...ConsumerOption) (*Consumer, error) {
	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}
	return newConsumer(c, groupID, opts...)
}

func (c *Client) GetTopicMetadata(ctx context.Context, topic string) ([]PartitionMetadata, error) {
	if c.closed {
		return nil, fmt.Errorf("client is closed")
	}
	return nil, nil
}
