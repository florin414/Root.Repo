package sdk

import (
	"encoding/json"
	"time"
)

type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Metadata  RecordMetadata
	Timestamp time.Time
}

type RecordMetadata struct {
	RecordID      string
	CorrelationID string
	CausationID   string
	ProducedBy    string
	ProducedAt    time.Time
	SourceService string
	SourceHost    string
	Version       string
	Sequence      int64
	RetryCount    int
	DLQCount      int
	ConsumedAt    time.Time
	ProcessedAt   time.Time
}

type PartitionMetadata struct {
	Topic     string
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
}

type AuditTrail struct {
	CorrelationID string
	RecordID      string
	ParentID      string
	Operation     string
	Service       string
	Timestamp     time.Time
	Duration      time.Duration
	Status        string
	ErrorMessage  string
}

type RecordContext struct {
	CorrelationID string
	CausationID   string
	RecordID      string
	Timestamp     time.Time
}

func NewRecord(topic string, value []byte) *Record {
	return &Record{
		Topic:     topic,
		Value:     value,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
		Metadata: RecordMetadata{
			RecordID:      generateID(),
			ProducedAt:    time.Now(),
			SourceService: getServiceName(),
			SourceHost:    getHostname(),
			Version:       "1.0",
		},
	}
}

func NewRecordWithContext(topic string, value []byte, ctx RecordContext) *Record {
	record := NewRecord(topic, value)
	record.Metadata.CorrelationID = ctx.CorrelationID
	record.Metadata.CausationID = ctx.RecordID
	return record
}

func (r *Record) WithCorrelationID(correlationID string) *Record {
	r.Metadata.CorrelationID = correlationID
	return r
}

func (r *Record) WithKey(key []byte) *Record {
	r.Key = key
	return r
}

func (r *Record) WithHeader(key, value string) *Record {
	r.Headers[key] = value
	return r
}

func (r *Record) WithPartition(partition int32) *Record {
	r.Partition = partition
	return r
}

func (r *Record) Encode() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Record) Decode(data []byte) error {
	return json.Unmarshal(data, r)
}

func (r *Record) CreateAudit(operation string, status string) AuditTrail {
	return AuditTrail{
		CorrelationID: r.Metadata.CorrelationID,
		RecordID:      r.Metadata.RecordID,
		ParentID:      r.Metadata.CausationID,
		Operation:     operation,
		Service:       r.Metadata.SourceService,
		Timestamp:     time.Now(),
		Status:        status,
	}
}

func (r *Record) CreateContext() RecordContext {
	return RecordContext{
		CorrelationID: r.Metadata.CorrelationID,
		CausationID:   r.Metadata.CausationID,
		RecordID:      r.Metadata.RecordID,
		Timestamp:     time.Now(),
	}
}

type ConsumerRecord struct {
	*Record
	Consumer string
	Group    string
}

type ProducerRecord struct {
	*Record
	Producer string
}
