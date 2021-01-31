package queue

import (
	"context"
	"time"

	"github.com/myfantasy/mft"
)

// MessageWithMeta one message with meta
type MessageWithMeta struct {
	ID int64 `json:"id"`
	// ExternalID, source id, when 0 then equal ID
	ExternalID int64     `json:"eid,omitempty"`
	ExternalDt int64     `json:"s_dt,omitempty"`
	Dt         time.Time `json:"dt"`
	Message    []byte    `json:"msg"`
	Source     string    `json:"src,omitempty"`
	IsSaved    bool      `json:"is_saved"`
}

// MessageOnlyMeta one message only meta
type MessageOnlyMeta struct {
	ID int64 `json:"id"`
	// ExternalID, source id, when 0 then equal ID
	ExternalID int64     `json:"eid,omitempty"`
	ExternalDt int64     `json:"s_dt,omitempty"`
	Dt         time.Time `json:"dt"`
	Source     string    `json:"src,omitempty"`
	IsSaved    bool      `json:"is_saved"`
}

// Queue - queue of messages
type Queue interface {
	Add(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, saveMode int) (id int64, err *mft.Error)

	// Get - gets messages from queue not more then cntLimit count and id more idStart
	// returns messages == nil when no elements
	Get(ctx context.Context, idStart int64, cntLimit int) (messages []*MessageWithMeta, err *mft.Error)

	// SaveAll save all waiting for save block and metadata and else
	SaveAll(ctx context.Context) (err *mft.Error)

	// AddUnique message to queue
	// externalDt is unix time
	// externalID is source id (should be != 0 !!!!)
	AddUnique(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, saveMode int) (id int64, err *mft.Error)

	// SubscriberSetLastRead - set last read info
	// if id == 0 remove subscribe
	SubscriberSetLastRead(ctx context.Context, subscriber string, id int64, saveMode int) (err *mft.Error)

	// SubscriberGetLastRead - get last read info
	SubscriberGetLastRead(ctx context.Context, subscriber string) (id int64, err *mft.Error)
}

// CopyWM copy message to QueueMessageWithMeta
func (msg *SimpleQueueMessage) CopyWM() *MessageWithMeta {
	out := &MessageWithMeta{
		ID:         msg.ID,
		ExternalID: msg.ExternalID,
		Dt:         msg.Dt,
		ExternalDt: msg.ExternalDt,
		Message:    msg.Message,
		Source:     msg.Source,
	}

	return out
}

// CopyOM copy message to QueueMessageOnlyMeta
func (msg *MessageWithMeta) CopyOM() *MessageOnlyMeta {
	out := &MessageOnlyMeta{
		ID:         msg.ID,
		ExternalID: msg.ExternalID,
		Dt:         msg.Dt,
		ExternalDt: msg.ExternalDt,
		Source:     msg.Source,
	}

	return out
}
