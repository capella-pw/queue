package queue

import (
	"context"
	"time"

	"github.com/myfantasy/mft"
	"github.com/myfantasy/segment"
)

// MessageWithMeta one message with meta
type MessageWithMeta struct {
	ID int64 `json:"id"`
	// ExternalID - source id, when 0 then equal ID
	ExternalID int64     `json:"eid,omitempty"`
	ExternalDt int64     `json:"s_dt,omitempty"`
	Dt         time.Time `json:"dt"`
	Message    []byte    `json:"msg"`
	Source     string    `json:"src,omitempty"`
	IsSaved    bool      `json:"is_saved"`
	Segment    int64     `json:"sg,omitempty"`
}

// MessageOnlyMeta one message only meta
type MessageOnlyMeta struct {
	ID int64 `json:"id"`
	// ExternalID - source id, when 0 then equal ID
	ExternalID int64     `json:"eid,omitempty"`
	ExternalDt int64     `json:"s_dt,omitempty"`
	Dt         time.Time `json:"dt"`
	Source     string    `json:"src,omitempty"`
	IsSaved    bool      `json:"is_saved"`
	Segment    int64     `json:"sg,omitempty"`
}

// MessageWithMeta one message with meta
type Message struct {
	ExternalID int64  `json:"eid,omitempty"`
	ExternalDt int64  `json:"s_dt,omitempty"`
	Message    []byte `json:"msg"`
	Source     string `json:"src,omitempty"`
	Segment    int64  `json:"sg,omitempty"`
}

// Queue - queue of messages
type Queue interface {
	Add(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, segment int64, saveMode int) (id int64, err *mft.Error)
	AddList(ctx context.Context, messages []Message, saveMode int) (ids []int64, err *mft.Error)

	// Get - gets messages from queue not more then cntLimit count and id more idStart
	// returns messages == nil when no elements
	Get(ctx context.Context, idStart int64, cntLimit int) (messages []*MessageWithMeta, err *mft.Error)

	// GetSegment - gets messages from queue not more then cntLimit count and id more idStart
	// returns messages == nil when no elements
	// message should be in segment
	// lastId last readed message ID from queue
	GetSegment(ctx context.Context, idStart int64, cntLimit int,
		segments *segment.Segments,
	) (messages []*MessageWithMeta, lastId int64, err *mft.Error)

	// SaveAll save all waiting for save block and metadata and else
	SaveAll(ctx context.Context) (err *mft.Error)

	// AddUnique message to queue
	// externalDt is unix time
	// externalID is source id (should be != 0 !!!!)
	AddUnique(ctx context.Context, message []byte, externalID int64, externalDt int64, source string, segment int64, saveMode int) (id int64, err *mft.Error)
	AddUniqueList(ctx context.Context, messages []Message, saveMode int) (ids []int64, err *mft.Error)

	// SubscriberSetLastRead - set last read info
	// if id == 0 remove subscribe
	SubscriberSetLastRead(ctx context.Context, subscriber string, id int64, saveMode int) (err *mft.Error)

	// SubscriberGetLastRead - get last read info
	SubscriberGetLastRead(ctx context.Context, subscriber string) (id int64, err *mft.Error)

	// SubscriberAddReplicaMember - add member to replica. Replica is group to control replication
	SubscriberAddReplicaMember(ctx context.Context, subscriber string) (err *mft.Error)

	// SubscriberRemoveReplicaMember - remove member from replica. Replica is group to control replication
	SubscriberRemoveReplicaMember(ctx context.Context, subscriber string) (err *mft.Error)

	// SubscriberGetReplicaCount - get how many members from replica get message. Replica is group to control replication
	SubscriberGetReplicaCount(ctx context.Context, id int64) (cnt int, err *mft.Error)
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
		Segment:    msg.Segment,
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
		Segment:    msg.Segment,
	}

	return out
}

// CopyOM copy message to QueueMessageOnlyMeta
func (msg *MessageWithMeta) ToMessage() Message {
	out := Message{
		ExternalID: msg.ExternalID,
		ExternalDt: msg.ExternalDt,
		Source:     msg.Source,
		Message:    msg.Message,
		Segment:    msg.Segment,
	}

	return out
}
