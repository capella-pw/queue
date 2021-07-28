package db

import (
	"context"
	"time"

	"github.com/myfantasy/mft"
)

type IAddStorageRequest struct {
	Key    int64   `json:"key"`
	Record IRecord `json:"ir,omitempty"`
	// ConflictInit if true and exists conflict record then conflict will be begin
	// ConflictInit if false and exists conflict record then error
	ConflictInit bool `json:"conflict_init,omitempty"`
}

type SAddStorageRequest struct {
	Key    int64   `json:"key"`
	Record SRecord `json:"ir,omitempty"`
	// ConflictInit if true and exists conflict record then conflict will be begin
	// ConflictInit if false and exists conflict record then error
	ConflictInit bool `json:"conflict_init,omitempty"`
}

type GetItemRequest struct {
	// Segment - segment for search; if NoSegment == true segments is not used
	Segment int64 `json:"segment,omitempty"`
	// NoSegment if true segments is not used
	NoSegment bool `json:"no_segment,omitempty"`

	// GetList if set then Key => FromKey and used ToKey
	GetList bool `json:"get_list,omitempty"`
}

type IGetItemRequest struct {
	Key int64 `json:"key"`
	GetItemRequest
	// ToKey - get values from Key - to ToKey; requare GetList flag
	ToKey int64 `json:"to_key,omitempty"`
}

type SGetItemRequest struct {
	Key string `json:"key"`
	GetItemRequest
	// ToKey - get values from Key - to ToKey; requare GetList flag
	ToKey string `json:"to_key,omitempty"`
}

type ReindexTask struct {
	Start      *time.Time `json:"start"`
	End        *time.Time `json:"end"`
	StartedNow bool       `json:"started_now"`
}

type Storage interface {
	ISet(ctx context.Context, user DBUser, req IAddStorageRequest) (err *mft.Error)
	SSet(ctx context.Context, user DBUser, req SAddStorageRequest) (err *mft.Error)

	ISetList(ctx context.Context, user DBUser, req []IAddStorageRequest) (err *mft.Error)
	SSetList(ctx context.Context, user DBUser, req []SAddStorageRequest) (err *mft.Error)

	IGet(ctx context.Context, user DBUser, req IGetItemRequest) (res map[int64]*IItem, err *mft.Error)
	SGet(ctx context.Context, user DBUser, req SGetItemRequest) (res map[string]*SItem, err *mft.Error)

	// ReIndexItems - reindex current items: add index by last version and remove by previous
	ReIndexItems(ctx context.Context, user DBUser, req []IGetItemRequest) (err *mft.Error)
	// ReIndexAllTaskDo starts reindex if starts by this command then StartedNow = true
	ReIndexAllTaskDo(ctx context.Context, user DBUser) (resp ReindexTask, err *mft.Error)
	ReIndexAllTaskGet(ctx context.Context, user DBUser) (resp ReindexTask, err *mft.Error)

	OptimizeAll(ctx context.Context, user DBUser, deph int) (err *mft.Error)
	Optimize(ctx context.Context, user DBUser, segment, deph int) (err *mft.Error)
}
