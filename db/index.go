package db

import (
	"context"

	"github.com/capella-pw/queue/cn"
	"github.com/myfantasy/mft"
)

const (
	IndexObjectType = "INDEX"
)

const (
	INDEX_OWNER = "IX_OWNER"
	IAdd        = "I_ADD"
	SAdd        = "S_ADD"
)

type IAddIndexRequest struct {
	Key      int64         `json:"key"`
	IRecord  *IIndexRecord `json:"ir,omitempty"`
	SRecord  *SIndexRecord `json:"sr,omitempty"`
	SaveMode cn.SaveMode   `json:"save_mode,omitempty"`
}
type SAddIndexRequest struct {
	Key      string        `json:"key"`
	IRecord  *IIndexRecord `json:"ir,omitempty"`
	SRecord  *SIndexRecord `json:"sr,omitempty"`
	SaveMode cn.SaveMode   `json:"save_mode,omitempty"`
}

type GetIndexRequest struct {
	// Segment - segment for search; if NoSegment == true segments is not used
	Segment int64 `json:"segment,omitempty"`
	// NoSegment if true segments is not used
	NoSegment bool `json:"no_segment,omitempty"`

	// GetList if set then Key => FromKey and used ToKey
	GetList bool `json:"get_list,omitempty"`
}

type IGetIndexRequest struct {
	Key int64 `json:"key"`
	GetIndexRequest
	// ToKey - get values from Key - to ToKey; requare GetList flag
	ToKey int64 `json:"to_key,omitempty"`
}

type SGetIndexRequest struct {
	Key string `json:"key"`
	GetIndexRequest
	// ToKey - get values from Key - to ToKey; requare GetList flag
	ToKey string `json:"to_key,omitempty"`
}

type GetIndexResponce struct {
	IRecords []IIndexRecord `json:"ir,omitempty"`
	SRecords []SIndexRecord `json:"sr,omitempty"`
}

type OptimizeIndexRequest struct {
	Segment int64 `json:"segment,omitempty"`
	Deph    int   `json:"deph,omitempty"`
}

type Index interface {
	Name(ctx context.Context, user cn.CapUser) (name string, err *mft.Error)
	NameSet(ctx context.Context, user cn.CapUser, name string) (err *mft.Error)

	IAdd(ctx context.Context, user cn.CapUser, req IAddIndexRequest) (err *mft.Error)
	SAdd(ctx context.Context, user cn.CapUser, req SAddIndexRequest) (err *mft.Error)

	IAddList(ctx context.Context, user cn.CapUser, req []IAddIndexRequest) (err *mft.Error)
	SAddList(ctx context.Context, user cn.CapUser, req []SAddIndexRequest) (err *mft.Error)

	IGet(ctx context.Context, user cn.CapUser, req IGetIndexRequest) (res map[int64]GetIndexResponce, err *mft.Error)
	SGet(ctx context.Context, user cn.CapUser, req SGetIndexRequest) (res map[string]GetIndexResponce, err *mft.Error)

	IDelete(ctx context.Context, user cn.CapUser, req IAddIndexRequest) (err *mft.Error)
	SDelete(ctx context.Context, user cn.CapUser, req SAddIndexRequest) (err *mft.Error)

	IDeleteList(ctx context.Context, user cn.CapUser, req []IAddIndexRequest) (err *mft.Error)
	SDeleteList(ctx context.Context, user cn.CapUser, req []SAddIndexRequest) (err *mft.Error)

	OptimizeAll(ctx context.Context, user cn.CapUser, deph int) (err *mft.Error)
	Optimize(ctx context.Context, user cn.CapUser, req OptimizeIndexRequest) (err *mft.Error)
}
