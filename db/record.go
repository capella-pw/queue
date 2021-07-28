package db

import (
	"encoding/json"
	"time"
)

type Record struct {
	Version   int64               `json:"version"`
	Segment   int64               `json:"segment,omitempty"`
	Data      json.RawMessage     `json:"data"`
	IIndex    map[string][]int64  `json:"iindex,omitempty"`
	SIndex    map[string][]string `json:"sindex,omitempty"`
	IsDeleted bool                `json:"is_deleted,omitempty"`
	// OverVersions - Previous versions, this item is child of this versions
	OverVersions []int64   `json:"over_version,omitempty"`
	Date         time.Time `json:"date"`
}

type IRecord struct {
	ID int64 `json:"id"`
	Record
}

type SRecord struct {
	ID string `json:"id"`
	Record
}

func (r *IRecord) ToJson() json.RawMessage {
	b, er0 := json.MarshalIndent(r, "", "  ")
	if er0 != nil {
		panic(GenerateErrorE(10400000, er0))
	}
	return b
}

func (r *SRecord) ToJson() json.RawMessage {
	b, er0 := json.MarshalIndent(r, "", "  ")
	if er0 != nil {
		panic(GenerateErrorE(10400001, er0))
	}
	return b
}

type IItem struct {
	ID int64 `json:"id"`

	// Current version
	Version int64 `json:"version"`
	// Records actual records Version is key
	Records map[int64]*IRecord `json:"records"`

	ConflictVersions []int64 `json:"conflict,omitempty"`
}

type SItem struct {
	ID int64 `json:"id"`

	// Current version
	Version int64 `json:"version"`
	// Records actual records Version is key
	Records map[int64]*SRecord `json:"records"`

	ConflictVersions []int64 `json:"conflict,omitempty"`
}

func (r *IItem) ToJson() json.RawMessage {
	b, er0 := json.MarshalIndent(r, "", "  ")
	if er0 != nil {
		panic(GenerateErrorE(10400002, er0))
	}
	return b
}

func (r *SItem) ToJson() json.RawMessage {
	b, er0 := json.MarshalIndent(r, "", "  ")
	if er0 != nil {
		panic(GenerateErrorE(10400003, er0))
	}
	return b
}

type IIndexRecord struct {
	ID      int64 `json:"id"`
	Version int64 `json:"version"`
	Segment int64 `json:"segment,omitempty"`
}

type SIndexRecord struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
	Segment int64  `json:"segment,omitempty"`
}
