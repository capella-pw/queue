package db

import (
	"context"
	"sort"

	"github.com/capella-pw/queue/cn"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

type SimpleIndex struct {
	Name      string                      `json:"name"`
	ISegments []*ISimpleIndexBlockSegment `json:"isegments"`
	SSegments []*SSimpleIndexBlockSegment `json:"ssegments"`

	mx mfs.PMutex

	// case nil then ignore
	CheckPermissionFunc func(user cn.CapUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) `json:"-"`
	// case nil then ignore
	ThrowErrorFunc func(err *mft.Error) bool `json:"-"`
}

type ISimpleIndexKeyStorage struct {
	Key      int64          `json:"key"`
	IRecords []IIndexRecord `json:"ir"`
	SRecords []SIndexRecord `json:"sr"`
}

type ISimpleIndexBlock struct {
	ID int64 `json:"block_id"`

	KeyStart int64 `json:"key_start"`
	KeyEnd   int64 `json:"key_end"`

	CountKeys  int `json:"count_keys"`
	CountItems int `json:"count_items"`

	Keys []*ISimpleIndexKeyStorage `json:"-"`
}

type ISimpleIndexBlockSegment struct {
	Segment int64                `json:"segments"`
	Blocks  []*ISimpleIndexBlock `json:"block"`
}

type SSimpleIndexKeyStorage struct {
	Key      string         `json:"key"`
	IRecords []IIndexRecord `json:"ir"`
	SRecords []SIndexRecord `json:"sr"`
}

type SSimpleIndexBlock struct {
	ID int64 `json:"block_id"`

	KeyStart string `json:"key_start"`
	KeyEnd   string `json:"key_end"`

	CountKeys  int `json:"count_keys"`
	CountItems int `json:"count_items"`

	Keys []*SSimpleIndexKeyStorage `json:"-"`
}

type SSimpleIndexBlockSegment struct {
	Segment int64                `json:"segments"`
	Blocks  []*SSimpleIndexBlock `json:"block"`
}

func (si *SimpleIndex) ThrowError(err *mft.Error) bool {
	if si == nil {
		return false
	}
	if si.ThrowErrorFunc == nil {
		return false
	}
	return si.ThrowErrorFunc(err)
}

func (si *SimpleIndex) CheckPermission(user cn.CapUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) {
	if si == nil {
		return false, nil
	}

	if si.CheckPermissionFunc == nil {
		return true, nil
	}

	return si.CheckPermissionFunc(user, objectType, action, objectName)
}

func (si *SimpleIndex) CheckOneOfPermission(user cn.CapUser, objectType string, objectName string, actions ...string) (allowed bool, err *mft.Error) {
	if si == nil {
		return false, nil
	}

	if si.CheckPermissionFunc == nil {
		return true, nil
	}

	for _, action := range actions {
		allowed, err = si.CheckPermissionFunc(user, objectType, action, objectName)
		if err != nil {
			return allowed, err
		}
		if allowed {
			return allowed, err
		}
	}

	return false, nil
}

func (si *SimpleIndex) IAdd(ctx context.Context, user cn.CapUser, req IAddIndexRequest) (err *mft.Error) {
	allowed, err := si.CheckOneOfPermission(user, IndexObjectType, si.Name, INDEX_OWNER, IAdd)
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForCapUser(user, 10401001)
	}

	if !si.mx.RTryLock(ctx) {
		return GenerateError(10401000)
	}

	var seg int64
	if req.IRecord != nil && req.SRecord != nil {
		// TODO: check segments
	}
	if req.IRecord == nil && req.SRecord == nil {
		// TODO: errors
	}
	if req.IRecord != nil {
		seg = req.IRecord.Segment
	}
	if req.SRecord != nil {
		seg = req.SRecord.Segment
	}
	ix := sort.Search(len(si.ISegments), func(i int) bool {
		return si.ISegments[i].Segment >= seg
	})
	if ix < 0 || ix >= len(si.ISegments) || si.ISegments[ix].Segment != seg {
		// add new segment
		// fill ix
	}
	err = si.ISegments[ix].Add(ctx, si, req)

	// save

	return err
}
func (si *SimpleIndex) SAdd(ctx context.Context, user cn.CapUser, req SAddIndexRequest) (err *mft.Error) {
	allowed, err := si.CheckOneOfPermission(user, IndexObjectType, si.Name, INDEX_OWNER, SAdd)
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForCapUser(user, 10401051)
	}

	if !si.mx.RTryLock(ctx) {
		return GenerateError(10401050)
	}

	var seg int64
	if req.IRecord != nil && req.SRecord != nil {
		// TODO: check segments
	}
	if req.IRecord == nil && req.SRecord == nil {
		// TODO: errors
	}
	if req.IRecord != nil {
		seg = req.IRecord.Segment
	}
	if req.SRecord != nil {
		seg = req.SRecord.Segment
	}
	ix := sort.Search(len(si.SSegments), func(i int) bool {
		return si.SSegments[i].Segment >= seg
	})
	if ix < 0 || ix >= len(si.SSegments) || si.SSegments[ix].Segment != seg {
		// add new segment
		// fill ix
	}

	err = si.SSegments[ix].Add(ctx, si, req)

	// save

	return err
}

func (ibs *ISimpleIndexBlockSegment) Add(ctx context.Context, si *SimpleIndex, req IAddIndexRequest) (err *mft.Error) {
	ix := sort.Search(len(ibs.Blocks), func(i int) bool {
		return ibs.Blocks[i].KeyStart <= req.Key
	})

	if ix < 0 || ix >= len(ibs.Blocks) {
		// add new block
		// fill ix
	}

	// TODO: check block size and add new block

	// --------

	err = ibs.Blocks[ix].Add(ctx, si, ibs, req)

	return err
}

func (ibs *SSimpleIndexBlockSegment) Add(ctx context.Context, si *SimpleIndex, req SAddIndexRequest) (err *mft.Error) {
	ix := sort.Search(len(ibs.Blocks), func(i int) bool {
		return ibs.Blocks[i].KeyStart <= req.Key
	})

	if ix < 0 || ix >= len(ibs.Blocks) {
		// add new block
		// fill ix
	}

	// TODO: check block size and add new block

	// --------

	err = ibs.Blocks[ix].Add(ctx, si, ibs, req)

	return err
}

func (ib *ISimpleIndexBlock) Add(ctx context.Context, si *SimpleIndex, ibs *ISimpleIndexBlockSegment, req IAddIndexRequest) (err *mft.Error) {
	ix := sort.Search(len(ib.Keys), func(i int) bool {
		return ib.Keys[i].Key <= req.Key
	})
	if ix < 0 || ix >= len(ib.Keys) {
		// add new record
		// fill ix
	}

	// TODO: check record exists

	// --------

	err = ib.Keys[ix].Add(ctx, si, ibs, ib, req)
	return err
}
func (ib *SSimpleIndexBlock) Add(ctx context.Context, si *SimpleIndex, ibs *SSimpleIndexBlockSegment, req SAddIndexRequest) (err *mft.Error) {
	ix := sort.Search(len(ib.Keys), func(i int) bool {
		return ib.Keys[i].Key <= req.Key
	})
	if ix < 0 || ix >= len(ib.Keys) {
		// add new record
		// fill ix
	}

	// TODO: check record exists

	// --------

	err = ib.Keys[ix].Add(ctx, si, ibs, ib, req)
	return err
}

func (ics *ISimpleIndexKeyStorage) Add(ctx context.Context, si *SimpleIndex, ibs *ISimpleIndexBlockSegment,
	ib *ISimpleIndexBlock, req IAddIndexRequest) (err *mft.Error) {
	ics.IRecords = IRecordsAdd(ics.IRecords, req.IRecord)
	ics.SRecords = SRecordsAdd(ics.SRecords, req.SRecord)

	return nil
}
func (ics *SSimpleIndexKeyStorage) Add(ctx context.Context, si *SimpleIndex, ibs *SSimpleIndexBlockSegment,
	ib *SSimpleIndexBlock, req SAddIndexRequest) (err *mft.Error) {
	ics.IRecords = IRecordsAdd(ics.IRecords, req.IRecord)
	ics.SRecords = SRecordsAdd(ics.SRecords, req.SRecord)

	return nil
}

func IRecordsAdd(records []IIndexRecord, record *IIndexRecord) (res []IIndexRecord) {
	if record == nil {
		return records
	}
	if len(records) == 0 {
		res = append(records, *record)
		return res
	}
	ix := sort.Search(len(records), func(i int) bool {
		if records[i].ID > record.ID {
			return true
		}

		if records[i].ID == record.ID && records[i].Version >= record.Version {
			return true
		}

		return false
	})

	if ix < 0 {
		res = append(res, *record)
		res = append(res, records...)
		return res
	}
	if ix >= len(records) {
		res = records
		res = append(res, *record)
		return res
	}

	if records[ix].ID == record.ID && records[ix].Version == record.Version {
		return records
	}

	res = append(res, records[:ix]...)
	res = append(res, *record)
	res = append(res, records[ix:]...)
	return res
}

func SRecordsAdd(records []SIndexRecord, record *SIndexRecord) (res []SIndexRecord) {
	if record == nil {
		return records
	}
	if len(records) == 0 {
		res = append(records, *record)
		return res
	}
	ix := sort.Search(len(records), func(i int) bool {
		if records[i].ID > record.ID {
			return true
		}

		if records[i].ID == record.ID && records[i].Version >= record.Version {
			return true
		}

		return false
	})

	if ix < 0 {
		res = append(res, *record)
		res = append(res, records...)
		return res
	}
	if ix >= len(records) {
		res = records
		res = append(res, *record)
		return res
	}

	if records[ix].ID == record.ID && records[ix].Version == record.Version {
		return records
	}

	res = append(res, records[:ix]...)
	res = append(res, *record)
	res = append(res, records[ix:]...)
	return res
}

func (si *SimpleIndex) Save(ctx context.Context, user cn.CapUser) (err *mft.Error) {
	// Get struct
	// Save all blocks
	// Save struct
	return nil
}
