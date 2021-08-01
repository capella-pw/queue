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

	IdGenerator *mft.G `json:"-"`

	MaxItemsCountInBlock int `json:"max_itm_cnt_in_block"`
	MaxKeysCountInBlock  int `json:"max_key_cnt_in_block"`

	LastSavedVersion int64 `json:"last_saved_version"`
	LastVersion      int64 `json:"-"`
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

	LastSavedVersion int64 `json:"last_saved_version"`
	LastVersion      int64 `json:"-"`

	mx mfs.PMutex
}

type ISimpleIndexBlockSegment struct {
	Segment int64                `json:"segments"`
	Blocks  []*ISimpleIndexBlock `json:"-"`

	LastSavedVersion int64 `json:"last_saved_version"`
	LastVersion      int64 `json:"-"`

	mx mfs.PMutex
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

	LastSavedVersion int64 `json:"last_saved_version"`
	LastVersion      int64 `json:"-"`

	mx mfs.PMutex
}

type SSimpleIndexBlockSegment struct {
	Segment int64                `json:"segments"`
	Blocks  []*SSimpleIndexBlock `json:"-"`

	LastSavedVersion int64 `json:"last_saved_version"`
	LastVersion      int64 `json:"-"`

	mx mfs.PMutex
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

	var seg int64
	if req.IRecord != nil && req.SRecord != nil {
		if req.IRecord.Segment != req.SRecord.Segment {
			return GenerateError(10401002, req.IRecord.Segment, req.SRecord.Segment)
		}
	}
	if req.IRecord == nil && req.SRecord == nil {
		return GenerateError(10401003)
	}
	if req.IRecord != nil {
		seg = req.IRecord.Segment
	}
	if req.SRecord != nil {
		seg = req.SRecord.Segment
	}

	if !si.mx.RTryLock(ctx) {
		return GenerateError(10401000)
	}

	ix := sort.Search(len(si.ISegments), func(i int) bool {
		return si.ISegments[i].Segment >= seg
	})
	if ix >= len(si.ISegments) || si.ISegments[ix].Segment != seg {
		if !si.mx.TryPromote(ctx) {
			return GenerateError(10401004)
		}

		ix = sort.Search(len(si.ISegments), func(i int) bool {
			return si.ISegments[i].Segment >= seg
		})
		if ix >= len(si.ISegments) {
			si.ISegments = append(si.ISegments, &ISimpleIndexBlockSegment{
				Segment:     seg,
				Blocks:      make([]*ISimpleIndexBlock, 0),
				LastVersion: si.IdGenerator.RvGetPart(),
			})

			si.LastVersion = si.IdGenerator.RvGetPart()
		}
		if si.ISegments[ix].Segment != seg {
			segments := make([]*ISimpleIndexBlockSegment, 0, len(si.ISegments)+1)

			segments = append(segments, si.ISegments[:ix]...)
			segments = append(segments, &ISimpleIndexBlockSegment{
				Segment:     seg,
				Blocks:      make([]*ISimpleIndexBlock, 0),
				LastVersion: si.IdGenerator.RvGetPart(),
			})
			segments = append(segments, si.ISegments[ix:]...)

			si.LastVersion = si.IdGenerator.RvGetPart()
		}

		si.mx.Reduce()
	}

	segment := si.ISegments[ix]
	si.mx.RUnlock()

	err = segment.Add(ctx, si, req)

	// save

	return err
}
func (si *SimpleIndex) SAdd(ctx context.Context, user cn.CapUser, req SAddIndexRequest) (err *mft.Error) {
	allowed, err := si.CheckOneOfPermission(user, IndexObjectType, si.Name, INDEX_OWNER, IAdd)
	if err != nil {
		return err
	}
	if !allowed {
		return GenerateErrorForCapUser(user, 10401051)
	}

	var seg int64
	if req.IRecord != nil && req.SRecord != nil {
		if req.IRecord.Segment != req.SRecord.Segment {
			return GenerateError(10401052, req.IRecord.Segment, req.SRecord.Segment)
		}
	}
	if req.IRecord == nil && req.SRecord == nil {
		return GenerateError(10401003)
	}
	if req.IRecord != nil {
		seg = req.IRecord.Segment
	}
	if req.SRecord != nil {
		seg = req.SRecord.Segment
	}

	if !si.mx.RTryLock(ctx) {
		return GenerateError(10401050)
	}

	ix := sort.Search(len(si.SSegments), func(i int) bool {
		return si.SSegments[i].Segment >= seg
	})
	if ix >= len(si.SSegments) || si.SSegments[ix].Segment != seg {
		if !si.mx.TryPromote(ctx) {
			return GenerateError(10401054)
		}

		ix = sort.Search(len(si.SSegments), func(i int) bool {
			return si.SSegments[i].Segment >= seg
		})
		if ix >= len(si.SSegments) {
			si.SSegments = append(si.SSegments, &SSimpleIndexBlockSegment{
				Segment:     seg,
				Blocks:      make([]*SSimpleIndexBlock, 0),
				LastVersion: si.IdGenerator.RvGetPart(),
			})

			si.LastVersion = si.IdGenerator.RvGetPart()
		}
		if si.SSegments[ix].Segment != seg {
			segments := make([]*SSimpleIndexBlockSegment, 0, len(si.SSegments)+1)

			segments = append(segments, si.SSegments[:ix]...)
			segments = append(segments, &SSimpleIndexBlockSegment{
				Segment:     seg,
				Blocks:      make([]*SSimpleIndexBlock, 0),
				LastVersion: si.IdGenerator.RvGetPart(),
			})
			segments = append(segments, si.SSegments[ix:]...)

			si.LastVersion = si.IdGenerator.RvGetPart()
		}

		si.mx.Reduce()
	}

	segment := si.SSegments[ix]
	si.mx.RUnlock()

	err = segment.Add(ctx, si, req)

	// save

	return err
}

func (ibs *ISimpleIndexBlockSegment) Add(ctx context.Context, si *SimpleIndex, req IAddIndexRequest) (err *mft.Error) {
	if !ibs.mx.RTryLock(ctx) {
		return GenerateError(10401102)
	}
	defer ibs.mx.RUnlock()

	if len(ibs.Blocks) == 0 {
		if !ibs.mx.TryPromote(ctx) {
			return GenerateError(10401103)
		}
		if len(ibs.Blocks) == 0 {
			ibs.Blocks = []*ISimpleIndexBlock{
				{
					KeyStart: req.Key,
					KeyEnd:   req.Key,
					ID:       si.IdGenerator.RvGetPart(),
				},
			}
			ibs.LastVersion = si.IdGenerator.RvGetPart()
		}
		ibs.mx.Reduce()
	}

	ixS := sort.Search(len(ibs.Blocks), func(i int) bool {
		return ibs.Blocks[i].KeyStart >= req.Key
	})
	ixE := sort.Search(len(ibs.Blocks), func(i int) bool {
		return ibs.Blocks[i].KeyEnd >= req.Key
	})

	var ix int

	if ixS <= ixE {
		if ixE == len(ibs.Blocks) {
			ix = ixE - 1
		} else {
			ix = ixE
		}

		if ibs.Blocks[ix].CountKeys > 1 {
			if !ibs.mx.TryPromote(ctx) {
				return GenerateError(10401104)
			}
			ixS = sort.Search(len(ibs.Blocks), func(i int) bool {
				return ibs.Blocks[i].KeyStart >= req.Key
			})
			ixE = sort.Search(len(ibs.Blocks), func(i int) bool {
				return ibs.Blocks[i].KeyEnd >= req.Key
			})

			if ixS <= ixE {
				if ixE == len(ibs.Blocks) {
					ix = ixE - 1
				} else {
					ix = ixE
				}
			} else {
				ibs.mx.Reduce()
				return GenerateError(10401105)
			}

			if ibs.Blocks[ix].CountKeys >= si.MaxKeysCountInBlock {
				if ibs.Blocks[ix].CountKeys >= si.MaxKeysCountInBlock {
					ib1, ib2 := ibs.Blocks[ix].SplitHalf(si)
					blocks := make([]*ISimpleIndexBlock, 0, len(ibs.Blocks)+1)
					blocks = append(blocks, ibs.Blocks[:ix]...)
					blocks = append(blocks, ib1, ib2)
					blocks = append(blocks, ibs.Blocks[ix+1:]...)
					ibs.Blocks = blocks

					ibs.LastVersion = si.IdGenerator.RvGetPart()

					if req.Key >= ib2.KeyStart {
						ix++
					}
				}
			} else if ibs.Blocks[ix].CountItems >= si.MaxItemsCountInBlock {
				if ibs.Blocks[ix].CountItems >= si.MaxItemsCountInBlock {
					ib1, ib2 := ibs.Blocks[ix].SplitHalf(si)
					blocks := make([]*ISimpleIndexBlock, 0, len(ibs.Blocks)+1)
					blocks = append(blocks, ibs.Blocks[:ix]...)
					blocks = append(blocks, ib1, ib2)
					blocks = append(blocks, ibs.Blocks[ix+1:]...)
					ibs.Blocks = blocks

					ibs.LastVersion = si.IdGenerator.RvGetPart()

					if req.Key >= ib2.KeyStart {
						ix++
					}
				}
			}
			ibs.mx.Reduce()
		}
	}

	if ixS > ixE {
		// Impossible situation
		return GenerateError(10401101)

	}

	err = ibs.Blocks[ix].Add(ctx, si, ibs, req)

	return err
}

func (ibs *SSimpleIndexBlockSegment) Add(ctx context.Context, si *SimpleIndex, req SAddIndexRequest) (err *mft.Error) {
	if !ibs.mx.RTryLock(ctx) {
		return GenerateError(10401152)
	}
	defer ibs.mx.RUnlock()

	if len(ibs.Blocks) == 0 {
		if !ibs.mx.TryPromote(ctx) {
			return GenerateError(10401153)
		}
		if len(ibs.Blocks) == 0 {
			ibs.Blocks = []*SSimpleIndexBlock{
				{
					KeyStart: req.Key,
					KeyEnd:   req.Key,
					ID:       si.IdGenerator.RvGetPart(),
				},
			}
			ibs.LastVersion = si.IdGenerator.RvGetPart()
		}
		ibs.mx.Reduce()
	}

	ixS := sort.Search(len(ibs.Blocks), func(i int) bool {
		return ibs.Blocks[i].KeyStart >= req.Key
	})
	ixE := sort.Search(len(ibs.Blocks), func(i int) bool {
		return ibs.Blocks[i].KeyEnd >= req.Key
	})

	var ix int

	if ixS <= ixE {
		if ixE == len(ibs.Blocks) {
			ix = ixE - 1
		} else {
			ix = ixE
		}

		if ibs.Blocks[ix].CountKeys > 1 {
			if !ibs.mx.TryPromote(ctx) {
				return GenerateError(10401154)
			}
			ixS = sort.Search(len(ibs.Blocks), func(i int) bool {
				return ibs.Blocks[i].KeyStart >= req.Key
			})
			ixE = sort.Search(len(ibs.Blocks), func(i int) bool {
				return ibs.Blocks[i].KeyEnd >= req.Key
			})

			if ixS <= ixE {
				if ixE == len(ibs.Blocks) {
					ix = ixE - 1
				} else {
					ix = ixE
				}
			} else {
				ibs.mx.Reduce()
				return GenerateError(10401155)
			}

			if ibs.Blocks[ix].CountKeys >= si.MaxKeysCountInBlock {
				if ibs.Blocks[ix].CountKeys >= si.MaxKeysCountInBlock {
					ib1, ib2 := ibs.Blocks[ix].SplitHalf(si)
					blocks := make([]*SSimpleIndexBlock, 0, len(ibs.Blocks)+1)
					blocks = append(blocks, ibs.Blocks[:ix]...)
					blocks = append(blocks, ib1, ib2)
					blocks = append(blocks, ibs.Blocks[ix+1:]...)
					ibs.Blocks = blocks

					ibs.LastVersion = si.IdGenerator.RvGetPart()

					if req.Key >= ib2.KeyStart {
						ix++
					}
				}
			} else if ibs.Blocks[ix].CountItems >= si.MaxItemsCountInBlock {
				if ibs.Blocks[ix].CountItems >= si.MaxItemsCountInBlock {
					ib1, ib2 := ibs.Blocks[ix].SplitHalf(si)
					blocks := make([]*SSimpleIndexBlock, 0, len(ibs.Blocks)+1)
					blocks = append(blocks, ibs.Blocks[:ix]...)
					blocks = append(blocks, ib1, ib2)
					blocks = append(blocks, ibs.Blocks[ix+1:]...)
					ibs.Blocks = blocks

					ibs.LastVersion = si.IdGenerator.RvGetPart()

					if req.Key >= ib2.KeyStart {
						ix++
					}
				}
			}
			ibs.mx.Reduce()
		}
	}

	if ixS > ixE {
		// Impossible situation
		return GenerateError(10401151)

	}

	err = ibs.Blocks[ix].Add(ctx, si, ibs, req)

	return err
}

func (ib *ISimpleIndexBlock) SplitHalf(si *SimpleIndex) (ib1 *ISimpleIndexBlock, ib2 *ISimpleIndexBlock) {
	ix2 := len(ib.Keys) / 2
	ib1 = &ISimpleIndexBlock{
		KeyStart:    ib.Keys[0].Key,
		KeyEnd:      ib.Keys[0].Key,
		ID:          si.IdGenerator.RvGetPart(),
		LastVersion: si.IdGenerator.RvGetPart(),
	}
	ib2 = &ISimpleIndexBlock{
		KeyStart:    ib.Keys[ix2+1].Key,
		KeyEnd:      ib.Keys[ix2+1].Key,
		ID:          si.IdGenerator.RvGetPart(),
		LastVersion: si.IdGenerator.RvGetPart(),
	}
	for i := 0; i < len(ib.Keys); i++ {
		if i <= ix2 {
			ib1.Keys = append(ib1.Keys, ib.Keys[i])
			ib1.KeyEnd = ib.Keys[i].Key
			ib1.CountKeys++
			ib1.CountItems += len(ib.Keys[i].IRecords) + len(ib.Keys[i].SRecords)
		} else {
			ib2.Keys = append(ib2.Keys, ib.Keys[i])
			ib2.KeyEnd = ib.Keys[i].Key
			ib2.CountKeys++
			ib2.CountItems += len(ib.Keys[i].IRecords) + len(ib.Keys[i].SRecords)
		}
	}
	return ib1, ib2
}

func (ib *SSimpleIndexBlock) SplitHalf(si *SimpleIndex) (ib1 *SSimpleIndexBlock, ib2 *SSimpleIndexBlock) {
	ix2 := len(ib.Keys) / 2
	ib1 = &SSimpleIndexBlock{
		KeyStart:    ib.Keys[0].Key,
		KeyEnd:      ib.Keys[0].Key,
		ID:          si.IdGenerator.RvGetPart(),
		LastVersion: si.IdGenerator.RvGetPart(),
	}
	ib2 = &SSimpleIndexBlock{
		KeyStart:    ib.Keys[ix2+1].Key,
		KeyEnd:      ib.Keys[ix2+1].Key,
		ID:          si.IdGenerator.RvGetPart(),
		LastVersion: si.IdGenerator.RvGetPart(),
	}
	for i := 0; i < len(ib.Keys); i++ {
		if i <= ix2 {
			ib1.Keys = append(ib1.Keys, ib.Keys[i])
			ib1.KeyEnd = ib.Keys[i].Key
			ib1.CountKeys++
			ib1.CountItems += len(ib.Keys[i].IRecords) + len(ib.Keys[i].SRecords)
		} else {
			ib2.Keys = append(ib2.Keys, ib.Keys[i])
			ib2.KeyEnd = ib.Keys[i].Key
			ib2.CountKeys++
			ib2.CountItems += len(ib.Keys[i].IRecords) + len(ib.Keys[i].SRecords)
		}
	}
	return ib1, ib2
}

func (ib *ISimpleIndexBlock) Add(ctx context.Context, si *SimpleIndex, ibs *ISimpleIndexBlockSegment, req IAddIndexRequest) (err *mft.Error) {
	if !ib.mx.TryLock(ctx) {
		return GenerateError(10401100)
	}
	defer ib.mx.Unlock()

	ix := sort.Search(len(ib.Keys), func(i int) bool {
		return ib.Keys[i].Key <= req.Key
	})

	if ix == len(ib.Keys) {
		ib.Keys = append(ib.Keys, &ISimpleIndexKeyStorage{
			Key: req.Key,
		})
		ib.LastVersion = si.IdGenerator.RvGetPart()
	} else if ib.Keys[ix].Key > req.Key {
		keys := []*ISimpleIndexKeyStorage{}
		keys = append(keys, ib.Keys[:ix]...)
		keys = append(keys, &ISimpleIndexKeyStorage{
			Key: req.Key,
		})
		keys = append(keys, ib.Keys[ix:]...)
		ib.LastVersion = si.IdGenerator.RvGetPart()
	}

	err = ib.Keys[ix].Add(ctx, si, ibs, ib, req)
	return err
}
func (ib *SSimpleIndexBlock) Add(ctx context.Context, si *SimpleIndex, ibs *SSimpleIndexBlockSegment, req SAddIndexRequest) (err *mft.Error) {
	if !ib.mx.TryLock(ctx) {
		return GenerateError(10401150)
	}
	defer ib.mx.Unlock()

	ix := sort.Search(len(ib.Keys), func(i int) bool {
		return ib.Keys[i].Key <= req.Key
	})

	if ix == len(ib.Keys) {
		ib.Keys = append(ib.Keys, &SSimpleIndexKeyStorage{
			Key: req.Key,
		})
		ib.LastVersion = si.IdGenerator.RvGetPart()
	} else if ib.Keys[ix].Key > req.Key {
		keys := []*SSimpleIndexKeyStorage{}
		keys = append(keys, ib.Keys[:ix]...)
		keys = append(keys, &SSimpleIndexKeyStorage{
			Key: req.Key,
		})
		keys = append(keys, ib.Keys[ix:]...)
		ib.LastVersion = si.IdGenerator.RvGetPart()
	}

	err = ib.Keys[ix].Add(ctx, si, ibs, ib, req)
	return err
}

func (ics *ISimpleIndexKeyStorage) Add(ctx context.Context, si *SimpleIndex, ibs *ISimpleIndexBlockSegment,
	ib *ISimpleIndexBlock, req IAddIndexRequest) (err *mft.Error) {
	var writed bool
	var w bool
	ics.IRecords, w = IRecordsAdd(ics.IRecords, req.IRecord)
	writed = writed && w
	ics.SRecords, w = SRecordsAdd(ics.SRecords, req.SRecord)
	writed = writed && w

	if writed {
		ib.LastVersion = si.IdGenerator.RvGetPart()
		if ib.KeyStart > req.Key {
			ib.KeyStart = req.Key
		}
		if ib.KeyEnd < req.Key {
			ib.KeyEnd = req.Key
		}
		ib.CountItems++
		ib.CountKeys = len(ib.Keys)
	}

	return nil
}
func (ics *SSimpleIndexKeyStorage) Add(ctx context.Context, si *SimpleIndex, ibs *SSimpleIndexBlockSegment,
	ib *SSimpleIndexBlock, req SAddIndexRequest) (err *mft.Error) {
	var writed bool
	var w bool
	ics.IRecords, w = IRecordsAdd(ics.IRecords, req.IRecord)
	writed = writed && w
	ics.SRecords, w = SRecordsAdd(ics.SRecords, req.SRecord)
	writed = writed && w

	if writed {
		ib.LastVersion = si.IdGenerator.RvGetPart()
		if ib.KeyStart > req.Key {
			ib.KeyStart = req.Key
		}
		if ib.KeyEnd < req.Key {
			ib.KeyEnd = req.Key
		}
		ib.CountItems++
		ib.CountKeys = len(ib.Keys)
	}

	return nil
}

func IRecordsAdd(records []IIndexRecord, record *IIndexRecord) (res []IIndexRecord, do bool) {
	if record == nil {
		return records, false
	}
	if len(records) == 0 {
		res = append(records, *record)
		return res, true
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

	if ix >= len(records) {
		res = records
		res = append(res, *record)
		return res, true
	}

	if records[ix].ID == record.ID && records[ix].Version == record.Version {
		return records, false
	}

	res = append(res, records[:ix]...)
	res = append(res, *record)
	res = append(res, records[ix:]...)
	return res, true
}

func SRecordsAdd(records []SIndexRecord, record *SIndexRecord) (res []SIndexRecord, do bool) {
	if record == nil {
		return records, false
	}
	if len(records) == 0 {
		res = append(records, *record)
		return res, true
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

	if ix >= len(records) {
		res = records
		res = append(res, *record)
		return res, true
	}

	if records[ix].ID == record.ID && records[ix].Version == record.Version {
		return records, false
	}

	res = append(res, records[:ix]...)
	res = append(res, *record)
	res = append(res, records[ix:]...)
	return res, true
}

func (si *SimpleIndex) Save(ctx context.Context, user cn.CapUser) (err *mft.Error) {
	// Get struct
	// Save all blocks
	// Save struct
	return nil
}
