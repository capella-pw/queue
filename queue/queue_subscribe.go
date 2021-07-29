package queue

import (
	"context"

	"github.com/capella-pw/queue/cn"
	"github.com/myfantasy/mft"
	"github.com/myfantasy/segment"
)

// SubscribeCopyUnique subscribe on to queue and copy (addUniqueList) to destination
func SubscribeCopyUnique(src Queue, dst Queue,
	userSrc cn.CapUser, userDst cn.CapUser, saveModeSrc int, saveModeDst int,
	subscriberName string, cntLimit int, doSaveDst bool,
	segments *segment.Segments,
) func(ctx context.Context) (isEmpty bool, err *mft.Error) {
	var id int64
	return func(ctx context.Context) (isEmpty bool, err *mft.Error) {
		if id == 0 {
			id, err = src.SubscriberGetLastRead(ctx, userSrc, subscriberName)
			if err != nil {
				return false, err
			}
		}

		mesages, lastID, err := src.GetSegment(ctx, userSrc, id, cntLimit, segments)
		if err != nil {
			return false, err
		}

		if len(mesages) == 0 && lastID <= id {
			return true, nil
		}

		if len(mesages) != 0 {
			messageSend := make([]Message, 0, len(mesages))

			for i := 0; i < len(mesages); i++ {
				messageSend = append(messageSend, mesages[i].ToMessage())
			}

			_, err = dst.AddUniqueList(ctx, userDst, messageSend, saveModeDst)

			if err != nil {
				return false, err
			}

			if doSaveDst {
				err = dst.SaveAll(ctx, userDst)
				if err != nil {
					return false, err
				}
			}
		}

		if lastID > id {
			err = src.SubscriberSetLastRead(ctx, userSrc, subscriberName, lastID, saveModeSrc)
			if err != nil {
				return false, err
			}

			id = lastID
		}

		return false, nil
	}
}
