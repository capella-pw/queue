package queue

import (
	"context"

	"github.com/myfantasy/mft"
)

// SubscribeCopyUnique subscribe on to queue and copy (addUniqueList) to destination
func SubscribeCopyUnique(src Queue, dst Queue, saveModeSrc int, saveModeDst int, subscriberName string, cntLimit int, doSaveDst bool) func(ctx context.Context) (isEmpty bool, err *mft.Error) {
	var id int64
	return func(ctx context.Context) (isEmpty bool, err *mft.Error) {
		if id == 0 {
			id, err = src.SubscriberGetLastRead(ctx, subscriberName)
			if err != nil {
				return false, err
			}
		}

		mesages, err := src.Get(ctx, id, cntLimit)
		if err != nil {
			return false, err
		}

		if len(mesages) == 0 {
			return true, nil
		}

		messageSend := make([]Message, 0, len(mesages))

		idMax := mesages[len(mesages)-1].ID
		for i := 0; i < len(mesages); i++ {
			messageSend = append(messageSend, mesages[i].ToMessage())
		}

		_, err = dst.AddUniqueList(ctx, messageSend, saveModeDst)

		if doSaveDst {
			err = dst.SaveAll(ctx)
			if err != nil {
				return false, err
			}
		}

		err = src.SubscriberSetLastRead(ctx, subscriberName, idMax, saveModeSrc)
		if err != nil {
			return false, err
		}

		id = idMax

		return false, nil
	}
}
