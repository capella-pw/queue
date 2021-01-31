package queue

import (
	"context"

	"github.com/myfantasy/mft"
)

// SubscribeCopy subscribe on to queue and copy to destination
func SubscribeCopy(src Queue, dst Queue, saveModeSrc int, saveModeDst int, subscriberName string, cntLimit int, ctxGenerator func() context.Context) func() (isEmpty bool, err *mft.Error) {
	var id int64
	return func() (isEmpty bool, err *mft.Error) {
		if id == 0 {
			id, err = src.SubscriberGetLastRead(ctxGenerator(), subscriberName)
			if err != nil {
				return false, err
			}
		}

		mesages, err := src.Get(ctxGenerator(), id, cntLimit)
		if err != nil {
			return false, err
		}

		if len(mesages) == 0 {
			return true, nil
		}

		for i := 0; i < len(mesages); i++ {
			_, err = dst.AddUnique(ctxGenerator(), mesages[i].Message, mesages[i].ExternalID, mesages[i].ExternalDt, mesages[i].Source, saveModeDst)
			if err != nil {
				return false, err
			}
			id = mesages[i].ID
		}

		err = src.SubscriberSetLastRead(ctxGenerator(), subscriberName, id, saveModeSrc)
		return false, err
	}
}
