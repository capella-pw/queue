package queue

import (
	"fmt"

	"github.com/myfantasy/mft"
)

// Errors codes and description
var Errors map[int]string = map[int]string{
	10010000: "SimpleQueue.Add: read lock queue fail wait",
	10010001: "SimpleQueueBlock.add: lock queue block fail wait",
	10010002: "SimpleQueueBlock.canAppend: read lock queue block fail wait",
	10010003: "SimpleQueue.checkAndAddCurrentBlockForWrite: promote queue fail wait",
	10010004: "SimpleQueue.Add: mxBlockSaveWait lock queue a fail wait",
	10010005: "SimpleQueue.Add: chWaitSaveMeta fail wait",
	10010006: "SimpleQueue.Add: chWaitBlockSave fail wait",
	10010007: "SimpleQueueBlock.add: block IsUnload !!!!",
	10010008: "SimpleQueueBlock.add: externat time in future ext time: %v now:%v",

	10011000: "SimpleQueue.getBlockForNext: block RLock fail wait",
	10011001: "SimpleQueueBlock.getItemsAfter: block RLock fail wait",
	10011002: "SimpleQueue.Get: queue RLock fail wait",

	10012000: "SimpleQueue.Save: queue Lock FileSave mutex fail wait",
	10012001: "SimpleQueue.Save: queue RLock fail wait",
	10012002: "SimpleQueue.Save: queue marshal fail",
	10012003: "SimpleQueue.Save: file %v save fail",
	10012004: "SimpleQueue.Save: queue Lock fail wait",

	10013000: "SimpleQueueBlock.Save: block Lock FileSave mutex fail wait",
	10013001: "SimpleQueueBlock.Save: block RLock fail wait",
	10013002: "SimpleQueueBlock.Save: block.data marshal fail",
	10013003: "SimpleQueueBlock.Save: file %v save fail",
	10013004: "SimpleQueueBlock.Save: block Lock fail wait",

	10014000: "SimpleQueue.SaveAll: block Lock BlockSaveWait mutex fail wait",

	10015000: "SimpleQueueBlock.deleteBlock: block Lock FileSave mutex fail wait",
	10015001: "SimpleQueueBlock.deleteBlock: block RLock fail wait",
	10015002: "SimpleQueueBlock.deleteBlock: block Promote to Lock fail wait",
	10015003: "SimpleQueue.deleteBlock: queue Lock mutex fail wait",

	10016000: "SimpleQueue.getStorageLock: queue RLock fail wait",

	10017000: "SimpleQueueBlock.clearOldStorageBlock: block Lock FileSave mutex fail wait",
	10017001: "SimpleQueue.clearOldStorageBlock: queue Lock mutex fail wait",

	10018000: "SimpleQueueBlock.move: block Lock FileSave mutex fail wait",
	10018001: "SimpleQueueBlock.move: block RLock fail wait",
	10018002: "SimpleQueueBlock.move: block.data marshal fail",
	10018003: "SimpleQueueBlock.move: file %v save fail",
	10018004: "SimpleQueueBlock.move: queue Lock fail wait",
	10018005: "SimpleQueueBlock.move: block Promote to Lock fail wait",

	10019000: "SimpleQueueBlock.unload: block Lock FileSave mutex fail wait",
	10019001: "SimpleQueueBlock.unload: block Lock fail wait",

	10020000: "SimpleQueueBlock.load: block Lock FileSave mutex fail wait",
	10020001: "SimpleQueueBlock.load: block Promote to Lock fail wait",
	10020002: "SimpleQueueBlock.load: json Unmarchal Fail",
	10020003: "SimpleQueueBlock.load: load from storage Fail file name: %v, mark:%v",

	10021000: "SimpleQueue.SetUnload: queue RLock fail wait",

	10022000: "SimpleQueueBlock.setNewStorage: block Lock FileSave mutex fail wait",
	10022001: "SimpleQueueBlock.setNewStorage: queue Lock fail wait",
	10022002: "SimpleQueueBlock.setNewStorage: block Lock fail wait",

	10023000: "SimpleQueueBlock.setNeedDelete: block Lock FileSave mutex fail wait",
	10023001: "SimpleQueueBlock.setNeedDelete: queue Lock fail wait",
	10023002: "SimpleQueueBlock.setNeedDelete: block Lock fail wait",

	10024000: "SimpleQueue.SetMarks: queue RLock fail wait",

	10025000: "SimpleQueue.SetDelete: queue RLock fail wait",

	10026000: "SimpleQueue.UpdateMarks: queue RLock fail wait",
	10026001: "SimpleQueue.UpdateMarks: block RLock mutex fail wait",
}

// GenerateError -
func GenerateError(key int, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCS(key, fmt.Sprintf(text, a...))
	}
	panic(fmt.Sprintf("queue.GenerateError, error not found code:%v", key))
}

// GenerateErrorE -
func GenerateErrorE(key int, err error, a ...interface{}) *mft.Error {
	if text, ok := Errors[key]; ok {
		return mft.ErrorCSE(key, fmt.Sprintf(text, a...), err)
	}
	panic(fmt.Sprintf("queue.GenerateErrorE, error not found code:%v error:%v", key, err))
}
