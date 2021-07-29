package cn

type SaveMode int

const (
	// NotSaveSaveMode - not save message to storage (not mark queue as need changed)
	NotSaveSaveMode SaveMode = 0
	// SaveImmediatelySaveMode - save message immediatly after add element
	SaveImmediatelySaveMode SaveMode = 1
	// SaveMarkSaveMode - not wait save message after add element (saving do on schedule)
	SaveMarkSaveMode SaveMode = 2
	// SaveWaitSaveMode - wait save message after add element (saving do on schedule)
	SaveWaitSaveMode SaveMode = 3
	// QueueSetDefaultMode - save mode setted in queue as DefaultSaveMode
	QueueSetDefaultMode SaveMode = 4
)
