package resource

import (
	"encoding/json"

	"github.com/myfantasy/mft"
)

// Resource - resource information
type Resource interface {
	// Description gets description of resource
	Description() json.RawMessage

	// Do command
	Do(request json.RawMessage) (result json.RawMessage, err *mft.Error)
}
