package main

import (
	"encoding/json"

	"github.com/capella-pw/queue/cluster/cap"
	"github.com/capella-pw/queue/compress"
	"github.com/myfantasy/mft"
)

type Settings struct {
	Cap       json.RawMessage            `json:"cap"`
	PG        map[string]*PGConnection   `json:"pg"`
	Transfers map[string]*TransferAction `json:"transfers"`
	Actions   map[string]*CallAction     `json:"actions"`

	CG       *cap.ConGroup `json:"-"`
	NeedStop bool          `json:"-"`
}

func SettingsCreate() (s *Settings) {
	s = &Settings{
		PG:        make(map[string]*PGConnection),
		Transfers: make(map[string]*TransferAction),
		Actions:   make(map[string]*CallAction),
		CG:        cap.ConGroupGenerate(),
	}
	return s
}

func SettingsCreateFromJson(data []byte, compressor *compress.Generator) (s *Settings, err *mft.Error) {
	s = SettingsCreate()

	er0 := json.Unmarshal(data, &s)
	if er0 != nil {
		return nil, mft.ErrorNew("SettingsCreateFromJson: Fail unmarshal json: ", er0)
	}

	s.CG, err = cap.ConGroupFromJson(s.Cap, compressor)
	if err != nil {
		return nil, mft.ErrorNew("SettingsCreateFromJson: Fail unmarshal connection group (CG): ", err)
	}

	for n, pg := range s.PG {
		er0 := pg.Init()
		if er0 != nil {
			return nil, mft.ErrorNew("SettingsCreateFromJson: Fail load PG: "+n, er0)
		}
	}

	return s, nil
}
