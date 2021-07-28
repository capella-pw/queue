package cn

// CapUser - user for capella
type CapUser interface {
	GetName() string
}

type CapUserName string

func (cun CapUserName) GetName() string {
	return string(cun)
}
