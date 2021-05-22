package authorization

import (
	"context"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/storage"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

// Object types
const (
	AuthorizationSelfObjectType = "AUTHORIZATION_SELF"
)

// Actions
const (
	AddUserAction      = "ADD_USER"
	SetUserAdminAction = "SET_USER_ADMIN"
	DropUserAction     = "DROP_USER"
	UserRuleSetAction  = "USER_RULE_SET"
	UserRuleDropAction = "USER_RULE_DROP"
	GetAction          = "GET_USER"
)

type User struct {
	Name    string                                `json:"name"`
	IsAdmin bool                                  `json:"is_admin,omitempty"`
	Rules   map[string]map[string]map[string]bool `json:"rule"`
}

func (u *User) Allow(objectType string, action string, objectName string) bool {
	return u.AllowRow(objectType, action, objectName) ||
		u.AllowRow(objectType, action, "*") ||
		u.AllowRow(objectType, "*", objectName) ||
		u.AllowRow(objectType, "*", "*") ||
		u.AllowRow("*", action, objectName) ||
		u.AllowRow("*", action, "*") ||
		u.AllowRow("*", "*", objectName) ||
		u.AllowRow("*", "*", "*")
}

func (u *User) AllowRow(objectType string, action string, objectName string) bool {

	if u.IsAdmin {
		return true
	}

	if u.Rules == nil {
		return false
	}

	a, ok := u.Rules[objectType]
	if !ok {
		return false
	}

	o, ok := a[action]
	if !ok {
		return false
	}

	v, ok := o[objectName]
	if !ok {
		return false
	}

	return v
}

func (u *User) Set(objectType string, action string, objectName string, value bool) {

	if u.Rules == nil {
		u.Rules = make(map[string]map[string]map[string]bool)
	}

	a, ok := u.Rules[objectType]
	if !ok {
		a = make(map[string]map[string]bool)
		u.Rules[objectType] = a
	}

	o, ok := a[action]
	if !ok {
		o = make(map[string]bool)
		a[action] = o
	}

	o[objectName] = value
}

func (u *User) Drop(objectType string, action string, objectName string) {
	if u.Rules == nil {
		return
	}

	a, ok := u.Rules[objectType]
	if !ok {
		return
	}

	o, ok := a[action]
	if !ok {
		return
	}

	delete(o, objectName)
	if len(o) == 0 {
		delete(a, action)
	}

	if len(a) == 0 {
		delete(u.Rules, objectType)
	}
}

// SecurityATRZ - authorization
type SecurityATRZ struct {
	Users map[string]*User `json:"users"`

	// OnChange event func (send self)
	OnChangeFunc func(s *SecurityATRZ) (err *mft.Error) `json:"-"`
	mx           mfs.PMutex
	mxFile       mfs.PMutex

	// case nil then ignore
	CheckPermissionFunc func(user cluster.ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) `json:"-"`
}

func (s *SecurityATRZ) CheckPermissionForInternal(user cluster.ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) {
	if s == nil {
		return false, nil
	}

	if s.CheckPermissionFunc == nil {
		return true, nil
	}

	return s.CheckPermissionFunc(user, objectType, action, objectName)
}

func (s *SecurityATRZ) OnChange() (err *mft.Error) {
	if s == nil {
		return nil
	}

	if s.OnChangeFunc == nil {
		return nil
	}

	err = s.OnChangeFunc(s)

	if err != nil {
		return GenerateErrorE(10310000, err)
	}

	return nil
}

func (s *SecurityATRZ) CheckPermission(user cluster.ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) {

	s.mx.RLock()
	defer s.mx.RUnlock()

	u, ok := s.Users[user.GetName()]

	if !ok {
		return false, nil
	}

	return u.Allow(objectType, action, objectName), nil
}

func (s *SecurityATRZ) AddUser(user cluster.ClusterUser, name string, isAdmin bool) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthorizationSelfObjectType, AddUserAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10310101, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10310102)
	}

	s.mx.Lock()

	_, ok := s.Users[name]

	if !ok {
		s.Users[name] = &User{
			Name:    name,
			IsAdmin: isAdmin,
			Rules:   make(map[string]map[string]map[string]bool),
		}
		s.mx.Unlock()
		return s.OnChange()
	}
	s.mx.Unlock()

	return GenerateError(10310100, name)
}

func (s *SecurityATRZ) SetUserAdmin(user cluster.ClusterUser, name string, isAdmin bool) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthorizationSelfObjectType, SetUserAdminAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10310201, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10310202)
	}

	s.mx.Lock()

	u, ok := s.Users[name]

	if ok {
		u.IsAdmin = isAdmin

		s.mx.Unlock()

		return s.OnChange()
	}
	s.mx.Unlock()

	return GenerateError(10310200, name)
}

func (s *SecurityATRZ) DropUser(user cluster.ClusterUser, name string) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthorizationSelfObjectType, DropUserAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10310301, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10310302)
	}

	s.mx.Lock()

	_, ok := s.Users[name]

	if ok {
		delete(s.Users, name)

		s.mx.Unlock()

		return s.OnChange()
	}
	s.mx.Unlock()

	return GenerateError(10310300, name)
}

func (s *SecurityATRZ) UserRuleSet(user cluster.ClusterUser, name string, objectType string, action string, objectName string, allowed bool) (err *mft.Error) {
	allowedCheck, err := s.CheckPermissionForInternal(user, AuthorizationSelfObjectType, UserRuleSetAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10310401, err)
	}

	if !allowedCheck {
		return GenerateErrorForClusterUser(user, 10310402)
	}

	s.mx.Lock()

	u, ok := s.Users[name]

	if ok {
		u.Set(objectType, action, objectName, allowed)

		s.mx.Unlock()

		return s.OnChange()
	}
	s.mx.Unlock()

	return GenerateError(10310400, name)
}

func (s *SecurityATRZ) UserRuleDrop(user cluster.ClusterUser, name string, objectType string, action string, objectName string) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthorizationSelfObjectType, UserRuleDropAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10310501, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10310502)
	}

	s.mx.Lock()

	u, ok := s.Users[name]

	if ok {
		u.Drop(objectType, action, objectName)

		s.mx.Unlock()

		return s.OnChange()
	}
	s.mx.Unlock()

	return GenerateError(10310500, name)
}

func (s *SecurityATRZ) Get(user cluster.ClusterUser) (sOut *SecurityATRZ, err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthorizationSelfObjectType, GetAction, "")

	if err != nil {
		return nil, GenerateErrorForClusterUserE(user, 10310802, err)
	}

	if !allowed {
		return nil, GenerateErrorForClusterUser(user, 10310803)
	}

	sOut = &SecurityATRZ{
		Users: make(map[string]*User),
	}

	s.mx.Lock()

	b, er0 := json.Marshal(s)

	s.mx.Unlock()
	if er0 != nil {
		return sOut, GenerateErrorE(10310800, er0)
	}

	er0 = json.Unmarshal(b, sOut)

	if er0 != nil {
		return sOut, GenerateErrorE(10310801, er0)
	}

	return sOut, nil
}

var WaitTimeout = time.Second * 5

func StorageOnChangeFuncGenerator(s storage.Storage, file string) func(sec *SecurityATRZ) (err *mft.Error) {
	return func(sec *SecurityATRZ) (err *mft.Error) {
		sec.mxFile.Lock()
		defer sec.mxFile.Unlock()

		sec.mx.RLock()
		data, er0 := json.MarshalIndent(sec, "", "  ")
		sec.mx.RUnlock()

		if er0 != nil {
			return GenerateErrorE(10310600, er0)
		}

		ctx, cancel := context.WithTimeout(context.Background(), WaitTimeout)
		defer cancel()
		err = s.Save(ctx, file, data)
		if err != nil {
			return GenerateErrorE(10310601, err)
		}

		return nil
	}
}

func StorageLoad(s storage.Storage, file string) (sec *SecurityATRZ, err *mft.Error) {

	sec = &SecurityATRZ{
		Users:        make(map[string]*User),
		OnChangeFunc: StorageOnChangeFuncGenerator(s, file),
	}

	sec.CheckPermissionFunc = sec.CheckPermission

	ctx, cancel := context.WithTimeout(context.Background(), WaitTimeout)
	defer cancel()
	body, err := s.Get(ctx, file)
	if err != nil {
		return nil, GenerateErrorE(10310700, err)
	}

	er0 := json.Unmarshal(body, sec)
	if er0 != nil {
		return nil, GenerateErrorE(10310701, er0)
	}

	return sec, nil
}
