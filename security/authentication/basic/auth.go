package basic

import (
	"context"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/capella-pw/queue/cluster"
	"github.com/capella-pw/queue/storage"
	"github.com/myfantasy/mfs"
	"github.com/myfantasy/mft"
)

const (
	AuthType = "basic"
)

// Object types
const (
	AuthBasicSelfObjectType = "AUTH_BASIC_SELF"
	AuthBasicObjectType     = "AUTH_BASIC"
)

// Actions
const (
	AddAction     = "ADD_USER"
	UpdateAction  = "UPDATE_USER"
	EnableAction  = "ENABLE_USER"
	DisableAction = "DISABLE_USER"
	GetAction     = "GET_ALL"
	DropAction    = "DROP_USER"

	ImpersonateAction = "IMPERSONATE"
)

type User struct {
	Name     string `json:"name"`
	IsEnable bool   `json:"is_enable"`
	PwdHash  string `json:"pwd_hash"`
}

type UserSend struct {
	Name string `json:"name"`
	Pwd  string `json:"pwd"`
}

type UserCut struct {
	Name     string `json:"name"`
	IsEnable bool   `json:"is_enable"`
}

func (u *User) UserCut() UserCut {
	return UserCut{
		Name:     u.Name,
		IsEnable: u.IsEnable,
	}
}

// SecurityATCB - authentication basic
type SecurityATCB struct {
	Users map[string]*User `json:"users"`

	// OnChange event func (send self)
	OnChangeFunc func(s *SecurityATCB) (err *mft.Error) `json:"-"`

	mx     mfs.PMutex
	mxFile mfs.PMutex

	// case nil then ignore
	CheckPermissionFunc func(user cluster.ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) `json:"-"`
}

func (s *SecurityATCB) CheckPermissionForInternal(user cluster.ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error) {
	if s == nil {
		return false, nil
	}

	if s.CheckPermissionFunc == nil {
		return true, nil
	}

	return s.CheckPermissionFunc(user, objectType, action, objectName)
}

func (s *SecurityATCB) OnChange() (err *mft.Error) {
	if s == nil {
		return nil
	}

	if s.OnChangeFunc == nil {
		return nil
	}

	err = s.OnChangeFunc(s)

	if err != nil {
		return GenerateErrorE(10300000, err)
	}

	return nil
}

func Sha512(name string, pwd string) string {
	sha := sha512.Sum512([]byte(name + ":" + pwd))

	return base64.StdEncoding.EncodeToString(sha[:])
}

func (s *SecurityATCB) Add(user cluster.ClusterUser, us UserSend) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthBasicSelfObjectType, AddAction, us.Name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10300102, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10300101)
	}

	u := User{
		Name:    us.Name,
		PwdHash: Sha512(us.Name, us.Pwd),
	}

	s.mx.Lock()
	_, ok := s.Users[u.Name]
	if !ok {
		s.Users[u.Name] = &u
	}
	s.mx.Unlock()

	if !ok {
		return s.OnChange()
	}

	return GenerateError(10300100, u.Name)
}

func (s *SecurityATCB) Update(user cluster.ClusterUser, us UserSend) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthBasicSelfObjectType, UpdateAction, us.Name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10300202, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10300201)
	}

	u := User{
		Name:    us.Name,
		PwdHash: Sha512(us.Name, us.Pwd),
	}

	s.mx.Lock()
	uOld, ok := s.Users[u.Name]
	if ok {
		u.IsEnable = uOld.IsEnable

		s.Users[u.Name] = &u
	}
	s.mx.Unlock()

	if ok {
		return s.OnChange()
	}

	return GenerateError(10300200, u.Name)
}

func (s *SecurityATCB) Enable(user cluster.ClusterUser, name string) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthBasicSelfObjectType, EnableAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10300302, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10300301)
	}

	s.mx.Lock()
	_, ok := s.Users[name]
	if ok {
		s.Users[name].IsEnable = true
	}
	s.mx.Unlock()

	if ok {
		return s.OnChange()
	}

	return GenerateError(10300300, name)
}

func (s *SecurityATCB) Disable(user cluster.ClusterUser, name string) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthBasicSelfObjectType, DisableAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10300402, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10300401)
	}

	s.mx.Lock()
	_, ok := s.Users[name]
	if ok {
		s.Users[name].IsEnable = false
	}
	s.mx.Unlock()

	if ok {
		return s.OnChange()
	}

	return GenerateError(10300400, name)
}

func (s *SecurityATCB) Drop(user cluster.ClusterUser, name string) (err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthBasicSelfObjectType, DropAction, name)

	if err != nil {
		return GenerateErrorForClusterUserE(user, 10300502, err)
	}

	if !allowed {
		return GenerateErrorForClusterUser(user, 10300501)
	}

	s.mx.Lock()
	_, ok := s.Users[name]
	if ok {
		delete(s.Users, name)
	}
	s.mx.Unlock()

	if ok {
		return s.OnChange()
	}

	return GenerateError(10300500, name)
}

func (s *SecurityATCB) Get(user cluster.ClusterUser) (users []UserCut, err *mft.Error) {
	allowed, err := s.CheckPermissionForInternal(user, AuthBasicSelfObjectType, GetAction, "")

	if err != nil {
		return users, GenerateErrorForClusterUserE(user, 10300801, err)
	}

	if !allowed {
		return users, GenerateErrorForClusterUser(user, 10300800)
	}

	s.mx.RLock()
	for _, v := range s.Users {
		users = append(users, v.UserCut())
	}
	s.mx.RUnlock()

	return users, nil
}

var WaitTimeout = time.Second * 5

func StorageOnChangeFuncGenerator(s storage.Storage, file string) func(sec *SecurityATCB) (err *mft.Error) {
	return func(sec *SecurityATCB) (err *mft.Error) {
		sec.mxFile.Lock()
		defer sec.mxFile.Unlock()

		sec.mx.RLock()
		data, er0 := json.MarshalIndent(sec, "", "  ")
		sec.mx.RUnlock()

		if er0 != nil {
			return GenerateErrorE(10300600, er0)
		}

		ctx, cancel := context.WithTimeout(context.Background(), WaitTimeout)
		defer cancel()
		err = s.Save(ctx, file, data)
		if err != nil {
			return GenerateErrorE(10300601, err)
		}

		return nil
	}
}

func StorageLoad(s storage.Storage, file string,
	checkPermissionFunc func(user cluster.ClusterUser, objectType string, action string, objectName string) (allowed bool, err *mft.Error),
) (sec *SecurityATCB, err *mft.Error) {

	sec = &SecurityATCB{
		Users:               make(map[string]*User),
		OnChangeFunc:        StorageOnChangeFuncGenerator(s, file),
		CheckPermissionFunc: checkPermissionFunc,
	}

	ctx, cancel := context.WithTimeout(context.Background(), WaitTimeout)
	defer cancel()
	body, err := s.Get(ctx, file)
	if err != nil {
		return nil, GenerateErrorE(10300700, err)
	}

	er0 := json.Unmarshal(body, sec)
	if er0 != nil {
		return nil, GenerateErrorE(10300701, er0)
	}

	return sec, nil
}

func PasswordMarshal(pwd string) []byte {
	b, er0 := json.Marshal(pwd)

	if er0 != nil {
		panic(GenerateErrorE(10301000, er0))
	}

	return b
}

func (s *SecurityATCB) CheckAuthFunc(ctx context.Context, serviceRequest *cluster.ServiceRequest,
) (ok bool, failResponce cluster.ResponceBody) {

	if serviceRequest.AuthentificationType != AuthType && serviceRequest.AuthentificationType != "" {
		GenerateError(10300907, serviceRequest.UserName)
	}

	s.mx.RLock()
	user, ok := s.Users[serviceRequest.UserName]
	s.mx.RUnlock()

	if !ok {
		failResponce.Err = GenerateError(10300900, serviceRequest.UserName)
		return false, failResponce
	}

	if !user.IsEnable {
		failResponce.Err = GenerateError(10300901, serviceRequest.UserName)
		return false, failResponce
	}

	if user.PwdHash != "" {
		var pwd string
		er0 := json.Unmarshal(serviceRequest.AuthentificationInfo, &pwd)
		if er0 != nil {
			failResponce.Err = GenerateErrorE(10300902, er0)
			return false, failResponce
		}

		hash := Sha512(serviceRequest.UserName, pwd)

		if user.PwdHash != hash {
			failResponce.Err = GenerateError(10300903, serviceRequest.UserName)
			return false, failResponce
		}
	}

	if serviceRequest.Request == nil {
		failResponce.Err = GenerateError(10300906)
		return false, failResponce
	}

	if serviceRequest.Request.User == "" || serviceRequest.ReplaceNameForce {
		serviceRequest.Request.User = serviceRequest.UserName
	}

	if serviceRequest.Request.User != serviceRequest.UserName {
		allowed, err := s.CheckPermissionForInternal(serviceRequest,
			AuthBasicObjectType, ImpersonateAction, serviceRequest.Request.User)

		if err != nil {
			failResponce.Err = GenerateErrorForClusterUserE(serviceRequest, 10300904, err)
			return false, failResponce
		}

		if !allowed {
			failResponce.Err = GenerateErrorForClusterUser(serviceRequest, 10300905,
				serviceRequest.Request.User, serviceRequest.UserName)
			return false, failResponce
		}
	}

	return true, failResponce
}
