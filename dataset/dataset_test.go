package dataset

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"go.uber.org/zap"
)

type User struct {
	ID                 string `db:"id" json:"id" q_config:"primary,join"`
	FirstName          string `db:"first_name" json:"first_name"`
	LastName           string `db:"last_name" json:"last_name"`
	UserName           string `db:"user_name" json:"user_name" q_config:"where:=,primary'"`
	PermissionLevel    int    `db:"permission_level" json:"permission_level" q_config:"update,join,default:0"`
	Birthday           string `db:"birthday" json:"birthday" q_config:"data_type:DATE"`
	LastLoginTimestamp string `db:"last_login_timestamp" json:"last_login_timestamp" q_config:"update,skip,data_type:TIMESTAMP,update,default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" `
	CreatedTimestamp   string `db:"created_timestamp" json:"created_timestamp" q_config:"update,skip,data_type:TIMESTAMP,update,default:NOW()"`
	Active             bool   `db:"active" json:"active" q_config:"where:=,default:true,update"`
}

type UserPasswords struct {
	ID               string `db:"id" json:"id" q_config:"primary,foreign_key:id,foreign_table:User,where:=,join"`
	Password         string `db:"password" json:"password" q_config:"primary,where:="`
	Active           bool   `db:"active" json:"active" q_config:"where:=,default:true,update"`
	CreatedTimestamp string `db:"created_timestamp" json:"created_timestamp" q_config:"update,skip,data_type:TIMESTAMP,update,default:NOW()"`
}

type UserAPIKey struct {
	ID               string `db:"id" json:"id" q_config:"primary,foreign_key:id,foreign_table:User,where:=,join"`
	APIKey           string `db:"api_key" json:"api_key" q_config:"primary,where:="`
	Active           bool   `db:"active" json:"active" q_config:"where:=,default:true,update"`
	ExpiresTimestamp string `db:"expires_timestamp" json:"expires_timestamp" q_config:"data_type:TIMESTAMP,skip,default:DATE_ADD(CURRENT_TIMESTAMP(){{comma}}INTERVAL 30 DAY),where:>="`
	CreatedTimestamp string `db:"created_timestamp" json:"created_timestamp" q_config:"update,skip,data_type:TIMESTAMP,update,default:NOW()"`
}

func (u *UserPasswords) HashPassword(salt string) {
	h := sha256.New()
	h.Write([]byte(u.Password + salt))
	bs := h.Sum(nil)
	u.Password = base64.StdEncoding.EncodeToString(bs)
}
func TestDataset_SelectJoin(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	ds, err := New(context.Background(), "account", false, true, true, logger, nil, User{}, UserPasswords{})
	if err != nil {
		t.Fatal(err)
	}
	userPassword := UserPasswords{}
	_, err = ds.SelectJoin(context.Background(), nil, []string{"user_name", "password", "active"}, User{}, userPassword)
	if err != nil {
		t.Fatal(err)
	}

}
