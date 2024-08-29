package QueryHelper

import (
	"context"
	"github.com/Seann-Moser/ctx_cache"
	"testing"
)

type AccountUserRole struct {
	AccountID        string `json:"account_id" db:"account_id" qc:"primary;join;join_name::account_id"`
	UserID           string `json:"user_id" db:"user_id" qc:"primary;varchar(512);"`
	RoleID           string `json:"role_id" db:"role_id" qc:"primary;join;foreign_key::id;foreign_table::role"`
	UpdatedTimestamp string `json:"updated_timestamp" db:"updated_timestamp" qc:"skip;default::updated_timestamp"`
	CreatedTimestamp string `json:"created_timestamp" db:"created_timestamp" qc:"skip;default::created_timestamp"`
}

func TestCacheTest(t *testing.T) {

	ctx := context.Background()
	go ctx_cache.GlobalCacheMonitor.Start(ctx)
	table, err := NewTable[AccountUserRole]("test", "")
	if err != nil {
		t.Fatalf(err.Error())
	}

	q := QueryTable[AccountUserRole](table)
	//if !isSuperAdmin {
	q.Where(q.Column("user_id"), "=", "AND", 0, "")
	q.UseCache()
	q.SetName("rbac-accounts-for-user")
	q.Run(ctx, nil)

	aur := &AccountUserRole{
		AccountID: "",
		UserID:    "",
		RoleID:    "",
	}
	_, err = table.Insert(ctx, nil, *aur)

	qt := QueryTable[AccountUserRole](table)
	//if !isSuperAdmin {
	qt.Where(q.Column("user_id"), "=", "AND", 0, "")
	qt.UseCache()
	qt.SetName("rbac-accounts-for-user")
	qt.Run(ctx, nil)

}
