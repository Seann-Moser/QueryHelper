package examples

import (
	"context"
	"github.com/Seann-Moser/QueryHelper/dataset"
	"github.com/Seann-Moser/QueryHelper/table"
	"go.uber.org/zap"
	"log"
	"time"
)

type User struct {
	ID               string `db:"id" q_config:"primary,auto_generate_id,auto_generate_id_type:base64,join,join_name:user_id"`
	UserName         string `db:"user_name" q_config:"where:=,update"`
	Public           bool   `json:"public" db:"public" q_config:"default:true"`
	UpdatedTimestamp string `db:"updated_timestamp" json:"updated_timestamp" q_config:"skip,default:updated_timestamp" `
	CreatedTimestamp string `db:"created_timestamp" json:"created_timestamp" q_config:"skip,default:created_timestamp"`
}

type UserSettings struct {
	UserID           string `db:"user_id" q_config:"primary,auto_generate_id,auto_generate_id_type:base64,join,foreign_key:id,foreign_table:User"`
	Key              string `db:"key" q_config:"where:=,primary"`
	Value            string `db:"value" q_config:"update"`
	UpdatedTimestamp string `db:"updated_timestamp" json:"updated_timestamp" q_config:"skip,default:updated_timestamp" `
	CreatedTimestamp string `db:"created_timestamp" json:"created_timestamp" q_config:"skip,default:created_timestamp"`
}

func DatasetExample() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("failed to create logger")
	}
	ctx := context.Background()

	// set sqlx db to enable full functionality
	ds, err := dataset.New(ctx, "default", true, false, logger, nil, dataset.NewGoCache(context.Background(), 5*time.Second, logger), User{}, UserSettings{})
	if err != nil {
		logger.Fatal("failed creating dataset")
	}
	user := &User{UserName: "test-user"}
	_, userID, err := ds.Insert(ctx, user)
	if err != nil {
		logger.Fatal("failed inserting user")
	}
	user.ID = userID
	logger.Info("created user", zap.String("id", userID))
	users, err := table.SelectAll[User](ds.Select(ctx, user, "id"))
	if err != nil {
		logger.Fatal("failed inserting user")
	}

	for _, u := range users {
		logger.Info("user", zap.String("user_name", u.UserName))

	}

	userSetting := &UserSettings{UserID: userID, Key: "k", Value: "v"}
	_, _, err = ds.Insert(ctx, userSetting)
	if err != nil {
		logger.Fatal("failed inserting user")
	}

	userSettings, err := table.SelectAll[UserSettings](ds.SelectJoin(ctx, nil, []string{"user_name"}, UserSettings{}, user))
	if err != nil {
		logger.Fatal("failed inserting user")
	}
	for _, u := range userSettings {
		logger.Info("user settings", zap.String("key", u.Key), zap.String("key", u.Value))
	}

	userSetting.Value = "new"
	_, err = ds.Update(ctx, userSetting)
	if err != nil {
		logger.Fatal("failed inserting user")
	}

	_, err = ds.Delete(ctx, userSetting)
	if err != nil {
		logger.Fatal("failed inserting user")
	}

	_, err = ds.DeleteAllReferences(ctx, user)
	if err != nil {
		logger.Fatal("failed inserting user")
	}
}
