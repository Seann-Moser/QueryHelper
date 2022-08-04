### QueryHelper


### Import

```bash
go get github.com/Seann-Moser/QueryHelper
```

## V2
```go
import "github.com/Seann-Moser/QueryHelper/v2/table"
import "github.com/Seann-Moser/QueryHelper/v2/dataset"
```



### Struct Tags
```
db
q_config
```

#### q_config
#### Bool
```
primary, join, select, update, skip, null
```

or

```
primary:true,join:false,select:true, update, skip, null
```

#### Value
```
where, join_name, data_type, default, where_join, foreign_key, foreign_table
```


### Examples

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/Seann-Moser/QueryHelper/v2/dataset"
)

type Test struct {
	UserID      string `db:"user_id" q_config:"join,select,join_name:id"`
	Name        string `db:"name"`
	UserName    string `db:"user_name"`
	CreatedDate string `db:"created_date"`
	Password    string `db:"password"`
	UpdatedDate string `db:"updated_date"`
	Active      bool   `db:"active"`
}

type Test2 struct {
	TestID string `db:"test_id" q_config:"join,select,join_name:id"`
	Name   string `db:"name"`
	Active bool   `db:"active"`
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	//setup db

	db:= sqlx.NewDb(nil,"mysql")

	ds, err := dataset.NewDataset(context.Background(), "test", logger, db, Test{}, Test2{})
	if err != nil {
		log.Fatal(err)
	}
	rows, err := ds.SelectJoin(context.Background(), []string{"user_name", "name", "test_id"}, nil, Test{}, Test2{})
	if err != nil {
		log.Fatal(err)
	}
	for rows.next() {
		var username, name, testId string
		err = rows.Scan(&username, &name, &testId)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s %s %s", username, name, testId)
	}

}
```






Example 

```go
type Test struct{
    Name        string `db:"name" default:"jon smith" table:"primary"`
    UserName    string `db:"user_name" update:"true" null:"true"`
    CreatedDate string `db:"created_date" default:"NOW()" data_type:"timestamp" table:"skip_insert"`
}
func main() {
    newTable, err := GenerateTableFromStruct("default_db",Test{})
    if err != nil {
        log.Fatal(err)
    }
    //err = CreateMySqlTable(ctx,db,newTable) - Will create the table and schema if missing
    print(newTable.GenerateNamedInsertStatement())
    // INSERT INTO default_db.Test(name,user_name) VALUES(:name,:user_name);
    print(newTable.GenerateNamedUpdateStatement())
    // UPDATE default_db.Test SET user_name = :user_name WHERE name = :name
}
```