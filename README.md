### QueryHelper
Struct Tags
```
db
data_type
default
null
table
update
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