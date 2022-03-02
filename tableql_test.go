package tableql

import (
	"database/sql"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type Table struct {
	ID     uuid.UUID      `json:"uuid"`
	Int    int            `json:"Int"`
	String string         `json:"string"`
	Bool   bool           `json:"bool"`
	Time   time.Time      `json:"time"`
	Null   sql.NullString `json:"null"`
}

var client *gorm.DB

func query(input *string, data *[]map[string]interface{}) (err error) {
	bytes := []byte(*input)
	var params *Params
	params, err = Parse(&bytes)
	if err != nil {
		return
	}
	cli := client.Model(&Table{})
	if len(params.Select) > 0 {
		cli = cli.Select(params.Select)
	}
	if len(params.WhereExp) > 0 {
		if params.WhereArgs != nil {
			cli = cli.Where(params.WhereExp, params.WhereArgs)
		} else {
			cli = cli.Where(params.WhereExp)
		}
	}
	if len(params.OrderBy) > 0 {
		cli = cli.Order(strings.Join(params.OrderBy, ","))
	}
	if params.Limit > 0 {
		cli = cli.Limit(params.Limit)
	}
	if params.Offset > 0 {
		cli = cli.Offset(params.Offset)
	}
	err = cli.Find(data).Error
	return
}

func Test(t *testing.T) {
	var err error
	dsn := "host=127.0.0.1 user=postgres password=R00Tpostgres dbname=tableql port=5432 sslmode=disable"
	client, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.New(log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{
				SlowThreshold:             100 * time.Millisecond,
				LogLevel:                  logger.Warn,
				IgnoreRecordNotFoundError: false,
				Colorful:                  false,
			}),
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})
	if err != nil {
		t.Error(err)
	}
	err = client.AutoMigrate(&Table{})
	if err != nil {
		t.Error(err)
	}
	data := []Table{
		{
			ID:     uuid.MustParse("1b019d23-562e-440a-9088-23697ae41979"),
			Int:    1,
			String: "string1",
			Bool:   true,
			Time:   time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC),
			Null: sql.NullString{
				String: "not null",
				Valid:  true,
			},
		},
		{
			ID:     uuid.MustParse("44e9e7b7-e74d-476d-bdae-f47502790a44"),
			Int:    2,
			String: "string2",
			Bool:   false,
			Time:   time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC),
			Null: sql.NullString{
				Valid: false,
			},
		},
		{
			ID:     uuid.MustParse("094d82cf-d197-4fbb-b6dd-85acdd0191b2"),
			Int:    3,
			String: "string3",
			Bool:   true,
			Time:   time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC),
			Null: sql.NullString{
				String: "not null",
				Valid:  true,
			},
		},
	}
	err = client.Clauses(clause.OnConflict{UpdateAll: true}).Create(&data).Error
	if err != nil {
		t.Error(err)
	}
	t.Run("limit", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "limit": 1 }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
		}, data)
	})
	t.Run("offset", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "offset": 1 }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "44e9e7b7-e74d-476d-bdae-f47502790a44"},
			{"id": "094d82cf-d197-4fbb-b6dd-85acdd0191b2"},
		}, data)
	})
	t.Run("order_by", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "order_by": [ { "column": "id", "order": "ASC" } ] }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "094d82cf-d197-4fbb-b6dd-85acdd0191b2"},
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
			{"id": "44e9e7b7-e74d-476d-bdae-f47502790a44"},
		}, data)
	})
	t.Run("where eq", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "int": { "_eq": 2 } } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "44e9e7b7-e74d-476d-bdae-f47502790a44"},
		}, data)
	})
	t.Run("where neq", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "int": { "_neq": 2 } } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
			{"id": "094d82cf-d197-4fbb-b6dd-85acdd0191b2"},
		}, data)
	})
	t.Run("where in", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "int": { "_in": [ 2, 3 ] } } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "44e9e7b7-e74d-476d-bdae-f47502790a44"},
			{"id": "094d82cf-d197-4fbb-b6dd-85acdd0191b2"},
		}, data)
	})
	t.Run("where nin", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "int": { "_nin": [ 2, 3 ] } } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
		}, data)
	})
	t.Run("where is null", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "null": { "_is_null": true } } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "44e9e7b7-e74d-476d-bdae-f47502790a44"},
		}, data)
	})
	t.Run("where is not null", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "null": { "_is_null": false } } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
			{"id": "094d82cf-d197-4fbb-b6dd-85acdd0191b2"},
		}, data)
	})
	t.Run("where and", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "_and": [ { "null": { "_is_null": false } }, { "int": { "_eq": 1 } } ] } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
		}, data)
	})
	t.Run("where or", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "_or": [ { "int": { "_eq": 1 } }, { "int": { "_eq": 3 } } ] } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
			{"id": "094d82cf-d197-4fbb-b6dd-85acdd0191b2"},
		}, data)
	})
	t.Run("where not", func(t *testing.T) {
		var input = `{ "select": [ "id" ], "where": { "_not": { "int": { "_eq": 2 } } } }`
		var data []map[string]interface{}
		err := query(&input, &data)
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, []map[string]interface{}{
			{"id": "1b019d23-562e-440a-9088-23697ae41979"},
			{"id": "094d82cf-d197-4fbb-b6dd-85acdd0191b2"},
		}, data)
	})
}
