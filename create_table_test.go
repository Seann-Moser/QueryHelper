package QueryHelper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Assume MissingPrimaryKeyErr is defined somewhere
func TestBuildCreateTableQueries(t *testing.T) {
	// Test cases
	tests := []struct {
		name                   string
		dataset                string
		table                  string
		columns                map[string]Column
		expectedSchemaSQL      string
		expectedCreateTableSQL string
		expectError            bool
		errorMessage           string
	}{
		{
			name:    "Create table with single primary key",
			dataset: "test_schema",
			table:   "test_table",
			columns: map[string]Column{
				"id": {
					Name:    "id",
					Type:    "INT",
					Primary: true,
					Null:    false,
				},
				"name": {
					Name: "name",
					Type: "VARCHAR(255)",
					Null: false,
				},
			},
			expectedSchemaSQL:      "CREATE SCHEMA IF NOT EXISTS `test_schema`",
			expectedCreateTableSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (`id` INT NOT NULL,`name` VARCHAR(255) NOT NULL,\n\tPRIMARY KEY(`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			expectError:            false,
		},
		{
			name:    "Create table with composite primary key",
			dataset: "test_schema",
			table:   "test_table",
			columns: map[string]Column{
				"id": {
					Name:    "id",
					Type:    "INT",
					Primary: true,
					Null:    false,
				},
				"secondary_id": {
					Name:    "secondary_id",
					Type:    "INT",
					Primary: true,
					Null:    false,
				},
				"name": {
					Name: "name",
					Type: "VARCHAR(255)",
					Null: false,
				},
			},
			expectedSchemaSQL:      "CREATE SCHEMA IF NOT EXISTS `test_schema`",
			expectedCreateTableSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`test_table` (`id` INT NOT NULL,`secondary_id` INT NOT NULL,`name` VARCHAR(255) NOT NULL,\n\tCONSTRAINT `PK_test_schema_test_table` PRIMARY KEY (`id`,`secondary_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			expectError:            false,
		},
		{
			name:    "Create table with foreign key",
			dataset: "test_schema",
			table:   "orders",
			columns: map[string]Column{
				"order_id": {
					Name:    "order_id",
					Type:    "INT",
					Primary: true,
					Null:    false,
				},
				"customer_id": {
					Name:          "customer_id",
					Type:          "INT",
					Table:         "orders",
					Null:          false,
					ForeignKey:    "id",
					ForeignTable:  "customers",
					ForeignSchema: "test_schema",
				},
			},
			expectedSchemaSQL:      "CREATE SCHEMA IF NOT EXISTS `test_schema`",
			expectedCreateTableSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`orders` (`order_id` INT NOT NULL,`customer_id` INT NOT NULL,\n\tPRIMARY KEY(`order_id`),\n\tCONSTRAINT `FK_orders_customer_id` FOREIGN KEY (`customer_id`) REFERENCES `test_schema`.`customers` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			expectError:            false,
		},
		{
			name:    "Missing primary key",
			dataset: "test_schema",
			table:   "test_table",
			columns: map[string]Column{
				"name": {
					Name: "name",
					Type: "VARCHAR(255)",
					Null: false,
				},
			},
			expectError:  true,
			errorMessage: MissingPrimaryKeyErr.Error(),
		},
		{
			name:    "Column with default value",
			dataset: "test_schema",
			table:   "products",
			columns: map[string]Column{
				"product_id": {
					Name:    "product_id",
					Type:    "INT",
					Primary: true,
					Null:    false,
				},
				"price": {
					Name:    "price",
					Type:    "DECIMAL(10,2)",
					Null:    false,
					Default: "0.00",
				},
			},
			expectedSchemaSQL:      "CREATE SCHEMA IF NOT EXISTS `test_schema`",
			expectedCreateTableSQL: "CREATE TABLE IF NOT EXISTS `test_schema`.`products` (`product_id` INT NOT NULL,`price` DECIMAL(10,2) NOT NULL DEFAULT 0.00,\n\tPRIMARY KEY(`product_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			expectError:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the function under test
			s := &SqlDB{}
			schemaSQL, createTableSQL, err := s.BuildCreateTableQueries(tt.dataset, tt.table, tt.columns)

			if tt.expectError {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedSchemaSQL, schemaSQL)
				assert.Equal(t, tt.expectedCreateTableSQL, createTableSQL)
			}
		})
	}
}
