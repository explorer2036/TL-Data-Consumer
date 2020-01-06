package model

import "sort"

// database schema configuration in consul
//
// fixed_columns: ["userid", "source", "path"]
// relations:
//   - dtype: "data_accounts"
//     table: "accounts"
//     columns: ["acct", "type", "brokerage"]
//   - dtype: "data_accountdetail"
//     table: "account_detail"
// 	columns: ["acct", "description", "detail"]

// Relation defines fields for database schema
type Relation struct {
	DataType string   `yaml:"dtype"`
	Table    string   `yaml:"table"`
	Columns  []string `yaml:"columns"`
}

// TableSchemas reflects the schema configuration from consul to table schema
type TableSchemas struct {
	Columns   []string   `yaml:"fixed_columns"`
	Relations []Relation `yaml:"relations"`
}

// JSONSchema defines the schema for json data
type JSONSchema map[string]interface{}

// GetColumns returns the sorted columns
func (s JSONSchema) GetColumns() []string {
	var columns []string
	for key := range s {
		columns = append(columns, key)
	}

	sort.Strings(columns)
	return columns
}

// GetValues returns the values for sorted columns
func (s JSONSchema) GetValues() []interface{} {
	columns := s.GetColumns()

	var values []interface{}
	for _, key := range columns {
		values = append(values, s[key])
	}
	return values
}

// SchemasCarrier defines the transfer structure for engine and storage
type SchemasCarrier struct {
	Table   string
	Schemas []JSONSchema
}
