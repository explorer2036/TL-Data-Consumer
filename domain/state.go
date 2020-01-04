package domain

import (
	"fmt"
	"time"
)

// State (insert):
//	user | source | path | type | value | time | timestamp

// State defines the struct which maps to table 'state'
type State struct {
	UserID    string
	Source    string
	Path      string
	Type      string
	Value     string
	Time      time.Time
	Timestamp time.Time
}

// String returns the string format
func (s *State) String() string {
	return fmt.Sprintf("{%s,%s}", s.UserID, s.Value)
}

// GetTable returns the table name for State
func (s *State) GetTable() string {
	return "state"
}

// GetColumns returns the table columns for State
func (s *State) GetColumns() []string {
	return []string{"userid", "source", "path", "type", "value", "time", "timestamp"}
}

// GetValues returns the field values for State
func (s *State) GetValues() []interface{} {
	return []interface{}{s.UserID, s.Source, s.Path, s.Type, s.Value, s.Time, s.Timestamp}
}
