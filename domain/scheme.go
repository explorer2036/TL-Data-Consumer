package domain

// Schema is the base interface for all domain objects
type Schema interface {
	// returns the name of table it represents
	GetTable() string

	// returns the columns
	GetColumns() []string

	// returns the values corresponding to the columns
	GetValues() []interface{}

	// returns the string format
	String() string
}
