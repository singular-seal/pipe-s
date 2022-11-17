package scan

type ColumnState struct {
	ColumnName  string
	ColumnValue interface{}
}

type TableState struct {
	TableName      string
	DBName         string
	Position       []*ColumnState
	EstimatedCount int64
	FinishedCount  int64
	Done           bool
}
