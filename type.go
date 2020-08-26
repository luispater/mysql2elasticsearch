package mysql2elasticsearch

import (
	"reflect"
)

type DBDataTypeMapping struct {
	Index      int
	TableIndex int
	Field      string
	Type       reflect.Type
}

type ESQueueItem struct {
	Action     string
	IndexName  string
	PrimaryKey string
	Data       interface{}
}
