package unmarshal

import "github.com/gocql/gocql"

type DiffType struct {
	Path          Path
	ExpectedType  gocql.TypeInfo
	ExpectedValue []byte
	ReceivedType  gocql.TypeInfo
	ReceivedValue []byte
}

type DiffListLen struct {
	Path          Path
	ExpectedType  gocql.TypeInfo
	ExpectedLen   int
	ExpectedValue []byte
	ReceivedType  gocql.TypeInfo
	ReceivedLen   int
	ReceivedValue []byte
}

type DiffTupleLen struct {
	Path          Path
	ExpectedType  gocql.TypeInfo
	ExpectedLen   int
	ExpectedValue []byte
	ReceivedType  gocql.TypeInfo
	ReceivedLen   int
	ReceivedValue []byte
}

type DiffTupleElemType struct {
	Path             Path
	ExpectedElemType gocql.TypeInfo
	ExpectedValue    []byte
	ReceivedElemType gocql.TypeInfo
	ReceivedValue    []byte
}

type DiffUDTElemType struct {
	Path         Path
	ExpectedName string
	ExpectedType gocql.TypeInfo
	ReceivedName string
	ReceivedType gocql.TypeInfo
}

type DiffUDTElemNotFound struct {
	Path             Path
	ElemName         string
	ExpectedElemType gocql.TypeInfo
	ExpectedValue    []byte
	ReceivedElemType gocql.TypeInfo
	ReceivedValue    []byte
}

type DiffListElemType struct {
	Path             Path
	ExpectedElemType gocql.TypeInfo
	ExpectedValue    []byte
	ReceivedElemType gocql.TypeInfo
	ReceivedValue    []byte
}

type DiffList []interface{}

func (l *DiffList) Add(item interface{}) {
	*l = append(*l, item)
}

func (l *DiffList) Len() int {
	return len(*l)
}

type DiffValue struct {
	Path          Path
	ExpectedType  gocql.TypeInfo
	ExpectedValue []byte
	ReceivedType  gocql.TypeInfo
	ReceivedValue []byte
}
