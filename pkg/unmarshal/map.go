package unmarshal

import (
	"encoding/json"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
)

type MapTypeWrapper struct {
	originalType gocql.CollectionType
}

func (m MapTypeWrapper) Type() gocql.Type {
	return m.originalType.Type()
}

func (m MapTypeWrapper) Version() byte {
	return m.originalType.Version()
}

func (m MapTypeWrapper) Custom() string {
	return m.originalType.Custom()
}

var stringType = reflect.TypeOf(*new(string))

func (m MapTypeWrapper) New() interface{} {
	return MapDataType{}
	//return reflect.New(reflect.MapOf(stringType, reflect.TypeOf(m.originalType.Elem.New()))).Interface()
}

type MapDataType map[string]interface{}

func (md MapDataType) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	wrt, ok := info.(MapTypeWrapper)
	if !ok {
		return errors.Errorf("wrong gocql info type: %T", info)
	}

	pversion := wrt.originalType.NativeType.Version()

	//value := info.New()

	//rv := reflect.ValueOf(value)
	//rv = rv.Elem()
	//t := rv.Type()

	if data == nil {
		//rv.Set(reflect.Zero(t))
		return nil
	}
	//rv.Set(reflect.MakeMap(t))
	n, p, err := readCollectionSize(pversion, data)
	if err != nil {
		return err
	}
	data = data[p:]
	for i := 0; i < n; i++ {
		m, p, err := readCollectionSize(pversion, data)
		if err != nil {
			return err
		}
		data = data[p:]
		if len(data) < m {
			return unmarshalErrorf("unmarshal map: unexpected eof")
		}
		key := wrt.originalType.Key.New()
		if err := gocql.Unmarshal(wrt.originalType.Key, data[:m], key); err != nil {
			return err
		}
		data = data[m:]

		m, p, err = readCollectionSize(pversion, data)
		if err != nil {
			return err
		}
		data = data[p:]
		if len(data) < m {
			return unmarshalErrorf("unmarshal map: unexpected eof")
		}
		val := wrt.originalType.Elem.New()
		if err := Unmarshal(wrt.originalType.Elem, data[:m], val); err != nil {
			return err
		}
		data = data[m:]

		type Stringer interface {
			String() string
		}

		var strKey string
		// Convert key to string
		switch casted := key.(type) {
		case Stringer:
			strKey = casted.String()
		case []byte:
			strKey = string(casted)
		default:
			out, err := json.Marshal(key)
			if err != nil {
				return errors.Wrapf(err, "cannot marshal key: %T", key)
			}
			strKey = string(out)
		}

		md[strKey] = val
	}
	return nil
}

type MapKeyTypeWrapper struct {
	originalType gocql.TypeInfo
}

func (m MapKeyTypeWrapper) Type() gocql.Type {
	return m.originalType.Type()
}

func (m MapKeyTypeWrapper) Version() byte {
	return m.originalType.Version()
}

func (m MapKeyTypeWrapper) Custom() string {
	return m.originalType.Custom()
}

func (m MapKeyTypeWrapper) New() interface{} {
	return MapKeyDataType("")
}

type MapKeyDataType string

func (m *MapKeyDataType) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	wrt, ok := info.(MapKeyTypeWrapper)
	if !ok {
		return errors.Errorf("wrong gocql info type: %T", info)
	}
	origType := wrt.originalType
	val := origType.New()
	refT := reflect.TypeOf(val)

	if err := gocql.Unmarshal(origType, data, val); err != nil {
		return err
	}

	switch refT.Kind() {
	case reflect.Map:
	case reflect.Slice:
	case reflect.Struct:
		switch val.(type) {
		case big.Int:
			//(&val.(big.Int)).String()
		case inf.Dec:
		}
	case reflect.Ptr:

	}

}

//func convert(val interface{}) (string, error) {
//	origType := wrt.originalType
//	val := origType.New()
//	refT := reflect.TypeOf(val)
//
//	if err := gocql.Unmarshal(origType, data, val); err != nil {
//		return err
//	}
//
//	switch refT.Kind() {
//	case reflect.Map:
//	case reflect.Slice:
//	case reflect.Struct:
//	case reflect.Ptr:
//
//	}
//}
