package unmarshal

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gemini/pkg/murmur"
	"hash"
	"unsafe"
)

// getHash get hash for protocol V2 and older
func getHash(pathname Path, info gocql.TypeInfo, h hash.Hash64, data []byte) error {
	switch info.Type() {
	case gocql.TypeList, gocql.TypeSet:
		return getHashList(pathname, info, h, data)
	case gocql.TypeMap:
		return getHashMap(pathname, info, h, data)
	case gocql.TypeTuple:
		return getHashOldTuple(pathname, info, h, data)
	case gocql.TypeUDT:
		return getHashOldUDT(pathname, info, h, data)
	default:
		_, err := h.Write(data)
		return err
	}
}

// GetColumnHash get hash for protocol V2 and older
func GetColumnHash(info gocql.TypeInfo, data []byte) (uint64, error) {
	hash := murmur.Hash{}
	err := getHash(nil, info, &hash, data)
	if err != nil {
		return 0, err
	}
	return hash.Sum64(), nil
}

func readListElement(protoVersion byte, data []byte) (out []byte, size int, err error) {
	var p int
	size, p, err = readCollectionSize(protoVersion, data)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unmarshal: failed to read collection element length")
	}
	data = data[p:]
	if len(data) < size {
		return nil, 0, unmarshalErrorf("unmarshal list: unexpected eof")
	}
	return data, size, nil
}

func DiffRows(oracleInfo gocql.TypeInfo, oracleData []byte, testInfo gocql.TypeInfo, testData []byte, cb func(info gocql.TypeInfo, data []byte, info1 gocql.TypeInfo, data1 []byte) bool) {

}

func Compare(oracleInfo gocql.TypeInfo, oracleData []byte, testInfo gocql.TypeInfo, testData []byte, cb func(info gocql.TypeInfo, data []byte, info1 gocql.TypeInfo, data1 []byte) bool) {

}

func bytesToString(value []byte) string {
	return unsafe.String(&value[0], len(value))
}

func uint64ToBytes(val uint64) []byte {
	return []byte{byte(val >> 56), byte(val >> 48), byte(val >> 40), byte(val >> 32), byte(val >> 24), byte(val >> 16), byte(val >> 8), byte(val)}
}

//
//var errStopOnFirst = errors.New("stop on first difference")

//func walkthrue(pathname Path, info gocql.TypeInfo, data []byte, info1 gocql.TypeInfo, data1 []byte, diffs DiffList, stopOnFirst bool) error {
//	if info.Type() != info.Type() {
//		diffs.Add(DiffType{
//			Path:          pathname,
//			ExpectedType:  info,
//			ExpectedValue: data,
//			ReceivedType:  info1,
//			ReceivedValue: data1,
//		})
//		if stopOnFirst {
//			return errStopOnFirst
//		}
//		return nil
//	}
//
//	switch info.Type() {
//	case gocql.TypeInt, gocql.TypeBoolean, gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeBlob, gocql.TypeText, gocql.TypeBigInt, gocql.TypeCounter, gocql.TypeVarint, gocql.TypeSmallInt, gocql.TypeTinyInt, gocql.TypeFloat, gocql.TypeDouble, gocql.TypeDecimal, gocql.TypeTime, gocql.TypeTimestamp, gocql.TypeTimeUUID, gocql.TypeUUID, gocql.TypeInet, gocql.TypeDate:
//		if bytesToString(data) != bytesToString(data1) {
//			diffs.Add(DiffValue{
//				Path:          pathname,
//				ExpectedValue: data,
//				ExpectedType:  info,
//				ReceivedValue: data1,
//				ReceivedType:  info1,
//			})
//			if stopOnFirst {
//				return errStopOnFirst
//			}
//			return nil
//		}
//	case gocql.TypeList, gocql.TypeSet:
//		return compareList(pathname, info, data, info1, data1, diffs, stopOnFirst)
//	case gocql.TypeMap:
//		return unmarshalMap(info, data, value)
//	case gocql.TypeTuple:
//		return compareTuple(pathname, info, data, info1, data1, diffs, stopOnFirst)
//	case gocql.TypeUDT:
//		return compareUDT(pathname, info, data, info1, data1, diffs, stopOnFirst)
//	}
//
//	// detect protocol 2 UDT
//	if strings.HasPrefix(info.Custom(), "org.apache.cassandra.db.marshal.UserType") && info.Version() < 3 {
//		return ErrorUDTUnavailable
//	}
//
//	return fmt.Errorf("can not unmarshal %s into %T", info, value)
//}
//
//func castTypeAndReadLength(info gocql.CollectionType, data []byte) (size, read int, err error) {
//	size, read, err = readCollectionSize(info, data)
//	if err != nil {
//		return 0, 0, errors.Wrap(err, "unmarshal: failed to read collection length")
//	}
//	return size, read, nil
//}
//
//func readListElement(listInfo gocql.CollectionType, data []byte) (out []byte, size int, err error) {
//	var p int
//	size, p, err = readCollectionSize(listInfo, data)
//	if err != nil {
//		return nil, 0, errors.Wrap(err, "unmarshal: failed to read collection element length")
//	}
//	data = data[p:]
//	if len(data) < size {
//		return nil, 0, unmarshalErrorf("unmarshal list: unexpected eof")
//	}
//	return data[p:], size, nil
//}
//
//func compareList(pathname Path, info gocql.TypeInfo, data []byte, info1 gocql.TypeInfo, data1 []byte, diffs DiffList, stopOnFirst bool) error {
//	listInfo, n, p, err := castTypeAndReadLength(info, data)
//	if err != nil {
//		return errors.Wrapf(err, "%s failed to read oracle data", pathname)
//	}
//	listInfo1, n1, p1, err := castTypeAndReadLength(info, data)
//	if err != nil {
//		return errors.Wrapf(err, "%s failed to read test data", pathname)
//	}
//	if listInfo.Elem.Type() != listInfo1.Elem.Type() {
//		diffs.Add(DiffListElemType{
//			Path:             pathname,
//			ExpectedElemType: listInfo.Elem,
//			ExpectedValue:    data,
//			ReceivedElemType: listInfo1.Elem,
//			ReceivedValue:    data1,
//		})
//		if stopOnFirst {
//			return errStopOnFirst
//		}
//		return nil
//	}
//
//	if n != n1 {
//		diffs.Add(DiffListLen{
//			Path:          pathname,
//			ExpectedType:  info,
//			ExpectedLen:   n,
//			ExpectedValue: data,
//			ReceivedType:  info1,
//			ReceivedLen:   n1,
//			ReceivedValue: data1,
//		})
//		if stopOnFirst {
//			return errStopOnFirst
//		}
//		return nil
//	}
//	data = data[p:]
//	data1 = data[p1:]
//	var m, m1 int
//	for i := 0; i < n; i++ {
//		elemPath := pathname.Add(i)
//		data, m, err = readListElement(listInfo, data)
//		if err != nil {
//			return errors.Wrapf(err, "%s failed to read oracle data", pathname.String())
//		}
//		data1, m1, err = readListElement(listInfo, data1)
//		if err != nil {
//			return errors.Wrapf(err, "%s failed to read test data", pathname.String())
//		}
//		err = walkthrue(elemPath, listInfo.Elem, data[:m], listInfo1.Elem, data1[:m1], diffs, stopOnFirst)
//		if err != nil {
//			return err
//		}
//		data = data[m:]
//		data1 = data1[m1:]
//	}
//	return nil
//}
//
//func compareTuple(pathname Path, info gocql.TypeInfo, data []byte, info1 gocql.TypeInfo, data1 []byte, diffs DiffList, stopOnFirst bool) error {
//	tupleInfo, ok := info.(gocql.TupleTypeInfo)
//	if !ok {
//		return errors.Wrapf(unmarshalErrorf("unmarshal: can not unmarshal none collection type into tuple"), "%s failed to read oracle data", pathname)
//	}
//	tupleInfo1, ok := info1.(gocql.TupleTypeInfo)
//	if !ok {
//		return errors.Wrapf(unmarshalErrorf("unmarshal: can not unmarshal none collection type into tuple"), "%s failed to read test data", pathname)
//	}
//	if len(tupleInfo.Elems) != len(tupleInfo1.Elems) {
//		diffs.Add(DiffTupleLen{
//			Path:          pathname,
//			ExpectedType:  info,
//			ExpectedLen:   len(tupleInfo.Elems),
//			ExpectedValue: data,
//			ReceivedType:  info1,
//			ReceivedLen:   len(tupleInfo1.Elems),
//			ReceivedValue: data1,
//		})
//		if stopOnFirst {
//			return errStopOnFirst
//		}
//		return nil
//	}
//	var elem, elem1 []byte
//	for i, elementType := range tupleInfo.Elems {
//		elementType1 := tupleInfo1.Elems[i]
//		elemPath := pathname.Add(i)
//		if elementType.Type() != elementType1.Type() {
//			diffs.Add(DiffTupleElemType{
//				Path:             elemPath,
//				ExpectedElemType: elementType,
//				ExpectedValue:    data,
//				ReceivedElemType: elementType1,
//				ReceivedValue:    data1,
//			})
//			if stopOnFirst {
//				return errStopOnFirst
//			}
//			return nil
//		}
//		if len(data) >= 4 {
//			elem, data = readBytes(data)
//		}
//		if len(data1) >= 4 {
//			elem1, data1 = readBytes(data1)
//		}
//		err := walkthrue(elemPath, elementType, elem, elementType1, elem1, diffs, stopOnFirst)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func compareUDT(pathname Path, info gocql.TypeInfo, data []byte, info1 gocql.TypeInfo, data1 []byte, diffs DiffList, stopOnFirst bool) error {
//	udtInfo, ok := info.(gocql.UDTTypeInfo)
//	if !ok {
//		return errors.Wrapf(unmarshalErrorf("unmarshal: can not unmarshal none collection type into udt"), "%s failed to read oracle data", pathname)
//	}
//	udtInfo1, ok := info1.(gocql.UDTTypeInfo)
//	if !ok {
//		return errors.Wrapf(unmarshalErrorf("unmarshal: can not unmarshal none collection type into udt"), "%s failed to read test data", pathname)
//	}
//	if len(udtInfo.Elements) != len(udtInfo1.Elements) {
//		diffs.Add(DiffTupleLen{
//			Path:          pathname,
//			ExpectedType:  info,
//			ExpectedLen:   len(udtInfo.Elements),
//			ExpectedValue: data,
//			ReceivedType:  info1,
//			ReceivedLen:   len(udtInfo1.Elements),
//			ReceivedValue: data1,
//		})
//		if stopOnFirst {
//			return errStopOnFirst
//		}
//		return nil
//	}
//	var elem, elem1 []byte
//	diff := false
//	for i, elementType := range udtInfo.Elements {
//		elemPath := pathname.Add(elementType.Name)
//		elementType1 := udtInfo1.Elements[i]
//
//		if elementType.Name != elementType1.Name || elementType.Type.Type() != elementType1.Type.Type() {
//			diffs.Add(DiffUDTElemType{
//				Path:         elemPath,
//				ExpectedName: elementType.Name,
//				ExpectedType: elementType.Type,
//				ReceivedName: elementType1.Name,
//				ReceivedType: elementType1.Type,
//			})
//			diff = true
//		}
//	}
//
//	if diff {
//		if stopOnFirst {
//			return errStopOnFirst
//		}
//		return nil
//	}
//
//	for i, elementType := range udtInfo.Elements {
//		elementType1 := udtInfo1.Elements[i]
//		elemPath := pathname.Add(elementType.Name)
//		if len(data) >= 4 {
//			elem, data = readBytes(data)
//		}
//		if len(data1) >= 4 {
//			elem1, data1 = readBytes(data1)
//		}
//		err := walkthrue(elemPath, elementType.Type, elem, elementType1.Type, elem1, diffs, stopOnFirst)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func compareMap(pathname Path, info gocql.TypeInfo, data []byte, info1 gocql.TypeInfo, data1 []byte, diffs DiffList, stopOnFirst bool) error {
//	mapInfo, ok := info.(gocql.CollectionType)
//	if !ok {
//		return errors.Wrapf(unmarshalErrorf("unmarshal: can not unmarshal none collection type into map"), "%s failed to read oracle data", pathname)
//	}
//	mapInfo1, ok := info1.(gocql.CollectionType)
//	if !ok {
//		return errors.Wrapf(unmarshalErrorf("unmarshal: can not unmarshal none collection type into map"), "%s failed to read test data", pathname)
//	}
//
//	if len(data) == 0 {
//		if len(data1) == 0 {
//			return nil
//		}
//		panic("handle difference here")
//	}
//	n, p, err := readCollectionSize(mapInfo, data)
//	if err != nil {
//		return err
//	}
//	data = data[p:]
//
//	n1, p1, err := readCollectionSize(mapInfo1, data1)
//	if err != nil {
//		return err
//	}
//	data1 = data1[p:]
//
//	for i := 0; i < n; i++ {
//		m, p, err := readCollectionSize(mapInfo, data)
//		if err != nil {
//			return err
//		}
//		data = data[p:]
//		if len(data) < m {
//			return unmarshalErrorf("unmarshal map: unexpected eof")
//		}
//		if err := Unmarshal(mapInfo.Key, data[:m], nil); err != nil {
//			return err
//		}
//		data = data[m:]
//
//		m, p, err = readCollectionSize(mapInfo, data)
//		if err != nil {
//			return err
//		}
//		data = data[p:]
//		if len(data) < m {
//			return unmarshalErrorf("unmarshal map: unexpected eof")
//		}
//		val := reflect.New(t.Elem())
//		if err := Unmarshal(mapInfo.Elem, data[:m], val.Interface()); err != nil {
//			return err
//		}
//		data = data[m:]
//
//		rv.SetMapIndex(key.Elem(), val.Elem())
//	}
//	return nil
//}
