package unmarshal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"math/bits"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	protoVersion2 = 0x02
)

var (
	bigOne     = big.NewInt(1)
	emptyValue reflect.Value
)

var (
	ErrorUDTUnavailable = errors.New("UDT are not available on protocols less than 3, please update config")
)

// Unmarshaler is the interface implemented by objects that can unmarshal
// a Cassandra specific description of themselves.
type Unmarshaler interface {
	UnmarshalCQL(info gocql.TypeInfo, data []byte) error
}

// Unmarshal parses the CQL encoded data based on the info parameter that
// describes the Cassandra internal data type and stores the result in the
// value pointed by value.
//
// If value implements Unmarshaler, it's UnmarshalCQL method is called to
// unmarshal the data.
// If value is a pointer to pointer, it is set to nil if the CQL value is
// null. Otherwise, nulls are unmarshalled as zero value.
//
// Supported conversions are as follows, other type combinations may be added in the future:
//
//	CQL type                                | Go type (value)         | Note
//	varchar, ascii, blob, text              | *string                 |
//	varchar, ascii, blob, text              | *[]byte                 | non-nil buffer is reused
//	bool                                    | *bool                   |
//	tinyint, smallint, int, bigint, counter | *integer types          |
//	tinyint, smallint, int, bigint, counter | *big.Int                |
//	tinyint, smallint, int, bigint, counter | *string                 | formatted as base 10 number
//	float                                   | *float32                |
//	double                                  | *float64                |
//	decimal                                 | *inf.Dec                |
//	time                                    | *int64                  | nanoseconds since start of day
//	time                                    | *time.Duration          |
//	timestamp                               | *int64                  | milliseconds since Unix epoch
//	timestamp                               | *time.Time              |
//	list, set                               | *slice, *array          |
//	map                                     | *map[X]Y                |
//	uuid, timeuuid                          | *string                 | see UUID.String
//	uuid, timeuuid                          | *[]byte                 | raw UUID bytes
//	uuid, timeuuid                          | *gocql.UUID             |
//	timeuuid                                | *time.Time              | timestamp of the UUID
//	inet                                    | *net.IP                 |
//	inet                                    | *string                 | IPv4 or IPv6 address string
//	tuple                                   | *slice, *array          |
//	tuple                                   | *struct                 | struct fields are set in order of declaration
//	user-defined types                      | gocql.UDTUnmarshaler    | UnmarshalUDT is called
//	user-defined types                      | *map[string]interface{} |
//	user-defined types                      | *struct                 | cql tag is used to determine field name
//	date                                    | *time.Time              | time of beginning of the day (in UTC)
//	date                                    | *string                 | formatted with 2006-01-02 format
//	duration                                | *gocql.Duration         |
func Unmarshal(info gocql.TypeInfo, data []byte, value interface{}) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data)
	}

	if isNullableValue(value) {
		return unmarshalNullable(info, data, value)
	}

	switch info.Type() {
	case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeBlob, gocql.TypeText:
		return unmarshalVarchar(info, data, value)
	case gocql.TypeBoolean:
		return unmarshalBool(info, data, value)
	case gocql.TypeInt:
		return unmarshalInt(info, data, value)
	case gocql.TypeBigInt, gocql.TypeCounter:
		return unmarshalBigInt(info, data, value)
	case gocql.TypeVarint:
		return unmarshalVarint(info, data, value)
	case gocql.TypeSmallInt:
		return unmarshalSmallInt(info, data, value)
	case gocql.TypeTinyInt:
		return unmarshalTinyInt(info, data, value)
	case gocql.TypeFloat:
		return unmarshalFloat(info, data, value)
	case gocql.TypeDouble:
		return unmarshalDouble(info, data, value)
	case gocql.TypeDecimal:
		return unmarshalDecimal(info, data, value)
	case gocql.TypeTime:
		return unmarshalTime(info, data, value)
	case gocql.TypeTimestamp:
		return unmarshalTimestamp(info, data, value)
	case gocql.TypeList, gocql.TypeSet:
		return unmarshalList(info, data, value)
	case gocql.TypeMap:
		return unmarshalMap(info, data, value)
	case gocql.TypeTimeUUID:
		return unmarshalTimeUUID(info, data, value)
	case gocql.TypeUUID:
		return unmarshalUUID(info, data, value)
	case gocql.TypeInet:
		return unmarshalInet(info, data, value)
	case gocql.TypeTuple:
		return unmarshalTuple(info, data, value)
	case gocql.TypeUDT:
		return unmarshalUDT(info, data, value)
	case gocql.TypeDate:
		return unmarshalDate(info, data, value)
	case gocql.TypeDuration:
		return unmarshalDuration(info, data, value)
	}

	// detect protocol 2 UDT
	if strings.HasPrefix(info.Custom(), "org.apache.cassandra.db.marshal.UserType") && info.Version() < 3 {
		return ErrorUDTUnavailable
	}

	// TODO(tux21b): add the remaining types
	return fmt.Errorf("can not unmarshal %s into %T", info, value)
}

func isNullableValue(value interface{}) bool {
	v := reflect.ValueOf(value)
	return v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Ptr
}

func isNullData(_ gocql.TypeInfo, data []byte) bool {
	return data == nil
}

func unmarshalNullable(info gocql.TypeInfo, data []byte, value interface{}) error {
	valueRef := reflect.ValueOf(value)

	if isNullData(info, data) {
		nilValue := reflect.Zero(valueRef.Type().Elem())
		valueRef.Elem().Set(nilValue)
		return nil
	}

	newValue := reflect.New(valueRef.Type().Elem().Elem())
	valueRef.Elem().Set(newValue)
	return Unmarshal(info, data, newValue.Interface())
}

func unmarshalVarchar(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *string:
		*v = string(data)
		return nil
	case *[]byte:
		if data != nil {
			*v = append((*v)[:0], data...)
		} else {
			*v = nil
		}
		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()
	switch {
	case k == reflect.String:
		rv.SetString(string(data))
		return nil
	case k == reflect.Slice && t.Elem().Kind() == reflect.Uint8, k == reflect.Interface:
		var dataCopy []byte
		if data != nil {
			dataCopy = make([]byte, len(data))
			copy(dataCopy, data)
		}
		if k == reflect.Slice {
			rv.SetBytes(dataCopy)
		} else {
			rv.Set(reflect.ValueOf(dataCopy))
		}
		return nil
	}

	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func decInt(x []byte) int32 {
	if len(x) != 4 {
		return 0
	}
	return int32(x[0])<<24 | int32(x[1])<<16 | int32(x[2])<<8 | int32(x[3])
}

func decShort(p []byte) int16 {
	if len(p) != 2 {
		return 0
	}
	return int16(p[0])<<8 | int16(p[1])
}

func decTiny(p []byte) int8 {
	if len(p) != 1 {
		return 0
	}
	return int8(p[0])
}

func bytesToInt64(data []byte) (ret int64) {
	for i := range data {
		ret |= int64(data[i]) << (8 * uint(len(data)-i-1))
	}
	return ret
}

func bytesToUint64(data []byte) (ret uint64) {
	for i := range data {
		ret |= uint64(data[i]) << (8 * uint(len(data)-i-1))
	}
	return ret
}

func unmarshalBigInt(info gocql.TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, decBigInt(data), data, value)
}

func unmarshalInt(info gocql.TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, int64(decInt(data)), data, value)
}

func unmarshalSmallInt(info gocql.TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, int64(decShort(data)), data, value)
}

func unmarshalTinyInt(info gocql.TypeInfo, data []byte, value interface{}) error {
	return unmarshalIntlike(info, int64(decTiny(data)), data, value)
}

func unmarshalVarint(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case *big.Int:
		return unmarshalIntlike(info, 0, data, value)
	case *uint64:
		if len(data) == 9 && data[0] == 0 {
			*v = bytesToUint64(data[1:])
			return nil
		}
	}

	if len(data) > 8 {
		return unmarshalErrorf("unmarshal int: varint value %v out of range for %T (use big.Int)", data, value)
	}

	int64Val := bytesToInt64(data)
	if len(data) > 0 && len(data) < 8 && data[0]&0x80 > 0 {
		int64Val -= 1 << uint(len(data)*8)
	}
	return unmarshalIntlike(info, int64Val, data, value)
}

func unmarshalIntlike(info gocql.TypeInfo, int64Val int64, data []byte, value interface{}) error {
	switch v := value.(type) {
	case *int:
		if ^uint(0) == math.MaxUint32 && (int64Val < math.MinInt32 || int64Val > math.MaxInt32) {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int(int64Val)
		return nil
	case *uint:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case gocql.TypeInt:
			*v = uint(unitVal) & 0xFFFFFFFF
		case gocql.TypeSmallInt:
			*v = uint(unitVal) & 0xFFFF
		case gocql.TypeTinyInt:
			*v = uint(unitVal) & 0xFF
		default:
			if ^uint(0) == math.MaxUint32 && (int64Val < 0 || int64Val > math.MaxUint32) {
				return unmarshalErrorf("unmarshal int: value %d out of range for %T", unitVal, *v)
			}
			*v = uint(unitVal)
		}
		return nil
	case *int64:
		*v = int64Val
		return nil
	case *uint64:
		switch info.Type() {
		case gocql.TypeInt:
			*v = uint64(int64Val) & 0xFFFFFFFF
		case gocql.TypeSmallInt:
			*v = uint64(int64Val) & 0xFFFF
		case gocql.TypeTinyInt:
			*v = uint64(int64Val) & 0xFF
		default:
			*v = uint64(int64Val)
		}
		return nil
	case *int32:
		if int64Val < math.MinInt32 || int64Val > math.MaxInt32 {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int32(int64Val)
		return nil
	case *uint32:
		switch info.Type() {
		case gocql.TypeInt:
			*v = uint32(int64Val) & 0xFFFFFFFF
		case gocql.TypeSmallInt:
			*v = uint32(int64Val) & 0xFFFF
		case gocql.TypeTinyInt:
			*v = uint32(int64Val) & 0xFF
		default:
			if int64Val < 0 || int64Val > math.MaxUint32 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
			}
			*v = uint32(int64Val) & 0xFFFFFFFF
		}
		return nil
	case *int16:
		if int64Val < math.MinInt16 || int64Val > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int16(int64Val)
		return nil
	case *uint16:
		switch info.Type() {
		case gocql.TypeSmallInt:
			*v = uint16(int64Val) & 0xFFFF
		case gocql.TypeTinyInt:
			*v = uint16(int64Val) & 0xFF
		default:
			if int64Val < 0 || int64Val > math.MaxUint16 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
			}
			*v = uint16(int64Val) & 0xFFFF
		}
		return nil
	case *int8:
		if int64Val < math.MinInt8 || int64Val > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = int8(int64Val)
		return nil
	case *uint8:
		if info.Type() != gocql.TypeTinyInt && (int64Val < 0 || int64Val > math.MaxUint8) {
			return unmarshalErrorf("unmarshal int: value %d out of range for %T", int64Val, *v)
		}
		*v = uint8(int64Val) & 0xFF
		return nil
	case *big.Int:
		decBigInt2C(data, v)
		return nil
	case *string:
		*v = strconv.FormatInt(int64Val, 10)
		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()

	switch rv.Type().Kind() {
	case reflect.Int:
		if ^uint(0) == math.MaxUint32 && (int64Val < math.MinInt32 || int64Val > math.MaxInt32) {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Int64:
		rv.SetInt(int64Val)
		return nil
	case reflect.Int32:
		if int64Val < math.MinInt32 || int64Val > math.MaxInt32 {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Int16:
		if int64Val < math.MinInt16 || int64Val > math.MaxInt16 {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Int8:
		if int64Val < math.MinInt8 || int64Val > math.MaxInt8 {
			return unmarshalErrorf("unmarshal int: value %d out of range", int64Val)
		}
		rv.SetInt(int64Val)
		return nil
	case reflect.Uint:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case gocql.TypeInt:
			rv.SetUint(unitVal & 0xFFFFFFFF)
		case gocql.TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case gocql.TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			if ^uint(0) == math.MaxUint32 && (int64Val < 0 || int64Val > math.MaxUint32) {
				return unmarshalErrorf("unmarshal int: value %d out of range for %s", unitVal, rv.Type())
			}
			rv.SetUint(unitVal)
		}
		return nil
	case reflect.Uint64:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case gocql.TypeInt:
			rv.SetUint(unitVal & 0xFFFFFFFF)
		case gocql.TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case gocql.TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			rv.SetUint(unitVal)
		}
		return nil
	case reflect.Uint32:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case gocql.TypeInt:
			rv.SetUint(unitVal & 0xFFFFFFFF)
		case gocql.TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case gocql.TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			if int64Val < 0 || int64Val > math.MaxUint32 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %s", int64Val, rv.Type())
			}
			rv.SetUint(unitVal & 0xFFFFFFFF)
		}
		return nil
	case reflect.Uint16:
		unitVal := uint64(int64Val)
		switch info.Type() {
		case gocql.TypeSmallInt:
			rv.SetUint(unitVal & 0xFFFF)
		case gocql.TypeTinyInt:
			rv.SetUint(unitVal & 0xFF)
		default:
			if int64Val < 0 || int64Val > math.MaxUint16 {
				return unmarshalErrorf("unmarshal int: value %d out of range for %s", int64Val, rv.Type())
			}
			rv.SetUint(unitVal & 0xFFFF)
		}
		return nil
	case reflect.Uint8:
		if info.Type() != gocql.TypeTinyInt && (int64Val < 0 || int64Val > math.MaxUint8) {
			return unmarshalErrorf("unmarshal int: value %d out of range for %s", int64Val, rv.Type())
		}
		rv.SetUint(uint64(int64Val) & 0xff)
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func decBigInt(data []byte) int64 {
	if len(data) != 8 {
		return 0
	}
	return int64(data[0])<<56 | int64(data[1])<<48 |
		int64(data[2])<<40 | int64(data[3])<<32 |
		int64(data[4])<<24 | int64(data[5])<<16 |
		int64(data[6])<<8 | int64(data[7])
}

func unmarshalBool(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *bool:
		*v = decBool(data)
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Bool:
		rv.SetBool(decBool(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func decBool(v []byte) bool {
	if len(v) == 0 {
		return false
	}
	return v[0] != 0
}

func unmarshalFloat(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float32:
		*v = math.Float32frombits(uint32(decInt(data)))
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Float32:
		rv.SetFloat(float64(math.Float32frombits(uint32(decInt(data)))))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalDouble(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *float64:
		*v = math.Float64frombits(uint64(decBigInt(data)))
		return nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Float64:
		rv.SetFloat(math.Float64frombits(uint64(decBigInt(data))))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalDecimal(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *inf.Dec:
		scale := decInt(data[0:4])
		unscaled := decBigInt2C(data[4:], nil)
		*v = *inf.NewDecBig(unscaled, inf.Scale(scale))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

// decBigInt2C sets the value of n to the big-endian two's complement
// value stored in the given data. If data[0]&80 != 0, the number
// is negative. If data is empty, the result will be 0.
func decBigInt2C(data []byte, n *big.Int) *big.Int {
	if n == nil {
		n = new(big.Int)
	}
	n.SetBytes(data)
	if len(data) > 0 && data[0]&0x80 > 0 {
		n.Sub(n, new(big.Int).Lsh(bigOne, uint(len(data))*8))
	}
	return n
}

func unmarshalTime(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int64:
		*v = decBigInt(data)
		return nil
	case *time.Duration:
		*v = time.Duration(decBigInt(data))
		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Int64:
		rv.SetInt(decBigInt(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalTimestamp(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *int64:
		*v = decBigInt(data)
		return nil
	case *time.Time:
		if len(data) == 0 {
			*v = time.Time{}
			return nil
		}
		x := decBigInt(data)
		sec := x / 1000
		nsec := (x - sec*1000) * 1000000
		*v = time.Unix(sec, nsec).In(time.UTC)
		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	switch rv.Type().Kind() {
	case reflect.Int64:
		rv.SetInt(decBigInt(data))
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalDate(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *time.Time:
		if len(data) == 0 {
			*v = time.Time{}
			return nil
		}
		var origin uint32 = 1 << 31
		var current = binary.BigEndian.Uint32(data)
		timestamp := (int64(current) - int64(origin)) * 86400000
		*v = time.Unix(0, timestamp*int64(time.Millisecond)).In(time.UTC)
		return nil
	case *string:
		if len(data) == 0 {
			*v = ""
			return nil
		}
		var origin uint32 = 1 << 31
		var current = binary.BigEndian.Uint32(data)
		timestamp := (int64(current) - int64(origin)) * 86400000
		*v = time.Unix(0, timestamp*int64(time.Millisecond)).In(time.UTC).Format("2006-01-02")
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalDuration(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *gocql.Duration:
		if len(data) == 0 {
			*v = gocql.Duration{
				Months:      0,
				Days:        0,
				Nanoseconds: 0,
			}
			return nil
		}
		months, days, nanos := decVints(data)
		*v = gocql.Duration{
			Months:      months,
			Days:        days,
			Nanoseconds: nanos,
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func decVints(data []byte) (int32, int32, int64) {
	month, i := decVint(data)
	days, j := decVint(data[i:])
	nanos, _ := decVint(data[i+j:])
	return int32(month), int32(days), nanos
}

func decVint(data []byte) (int64, int) {
	firstByte := data[0]
	if firstByte&0x80 == 0 {
		return decIntZigZag(uint64(firstByte)), 1
	}
	numBytes := bits.LeadingZeros32(uint32(^firstByte)) - 24
	ret := uint64(firstByte & (0xff >> uint(numBytes)))
	for i := 0; i < numBytes; i++ {
		ret <<= 8
		ret |= uint64(data[i+1] & 0xff)
	}
	return decIntZigZag(ret), numBytes + 1
}

func decIntZigZag(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

func readCollectionSize(protoVersion byte, data []byte) (size, read int, err error) {
	if protoVersion > protoVersion2 {
		if len(data) < 4 {
			return 0, 0, unmarshalErrorf("unmarshal list: unexpected eof")
		}
		size = int(data[0])<<24 | int(data[1])<<16 | int(data[2])<<8 | int(data[3])
		read = 4
	} else {
		if len(data) < 2 {
			return 0, 0, unmarshalErrorf("unmarshal list: unexpected eof")
		}
		size = int(data[0])<<8 | int(data[1])
		read = 2
	}
	return
}

func unmarshalList(info gocql.TypeInfo, data []byte, value interface{}) error {
	listInfo, ok := info.(gocql.CollectionType)
	if !ok {
		return unmarshalErrorf("unmarshal: can not unmarshal none collection type into list")
	}
	pversion := listInfo.NativeType.Version()

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()

	switch k {
	case reflect.Slice, reflect.Array:
		if data == nil {
			if k == reflect.Array {
				return unmarshalErrorf("unmarshal list: can not store nil in array value")
			}
			if rv.IsNil() {
				return nil
			}
			rv.Set(reflect.Zero(t))
			return nil
		}
		n, p, err := readCollectionSize(pversion, data)
		if err != nil {
			return err
		}
		data = data[p:]
		if k == reflect.Array {
			if rv.Len() != n {
				return unmarshalErrorf("unmarshal list: array with wrong size")
			}
		} else {
			rv.Set(reflect.MakeSlice(t, n, n))
		}
		for i := 0; i < n; i++ {
			m, p, err := readCollectionSize(pversion, data)
			if err != nil {
				return err
			}
			data = data[p:]
			if len(data) < m {
				return unmarshalErrorf("unmarshal list: unexpected eof")
			}
			if err := Unmarshal(listInfo.Elem, data[:m], rv.Index(i).Addr().Interface()); err != nil {
				return err
			}
			data = data[m:]
		}
		return nil
	}
	return unmarshalErrorf("can not unmarshal %s into %T", info, value)
}

func unmarshalMap(info gocql.TypeInfo, data []byte, value interface{}) error {
	mapInfo, ok := info.(gocql.CollectionType)
	if !ok {
		return unmarshalErrorf("unmarshal: can not unmarshal none collection type into map")
	}
	pversion := mapInfo.NativeType.Version()

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}
	rv = rv.Elem()
	t := rv.Type()
	if t.Kind() != reflect.Map {
		return unmarshalErrorf("can not unmarshal %s into %T", info, value)
	}
	if data == nil {
		rv.Set(reflect.Zero(t))
		return nil
	}
	rv.Set(reflect.MakeMap(t))
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
		key := reflect.New(t.Key())
		if err := Unmarshal(mapInfo.Key, data[:m], key.Interface()); err != nil {
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
		val := reflect.New(t.Elem())
		if err := Unmarshal(mapInfo.Elem, data[:m], val.Interface()); err != nil {
			return err
		}
		data = data[m:]

		rv.SetMapIndex(key.Elem(), val.Elem())
	}
	return nil
}

func unmarshalUUID(info gocql.TypeInfo, data []byte, value interface{}) error {
	if len(data) == 0 {
		switch v := value.(type) {
		case *string:
			*v = ""
		case *[]byte:
			*v = nil
		case *gocql.UUID:
			*v = gocql.UUID{}
		default:
			return unmarshalErrorf("can not unmarshal X %s into %T", info, value)
		}

		return nil
	}

	if len(data) != 16 {
		return unmarshalErrorf("unable to parse UUID: UUIDs must be exactly 16 bytes long")
	}

	switch v := value.(type) {
	case *[16]byte:
		copy((*v)[:], data)
		return nil
	case *gocql.UUID:
		copy((*v)[:], data)
		return nil
	}

	u, err := gocql.UUIDFromBytes(data)
	if err != nil {
		return unmarshalErrorf("unable to parse UUID: %s", err)
	}

	switch v := value.(type) {
	case *string:
		*v = u.String()
		return nil
	case *[]byte:
		*v = u[:]
		return nil
	}
	return unmarshalErrorf("can not unmarshal X %s into %T", info, value)
}

func unmarshalTimeUUID(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *time.Time:
		id, err := gocql.UUIDFromBytes(data)
		if err != nil {
			return err
		} else if id.Version() != 1 {
			return unmarshalErrorf("invalid timeuuid")
		}
		*v = id.Time()
		return nil
	default:
		return unmarshalUUID(info, data, value)
	}
}

func unmarshalInet(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case *net.IP:
		if x := len(data); !(x == 4 || x == 16) {
			return unmarshalErrorf("cannot unmarshal %s into %T: invalid sized IP: got %d bytes not 4 or 16", info, value, x)
		}
		buf := copyBytes(data)
		ip := net.IP(buf)
		if v4 := ip.To4(); v4 != nil {
			*v = v4
			return nil
		}
		*v = ip
		return nil
	case *string:
		if len(data) == 0 {
			*v = ""
			return nil
		}
		ip := net.IP(data)
		if v4 := ip.To4(); v4 != nil {
			*v = v4.String()
			return nil
		}
		*v = ip.String()
		return nil
	}
	return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
}

func readBytes(p []byte) ([]byte, []byte) {
	// TODO: really should use a framer
	size := readInt(p)
	p = p[4:]
	if size < 0 {
		return nil, p
	}
	return p[:size], p[size:]
}

func readInt(p []byte) int32 {
	return int32(p[0])<<24 | int32(p[1])<<16 | int32(p[2])<<8 | int32(p[3])
}

// currently only support unmarshal into a list of values, this makes it possible
// to support tuples without changing the query API. In the future this can be extend
// to allow unmarshalling into custom tuple types.
func unmarshalTuple(info gocql.TypeInfo, data []byte, value interface{}) error {
	if v, ok := value.(Unmarshaler); ok {
		return v.UnmarshalCQL(info, data)
	}

	tuple := info.(gocql.TupleTypeInfo)
	switch v := value.(type) {
	case []interface{}:
		for i, elem := range tuple.Elems {
			// each element inside data is a [bytes]
			var p []byte
			if len(data) >= 4 {
				p, data = readBytes(data)
			}
			err := Unmarshal(elem, p, v[i])
			if err != nil {
				return err
			}
		}

		return nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Ptr {
		return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
	}

	rv = rv.Elem()
	t := rv.Type()
	k := t.Kind()

	switch k {
	case reflect.Struct:
		if v := t.NumField(); v != len(tuple.Elems) {
			return unmarshalErrorf("can not unmarshal tuple into struct %v, not enough fields have %d need %d", t, v, len(tuple.Elems))
		}

		for i, elem := range tuple.Elems {
			var p []byte
			if len(data) >= 4 {
				p, data = readBytes(data)
			}

			v := elem.New()
			if err := Unmarshal(elem, p, v); err != nil {
				return err
			}

			switch rv.Field(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					rv.Field(i).Set(reflect.ValueOf(v))
				} else {
					rv.Field(i).Set(reflect.Zero(reflect.TypeOf(v)))
				}
			default:
				rv.Field(i).Set(reflect.ValueOf(v).Elem())
			}
		}

		return nil
	case reflect.Slice, reflect.Array:
		if k == reflect.Array {
			size := rv.Len()
			if size != len(tuple.Elems) {
				return unmarshalErrorf("can not unmarshal tuple into array of length %d need %d elements", size, len(tuple.Elems))
			}
		} else {
			rv.Set(reflect.MakeSlice(t, len(tuple.Elems), len(tuple.Elems)))
		}

		for i, elem := range tuple.Elems {
			var p []byte
			if len(data) >= 4 {
				p, data = readBytes(data)
			}

			v := elem.New()
			if err := Unmarshal(elem, p, v); err != nil {
				return err
			}

			switch rv.Index(i).Kind() {
			case reflect.Ptr:
				if p != nil {
					rv.Index(i).Set(reflect.ValueOf(v))
				} else {
					rv.Index(i).Set(reflect.Zero(reflect.TypeOf(v)))
				}
			default:
				rv.Index(i).Set(reflect.ValueOf(v).Elem())
			}
		}

		return nil
	}

	return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
}

// UDTMarshaler is an interface which should be implemented by users wishing to
// handle encoding UDT types to sent to Cassandra. Note: due to current implentations
// methods defined for this interface must be value receivers not pointer receivers.
type UDTMarshaler interface {
	// MarshalUDT will be called for each field in the the UDT returned by Cassandra,
	// the implementor should marshal the type to return by for example calling
	// Marshal.
	MarshalUDT(name string, info gocql.TypeInfo) ([]byte, error)
}

// UDTUnmarshaler should be implemented by users wanting to implement custom
// UDT unmarshaling.
type UDTUnmarshaler interface {
	// UnmarshalUDT will be called for each field in the UDT return by Cassandra,
	// the implementor should unmarshal the data into the value of their chosing,
	// for example by calling Unmarshal.
	UnmarshalUDT(name string, info gocql.TypeInfo, data []byte) error
}

func unmarshalUDT(info gocql.TypeInfo, data []byte, value interface{}) error {
	switch v := value.(type) {
	case Unmarshaler:
		return v.UnmarshalCQL(info, data)
	case UDTUnmarshaler:
		udt := info.(gocql.UDTTypeInfo)

		for _, e := range udt.Elements {
			if len(data) == 0 {
				return nil
			}

			var p []byte
			p, data = readBytes(data)

			if err := v.UnmarshalUDT(e.Name, e.Type, p); err != nil {
				return err
			}
		}

		return nil
	case *map[string]interface{}:
		udt := info.(gocql.UDTTypeInfo)

		rv := reflect.ValueOf(value)
		if rv.Kind() != reflect.Ptr {
			return unmarshalErrorf("can not unmarshal into non-pointer %T", value)
		}

		rv = rv.Elem()
		t := rv.Type()
		if t.Kind() != reflect.Map {
			return unmarshalErrorf("can not unmarshal %s into %T", info, value)
		} else if data == nil {
			rv.Set(reflect.Zero(t))
			return nil
		}

		rv.Set(reflect.MakeMap(t))
		m := *v

		for _, e := range udt.Elements {
			if len(data) == 0 {
				return nil
			}

			val := reflect.New(goType(e.Type))

			var p []byte
			p, data = readBytes(data)

			if err := Unmarshal(e.Type, p, val.Interface()); err != nil {
				return err
			}

			m[e.Name] = val.Elem().Interface()
		}

		return nil
	}

	k := reflect.ValueOf(value).Elem()
	if k.Kind() != reflect.Struct || !k.IsValid() {
		return unmarshalErrorf("cannot unmarshal %s into %T", info, value)
	}

	if len(data) == 0 {
		if k.CanSet() {
			k.Set(reflect.Zero(k.Type()))
		}

		return nil
	}

	t := k.Type()
	fields := make(map[string]reflect.Value, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		if tag := sf.Tag.Get("cql"); tag != "" {
			fields[tag] = k.Field(i)
		}
	}

	udt := info.(gocql.UDTTypeInfo)
	for _, e := range udt.Elements {
		if len(data) < 4 {
			// UDT def does not match the column value
			return nil
		}

		var p []byte
		p, data = readBytes(data)

		f, ok := fields[e.Name]
		if !ok {
			f = k.FieldByName(e.Name)
			if f == emptyValue {
				// skip fields which exist in the UDT but not in
				// the struct passed in
				continue
			}
		}

		if !f.IsValid() || !f.CanAddr() {
			return unmarshalErrorf("cannot unmarshal %s into %T: field %v is not valid", info, value, e.Name)
		}

		fk := f.Addr().Interface()
		if err := Unmarshal(e.Type, p, fk); err != nil {
			return err
		}
	}

	return nil
}

type Error string

func (m Error) Error() string {
	return string(m)
}

func unmarshalErrorf(format string, args ...interface{}) Error {
	return Error(fmt.Sprintf(format, args...))
}

func copyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}

func goType(t gocql.TypeInfo) reflect.Type {
	switch t.Type() {
	case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText:
		return reflect.TypeOf(*new(string))
	case gocql.TypeBigInt, gocql.TypeCounter:
		return reflect.TypeOf(*new(int64))
	case gocql.TypeTime:
		return reflect.TypeOf(*new(time.Duration))
	case gocql.TypeTimestamp:
		return reflect.TypeOf(*new(time.Time))
	case gocql.TypeBlob:
		return reflect.TypeOf(*new([]byte))
	case gocql.TypeBoolean:
		return reflect.TypeOf(*new(bool))
	case gocql.TypeFloat:
		return reflect.TypeOf(*new(float32))
	case gocql.TypeDouble:
		return reflect.TypeOf(*new(float64))
	case gocql.TypeInt:
		return reflect.TypeOf(*new(int))
	case gocql.TypeSmallInt:
		return reflect.TypeOf(*new(int16))
	case gocql.TypeTinyInt:
		return reflect.TypeOf(*new(int8))
	case gocql.TypeDecimal:
		return reflect.TypeOf(*new(*inf.Dec))
	case gocql.TypeUUID, gocql.TypeTimeUUID:
		return reflect.TypeOf(*new(gocql.UUID))
	case gocql.TypeList, gocql.TypeSet:
		return reflect.SliceOf(goType(t.(gocql.CollectionType).Elem))
	case gocql.TypeMap:
		return reflect.MapOf(goType(t.(gocql.CollectionType).Key), goType(t.(gocql.CollectionType).Elem))
	case gocql.TypeVarint:
		return reflect.TypeOf(*new(*big.Int))
	case gocql.TypeTuple:
		// what can we do here? all there is to do is to make a list of interface{}
		tuple := t.(gocql.TupleTypeInfo)
		return reflect.TypeOf(make([]interface{}, len(tuple.Elems)))
	case gocql.TypeUDT:
		return reflect.TypeOf(make(map[string]interface{}))
	case gocql.TypeDate:
		return reflect.TypeOf(*new(time.Time))
	case gocql.TypeDuration:
		return reflect.TypeOf(*new(gocql.Duration))
	default:
		return nil
	}
}
