package gemini

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

const (
	TYPE_ASCII     = SimpleType("ascii")
	TYPE_BIGINT    = SimpleType("bigint")
	TYPE_BLOB      = SimpleType("blob")
	TYPE_BOOLEAN   = SimpleType("boolean")
	TYPE_DATE      = SimpleType("date")
	TYPE_DECIMAL   = SimpleType("decimal")
	TYPE_DOUBLE    = SimpleType("double")
	TYPE_DURATION  = SimpleType("duration")
	TYPE_FLOAT     = SimpleType("float")
	TYPE_INET      = SimpleType("inet")
	TYPE_INT       = SimpleType("int")
	TYPE_SMALLINT  = SimpleType("smallint")
	TYPE_TEXT      = SimpleType("text")
	TYPE_TIME      = SimpleType("time")
	TYPE_TIMESTAMP = SimpleType("timestamp")
	TYPE_TIMEUUID  = SimpleType("timeuuid")
	TYPE_TINYINT   = SimpleType("tinyint")
	TYPE_UUID      = SimpleType("uuid")
	TYPE_VARCHAR   = SimpleType("varchar")
	TYPE_VARINT    = SimpleType("varint")

	MaxUDTParts = 10
)

// TODO: Add support for time when gocql bug is fixed.
var (
	pkTypes = []SimpleType{TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT, TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT /*TYPE_TIME,*/, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT}
	types   = append(append([]SimpleType{}, pkTypes...), TYPE_BOOLEAN, TYPE_DURATION)
)

type SimpleType string

func (st SimpleType) Name() string {
	return string(st)
}

func (st SimpleType) CQLDef() string {
	return string(st)
}

func (st SimpleType) CQLHolder() string {
	return "?"
}

func (st SimpleType) Indexable() bool {
	if st == TYPE_DURATION {
		return false
	}
	return true
}

func (st SimpleType) GenValue(p *PartitionRange) []interface{} {
	var val interface{}
	switch st {
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR:
		val = randStringWithTime(p.Rand, nonEmptyRandIntRange(p.Rand, p.Max, p.Max, 10), randTime(p.Rand))
	case TYPE_BLOB:
		val = hex.EncodeToString([]byte(randStringWithTime(p.Rand, nonEmptyRandIntRange(p.Rand, p.Max, p.Max, 10), randTime(p.Rand))))
	case TYPE_BIGINT:
		val = p.Rand.Int63()
	case TYPE_BOOLEAN:
		val = p.Rand.Int()%2 == 0
	case TYPE_DATE:
		val = randDate(p.Rand)
	case TYPE_TIME, TYPE_TIMESTAMP:
		val = randTime(p.Rand)
	case TYPE_DECIMAL:
		val = inf.NewDec(randInt64Range(p.Rand, int64(p.Min), int64(p.Max)), 3)
	case TYPE_DOUBLE:
		val = randFloat64Range(p.Rand, float64(p.Min), float64(p.Max))
	case TYPE_DURATION:
		val = (time.Minute * time.Duration(randIntRange(p.Rand, p.Min, p.Max))).String()
	case TYPE_FLOAT:
		val = randFloat32Range(p.Rand, float32(p.Min), float32(p.Max))
	case TYPE_INET:
		val = net.ParseIP(randIpV4Address(p.Rand, p.Rand.Intn(255), 2))
	case TYPE_INT:
		val = nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10)
	case TYPE_SMALLINT:
		val = int16(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
	case TYPE_TIMEUUID, TYPE_UUID:
		r := gocql.UUIDFromTime(randTime(p.Rand))
		val = r.String()
	case TYPE_TINYINT:
		val = int8(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
	case TYPE_VARINT:
		val = big.NewInt(randInt64Range(p.Rand, int64(p.Min), int64(p.Max)))
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
	return []interface{}{
		val,
	}
}

func (st SimpleType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
	var (
		left  interface{}
		right interface{}
	)
	switch st {
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR:
		startTime := randTime(p.Rand)
		start := nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10)
		end := start + nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10)
		left = nonEmptyRandStringWithTime(p.Rand, start, startTime)
		right = nonEmptyRandStringWithTime(p.Rand, end, randTimeNewer(p.Rand, startTime))
	case TYPE_BLOB:
		startTime := randTime(p.Rand)
		start := nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10)
		end := start + nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10)
		left = hex.EncodeToString([]byte(nonEmptyRandStringWithTime(p.Rand, start, startTime)))
		right = hex.EncodeToString([]byte(nonEmptyRandStringWithTime(p.Rand, end, randTimeNewer(p.Rand, startTime))))
	case TYPE_BIGINT:
		start := nonEmptyRandInt64Range(p.Rand, int64(p.Min), int64(p.Max), 10)
		end := start + nonEmptyRandInt64Range(p.Rand, int64(p.Min), int64(p.Max), 10)
		left = start
		right = end
	case TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP:
		start := randTime(p.Rand)
		end := randTimeNewer(p.Rand, start)
		left = start
		right = end
	case TYPE_DECIMAL:
		start := nonEmptyRandInt64Range(p.Rand, int64(p.Min), int64(p.Max), 10)
		end := start + nonEmptyRandInt64Range(p.Rand, int64(p.Min), int64(p.Max), 10)
		left = inf.NewDec(start, 3)
		right = inf.NewDec(end, 3)
	case TYPE_DOUBLE:
		start := nonEmptyRandFloat64Range(p.Rand, float64(p.Min), float64(p.Max), 10)
		end := start + nonEmptyRandFloat64Range(p.Rand, float64(p.Min), float64(p.Max), 10)
		left = start
		right = end
	case TYPE_DURATION:
		start := time.Minute * time.Duration(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
		end := start + time.Minute*time.Duration(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
		left = start
		right = end
	case TYPE_FLOAT:
		start := nonEmptyRandFloat32Range(p.Rand, float32(p.Min), float32(p.Max), 10)
		end := start + nonEmptyRandFloat32Range(p.Rand, float32(p.Min), float32(p.Max), 10)
		left = start
		right = end
	case TYPE_INET:
		start := randIpV4Address(p.Rand, 0, 3)
		end := randIpV4Address(p.Rand, 255, 3)
		left = net.ParseIP(start)
		right = net.ParseIP(end)
	case TYPE_INT:
		start := nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10)
		end := start + nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10)
		left = start
		right = end
	case TYPE_SMALLINT:
		start := int16(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
		end := start + int16(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
		left = start
		right = end
	case TYPE_TIMEUUID, TYPE_UUID:
		start := randTime(p.Rand)
		end := randTimeNewer(p.Rand, start)
		left = gocql.UUIDFromTime(start).String()
		right = gocql.UUIDFromTime(end).String()
	case TYPE_TINYINT:
		start := int8(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
		end := start + int8(nonEmptyRandIntRange(p.Rand, p.Min, p.Max, 10))
		left = start
		right = end
	case TYPE_VARINT:
		end := &big.Int{}
		start := big.NewInt(randInt64Range(p.Rand, int64(p.Min), int64(p.Max)))
		end.Set(start)
		end = end.Add(start, big.NewInt(randInt64Range(p.Rand, int64(p.Min), int64(p.Max))))
		left = start
		right = end
	default:
		panic(fmt.Sprintf("generate value range: not supported type %s", st))
	}
	return []interface{}{left}, []interface{}{right}
}

type TupleType struct {
	Types  []SimpleType `json:"types"`
	Frozen bool         `json:"frozen"`
}

func (tt TupleType) Name() string {
	names := make([]string, len(tt.Types), len(tt.Types))
	for i, t := range tt.Types {
		names[i] = t.Name()
	}
	return "Type: " + strings.Join(names, ",")
}

func (tt TupleType) CQLDef() string {
	names := make([]string, len(tt.Types), len(tt.Types))
	for i, t := range tt.Types {
		names[i] = t.CQLDef()
	}
	if tt.Frozen {
		return "frozen<tuple<" + strings.Join(names, ",") + ">>"
	}
	return "tuple<" + strings.Join(names, ",") + ">"
}

func (tt TupleType) CQLHolder() string {
	return "(" + strings.TrimRight(strings.Repeat("?,", len(tt.Types)), ",") + ")"
}

func (st TupleType) Indexable() bool {
	for _, t := range st.Types {
		if t == TYPE_DURATION {
			return false
		}
	}
	return true
}

func (tt TupleType) GenValue(p *PartitionRange) []interface{} {
	vals := make([]interface{}, 0, len(tt.Types))
	for _, t := range tt.Types {
		vals = append(vals, t.GenValue(p)...)
	}
	return vals
}

func (tt TupleType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
	left := make([]interface{}, 0, len(tt.Types))
	right := make([]interface{}, 0, len(tt.Types))
	for _, t := range tt.Types {
		ttLeft, ttRight := t.GenValueRange(p)
		left = append(left, ttLeft...)
		right = append(right, ttRight...)
	}
	return left, right
}

type UDTType struct {
	Types    map[string]SimpleType `json:"types"`
	TypeName string                `json:"type_name"`
	Frozen   bool                  `json:"frozen"`
}

func (tt UDTType) Name() string {
	return tt.TypeName
}

func (tt UDTType) CQLDef() string {
	if tt.Frozen {
		return "frozen<" + tt.TypeName + ">"
	}
	return tt.TypeName
}

func (tt UDTType) CQLHolder() string {
	return "?"
}

func (tt UDTType) Indexable() bool {
	for _, t := range tt.Types {
		if t == TYPE_DURATION {
			return false
		}
	}
	return true
}

func (tt UDTType) GenValue(p *PartitionRange) []interface{} {
	vals := make(map[string]interface{})
	for name, typ := range tt.Types {
		vals[name] = typ.GenValue(p)[0]
	}
	return []interface{}{vals}
}

func (tt UDTType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
	left := make(map[string]interface{})
	right := make(map[string]interface{})
	for name, t := range tt.Types {
		ttLeft, ttRight := t.GenValueRange(p)
		left[name] = ttLeft[0]
		right[name] = ttRight[0]
	}
	return []interface{}{left}, []interface{}{right}
}

type SetType struct {
	Type   SimpleType `json:"type"`
	Frozen bool       `json:"frozen"`
}

func (ct SetType) Name() string {
	if ct.Frozen {
		return "frozen<set<" + ct.Type.Name() + ">>"
	}
	return "set<" + ct.Type.Name() + ">"
}

func (ct SetType) CQLDef() string {
	if ct.Frozen {
		return "frozen<set<" + ct.Type.Name() + ">>"
	}
	return "set<" + ct.Type.Name() + ">"
}

func (ct SetType) CQLHolder() string {
	return "?"
}

func (ct SetType) GenValue(p *PartitionRange) []interface{} {
	count := p.Rand.Intn(9) + 1
	vals := make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		vals[i] = ct.Type.GenValue(p)[0]
	}
	return []interface{}{vals}
}

func (ct SetType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
	count := p.Rand.Intn(9) + 1
	left := make([]interface{}, 0, len(ct.Type))
	right := make([]interface{}, 0, len(ct.Type))
	for i := 0; i < count; i++ {
		ttLeft, ttRight := ct.Type.GenValueRange(p)
		left = append(left, ttLeft...)
		right = append(right, ttRight...)
	}
	return []interface{}{left}, []interface{}{right}
}

func (ct SetType) Indexable() bool {
	return false
}

type ListType struct {
	SetType
}

func (lt ListType) Name() string {
	if lt.Frozen {
		return "frozen<list<" + lt.Type.Name() + ">>"
	}
	return "list<" + lt.Type.Name() + ">"
}

func (lt ListType) CQLDef() string {
	if lt.Frozen {
		return "frozen<list<" + lt.Type.Name() + ">>"
	}
	return "list<" + lt.Type.Name() + ">"
}

type MapType struct {
	KeyType   SimpleType `json:"key_type"`
	ValueType SimpleType `json:"value_type"`
	Frozen    bool       `json:"frozen"`
}

func (mt MapType) Name() string {
	if mt.Frozen {
		return "frozen<map<" + mt.KeyType.Name() + "," + mt.ValueType.Name() + ">>"
	}
	return "map<" + mt.KeyType.Name() + "," + mt.ValueType.Name() + ">"
}

func (mt MapType) CQLHolder() string {
	return "?"
}

func (mt MapType) GenValue(p *PartitionRange) []interface{} {
	count := p.Rand.Intn(9) + 1
	vals := make(map[interface{}]interface{})
	for i := 0; i < count; i++ {
		vals[mt.KeyType.GenValue(p)[0]] = mt.ValueType.GenValue(p)[0]
	}
	return []interface{}{vals}
}

func (mt MapType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
	count := p.Rand.Intn(9) + 1
	left := make(map[interface{}]interface{})
	right := make(map[interface{}]interface{})
	for i := 0; i < count; i++ {
		leftKey, rightKey := mt.KeyType.GenValueRange(p)
		leftVal, rightVal := mt.KeyType.GenValueRange(p)
		left[leftKey[0]] = leftVal[0]
		right[rightKey[0]] = rightVal[0]
	}
	return []interface{}{left}, []interface{}{right}
}

func (mt MapType) CQLDef() string {
	if mt.Frozen {
		return "frozen<map<" + mt.KeyType.CQLDef() + "," + mt.ValueType.CQLDef() + ">>"
	}
	return "map<" + mt.KeyType.CQLDef() + "," + mt.ValueType.CQLDef() + ">"
}

func (mt MapType) Indexable() bool {
	return false
}

func genColumnName(prefix string, idx int) string {
	return fmt.Sprintf("%s%d", prefix, idx)
}

func genColumnType(numColumns int) Type {
	n := rand.Intn(numColumns + 5)
	switch n {
	case numColumns:
		return genTupleType()
	case numColumns + 1:
		return genUDTType()
	case numColumns + 2:
		return genSetType()
	case numColumns + 3:
		return genListType()
	case numColumns + 4:
		return genMapType()
	default:
		return genSimpleType()
	}
}

func genSimpleType() SimpleType {
	return types[rand.Intn(len(types))]
}

func genTupleType() Type {
	n := rand.Intn(5)
	if n < 2 {
		n = 2
	}
	typeList := make([]SimpleType, n, n)
	for i := 0; i < n; i++ {
		typeList[i] = genSimpleType()
	}
	return TupleType{
		Types:  typeList,
		Frozen: rand.Uint32()%2 == 0,
	}
}

func genUDTType() UDTType {
	udtNum := rand.Uint32()
	typeName := fmt.Sprintf("udt_%d", udtNum)
	ts := make(map[string]SimpleType)

	for i := 0; i < rand.Intn(MaxUDTParts)+1; i++ {
		ts[typeName+fmt.Sprintf("_%d", i)] = genSimpleType()
	}

	return UDTType{
		Types:    ts,
		TypeName: typeName,
		Frozen:   true,
	}
}

func genSetType() SetType {
	var t SimpleType
	for {
		t = genSimpleType()
		if t != TYPE_DURATION {
			break
		}
	}
	return SetType{
		Type:   t,
		Frozen: rand.Uint32()%2 == 0,
	}
}

func genListType() ListType {
	return ListType{
		SetType: genSetType(),
	}
}

func genMapType() Type {
	var t SimpleType
	for {
		t = genSimpleType()
		if t != TYPE_DURATION {
			break
		}
	}
	return MapType{
		KeyType:   t,
		ValueType: genSimpleType(),
		Frozen:    rand.Uint32()%2 == 0,
	}
}

func genPrimaryKeyColumnType() Type {
	n := rand.Intn(len(pkTypes))
	return types[n]
}

func genIndexName(prefix string, idx int) string {
	return fmt.Sprintf("%s_idx", genColumnName(prefix, idx))
}
