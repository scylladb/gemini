package gemini

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
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
)

// TODO: Add support for time when gocql bug is fixed.
var (
	pkTypes               = []SimpleType{TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT, TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT /*TYPE_TIME,*/, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT}
	types                 = append(append([]SimpleType{}, pkTypes...), TYPE_BOOLEAN, TYPE_DURATION)
	compatibleColumnTypes = map[SimpleType][]SimpleType{
		TYPE_ASCII: {
			TYPE_TEXT,
			TYPE_BLOB,
		},
		TYPE_BIGINT: {
			TYPE_BLOB,
		},
		TYPE_BOOLEAN: {
			TYPE_BLOB,
		},
		TYPE_DECIMAL: {
			TYPE_BLOB,
		},
		TYPE_FLOAT: {
			TYPE_BLOB,
		},
		TYPE_INET: {
			TYPE_BLOB,
		},
		TYPE_INT: {
			TYPE_VARINT,
			TYPE_BLOB,
		},
		TYPE_TIMESTAMP: {
			TYPE_BLOB,
		},
		TYPE_TIMEUUID: {
			TYPE_UUID,
			TYPE_BLOB,
		},
		TYPE_UUID: {
			TYPE_BLOB,
		},
		TYPE_VARCHAR: {
			TYPE_TEXT,
			TYPE_BLOB,
		},
		TYPE_VARINT: {
			TYPE_BLOB,
		},
	}
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

func (st SimpleType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	var replacement string
	switch st {
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR, TYPE_INET, TYPE_DATE:
		replacement = fmt.Sprintf("'%s'", value[0])
	case TYPE_BLOB:
		if v, ok := value[0].(string); ok {
			if len(v) > 100 {
				v = v[:100]
			}
			replacement = "textasblob('" + v + "')"
		}
	case TYPE_BIGINT, TYPE_INT, TYPE_SMALLINT, TYPE_TINYINT:
		replacement = fmt.Sprintf("%d", value[0])
	case TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT:
		replacement = fmt.Sprintf("%.2f", value[0])
	case TYPE_BOOLEAN:
		if v, ok := value[0].(bool); ok {
			replacement = fmt.Sprintf("%t", v)
		}
	case TYPE_TIME, TYPE_TIMESTAMP:
		if v, ok := value[0].(time.Time); ok {
			replacement = "'" + v.Format(time.RFC3339) + "'"
		}
	case TYPE_DURATION, TYPE_TIMEUUID, TYPE_UUID:
		replacement = fmt.Sprintf("%s", value[0])
	case TYPE_VARINT:
		if s, ok := value[0].(*big.Int); ok {
			replacement = fmt.Sprintf("%d", s.Int64())
		}
	default:
		panic(fmt.Sprintf("cql pretty: not supported type %s", st))
	}
	return strings.Replace(query, "?", replacement, 1), 1
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
		ln := p.Rand.Intn(p.MaxStringLength) + p.MinStringLength
		val = randStringWithTime(p.Rand, ln, randTime(p.Rand))
	case TYPE_BLOB:
		ln := p.Rand.Intn(p.MaxBlobLength) + p.MinBlobLength
		val = hex.EncodeToString([]byte(randStringWithTime(p.Rand, ln, randTime(p.Rand))))
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
		val = randIntRange(p.Rand, p.Min, p.Max)
	case TYPE_SMALLINT:
		val = int16(randIntRange(p.Rand, p.Min, p.Max))
	case TYPE_TIMEUUID, TYPE_UUID:
		r := gocql.UUIDFromTime(randTime(p.Rand))
		val = r.String()
	case TYPE_TINYINT:
		val = int8(randIntRange(p.Rand, p.Min, p.Max))
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
		start := randIntRange(p.Rand, p.Min, p.Max)
		end := start + randIntRange(p.Rand, p.Min, p.Max)
		left = nonEmptyRandStringWithTime(p.Rand, start, startTime)
		right = nonEmptyRandStringWithTime(p.Rand, end, randTimeNewer(p.Rand, startTime))
	case TYPE_BLOB:
		startTime := randTime(p.Rand)
		start := randIntRange(p.Rand, p.Min, p.Max)
		end := start + randIntRange(p.Rand, p.Min, p.Max)
		left = hex.EncodeToString(nonEmptyRandBlobWithTime(p.Rand, start, startTime))
		right = hex.EncodeToString(nonEmptyRandBlobWithTime(p.Rand, end, randTimeNewer(p.Rand, startTime)))
	case TYPE_BIGINT:
		start := randInt64Range(p.Rand, int64(p.Min), int64(p.Max))
		end := start + randInt64Range(p.Rand, int64(p.Min), int64(p.Max))
		left = start
		right = end
	case TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP:
		start := randTime(p.Rand)
		end := randTimeNewer(p.Rand, start)
		left = start
		right = end
	case TYPE_DECIMAL:
		start := randInt64Range(p.Rand, int64(p.Min), int64(p.Max))
		end := start + randInt64Range(p.Rand, int64(p.Min), int64(p.Max))
		left = inf.NewDec(start, 3)
		right = inf.NewDec(end, 3)
	case TYPE_DOUBLE:
		start := randFloat64Range(p.Rand, float64(p.Min), float64(p.Max))
		end := start + randFloat64Range(p.Rand, float64(p.Min), float64(p.Max))
		left = start
		right = end
	case TYPE_DURATION:
		start := time.Minute * time.Duration(randIntRange(p.Rand, p.Min, p.Max))
		end := start + time.Minute*time.Duration(randIntRange(p.Rand, p.Min, p.Max))
		left = start
		right = end
	case TYPE_FLOAT:
		start := randFloat32Range(p.Rand, float32(p.Min), float32(p.Max))
		end := start + randFloat32Range(p.Rand, float32(p.Min), float32(p.Max))
		left = start
		right = end
	case TYPE_INET:
		start := randIpV4Address(p.Rand, 0, 3)
		end := randIpV4Address(p.Rand, 255, 3)
		left = net.ParseIP(start)
		right = net.ParseIP(end)
	case TYPE_INT:
		start := randIntRange(p.Rand, p.Min, p.Max)
		end := start + randIntRange(p.Rand, p.Min, p.Max)
		left = start
		right = end
	case TYPE_SMALLINT:
		start := int16(randIntRange(p.Rand, p.Min, p.Max))
		end := start + int16(randIntRange(p.Rand, p.Min, p.Max))
		left = start
		right = end
	case TYPE_TIMEUUID, TYPE_UUID:
		start := randTime(p.Rand)
		end := randTimeNewer(p.Rand, start)
		left = gocql.UUIDFromTime(start).String()
		right = gocql.UUIDFromTime(end).String()
	case TYPE_TINYINT:
		start := int8(randIntRange(p.Rand, p.Min, p.Max))
		end := start + int8(randIntRange(p.Rand, p.Min, p.Max))
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

func (tt TupleType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	var cnt, tmp int
	for i, t := range tt.Types {
		query, tmp = t.CQLPretty(query, value[i:])
		cnt += tmp
	}
	return query, cnt
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

func (tt UDTType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	if s, ok := value[0].(map[string]interface{}); ok {
		vv := "{"
		for k, v := range tt.Types {
			vv += fmt.Sprintf("%s:?,", k)
			vv, _ = v.CQLPretty(vv, []interface{}{s[k]})
		}
		vv = strings.TrimSuffix(vv, ",")
		vv += "}"
		return strings.Replace(query, "?", vv, 1), 1
	}
	panic(fmt.Sprintf("udt pretty, unknown type %v", tt))
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

type BagType struct {
	Kind   string     `json:"kind"` // We need to differentiate between sets and lists
	Type   SimpleType `json:"type"`
	Frozen bool       `json:"frozen"`
}

func (ct BagType) Name() string {
	if ct.Frozen {
		return "frozen<" + ct.Kind + "<" + ct.Type.Name() + ">>"
	}
	return ct.Kind + "<" + ct.Type.Name() + ">"
}

func (ct BagType) CQLDef() string {
	if ct.Frozen {
		return "frozen<" + ct.Kind + "<" + ct.Type.Name() + ">>"
	}
	return ct.Kind + "<" + ct.Type.Name() + ">"
}

func (ct BagType) CQLHolder() string {
	return "?"
}

func (ct BagType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	switch reflect.TypeOf(value[0]).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(value[0])
		vv := "{"
		vv += strings.Repeat("?,", s.Len())
		vv = strings.TrimRight(vv, ",")
		vv += "}"
		for i := 0; i < s.Len(); i++ {
			vv, _ = ct.Type.CQLPretty(vv, []interface{}{s.Index(i).Interface()})
		}
		return strings.Replace(query, "?", vv, 1), 1
	}
	panic(fmt.Sprintf("set cql pretty, unknown type %v", ct))
}

func (ct BagType) GenValue(p *PartitionRange) []interface{} {
	count := p.Rand.Intn(9) + 1
	vals := make([]interface{}, count, count)
	for i := 0; i < count; i++ {
		vals[i] = ct.Type.GenValue(p)[0]
	}
	return []interface{}{vals}
}

func (ct BagType) GenValueRange(p *PartitionRange) ([]interface{}, []interface{}) {
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

func (ct BagType) Indexable() bool {
	return false
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

func (mt MapType) CQLPretty(query string, value []interface{}) (string, int) {
	switch reflect.TypeOf(value[0]).Kind() {
	case reflect.Map:
		s := reflect.ValueOf(value[0]).MapRange()
		vv := "{"
		for s.Next() {
			vv += fmt.Sprintf("%v:?,", s.Key().Interface())
			vv, _ = mt.ValueType.CQLPretty(vv, []interface{}{s.Value().Interface()})
		}
		vv = strings.TrimSuffix(vv, ",")
		vv += "}"
		return strings.Replace(query, "?", vv, 1), 1
	}
	panic(fmt.Sprintf("map cql pretty, unknown type %v", mt))
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

func genColumnType(numColumns int, sc *SchemaConfig) Type {
	n := rand.Intn(numColumns + 5)
	switch n {
	case numColumns:
		return genTupleType(sc)
	case numColumns + 1:
		return genUDTType(sc)
	case numColumns + 2:
		return genSetType(sc)
	case numColumns + 3:
		return genListType(sc)
	case numColumns + 4:
		return genMapType(sc)
	default:
		return genSimpleType(sc)
	}
}

func genSimpleType(sc *SchemaConfig) SimpleType {
	return types[rand.Intn(len(types))]
}

func genTupleType(sc *SchemaConfig) Type {
	n := rand.Intn(sc.MaxTupleParts)
	if n < 2 {
		n = 2
	}
	typeList := make([]SimpleType, n, n)
	for i := 0; i < n; i++ {
		typeList[i] = genSimpleType(sc)
	}
	return TupleType{
		Types:  typeList,
		Frozen: rand.Uint32()%2 == 0,
	}
}

func genUDTType(sc *SchemaConfig) UDTType {
	udtNum := rand.Uint32()
	typeName := fmt.Sprintf("udt_%d", udtNum)
	ts := make(map[string]SimpleType)

	for i := 0; i < rand.Intn(sc.MaxUDTParts)+1; i++ {
		ts[typeName+fmt.Sprintf("_%d", i)] = genSimpleType(sc)
	}

	return UDTType{
		Types:    ts,
		TypeName: typeName,
		Frozen:   true,
	}
}

func genSetType(sc *SchemaConfig) BagType {
	return genBagType("set", sc)
}

func genListType(sc *SchemaConfig) BagType {
	return genBagType("list", sc)
}

func genBagType(kind string, sc *SchemaConfig) BagType {
	var t SimpleType
	for {
		t = genSimpleType(sc)
		if t != TYPE_DURATION {
			break
		}
	}
	return BagType{
		Kind:   kind,
		Type:   t,
		Frozen: rand.Uint32()%2 == 0,
	}
}

func genMapType(sc *SchemaConfig) MapType {
	var t SimpleType
	for {
		t = genSimpleType(sc)
		if t != TYPE_DURATION {
			break
		}
	}
	return MapType{
		KeyType:   t,
		ValueType: genSimpleType(sc),
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

// JSON Marshalling

func (cd *ColumnDef) UnmarshalJSON(data []byte) error {
	dataMap := make(map[string]interface{})
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return err
	}

	t, err := getSimpleTypeColumn(dataMap)
	if err != nil {
		t, err = getUDTTypeColumn(dataMap)
		if err != nil {
			t, err = getTupleTypeColumn(dataMap)
			if err != nil {
				t, err = getMapTypeColumn(dataMap)
				if err != nil {
					t, err = getBagTypeColumn(dataMap)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	*cd = ColumnDef{
		Name: t.Name,
		Type: t.Type,
	}
	return nil
}

func getMapTypeColumn(data map[string]interface{}) (ColumnDef, error) {
	st := struct {
		Name string
		Type map[string]interface{}
	}{}
	err := mapstructure.Decode(data, &st)

	if _, ok := st.Type["frozen"]; !ok {
		return ColumnDef{}, errors.Errorf("not a map type, value=%v", st)
	}

	if _, ok := st.Type["value_type"]; !ok {
		return ColumnDef{}, errors.Errorf("not a map type, value=%v", st)
	}

	if _, ok := st.Type["key_type"]; !ok {
		return ColumnDef{}, errors.Errorf("not a map type, value=%v", st)
	}

	var frozen bool
	if err := mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode bool value for MapType::Frozen, value=%v", st)
	}
	var valueType SimpleType
	if err := mapstructure.Decode(st.Type["value_type"], &valueType); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode SimpleType value for MapType::ValueType, value=%v", st)
	}
	var keyType SimpleType
	if err := mapstructure.Decode(st.Type["key_type"], &keyType); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode bool value for MapType::KeyType, value=%v", st)
	}
	return ColumnDef{
		Name: st.Name,
		Type: MapType{
			Frozen:    frozen,
			ValueType: valueType,
			KeyType:   keyType,
		},
	}, err
}

func getBagTypeColumn(data map[string]interface{}) (ColumnDef, error) {
	st := struct {
		Name string
		Type map[string]interface{}
	}{}
	err := mapstructure.Decode(data, &st)

	var kind string
	if err := mapstructure.Decode(st.Type["kind"], &kind); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode string value for BagType::Frozen, value=%v", st)
	}
	var frozen bool
	if err := mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode bool value for BagType::Frozen, value=%v", st)
	}
	var typ SimpleType
	if err := mapstructure.Decode(st.Type["type"], &typ); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode SimpleType value for BagType::ValueType, value=%v", st)
	}
	return ColumnDef{
		Name: st.Name,
		Type: BagType{
			Kind:   kind,
			Frozen: frozen,
			Type:   typ,
		},
	}, err
}

func getTupleTypeColumn(data map[string]interface{}) (ColumnDef, error) {
	st := struct {
		Name string
		Type map[string]interface{}
	}{}
	err := mapstructure.Decode(data, &st)

	if _, ok := st.Type["types"]; !ok {
		return ColumnDef{}, errors.Errorf("not a tuple type, value=%v", st)
	}

	var types []SimpleType
	if err := mapstructure.Decode(st.Type["types"], &types); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode []SimpleType value for TupleType::Types, value=%v", st)
	}
	var frozen bool
	if err := mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode bool value for TupleType::Types, value=%v", st)
	}
	return ColumnDef{
		Name: st.Name,
		Type: TupleType{
			Types:  types,
			Frozen: frozen,
		},
	}, err
}

func getUDTTypeColumn(data map[string]interface{}) (ColumnDef, error) {
	st := struct {
		Name string
		Type map[string]interface{}
	}{}
	err := mapstructure.Decode(data, &st)

	if _, ok := st.Type["types"]; !ok {
		return ColumnDef{}, errors.Errorf("not a UDT type, value=%v", st)
	}
	if _, ok := st.Type["type_name"]; !ok {
		return ColumnDef{}, errors.Errorf("not a UDT type, value=%v", st)
	}

	var types map[string]SimpleType
	if err := mapstructure.Decode(st.Type["types"], &types); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode []SimpleType value for UDTType::Types, value=%v", st)
	}
	var frozen bool
	if err := mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode bool value for UDTType::Frozen, value=%v", st)
	}
	var typeName string
	if err := mapstructure.Decode(st.Type["type_name"], &typeName); err != nil {
		return ColumnDef{}, errors.Wrapf(err, "can't decode string value for UDTType::TypeName, value=%v", st)
	}
	return ColumnDef{
		Name: st.Name,
		Type: UDTType{
			Types:    types,
			TypeName: typeName,
			Frozen:   frozen,
		},
	}, err
}

func getSimpleTypeColumn(data map[string]interface{}) (ColumnDef, error) {
	st := struct {
		Name string
		Type SimpleType
	}{}
	err := mapstructure.Decode(data, &st)
	if err != nil {
		return ColumnDef{}, err
	}
	return ColumnDef{
		Name: st.Name,
		Type: st.Type,
	}, err
}
