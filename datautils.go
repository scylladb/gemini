package gemini

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"

	"github.com/segmentio/ksuid"
)

func randIntRange(min int, max int) int {
	return rand.Intn(max-min) + min
}

func nonEmptyRandIntRange(min int, max int, def int) int {
	if max > min && min > 0 {
		return randIntRange(min, max)
	}
	return randIntRange(1, def)
}

func randInt64Range(min int64, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func nonEmptyRandInt64Range(min int64, max int64, def int64) int64 {
	if max > min && min > 0 {
		return randInt64Range(min, max)
	}
	return randInt64Range(1, def)
}

func randFloat32Range(min float32, max float32) float32 {
	return rand.Float32() * (max - min)
}

func nonEmptyRandFloat32Range(min float32, max float32, def float32) float32 {
	if max > min && min > 0 {
		return randFloat32Range(min, max)
	}
	return randFloat32Range(1, def)
}

func randFloat64Range(min float64, max float64) float64 {
	return rand.Float64() * (max - min)
}

func nonEmptyRandFloat64Range(min float64, max float64, def float64) float64 {
	if max > min && min > 0 {
		return randFloat64Range(min, max)
	}
	return randFloat64Range(1, def)
}

func randString(len int) string {
	return nonEmptyRandStringWithTime(len, time.Now().UTC())
}

func randStringWithTime(len int, t time.Time) string {
	id, _ := ksuid.NewRandomWithTime(t)

	var buf strings.Builder
	buf.WriteString(id.String())
	if buf.Len() >= len {
		return buf.String()[:len]
	}

	// Pad some extra random data
	buff := make([]byte, len-buf.Len())
	rand.Read(buff)
	buf.WriteString(base64.StdEncoding.EncodeToString(buff))

	return buf.String()[:len]
}

func nonEmptyRandStringWithTime(len int, t time.Time) string {
	if len <= 0 {
		len = 1
	}
	return randStringWithTime(len, t)
}

func randTime() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC).Unix()

	sec := rand.Int63n(max-min) + min
	return time.Unix(sec, 0)
}

func randTimeNewer(d time.Time) time.Time {
	min := time.Date(d.Year()+1, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC).Unix()

	sec := rand.Int63n(max-min+1) + min
	return time.Unix(sec, 0)
}

func randIpV4Address(v, pos int) string {
	if pos < 0 || pos > 4 {
		panic(fmt.Sprintf("invalid position for the desired value of the IP part %d, 0-3 supported", pos))
	}
	if v < 0 || v > 255 {
		panic(fmt.Sprintf("invalid value for the desired position %d of the IP, 0-255 suppoerted", v))
	}
	var blocks []string
	for i := 0; i < 4; i++ {
		if i == pos {
			blocks = append(blocks, strconv.Itoa(v))
		} else {
			blocks = append(blocks, strconv.Itoa(rand.Intn(255)))
		}
	}
	return strings.Join(blocks, ".")
}

func genValue(columnType string, p *PartitionRange) interface{} {
	switch columnType {
	case "ascii", "blob", "text", "varchar":
		return randStringWithTime(nonEmptyRandIntRange(p.Max, p.Max, 10), randTime())
	case "bigint":
		return rand.Int63()
	case "boolean":
		return rand.Int()%2 == 0
	case "date", "time", "timestamp":
		return randTime()
	case "decimal":
		return inf.NewDec(randInt64Range(int64(p.Min), int64(p.Max)), 3)
	case "double":
		return randFloat64Range(float64(p.Min), float64(p.Max))
	case "duration":
		return time.Minute*time.Duration(randIntRange(p.Min, p.Max))
	case "float":
		return randFloat32Range(float32(p.Min), float32(p.Max))
	case "inet":
		return net.ParseIP(randIpV4Address(rand.Intn(255), 2))
	case "int":
		return nonEmptyRandIntRange(p.Min, p.Max, 10)
	case "smallint":
		return int16(nonEmptyRandIntRange(p.Min, p.Max, 10))
	case "timeuuid", "uuid":
		r := gocql.UUIDFromTime(randTime())
		return r.String()
	case "tinyint":
		return int8(nonEmptyRandIntRange(p.Min, p.Max, 10))
	case "varint":
		return big.NewInt(randInt64Range(int64(p.Min), int64(p.Max)))
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", columnType))
	}
}

func appendValue(columnType string, p *PartitionRange, values []interface{}) []interface{} {
	return append(values, genValue(columnType, p))
}

func appendValueRange(columnType string, p *PartitionRange, values []interface{}) []interface{} {
	switch columnType {
	case "ascii", "blob", "text", "varchar":
		startTime := randTime()
		start := nonEmptyRandIntRange(p.Min, p.Max, 10)
		end := start + nonEmptyRandIntRange(p.Min, p.Max, 10)
		values = append(values, nonEmptyRandStringWithTime(start, startTime))
		values = append(values, nonEmptyRandStringWithTime(end, randTimeNewer(startTime)))
	case "bigint":
		start := nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		end := start + nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		values = append(values, start)
		values = append(values, end)
	case "date", "time", "timestamp":
		start := randTime()
		end := randTimeNewer(start)
		values = append(values, start)
		values = append(values, end)
	case "decimal":
		start := nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		end := start + nonEmptyRandInt64Range(int64(p.Min), int64(p.Max), 10)
		values = append(values, inf.NewDec(start, 3))
		values = append(values, inf.NewDec(end, 3))
	case "double":
		start := nonEmptyRandFloat64Range(float64(p.Min), float64(p.Max), 10)
		end := start + nonEmptyRandFloat64Range(float64(p.Min), float64(p.Max), 10)
		values = append(values, start)
		values = append(values, end)
	case "duration":
		start := time.Minute * time.Duration(nonEmptyRandIntRange(p.Min, p.Max, 10))
		end := start + time.Minute*time.Duration(nonEmptyRandIntRange(p.Min, p.Max, 10))
		values = append(values, start)
		values = append(values, end)
	case "float":
		start := nonEmptyRandFloat32Range(float32(p.Min), float32(p.Max), 10)
		end := start + nonEmptyRandFloat32Range(float32(p.Min), float32(p.Max), 10)
		values = append(values, start)
		values = append(values, end)
	case "inet":
		start := randIpV4Address(0, 3)
		end := randIpV4Address(255, 3)
		values = append(values, net.ParseIP(start))
		values = append(values, net.ParseIP(end))
	case "int":
		start := nonEmptyRandIntRange(p.Min, p.Max, 10)
		end := start + nonEmptyRandIntRange(p.Min, p.Max, 10)
		values = append(values, start)
		values = append(values, end)
	case "smallint":
		start := int16(nonEmptyRandIntRange(p.Min, p.Max, 10))
		end := start + int16(nonEmptyRandIntRange(p.Min, p.Max, 10))
		values = append(values, start)
		values = append(values, end)
	case "timeuuid", "uuid":
		start := randTime()
		end := randTimeNewer(start)
		values = append(values, gocql.UUIDFromTime(start).String())
		values = append(values, gocql.UUIDFromTime(end).String())
	case "tinyint":
		start := int8(nonEmptyRandIntRange(p.Min, p.Max, 10))
		end := start + int8(nonEmptyRandIntRange(p.Min, p.Max, 10))
		values = append(values, start)
		values = append(values, end)
	case "varint":
		end := &big.Int{}
		start := big.NewInt(randInt64Range(int64(p.Min), int64(p.Max)))
		end.Set(start)
		end = end.Add(start, big.NewInt(randInt64Range(int64(p.Min), int64(p.Max))))
		values = append(values, start)
		values = append(values, end)
	default:
		panic(fmt.Sprintf("generate value range: not supported type %s", columnType))
	}
	return values
}
