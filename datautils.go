package gemini

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

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

func randDate() string {
	time := randTime()
	return time.Format("2006-01-02")
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

func appendValue(columnType Type, p *PartitionRange, values []interface{}) []interface{} {
	return append(values, columnType.GenValue(p)...)
}

func appendValueRange(columnType Type, p *PartitionRange, values []interface{}) []interface{} {
	left, right := columnType.GenValueRange(p)
	values = append(values, left...)
	values = append(values, right...)
	return values
}
