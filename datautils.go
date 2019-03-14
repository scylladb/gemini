package gemini

import (
	"encoding/base64"
	"math/rand"
	"strings"
	"time"

	"github.com/segmentio/ksuid"
)

func randRange(min int, max int) int {
	return rand.Intn(max-min) + min
}

func nonEmptyRandRange(min int, max int, def int) int {
	if max > min && min > 0 {
		return randRange(min, max)
	}
	return randRange(1, def)
}

func randRange64(min int64, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func nonEmptyRandRange64(min int64, max int64, def int64) int64 {
	if max > min && min > 0 {
		return randRange64(min, max)
	}
	return randRange64(1, def)
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

func randDate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC).Unix()

	sec := rand.Int63n(max-min) + min
	return time.Unix(sec, 0)
}

func randDateNewer(d time.Time) time.Time {
	min := time.Date(d.Year()+1, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC).Unix()

	sec := rand.Int63n(max-min+1) + min
	return time.Unix(sec, 0)
}
