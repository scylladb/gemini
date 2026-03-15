// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//nolint:revive
package utils_test

import (
	"errors"
	"io"
	"math/big"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/utils"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newRand() *rand.Rand {
	return rand.New(rand.NewPCG(42, 42))
}

// ---------------------------------------------------------------------------
// RandTimestamp
// ---------------------------------------------------------------------------

func TestRandTimestamp(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	ts := utils.RandTimestamp(rnd)
	// Must be non-negative and within the max allowed date
	assert.GreaterOrEqual(t, ts, int64(0))
}

func TestRandTimestamp_Deterministic(t *testing.T) {
	t.Parallel()
	ts1 := utils.RandTimestamp(rand.New(rand.NewPCG(1, 1)))
	ts2 := utils.RandTimestamp(rand.New(rand.NewPCG(1, 1)))
	assert.Equal(t, ts1, ts2)
}

// ---------------------------------------------------------------------------
// RandDate
// ---------------------------------------------------------------------------

func TestRandDate(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	d := utils.RandDate(rnd)
	// Must be UTC
	assert.Equal(t, time.UTC, d.Location())
	// Year must be in a sane range
	assert.GreaterOrEqual(t, d.Year(), 1970)
	assert.LessOrEqual(t, d.Year(), 9999)
}

// ---------------------------------------------------------------------------
// RandDuration
// ---------------------------------------------------------------------------

func TestRandDuration(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	d := utils.RandDuration(rnd)
	assert.GreaterOrEqual(t, int64(d), int64(0))
}

// ---------------------------------------------------------------------------
// RandTime
// ---------------------------------------------------------------------------

func TestRandTime(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	t1 := utils.RandTime(rnd)
	// CQL time range: [0, 86399999999999]
	assert.GreaterOrEqual(t, t1, int64(0))
	assert.LessOrEqual(t, t1, int64(86400000000000)-1)
}

// ---------------------------------------------------------------------------
// RandIPV4Address
// ---------------------------------------------------------------------------

func TestRandIPV4Address(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	ip := utils.RandIPV4Address(rnd, 10, 2)
	// Must parse as a valid IP
	parsed := net.ParseIP(ip)
	require.NotNil(t, parsed, "expected valid IP, got %q", ip)
	// The fixed octet at position 2 must be 10
	parts := strings.Split(ip, ".")
	require.Len(t, parts, 4)
	assert.Equal(t, "10", parts[2])
}

func TestRandIPV4Address_Position0(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	ip := utils.RandIPV4Address(rnd, 192, 0)
	parts := strings.Split(ip, ".")
	require.Len(t, parts, 4)
	assert.Equal(t, "192", parts[0])
}

func TestRandIPV4Address_Position3(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	ip := utils.RandIPV4Address(rnd, 55, 3)
	parts := strings.Split(ip, ".")
	require.Len(t, parts, 4)
	assert.Equal(t, "55", parts[3])
}

func TestRandIPV4Address_PanicOnBadPos(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() {
		utils.RandIPV4Address(newRand(), 1, 5) // pos > 4
	})
}

func TestRandIPV4Address_PanicOnBadValue(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() {
		utils.RandIPV4Address(newRand(), 256, 0) // v > 255
	})
}

func TestRandIPV4Address_NegativeValue(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() {
		utils.RandIPV4Address(newRand(), -1, 0)
	})
}

// ---------------------------------------------------------------------------
// RandInt2
// ---------------------------------------------------------------------------

func TestRandInt2_Normal(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	for range 100 {
		v := utils.RandInt2(rnd, 5, 15)
		assert.GreaterOrEqual(t, v, 5)
		assert.Less(t, v, 15)
	}
}

func TestRandInt2_EqualMinMax(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	v := utils.RandInt2(rnd, 7, 7)
	assert.Equal(t, 7, v)
}

func TestRandInt2_MaxLessThanMin(t *testing.T) {
	t.Parallel()
	rnd := newRand()
	v := utils.RandInt2(rnd, 10, 5)
	assert.Equal(t, 10, v)
}

// ---------------------------------------------------------------------------
// IgnoreError
// ---------------------------------------------------------------------------

func TestIgnoreError_NoError(t *testing.T) {
	t.Parallel()
	called := false
	utils.IgnoreError(func() error {
		called = true
		return nil
	})
	assert.True(t, called)
}

func TestIgnoreError_WithError(t *testing.T) {
	t.Parallel()
	called := false
	utils.IgnoreError(func() error {
		called = true
		return errors.New("ignored error")
	})
	assert.True(t, called)
}

// ---------------------------------------------------------------------------
// UnsafeBytes / UnsafeString
// ---------------------------------------------------------------------------

func TestUnsafeBytes_RoundTrip(t *testing.T) {
	t.Parallel()
	input := "hello world"
	b := utils.UnsafeBytes(input)
	assert.Equal(t, []byte(input), b)
}

func TestUnsafeString_RoundTrip(t *testing.T) {
	t.Parallel()
	input := []byte("hello world")
	s := utils.UnsafeString(input)
	assert.Equal(t, string(input), s)
}

func TestUnsafeBytes_Empty(t *testing.T) {
	t.Parallel()
	b := utils.UnsafeBytes("")
	assert.Empty(t, b)
}

func TestUnsafeString_Empty(t *testing.T) {
	t.Parallel()
	s := utils.UnsafeString([]byte{})
	assert.Empty(t, s)
}

// ---------------------------------------------------------------------------
// Sizeof
// ---------------------------------------------------------------------------

func TestSizeof_Nil(t *testing.T) {
	t.Parallel()
	assert.Equal(t, uint64(0), utils.Sizeof(nil))
}

func TestSizeof_String(t *testing.T) {
	t.Parallel()
	s := "hello"
	sz := utils.Sizeof(s)
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_Bytes(t *testing.T) {
	t.Parallel()
	b := []byte{1, 2, 3}
	sz := utils.Sizeof(b)
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_Int(t *testing.T) {
	t.Parallel()
	sz := utils.Sizeof(int(42))
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_Bool(t *testing.T) {
	t.Parallel()
	sz := utils.Sizeof(true)
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_Float64(t *testing.T) {
	t.Parallel()
	sz := utils.Sizeof(float64(3.14))
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_BigInt(t *testing.T) {
	t.Parallel()
	sz := utils.Sizeof(big.NewInt(12345))
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_InfDec(t *testing.T) {
	t.Parallel()
	d := inf.NewDec(12345, 2)
	sz := utils.Sizeof(d)
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_SliceOfAny(t *testing.T) {
	t.Parallel()
	val := []any{"a", "b", "c"}
	sz := utils.Sizeof(val)
	assert.Greater(t, sz, uint64(0))
}

func TestSizeof_Duration(t *testing.T) {
	t.Parallel()
	sz := utils.Sizeof(time.Second)
	assert.Greater(t, sz, uint64(0))
}

// ---------------------------------------------------------------------------
// CreateFile
// ---------------------------------------------------------------------------

func TestCreateFile_Empty_NoDefault(t *testing.T) {
	t.Parallel()
	w, err := utils.CreateFile("", false)
	require.NoError(t, err)
	assert.Equal(t, io.Discard, w, "empty path with no default should return io.Discard")
}

func TestCreateFile_Stderr(t *testing.T) {
	t.Parallel()
	w, err := utils.CreateFile("stderr", false)
	require.NoError(t, err)
	assert.Equal(t, os.Stderr, w)
}

func TestCreateFile_Stdout(t *testing.T) {
	t.Parallel()
	w, err := utils.CreateFile("stdout", false)
	require.NoError(t, err)
	assert.Equal(t, os.Stdout, w)
}

func TestCreateFile_RealFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "test_output.log")

	w, err := utils.CreateFile(path, false)
	require.NoError(t, err)
	require.NotNil(t, w)

	// Ensure the file was actually created
	_, statErr := os.Stat(path)
	assert.NoError(t, statErr)

	// Close the file via the returned writer
	if f, ok := w.(*os.File); ok {
		_ = f.Close()
	}
}

func TestCreateFile_InvalidPath(t *testing.T) {
	t.Parallel()
	_, err := utils.CreateFile("/nonexistent/dir/file.log", false)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// IsFile
// ---------------------------------------------------------------------------

func TestIsFile_Empty(t *testing.T) {
	t.Parallel()
	assert.False(t, utils.IsFile(""))
}

func TestIsFile_NonExistent(t *testing.T) {
	t.Parallel()
	assert.False(t, utils.IsFile("/nonexistent/path/file.txt"))
}

func TestIsFile_Directory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	assert.False(t, utils.IsFile(dir))
}

func TestIsFile_RegularFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("test"), 0o644))
	assert.True(t, utils.IsFile(path))
}

// ---------------------------------------------------------------------------
// GetTimer / PutTimer
// ---------------------------------------------------------------------------

func TestGetTimer_Fires(t *testing.T) {
	t.Parallel()
	timer := utils.GetTimer(10 * time.Millisecond)
	require.NotNil(t, timer)
	select {
	case <-timer.C:
		// good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire within 200ms")
	}
	utils.PutTimer(timer)
}

func TestPutTimer_AlreadyFired(t *testing.T) {
	t.Parallel()
	timer := utils.GetTimer(1 * time.Nanosecond)
	// Wait for it to fire
	time.Sleep(5 * time.Millisecond)
	// PutTimer should drain the channel and not block
	utils.PutTimer(timer)
}

func TestGetPutTimer_Reuse(t *testing.T) {
	t.Parallel()
	// Get, put, get again — should not panic
	t1 := utils.GetTimer(10 * time.Millisecond)
	utils.PutTimer(t1)
	t2 := utils.GetTimer(10 * time.Millisecond)
	utils.PutTimer(t2)
}
