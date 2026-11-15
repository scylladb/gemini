// Copyright 2026 ScyllaDB
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

package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

// TestClassifyErrorBoundsLabels: an error message is not a metric label. A
// socket error holds the ephemeral port, so one label per message would grow
// a new series for every connection of a long run.
func TestClassifyErrorBoundsLabels(t *testing.T) {
	t.Parallel()

	socketErr := func(port int) error {
		return &net.OpError{
			Op:     "write",
			Net:    "tcp",
			Source: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: port},
			Addr:   &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 9042},
			Err:    os.NewSyscallError("write", syscall.EPIPE),
		}
	}

	assert.Equal(t, classifyError(socketErr(53214)), classifyError(socketErr(41871)),
		"two socket errors that differ only in the port must share one label")

	for _, tc := range []struct {
		err  error
		want string
	}{
		{err: socketErr(53214), want: "net_error"},
		{err: fmt.Errorf("dial: %w", syscall.ECONNREFUSED), want: "connection_refused"},
		{err: fmt.Errorf("read: %w", syscall.ECONNRESET), want: "connection_reset"},
		{err: fmt.Errorf("read: %w", io.EOF), want: "eof"},
		{err: context.DeadlineExceeded, want: "deadline_exceeded"},
		{err: context.Canceled, want: "canceled"},
		{err: gocql.ErrTimeoutNoResponse, want: "timeout_no_response"},
		{err: errors.New("something else"), want: "other"},
	} {
		assert.Equal(t, tc.want, classifyError(tc.err), "%v", tc.err)
	}
}
