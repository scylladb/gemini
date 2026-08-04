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

package scylla

import "errors"

// readError marks a failure to read a partition's history out of _logs. The
// lines that were written are complete and on disk, so the caller keeps them and
// keeps counting them. A bare error means the write failed instead, which makes
// the file itself untrustworthy.
type readError struct {
	err error
}

func newReadError(errs ...error) error {
	err := errors.Join(errs...)
	if err == nil {
		return nil
	}

	return &readError{err: err}
}

func (e *readError) Error() string { return "statement log read: " + e.err.Error() }

func (e *readError) Unwrap() error { return e.err }

// isReadError reports whether err came only from reading _logs, never from
// writing the statements file.
func isReadError(err error) bool {
	var re *readError

	return errors.As(err, &re)
}
