// Copyright 2019 ScyllaDB
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

package stmtlogger

import "github.com/pkg/errors"

type Compression int

const (
	NoCompression Compression = iota
	ZSTDCompression
	GZIPCompression
)

func (c Compression) String() string {
	switch c {
	case NoCompression:
		return "none"
	case ZSTDCompression:
		return "zstd"
	case GZIPCompression:
		return "gzip"
	default:
		panic("unknown compression")
	}
}

func MustParseCompression(value string) Compression {
	c, err := ParseCompression(value)
	if err != nil {
		panic(err)
	}

	return c
}

func ParseCompression(value string) (Compression, error) {
	switch value {
	case "none", "":
		return NoCompression, nil
	case "zstd":
		return ZSTDCompression, nil
	case "gzip":
		return GZIPCompression, nil
	default:
		return NoCompression, errors.Errorf("unknown compression %q", value)
	}
}
