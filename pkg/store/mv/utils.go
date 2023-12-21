// Copyright 2023 ScyllaDB
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

package mv

import (
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
)

func dereference(in interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(in)).Interface()
}

func readLen(p []byte) int32 {
	return *(*int32)(unsafe.Pointer(&p[0]))
}

func readLenOld(p []byte) int16 {
	return *(*int16)(unsafe.Pointer(&p[0]))
}

const protoVersion2 = 0x02

var ErrorUnmarshalEOF = errors.New("unexpected eof")
