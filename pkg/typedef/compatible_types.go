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

package typedef

var CompatibleColumnTypes = map[SimpleType]SimpleTypes{
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
