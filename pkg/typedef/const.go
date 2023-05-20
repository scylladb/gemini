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

const (
	GoCQLProtoDirectionMask = 0x80
	GoCQLProtoVersionMask   = 0x7F
	GoCQLProtoVersion1      = 0x01
	GoCQLProtoVersion2      = 0x02
	GoCQLProtoVersion3      = 0x03
	GoCQLProtoVersion4      = 0x04
	GoCQLProtoVersion5      = 0x05
)

const (
	SelectStatementType StatementType = iota
	SelectRangeStatementType
	SelectByIndexStatementType
	SelectFromMaterializedViewStatementType
	DeleteStatementType
	InsertStatement
	Updatetatement
	AlterColumnStatementType
	DropColumnStatementType
)

const (
	CQL_FEATURE_BASIC CQLFeature = iota + 1
	CQL_FEATURE_NORMAL
	CQL_FEATURE_ALL
)

const (
	KnownIssuesJSONWithTuples = "https://github.com/scylladb/scylla/issues/3708"
)
