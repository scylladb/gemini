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

package partitions

import (
	"encoding/binary"
	"strings"

	"github.com/gocql/gocql"

	"github.com/scylladb/gemini/pkg/typedef"
)

// KeyFingerprint returns the partition key of one partition as the CQL wire
// bytes of its partition key columns, each with a length prefix. get returns
// the value of a column. A generated value and the value that a SELECT
// returns for it give the same bytes, so a row read by an index query and the
// borrowed keys of its partition give the same fingerprint. ok is false when
// a column has no value or does not marshal.
func KeyFingerprint(pks typedef.Columns, get func(name string) any) (fp string, ok bool) {
	var sb strings.Builder
	var size [binary.MaxVarintLen64]byte

	for _, col := range pks {
		value := get(col.Name)
		if value == nil {
			return "", false
		}

		data, err := gocql.Marshal(col.Type.CQLType(), value)
		if err != nil {
			return "", false
		}

		n := binary.PutUvarint(size[:], uint64(len(data)))
		sb.Write(size[:n])
		sb.Write(data)
	}

	return sb.String(), true
}

// keysFingerprint returns the fingerprint of borrowed keys.
func (p *Partitions) keysFingerprint(keys *typedef.PartitionKeys) (string, bool) {
	if keys.Values == nil {
		return "", false
	}

	return KeyFingerprint(p.table.PartitionKeys, func(name string) any {
		values := keys.Values.Get(name)
		if len(values) != 1 {
			return nil
		}
		return values[0]
	})
}

// ClaimDivergedKey records that the partition with fingerprint fp differs
// between the clusters. It reports true when no earlier claim and no
// MarkInvalid named the partition. An index query reads rows of any
// partition. It reports a differing row only on true, so a partition that
// gemini made diverge, or that a report already named, is not reported again.
func (p *Partitions) ClaimDivergedKey(fp string) bool {
	_, seen := p.diverged.LoadOrStore(fp, struct{}{})
	return !seen
}

// recordDiverged records the partition of keys as diverged. It reports
// whether an earlier MarkInvalid or ClaimDivergedKey already named it.
func (p *Partitions) recordDiverged(keys *typedef.PartitionKeys) bool {
	fp, ok := p.keysFingerprint(keys)
	if !ok {
		return false
	}

	_, seen := p.diverged.LoadOrStore(fp, struct{}{})
	return seen
}
