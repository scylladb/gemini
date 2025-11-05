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

package statements

import (
	"fmt"
	"strings"

	"github.com/scylladb/gemini/pkg/typedef"
)

func GetCreateTable(t *typedef.Table, ks typedef.Keyspace) string {
	t.RLock()
	defer t.RUnlock()

	partitionKeys := make([]string, 0, len(t.PartitionKeys))
	clusteringKeys := make([]string, 0, len(t.ClusteringKeys))
	columns := make([]string, 0, len(t.PartitionKeys)+len(t.ClusteringKeys)+len(t.Columns))

	for _, pk := range t.PartitionKeys {
		partitionKeys = append(partitionKeys, pk.Name)
		columns = append(columns, fmt.Sprintf("%s %s", pk.Name, pk.Type.CQLDef()))
	}
	for _, ck := range t.ClusteringKeys {
		clusteringKeys = append(clusteringKeys, ck.Name)
		columns = append(columns, fmt.Sprintf("%s %s", ck.Name, ck.Type.CQLDef()))
	}
	for _, cdef := range t.Columns {
		columns = append(columns, fmt.Sprintf("%s %s", cdef.Name, cdef.Type.CQLDef()))
	}

	var stmt string
	if len(clusteringKeys) == 0 {
		stmt = fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s)))",
			ks.Name,
			t.Name,
			strings.Join(columns, ","),
			strings.Join(partitionKeys, ","),
		)
	} else {
		stmt = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY ((%s), %s))", ks.Name, t.Name, strings.Join(columns, ","),
			strings.Join(partitionKeys, ","), strings.Join(clusteringKeys, ","))
	}

	if len(t.TableOptions) > 0 {
		stmt = stmt + " WITH " + strings.Join(t.TableOptions, " AND ") + ";"
	}
	return stmt
}

func GetCreateTypes(t *typedef.Table, keyspace typedef.Keyspace) []string {
	t.RLock()
	defer t.RUnlock()

	stmts := make([]string, 0, len(t.Columns))

	for _, column := range t.Columns {
		c, ok := column.Type.(*typedef.UDTType)
		if !ok {
			continue
		}
		createType := "CREATE TYPE IF NOT EXISTS %s.%s (%s)"
		var typs []string
		for name, typ := range c.ValueTypes {
			typs = append(typs, name+" "+typ.CQLDef())
		}
		stmts = append(
			stmts,
			fmt.Sprintf(createType, keyspace.Name, c.TypeName, strings.Join(typs, ",")),
		)
	}
	return stmts
}
