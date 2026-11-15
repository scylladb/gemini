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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

func mismatchTable() *typedef.Table {
	return &typedef.Table{
		Name:           "t",
		PartitionKeys:  typedef.Columns{{Name: "pk", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck", Type: typedef.TypeInt}},
		Columns:        typedef.Columns{{Name: "v", Type: typedef.TypeText}},
	}
}

func mismatchRow(ck int32, v string) Row {
	pk, c, val := int32(1), ck, v
	return NewRow([]string{"pk", "ck", "v"}, []any{&pk, &c, &val})
}

// TestCompareReportsDuplicateRows: a cluster that returns one row twice has
// changed the result, even though every key is on both sides.
func TestCompareReportsDuplicateRows(t *testing.T) {
	t.Parallel()

	result := CompareCollectedRows(mismatchTable(),
		Rows{mismatchRow(1, "a"), mismatchRow(1, "a")},
		Rows{mismatchRow(1, "a")},
	)

	require.Error(t, result.ToError(), "a duplicated row must be a difference")
	assert.Len(t, result.TestOnlyRows, 1, "the extra copy must be reported on the test side")
	assert.Empty(t, result.OracleOnlyRows)
	assert.Equal(t, 1, result.MatchCount)

	result = CompareCollectedRows(mismatchTable(),
		Rows{mismatchRow(1, "a"), mismatchRow(2, "b")},
		Rows{mismatchRow(1, "a"), mismatchRow(1, "a"), mismatchRow(2, "b")},
	)

	require.Error(t, result.ToError())
	assert.Len(t, result.OracleOnlyRows, 1, "the extra copy must be reported on the oracle side")
	assert.Empty(t, result.TestOnlyRows)
}

// TestCompareReportsValueDiffWhenCountsDiffer: when the row counts differ, the
// rows that both sides hold must still be compared by value.
func TestCompareReportsValueDiffWhenCountsDiffer(t *testing.T) {
	t.Parallel()

	result := CompareCollectedRows(mismatchTable(),
		Rows{mismatchRow(1, "test value"), mismatchRow(2, "b")},
		Rows{mismatchRow(1, "oracle value"), mismatchRow(2, "b"), mismatchRow(3, "c")},
	)

	require.Error(t, result.ToError())
	assert.Len(t, result.OracleOnlyRows, 1, "row ck=3 is on the oracle only")
	require.Len(t, result.DifferentRows, 1, "row ck=1 differs in value and must be reported")
	assert.Equal(t, 1, result.MatchCount, "row ck=2 matches")
}

func TestCompareValuesSignedZeroAndDecimalScale(t *testing.T) {
	t.Parallel()

	negZero, posZero := math.Copysign(0, -1), 0.0

	assert.NotZero(t, compareValues(negZero, posZero), "-0 and +0 are different CQL values")
	assert.Negative(t, compareValues(negZero, posZero), "-0 orders before +0")
	assert.NotZero(t, compareValues(float32(negZero), float32(posZero)))
	assert.NotZero(t, compareValues(&negZero, &posZero))
	assert.False(t, ValuesEqual(negZero, &posZero))
	assert.Zero(t, compareValues(math.NaN(), math.NaN()), "NaN payloads are not part of CQL")
	assert.Zero(t, compareValues(1.5, 1.5))

	one, oneScaled := inf.NewDec(10, 1), inf.NewDec(100, 2) // 1.0 and 1.00
	assert.NotZero(t, compareValues(one, oneScaled), "1.0 and 1.00 are different CQL decimals")
	assert.Zero(t, compareValues(one, inf.NewDec(10, 1)))
	assert.Negative(t, compareValues(inf.NewDec(9, 1), oneScaled), "0.9 orders before 1.00: the value comes before the scale")
}
