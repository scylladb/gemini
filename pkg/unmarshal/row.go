package unmarshal

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/gemini/pkg/murmur"
	"golang.org/x/exp/slices"
)

var ErrRowsDifferent = errors.New("rows are different")

type diffLimit struct {
	pct     float32
	cntLow  int
	cntHigh int
}

func (l *diffLimit) Evaluate(val, total int) bool {
	if val <= l.cntLow {
		return false
	}
	if val >= l.cntHigh {
		return true
	}
	return (float32(val) / float32(total)) > l.pct
}

type Rows []*Row

func (r Rows) Compare(pkNames, ckNames []string, o Rows) error {
	if len(r) != len(o) {
		return ErrRowsDifferent
	}

	// TODO: for short rows, compare raw data

	err := r.calculateHashes(pkNames, ckNames)
	if err != nil {
		return errors.Wrap(err, "failed to calculate hashes")
	}

	err = o.calculateHashes(pkNames, ckNames)
	if err != nil {
		return errors.Wrap(err, "failed to calculate hashes")
	}

	r.sort()
	o.sort()

	if !r.equal(o) {
		return ErrRowsDifferent
	}
	return nil
}

func (r Rows) Diff(o Rows) (string, error) {
	var total []string
	for i, otherRow := range o {
		row := r[i]
		rowJSON, err := row.Columns.ToJSON()
		if err != nil {
			return "", err
		}
		otherJSON, err := otherRow.Columns.ToData()
		if err != nil {
			return "", err
		}
		if diff := cmp.Diff(rowJSON, otherJSON); diff != "" {
			total = append(total, fmt.Sprintf(
				`================== ROW %.2d ==================
================== EXPECTED ================
%s
================== ACTUAL ==================
%s
================== DIFF ====================
%s
`, i, rowJSON, otherJSON, diff))
		}
	}
	return strings.Join(total, "\n"), nil
}

func (r Rows) calculateHashes(pks []string, cks []string) error {
	for _, rec := range r {
		err := rec.CalculateHashes(pks, cks)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Rows) sort() {
	sort.Slice(r, func(i, j int) bool {
		if r[i].pkHash < r[j].pkHash {
			return true
		}
		if r[i].ckHash < r[j].ckHash {
			return true
		}
		return false
	})
}

func (r Rows) equal(o Rows) bool {
	for i, rec := range r {
		if rec.CompareHashes(o[i]) {
			return false
		}
	}
	return true
}

type Row struct {
	Columns Columns
	hash    uint64
	pkHash  uint64
	ckHash  uint64
}

func (r *Row) CalculateHashes(pks []string, cks []string) (err error) {
	r.pkHash, r.ckHash, r.hash, err = r.Columns.HashInfo(pks, cks)
	return err
}

func (r *Row) CompareHashes(o *Row) bool {
	return r.hash == o.hash && r.pkHash == o.pkHash && r.ckHash == o.ckHash
}

func NewRow(columns []gocql.ColumnInfo) *Row {
	cols := make(Columns, len(columns))
	for i := range columns {
		cols[i] = Column{
			info: columns[i],
		}
	}
	return &Row{
		Columns: cols,
	}
}

type Columns []Column

func (c Columns) ToData() (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(c))
	for _, col := range c {
		val, err := col.ToData()
		if err != nil {
			return nil, err
		}
		out[col.info.Name] = val
	}
	return out, nil
}

func (c Columns) ToJSON() (string, error) {
	rowData, err := c.ToData()
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(rowData)
	if err != nil {
		return "", errors.Wrapf(err, "failed to marshal row data")
	}
	return string(data), nil
}

func (c Columns) HashInfo(pk []string, ck []string) (pkHash, ckHash, dataHash uint64, err error) {
	pkh := &murmur.Hash{}
	ckh := &murmur.Hash{}
	datah := &murmur.Hash{}
	for _, col := range c {
		colHash, err := col.Hash()
		if err != nil {
			return 0, 0, 0, errors.Wrapf(err, "failed to get column %q hash", col.info.Name)
		}
		if slices.Contains(pk, col.info.Name) {
			pkh.Write2(uint64ToBytes(colHash)...)
			continue
		}
		if slices.Contains(ck, col.info.Name) {
			ckh.Write2(uint64ToBytes(colHash)...)
			continue
		}
		_, _ = datah.Write(uint64ToBytes(colHash))
	}
	return pkh.Sum64(), ckh.Sum64(), datah.Sum64(), nil
}

func (c Columns) ToInterfaces() []interface{} {
	out := make([]interface{}, len(c))
	for i := range c {
		out[i] = &c[i]
	}
	return out
}

type Column struct {
	info gocql.ColumnInfo
	data []byte
	hash uint64
}

// UnmarshalCQL implements gocql.Unmarshaler, to be used by gocql
func (r *Column) UnmarshalCQL(_ gocql.TypeInfo, data []byte) error {
	r.data = data
	return nil
}

func (r *Column) ToData() (interface{}, error) {
	val := r.info.TypeInfo.New()
	err := gocql.Unmarshal(r.info.TypeInfo, r.data, val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r Column) Value() interface{} {
	return r.data
}

func (r Column) Hash() (uint64, error) {
	if r.hash == 0 {
		var err error
		r.hash, err = GetColumnHash(r.info.TypeInfo, r.data)
		if err != nil {
			return 0, err
		}
	}
	return r.hash, nil
}
