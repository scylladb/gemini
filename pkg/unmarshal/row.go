package unmarshal

import (
	"github.com/gocql/gocql"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/scylladb/gemini/pkg/murmur"
	"golang.org/x/exp/slices"
	"sort"
)

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

func (r Rows) CalculateHashes(pks []string, cks []string) error {
	for _, rec := range r {
		err := rec.CalculateHashes(pks, cks)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Rows) ToData() error {
	for _, rec := range r {
		_, err := rec.Columns.ToData()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Rows) Sort() {
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

func (r Rows) Equal(o Rows) bool {
	if len(r) != len(o) {
		return false
	}
	for i, rec := range r {
		if rec.hash != o[i].hash {
			return false
		}
	}
	return true
}

func (r Rows) Diff(o Rows, limit diffLimit) {
	tmp := map[]struct{}{}{}
	expected := r.ToData()
	actual := o.ToData()
	difflib.UnifiedDiff{}
}

type Row struct {
	Columns Columns
	hash    uint64
	pkHash  uint64
	ckHash  uint64
}

func (r *Row) ToInterfaces() []interface{} {
	out := make([]interface{}, len(r.Columns))
	for i := range r.Columns {
		out[i] = &r.Columns[i]
	}
	return out
}

func (r *Row) CalculateHashes(pks []string, cks []string) error {
	err := r.Columns.CalculateHashes()
	if err != nil {
		return err
	}

	r.pkHash, err = r.Columns.HashOnly(pks)
	if err != nil {
		return err
	}

	r.ckHash, err = r.Columns.HashOnly(cks)
	if err != nil {
		return err
	}
	return nil
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

func (c Columns) CalculateHashes() error {
	for _, col := range c {
		_, err := col.Hash()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c Columns) Hash() (uint64, error) {
	hash := murmur.Hash{}
	for _, col := range c {
		colHash, err := col.Hash()
		if err != nil {
			return 0, err
		}
		_, err = hash.Write(uint64ToBytes(colHash))
		if err != nil {
			return 0, err
		}
	}
	return hash.Sum64(), nil
}

func (c Columns) HashOnly(names []string) (uint64, error) {
	hash := murmur.Hash{}
	for _, col := range c {
		if !slices.Contains(names, col.info.Name) {
			continue
		}
		colHash, err := col.Hash()
		if err != nil {
			return 0, err
		}
		_, err = hash.Write(uint64ToBytes(colHash))
		if err != nil {
			return 0, err
		}
	}
	return hash.Sum64(), nil
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
