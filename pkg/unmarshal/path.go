package unmarshal

import (
	"fmt"
	"strconv"
)

type Path []interface{}

func (p *Path) Add(item interface{}) Path {
	*p = append(*p, item)
	return *p
}

func (p *Path) Remove() Path {
	*p = (*p)[:len(*p)-1]
	return *p
}

func (p Path) String() string {
	out := make([]byte, 0)
	for _, val := range p {
		switch casted := val.(type) {
		case string:
			out = append(out, []byte(casted)...)
		case int:
			out = append(out, []byte(strconv.Itoa(casted))...)
		default:
			panic(fmt.Sprintf("wrong element type %T", val))
		}
		out = append(out, []byte(".")...)
	}
	return string(out)
}
