package big2

import (
	"math/big"
	"unsafe"
)

type Int string

func NewInt(x int64) {
	val := big.NewInt(x)

}

func fromBigINt(val *big.Int) string {
	return unsafe.String((*byte)(unsafe.Pointer(val)), unsafe.Sizeof(*val))
}
