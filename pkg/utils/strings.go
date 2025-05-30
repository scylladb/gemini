package utils

import (
	"golang.org/x/exp/constraints"
	"math/rand/v2"
	"strconv"
	"unsafe"
)

const uint64Size = int(unsafe.Sizeof(uint64(0)))
const hextable = "0123456789abcdef"

func RandString(rnd *rand.Rand, ln int) string {
	length := ln
	if val := length % uint64Size; val != 0 {
		length += uint64Size - val
	}

	binBuff := make([]byte, length)

	for i := 0; i < length; i += uint64Size {
		number := rnd.Uint64()
		for j := 0; j < uint64Size; j++ {
			binBuff[i+j] = hextable[(number>>j)&0x0f]
		}
	}

	return unsafe.String(unsafe.SliceData(binBuff), length)[:ln]
}

func FormatString[T constraints.Integer](dst []byte, integer T) string {
	dst = dst[:0]
	conv := strconv.AppendInt(dst, int64(integer), 10)

	return unsafe.String(unsafe.SliceData(conv), len(conv))
}
