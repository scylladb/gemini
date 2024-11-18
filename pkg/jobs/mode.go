package jobs

import "strings"

type Mode []string

const (
	WriteMode = "write"
	ReadMode  = "read"
	MixedMode = "mixed"
)

func (m Mode) IsWrite() bool {
	return m[0] == WriteMode
}

func (m Mode) IsRead() bool {
	if len(m) == 1 {
		return m[0] == ReadMode
	}

	return m[0] == ReadMode || m[1] == ReadMode
}

func ModeFromString(m string) Mode {
	switch strings.ToLower(m) {
	case WriteMode:
		return Mode{WriteMode}
	case ReadMode:
		return Mode{ReadMode}
	case MixedMode:
		return Mode{WriteMode, ReadMode}
	default:
		panic("unknown mode " + m)
	}
}
