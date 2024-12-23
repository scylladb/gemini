package drivers

import (
	"context"

	"github.com/scylladb/gemini/pkg/typedef"
)

type Nop struct{}

func NewNop() Nop {
	return Nop{}
}

func (n Nop) Execute(context.Context, *typedef.Stmt) error {
	return nil
}

func (n Nop) Fetch(context.Context, *typedef.Stmt) ([]map[string]any, error) {
	return nil, nil
}
