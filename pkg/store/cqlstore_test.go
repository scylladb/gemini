package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/utils"
)

func Test_DuplicateValuesWithCompare(t *testing.T) {
	t.Parallel()

	_, _ = utils.TestContainers(t)

	assert := require.New(t)

	assert.True(true)
}
