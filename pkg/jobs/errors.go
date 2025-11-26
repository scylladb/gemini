package jobs

import "errors"

// Sentinel errors used to signal that a worker stopped due to reaching
// the configured error cap. These are considered expected/shutdown paths
// and should not be logged as errors by the job runner.
var (
	ErrMutationJobStopped   = errors.New("mutation job stopped due to errors")
	ErrValidationJobStopped = errors.New("validation job stopped due to errors")
)
