package generators

import "go.uber.org/multierr"

type Partitions []*Partition

func (p Partitions) Close() error {
	var err error

	for _, part := range p {
		err = multierr.Append(err, part.Close())
	}

	return err
}

func NewPartitions(count, pkBufferSize int, wakeUpSignal chan struct{}) Partitions {
	partitions := make(Partitions, 0, count)

	for i := 0; i < count; i++ {
		partitions = append(partitions, NewPartition(wakeUpSignal, pkBufferSize))
	}

	return partitions
}
