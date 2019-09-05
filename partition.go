package gemini

import (
	"github.com/scylladb/gemini/inflight"
	"gopkg.in/tomb.v2"
)

type Partition struct {
	values    chan ValueWithToken
	oldValues chan ValueWithToken
	inFlight  inflight.InFlight
	t         *tomb.Tomb
}

// get returns a new value and ensures that it's corresponding token
// is not already in-flight.
func (s *Partition) get() (ValueWithToken, bool) {
	for {
		v := s.pick()
		if s.inFlight.AddIfNotPresent(v.Token) {
			return v, true
		}
	}
}

var emptyValueWithToken = ValueWithToken{}

// getOld returns a previously used value and token or a new if
// the old queue is empty.
func (s *Partition) getOld() (ValueWithToken, bool) {
	select {
	case <-s.t.Dying():
		return emptyValueWithToken, false
	case v, ok := <-s.oldValues:
		return v, ok
	}
}

// giveOld returns the supplied value for later reuse unless the value
// is empty in which case it removes the corresponding token from the
// in-flight tracking.
func (s *Partition) giveOld(v ValueWithToken) {
	if len(v.Value) == 0 {
		s.inFlight.Delete(v.Token)
		return
	}
	select {
	case s.oldValues <- v:
	default:
		// Old partition buffer is full, just drop the value
	}
}

func (s *Partition) pick() ValueWithToken {
	return <-s.values
}
