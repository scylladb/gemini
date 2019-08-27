package gemini

type Source struct {
	values    chan ValueWithToken
	oldValues chan ValueWithToken
}

//get returns a new value and ensures that it's corresponding token
//is not already in-flight.
func (s *Source) get() (ValueWithToken, bool) {
	return s.pick(), true
}

//getOld returns a previously used value and token or a new if
//the old queue is empty.
func (s *Source) getOld() (ValueWithToken, bool) {
	select {
	case v, ok := <-s.oldValues:
		return v, ok
		/*default:
		// There are no old values so we generate a new
		return s.get()
		*/
	}
}

// giveOld returns the supplied value for later reuse unless the value
//is empty in which case it removes the corresponding token from the
// in-flight tracking.
func (s *Source) giveOld(v ValueWithToken) {
	select {
	case s.oldValues <- v:
	default:
		// Old source is full, just drop the value
	}
}

func (s *Source) pick() ValueWithToken {
	return <-s.values
}
