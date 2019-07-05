package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/briandowns/spinner"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Status struct {
	WriteOps    int        `json:"write_ops"`
	WriteErrors int        `json:"write_errors"`
	ReadOps     int        `json:"read_ops"`
	ReadErrors  int        `json:"read_errors"`
	Errors      []JobError `json:"errors,omitempty"`
}

func (r *Status) Merge(sum *Status) Status {
	sum.WriteOps += r.WriteOps
	sum.WriteErrors += r.WriteErrors
	sum.ReadOps += r.ReadOps
	sum.ReadErrors += r.ReadErrors
	sum.Errors = append(sum.Errors, r.Errors...)
	return *sum
}

func (r *Status) PrintResult(w io.Writer) {
	if err := r.PrintResultAsJSON(w); err != nil {
		// In case there has been it has been a long run we want to display it anyway...
		fmt.Printf("Unable to print result as json, using plain text to stdout, error=%s\n", err)
		fmt.Printf("Gemini version: %s\n", version)
		fmt.Printf("Results:\n")
		fmt.Printf("\twrite ops:    %v\n", r.WriteOps)
		fmt.Printf("\tread ops:     %v\n", r.ReadOps)
		fmt.Printf("\twrite errors: %v\n", r.WriteErrors)
		fmt.Printf("\tread errors:  %v\n", r.ReadErrors)
		for i, err := range r.Errors {
			fmt.Printf("Error %d: %s\n", i, err)
		}
	}
}

func (r *Status) PrintResultAsJSON(w io.Writer) error {
	result := map[string]interface{}{
		"result":         r,
		"gemini_version": version,
	}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent(" ", " ")
	if err := encoder.Encode(result); err != nil {
		return errors.Wrap(err, "unable to create json from result")
	}
	return nil
}

func (r Status) String() string {
	return fmt.Sprintf("write ops: %v | read ops: %v | write errors: %v | read errors: %v", r.WriteOps, r.ReadOps, r.WriteErrors, r.ReadErrors)
}

func sampleStatus(p *Pump, c chan Status, sp *spinner.Spinner, logger *zap.Logger) Status {
	logger = logger.Named("sample_results")
	var testRes Status
	done := false
	for res := range c {
		testRes = res.Merge(&testRes)
		if sp != nil {
			sp.Suffix = fmt.Sprintf(" Running Gemini... %v", testRes)
		}
		if testRes.ReadErrors > 0 || testRes.WriteErrors > 0 {
			if failFast {
				if !done {
					done = true
					logger.Warn("Errors detected. Exiting.")
					p.Stop()
				}
			}
		}
	}
	return testRes
}
