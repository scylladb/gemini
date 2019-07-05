package main

import (
	"time"

	"github.com/briandowns/spinner"
)

func createSpinner() *spinner.Spinner {
	spinnerCharSet := []string{"|", "/", "-", "\\"}
	sp := spinner.New(spinnerCharSet, 1*time.Second)
	_ = sp.Color("black")
	sp.Start()
	return sp
}
