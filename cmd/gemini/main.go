// Copyright (C) 2018 ScyllaDB

package main

import (
	"fmt"
	"os"
)

var (
	commit  = "none"
	version = "dev"
	date    = "unknown"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
