module github.com/scylladb/gemini

go 1.21

require (
	github.com/briandowns/spinner v1.23.0
	github.com/gocql/gocql v1.8.0
	github.com/google/go-cmp v0.6.0
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/mitchellh/mapstructure v1.5.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.19.1
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx/v2 v2.8.0
	github.com/spf13/cobra v1.8.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20240604190554-fc45aab8b7f8
	golang.org/x/net v0.26.0
	golang.org/x/sync v0.7.0
	gonum.org/v1/gonum v0.15.0
	gopkg.in/inf.v0 v0.9.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fatih/color v1.17.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.54.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/scylladb/go-reflectx v1.0.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/term v0.21.0 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.8.0
