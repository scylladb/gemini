module github.com/scylladb/gemini

go 1.20

require (
	github.com/briandowns/spinner v1.23.0
	github.com/gocql/gocql v1.8.0
	github.com/google/go-cmp v0.5.9
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/mitchellh/mapstructure v1.5.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.15.0
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx/v2 v2.8.0
	github.com/spf13/cobra v1.7.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29
	golang.org/x/net v0.9.0
	golang.org/x/sync v0.1.0
	gonum.org/v1/gonum v0.13.0
	gopkg.in/inf.v0 v0.9.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/fatih/color v1.7.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.3 // indirect
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/scylladb/go-reflectx v1.0.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/term v0.7.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.8.0
