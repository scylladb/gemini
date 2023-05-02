module github.com/scylladb/gemini

go 1.12

require (
	github.com/briandowns/spinner v1.23.0
	github.com/gocql/gocql v0.0.0-20211015133455-b225f9b53fa1
	github.com/google/go-cmp v0.5.9
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/mitchellh/mapstructure v1.5.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.15.0
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx/v2 v2.8.0
	github.com/segmentio/ksuid v1.0.4
	github.com/spf13/cobra v1.7.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29
	golang.org/x/net v0.9.0
	golang.org/x/sync v0.1.0
	gonum.org/v1/gonum v0.13.0
	gopkg.in/inf.v0 v0.9.1
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.7.3
