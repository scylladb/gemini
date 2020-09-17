module github.com/scylladb/gemini

go 1.12

require (
	github.com/briandowns/spinner v1.11.1
	github.com/gocql/gocql v0.0.0-20200131111108-92af2e088537
	github.com/google/go-cmp v0.4.1
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.1.2
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.3
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx/v2 v2.0.2
	github.com/segmentio/ksuid v1.0.2
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0
	go.uber.org/zap v1.10.0
	golang.org/x/exp v0.0.0-20190510132918-efd6b22b2522
	golang.org/x/net v0.0.0-20190404232315-eb5bcb51f2a3
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	golang.org/x/sys v0.0.0-20190412213103-97732733099d // indirect
	gonum.org/v1/gonum v0.0.0-20190724133715-a8659125a966
	gopkg.in/inf.v0 v0.9.1
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.4.0
	golang.org/x/sync => golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
)
