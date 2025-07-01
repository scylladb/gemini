// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/utils"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if err := rootCmd.ParseFlags(os.Args); err != nil {
		log.Panicf("Failed to parse flags: %v", err)
	}

	versionJSON, err := rootCmd.PersistentFlags().GetBool("version-json")

	if versionFlag || versionJSON {
		if err != nil {
			log.Panicf("Failed to get version info as json flag: %v", err)
		}

		if versionJSON {
			var data []byte
			data, err = json.Marshal(versionInfo)
			if err != nil {
				log.Panicf("Failed to marshal version info: %v\n", err)
			}

			//nolint:forbidigo
			fmt.Println(string(data))
			return
		}

		//nolint:forbidigo
		fmt.Println(versionInfo.String())

		return
	}

	metrics.StartMetricsServer(ctx, metricsPort)

	if profilingPort != 0 {
		go func() {
			mux := http.NewServeMux()

			mux.HandleFunc("GET /debug/pprof/", pprof.Index)
			mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

			log.Fatal(http.ListenAndServe("0.0.0.0:"+strconv.Itoa(profilingPort), mux))
		}()
	}

	status := 0

	if err = rootCmd.ExecuteContext(ctx); err != nil {
		status = 1
	}

	utils.ExecuteFinalizers()
	cancel()
	os.Exit(status) //nolint:gocritic
}

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				commit = setting.Value
			case "vcs.time":
				date = setting.Value
			}
		}
	}

	var err error

	versionInfo, err = NewVersionInfo()
	if err != nil {
		panic(err)
	}

	rootCmd.Version = versionInfo.String()
}
