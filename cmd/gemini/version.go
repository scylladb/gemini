// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/go-github/v69/github"
	"github.com/pkg/errors"
)

const (
	defaultRepoOwner = "scylladb"

	gocqlPackage  = "github.com/gocql/gocql"
	githubTimeout = 5 * time.Second
	userAgent     = "gemini (github.com/scylladb/gemini)"
)

var (
	shaPattern = regexp.MustCompile(`^[0-9a-fA-F]{7,40}$`)

	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

type (
	ComponentInfo struct {
		Version    string `json:"version"`
		CommitDate string `json:"commit_date"`
		CommitSHA  string `json:"commit_sha"`
	}

	VersionInfo struct {
		Gemini ComponentInfo `json:"gemini"`
		Driver ComponentInfo `json:"scylla-driver"`
	}

	versionOptions struct {
		httpClient        *http.Client
		ctx               context.Context
		outputVersionFile string
	}

	VersionOption func(*versionOptions)
)

func WithHTTPClient(client *http.Client) VersionOption {
	return func(opts *versionOptions) {
		opts.httpClient = client
	}
}

func WithContext(ctx context.Context) VersionOption {
	return func(opts *versionOptions) {
		opts.ctx = ctx
	}
}

func WithOutputVersionFile(file string) VersionOption {
	return func(opts *versionOptions) {
		opts.outputVersionFile = file
	}
}

func NewVersionInfo(options ...VersionOption) (VersionInfo, error) {
	opts := &versionOptions{
		httpClient: &http.Client{
			Timeout:   githubTimeout,
			Transport: http.DefaultTransport,
		},
		ctx:               context.Background(),
		outputVersionFile: "./version.json",
	}

	for _, opt := range options {
		opt(opts)
	}

	f, err := os.OpenFile(opts.outputVersionFile, os.O_RDONLY, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return fetchAndSaveVersionInfo(opts.ctx, opts.httpClient, opts.outputVersionFile)
		}

		return VersionInfo{}, errors.Wrapf(err, "failed to open %s", opts.outputVersionFile)
	}

	var v VersionInfo

	if err = json.NewDecoder(f).Decode(&v); err != nil {
		return VersionInfo{}, errors.Wrapf(err, "failed to decode %s", opts.outputVersionFile)
	}

	if err = f.Close(); err != nil {
		return VersionInfo{}, errors.Wrapf(err, "failed to close %s", opts.outputVersionFile)
	}

	return v, nil
}

func fetchAndSaveVersionInfo(
	ctx context.Context,
	httpClient *http.Client,
	filePath string,
) (VersionInfo, error) {
	client := github.NewClient(httpClient)
	client.UserAgent = userAgent

	driverInfo, err := getDriverVersionInfo(ctx, client)
	if err != nil {
		return VersionInfo{}, errors.Wrapf(err, "failed to get scylla-gocql-driver version info")
	}

	v := VersionInfo{
		Gemini: getMainBuildInfo(),
		Driver: driverInfo,
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return VersionInfo{}, err
	}

	if err = json.NewEncoder(f).Encode(v); err != nil {
		return VersionInfo{}, err
	}

	if err = f.Sync(); err != nil {
		return VersionInfo{}, err
	}

	if err = f.Close(); err != nil {
		return VersionInfo{}, err
	}

	return v, nil
}

func getMainBuildInfo() ComponentInfo {
	ver, sha, buildDate := version, commit, date

	if ver != "dev" && sha != "unknown" && buildDate != "unknown" {
		return ComponentInfo{
			Version:    ver,
			CommitDate: buildDate,
			CommitSHA:  sha,
		}
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" {
			ver = info.Main.Version
		} else {
			ver = "(devel)"
		}

		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && sha == "unknown" {
				sha = setting.Value
			}

			if setting.Key == "vcs.time" && buildDate == "unknown" {
				buildDate = setting.Value
			}
		}
	}

	return ComponentInfo{
		Version:    ver,
		CommitDate: buildDate,
		CommitSHA:  sha,
	}
}

func extractRepoOwner(repoPath, defaultOwner string) string {
	if parts := strings.SplitN(repoPath, "/", 2); len(parts) == 2 {
		return parts[1]
	}

	return defaultOwner
}

func extractReleaseInfo(
	ctx context.Context,
	client *github.Client,
	repoOwner, versionToCheck string,
) (string, string, error) {
	releases, _, err := client.Repositories.ListReleases(
		ctx,
		repoOwner,
		"gocql",
		&github.ListOptions{
			PerPage: 100,
		},
	)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to get release info for %s", versionToCheck)
	}

	var releaseDate, tagName string

	for _, release := range releases {
		if release.GetTagName() == versionToCheck {
			tagName = release.GetTagName()
			releaseDate = release.GetCreatedAt().Format("2006-01-02T15:04:05Z")
			break
		}
	}

	if tagName == "" {
		return "", "", errors.New("failed to find release info")
	}

	reference, _, err := client.Git.GetRef(ctx, repoOwner, "gocql", "tags/"+tagName)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to get reference info for %s", tagName)
	}

	return releaseDate, reference.GetObject().GetSHA(), nil
}

func getDriverVersionInfo(ctx context.Context, client *github.Client) (ComponentInfo, error) {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return ComponentInfo{}, errors.New("failed to read build info")
	}

	var driverModule *debug.Module
	for _, dep := range buildInfo.Deps {
		if dep.Path == gocqlPackage {
			driverModule = dep
			break
		}
	}

	if driverModule == nil {
		return ComponentInfo{}, errors.New("failed to find scylla-gocql-driver module")
	}

	if replacement := driverModule.Replace; replacement != nil {
		repoOwner := extractRepoOwner(os.Getenv("GOCQL_REPO"), defaultRepoOwner)
		envVersion, exists := os.LookupEnv("GOCQL_VERSION")
		versionToCheck := replacement.Version

		if exists {
			versionToCheck = envVersion
		}

		if shaPattern.MatchString(versionToCheck) {
			gitCommit, _, err := client.Repositories.GetCommit(
				ctx,
				repoOwner,
				"gocql",
				versionToCheck,
				nil,
			)
			if err != nil {
				return ComponentInfo{}, err
			}

			return ComponentInfo{
				Version: replacement.Version,
				CommitDate: gitCommit.GetCommit().
					GetCommitter().
					GetDate().
					Format("2006-01-02T15:04:05Z"),
				CommitSHA: envVersion,
			}, nil
		}

		if strings.HasPrefix(versionToCheck, "v") {
			releaseDate, sha, err := extractReleaseInfo(ctx, client, repoOwner, versionToCheck)
			if err != nil {
				return ComponentInfo{}, err
			}

			return ComponentInfo{
				Version:    replacement.Version,
				CommitDate: releaseDate,
				CommitSHA:  sha,
			}, nil
		}
	}

	return ComponentInfo{
		Version:    driverModule.Version,
		CommitDate: "",
		CommitSHA:  "upstream release",
	}, nil
}

func (v VersionInfo) String() string {
	return fmt.Sprintf(`:
    version: %s
    commit sha: %s
    commit date: %s
scylla-gocql-driver :
    version: %s
    commit sha: %s
    commit date: %s`,
		v.Gemini.Version,
		v.Gemini.CommitSHA,
		v.Gemini.CommitDate,
		v.Driver.Version,
		v.Driver.CommitSHA,
		v.Driver.CommitDate,
	)
}
