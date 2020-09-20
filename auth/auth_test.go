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

package auth

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
)

func TestSetAuthenticator(t *testing.T) {
	username := "username"
	password := "password"
	expectedConfigWithAuth := gocql.NewCluster("localhost")
	expectedConfigWithAuth.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	expectedConfigWithoutAuth := gocql.NewCluster("localhost")
	type credentials struct {
		username string
		password string
	}
	tests := map[string]struct {
		input credentials
		want  *gocql.ClusterConfig
		err   string
	}{
		"testClusterWithCredentials": {
			input: credentials{username: username, password: password},
			want:  expectedConfigWithAuth,
		},
		"testClusterWithoutCredentials": {
			input: credentials{username: "", password: ""},
			want:  expectedConfigWithoutAuth,
		},
		"testClusterWithoutPassword": {
			input: credentials{username: username, password: ""},
			want:  expectedConfigWithoutAuth,
			err:   "Password not provided",
		},
		"testClusterWithoutUsername": {
			input: credentials{username: "", password: password},
			want:  expectedConfigWithoutAuth,
			err:   "Username not provided",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			clusterConfig, err := setAuthenticator(
				gocql.NewCluster("localhost"),
				test.input.username,
				test.input.password,
			)
			if test.err == "" && err != nil {
				t.Fatalf("Returned unexpected error '%s'", err.Error())
			} else if test.err != "" && err == nil {
				t.Fatalf("Expected error '%s' but non was returned", test.err)
			} else if test.err != "" && err != nil && err.Error() != test.err {
				t.Fatalf("Returned error '%s' doesn't match expected error '%s'", err.Error(), test.err)
			}
			if diff := cmp.Diff(test.want.Authenticator, clusterConfig.Authenticator); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
