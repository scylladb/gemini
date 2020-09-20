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
	"errors"

	"github.com/gocql/gocql"
)

func setAuthenticator(clusterConfig *gocql.ClusterConfig, username string, password string) (*gocql.ClusterConfig, error) {
	if username == "" && password == "" {
		return clusterConfig, nil
	}
	if username != "" && password != "" {
		clusterConfig.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
		return clusterConfig, nil
	}
	if username != "" {
		return clusterConfig, errors.New("Password not provided")
	}
	return clusterConfig, errors.New("Username not provided")
}
