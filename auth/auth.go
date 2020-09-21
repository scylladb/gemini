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

// BuildAuthenticator : Returns a new gocql.PasswordAuthenticator
// if both username and password are provided.
func BuildAuthenticator(username string, password string) (*gocql.PasswordAuthenticator, error) {
	if username == "" && password == "" {
		return nil, nil
	}
	if username != "" && password != "" {
		authenticator := gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
		return &authenticator, nil
	}
	if username != "" {
		return nil, errors.New("Password not provided")
	}
	return nil, errors.New("Username not provided")
}
