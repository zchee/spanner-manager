// Copyright 2026 The spanner-manager Authors.
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

package spannerutil

import (
	"errors"
	"fmt"
	"time"
)

// Config holds the connection configuration for a Spanner database.
type Config struct {
	// Project is the GCP project ID.
	Project string

	// Instance is the Spanner instance ID.
	Instance string

	// Database is the Spanner database ID.
	Database string

	// CredentialsFile is the path to a service account JSON key file.
	CredentialsFile string

	// EmulatorHost is the address of a Spanner emulator (e.g., "localhost:9010").
	EmulatorHost string

	// Timeout is the operation timeout.
	Timeout time.Duration
}

// DatabasePath returns the fully qualified Spanner database path.
func (c Config) DatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", c.Project, c.Instance, c.Database)
}

// InstancePath returns the fully qualified Spanner instance path.
func (c Config) InstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", c.Project, c.Instance)
}

// ProjectPath returns the fully qualified GCP project path.
func (c Config) ProjectPath() string {
	return fmt.Sprintf("projects/%s", c.Project)
}

// IsEmulator reports whether the configuration targets a Spanner emulator.
func (c Config) IsEmulator() bool {
	return c.EmulatorHost != ""
}

// Validate checks that the required fields are set for database operations.
func (c Config) Validate() error {
	var errs []error
	if c.Project == "" {
		errs = append(errs, errors.New("project is required"))
	}
	if c.Instance == "" {
		errs = append(errs, errors.New("instance is required"))
	}
	if c.Database == "" {
		errs = append(errs, errors.New("database is required"))
	}
	return errors.Join(errs...)
}

// ValidateInstance checks that project and instance are set (no database required).
func (c Config) ValidateInstance() error {
	var errs []error
	if c.Project == "" {
		errs = append(errs, errors.New("project is required"))
	}
	if c.Instance == "" {
		errs = append(errs, errors.New("instance is required"))
	}
	return errors.Join(errs...)
}
