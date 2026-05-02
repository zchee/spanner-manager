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

package cmd

import (
	"fmt"
	"time"

	progressbar "github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// writeProgress writes a human-readable progress state to stderr.
func writeProgress(cmd *cobra.Command, format string, args ...any) error {
	_, err := fmt.Fprintf(cmd.ErrOrStderr(), format+"\n", args...)
	return err
}

// runWithProgress runs an operation while showing an indeterminate progress state.
func runWithProgress(cmd *cobra.Command, description string, run func() error) error {
	bar := progressbar.NewOptions(
		-1,
		progressbar.OptionSetWriter(cmd.ErrOrStderr()),
		progressbar.OptionSetDescription(" "+description),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionClearOnFinish(),
	)
	if err := bar.RenderBlank(); err != nil {
		return err
	}

	stop := make(chan struct{})
	progressErr := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := bar.Add(1); err != nil {
					progressErr <- err
					return
				}
			case <-stop:
				progressErr <- nil
				return
			}
		}
	}()

	runErr := run()
	close(stop)
	<-progressErr
	_ = bar.Finish()
	if runErr != nil {
		return runErr
	}
	return nil
}
