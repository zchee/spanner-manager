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
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestWriteProgress(t *testing.T) {
	tests := map[string]struct {
		format string
		args   []any
		want   string
	}{
		"success: formats message with newline": {
			format: "Creating database: %s",
			args:   []any{"projects/p/instances/i/databases/d"},
			want:   "Creating database: projects/p/instances/i/databases/d\n",
		},
		"success: accepts message without arguments": {
			format: "Deleting rows from 0 table(s)",
			want:   "Deleting rows from 0 table(s)\n",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			command := &cobra.Command{}
			command.SetOut(&stdout)
			command.SetErr(&stderr)

			if err := writeProgress(command, tt.format, tt.args...); err != nil {
				t.Fatalf("writeProgress() error = %v, want nil", err)
			}
			if got := stdout.String(); got != "" {
				t.Fatalf("stdout = %q, want no progress on stdout", got)
			}
			if got := stderr.String(); got != tt.want {
				t.Fatalf("stderr = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWriteProgressReturnsWriterError(t *testing.T) {
	wantErr := errors.New("stderr closed")
	command := &cobra.Command{}
	command.SetErr(failingWriter{err: wantErr})

	err := writeProgress(command, "Creating database: %s", "db")
	if !errors.Is(err, wantErr) {
		t.Fatalf("writeProgress() error = %v, want %v", err, wantErr)
	}
}

func TestRunWithProgressShowsDescription(t *testing.T) {
	var stderr bytes.Buffer
	command := &cobra.Command{}
	command.SetErr(&stderr)

	runCalled := false
	if err := runWithProgress(command, "Creating database", func() error {
		runCalled = true
		return nil
	}); err != nil {
		t.Fatalf("runWithProgress() error = %v, want nil", err)
	}
	if !runCalled {
		t.Fatal("runWithProgress() did not run operation")
	}
	if got := stderr.String(); !strings.Contains(got, "Creating database") {
		t.Fatalf("stderr = %q, want progress description", got)
	}
}

func TestRunWithProgressReturnsOperationError(t *testing.T) {
	wantErr := errors.New("create failed")
	command := &cobra.Command{}
	command.SetErr(&bytes.Buffer{})

	err := runWithProgress(command, "Creating database", func() error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("runWithProgress() error = %v, want %v", err, wantErr)
	}
}

func TestRunWithProgressIgnoresRenderErrorsAfterOperationSucceeds(t *testing.T) {
	var failWrites atomic.Bool
	command := &cobra.Command{}
	command.SetErr(&conditionalFailingWriter{
		fail: &failWrites,
		err:  errors.New("stderr closed"),
	})

	runCalled := false
	err := runWithProgress(command, "Creating database", func() error {
		runCalled = true
		failWrites.Store(true)
		time.Sleep(250 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("runWithProgress() error = %v, want nil after operation success", err)
	}
	if !runCalled {
		t.Fatal("runWithProgress() did not run operation")
	}
}

func TestRunWithProgressPrefersOperationErrorOverRenderErrors(t *testing.T) {
	wantErr := errors.New("create failed")
	var failWrites atomic.Bool
	command := &cobra.Command{}
	command.SetErr(&conditionalFailingWriter{
		fail: &failWrites,
		err:  errors.New("stderr closed"),
	})

	err := runWithProgress(command, "Creating database", func() error {
		failWrites.Store(true)
		time.Sleep(250 * time.Millisecond)
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("runWithProgress() error = %v, want operation error %v", err, wantErr)
	}
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type conditionalFailingWriter struct {
	fail *atomic.Bool
	err  error
}

func (w *conditionalFailingWriter) Write(p []byte) (int, error) {
	if w.fail.Load() {
		return 0, w.err
	}
	return len(p), nil
}
