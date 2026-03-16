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
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps both Spanner admin and data plane clients.
type Client struct {
	config   Config
	admin    *database.DatabaseAdminClient
	instance *instance.InstanceAdminClient
	data     *spanner.Client
}

// NewClient creates a new Client with admin and data plane connections.
// For emulator connections, it uses insecure credentials automatically.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	opts, err := clientOptions(cfg)
	if err != nil {
		return nil, err
	}

	adminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating database admin client: %w", err)
	}

	instanceClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		adminClient.Close()
		return nil, fmt.Errorf("creating instance admin client: %w", err)
	}

	dataClient, err := spanner.NewClient(ctx, cfg.DatabasePath(), opts...)
	if err != nil {
		adminClient.Close()
		instanceClient.Close()
		return nil, fmt.Errorf("creating spanner data client: %w", err)
	}

	return &Client{
		config:   cfg,
		admin:    adminClient,
		instance: instanceClient,
		data:     dataClient,
	}, nil
}

// NewAdminClient creates a new Client with only admin connections (no data plane).
// Useful for operations that don't require a database to exist (e.g., create/drop database).
func NewAdminClient(ctx context.Context, cfg Config) (*Client, error) {
	if err := cfg.ValidateInstance(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	opts, err := clientOptions(cfg)
	if err != nil {
		return nil, err
	}

	adminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating database admin client: %w", err)
	}

	instanceClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		adminClient.Close()
		return nil, fmt.Errorf("creating instance admin client: %w", err)
	}

	return &Client{
		config:   cfg,
		admin:    adminClient,
		instance: instanceClient,
	}, nil
}

// Close closes all underlying clients.
func (c *Client) Close() error {
	var firstErr error
	if c.data != nil {
		c.data.Close()
	}
	if c.admin != nil {
		if err := c.admin.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.instance != nil {
		if err := c.instance.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// GetDatabaseDDL returns the DDL statements that define the database schema.
func (c *Client) GetDatabaseDDL(ctx context.Context) ([]string, error) {
	resp, err := c.admin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{
		Database: c.config.DatabasePath(),
	})
	if err != nil {
		return nil, fmt.Errorf("getting database DDL: %w", err)
	}
	return resp.Statements, nil
}

// CreateDatabase creates a new Spanner database with the given extra DDL statements.
func (c *Client) CreateDatabase(ctx context.Context, statements []string) error {
	op, err := c.admin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          c.config.InstancePath(),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", c.config.Database),
		ExtraStatements: statements,
	})
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for database creation: %w", err)
	}

	return nil
}

// DropDatabase drops the Spanner database.
func (c *Client) DropDatabase(ctx context.Context) error {
	if err := c.admin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: c.config.DatabasePath(),
	}); err != nil {
		return fmt.Errorf("dropping database: %w", err)
	}
	return nil
}

// UpdateDatabaseDDL applies DDL statements to update the database schema.
func (c *Client) UpdateDatabaseDDL(ctx context.Context, statements []string) error {
	op, err := c.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.config.DatabasePath(),
		Statements: statements,
	})
	if err != nil {
		return fmt.Errorf("updating database DDL: %w", err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for DDL update: %w", err)
	}

	return nil
}

// ApplyDML executes DML statements within a read-write transaction.
func (c *Client) ApplyDML(ctx context.Context, statements []string) error {
	_, err := c.data.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		for _, stmt := range statements {
			if _, err := txn.Update(ctx, spanner.Statement{SQL: stmt}); err != nil {
				return fmt.Errorf("executing DML %q: %w", stmt, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("applying DML: %w", err)
	}
	return nil
}

// ApplyPartitionedDML executes a partitioned DML statement.
func (c *Client) ApplyPartitionedDML(ctx context.Context, statement string) (int64, error) {
	count, err := c.data.PartitionedUpdate(ctx, spanner.Statement{SQL: statement})
	if err != nil {
		return 0, fmt.Errorf("executing partitioned DML: %w", err)
	}
	return count, nil
}

// ReadWriteTransaction executes the given function within a read-write transaction.
func (c *Client) ReadWriteTransaction(ctx context.Context, f func(ctx context.Context, txn *spanner.ReadWriteTransaction) error) error {
	_, err := c.data.ReadWriteTransaction(ctx, f)
	return err
}

// Single returns a read-only transaction for single reads.
func (c *Client) Single() *spanner.ReadOnlyTransaction {
	return c.data.Single()
}

// CreateInstance creates a new Spanner instance.
func (c *Client) CreateInstance(ctx context.Context, displayName, configID string, nodeCount int32) error {
	op, err := c.instance.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     c.config.ProjectPath(),
		InstanceId: c.config.Instance,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/%s", c.config.Project, configID),
			DisplayName: displayName,
			NodeCount:   nodeCount,
		},
	})
	if err != nil {
		return fmt.Errorf("creating instance: %w", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for instance creation: %w", err)
	}

	return nil
}

// DeleteInstance deletes a Spanner instance.
func (c *Client) DeleteInstance(ctx context.Context) error {
	if err := c.instance.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: c.config.InstancePath(),
	}); err != nil {
		return fmt.Errorf("deleting instance: %w", err)
	}
	return nil
}

// DataClient returns the underlying spanner data client for direct access.
func (c *Client) DataClient() *spanner.Client {
	return c.data
}

// clientOptions builds the gRPC/API client options from the configuration.
func clientOptions(cfg Config) ([]option.ClientOption, error) {
	var opts []option.ClientOption

	if cfg.IsEmulator() {
		// For the emulator, set the environment variable and use insecure credentials.
		os.Setenv("SPANNER_EMULATOR_HOST", cfg.EmulatorHost)
		opts = append(opts,
			option.WithEndpoint(cfg.EmulatorHost),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithoutAuthentication(),
		)
		return opts, nil
	}

	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	return opts, nil
}
