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

package diff

import (
	"strings"
	"testing"

	spanast "github.com/cloudspannerecosystem/memefish/ast"
	gocmp "github.com/google/go-cmp/cmp"

	"github.com/zchee/spanner-manager/sqlutil"
)

func mustParseDatabaseFromString(t *testing.T, ddl string) *Database {
	t.Helper()

	stmts, err := sqlutil.SplitStatements(ddl)
	if err != nil {
		t.Fatalf("SplitStatements() error = %v", err)
	}

	db, err := ParseDatabase(stmts)
	if err != nil {
		t.Fatalf("ParseDatabase() error = %v", err)
	}

	return db
}

func TestDiff_NoDifferences(t *testing.T) {
	ddl := []string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	}

	from, err := ParseDatabase(ddl)
	if err != nil {
		t.Fatal(err)
	}
	to, err := ParseDatabase(ddl)
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	if len(stmts) != 0 {
		t.Errorf("expected no differences, got %d statements", len(stmts))
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_AddTable(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
		"CREATE TABLE Accounts (AccountId INT64 NOT NULL) PRIMARY KEY (AccountId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	if len(stmts) == 0 {
		t.Fatal("expected statements for new table")
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "CREATE TABLE Accounts") {
			found = true
			if s.Kind != sqlutil.KindDDL {
				t.Errorf("expected DDL kind, got %v", s.Kind)
			}
		}
	}
	if !found {
		t.Error("expected CREATE TABLE Accounts statement")
	}
}

func TestDiff_DropTable(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
		"CREATE TABLE Accounts (AccountId INT64 NOT NULL) PRIMARY KEY (AccountId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if s.SQL == "DROP TABLE Accounts" {
			found = true
		}
	}
	if !found {
		t.Error("expected DROP TABLE Accounts statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_AddColumn(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "ADD COLUMN") && strings.Contains(s.SQL, "Name") {
			found = true
		}
	}
	if !found {
		t.Error("expected ALTER TABLE ... ADD COLUMN Name statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_DropColumn(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "DROP COLUMN") && strings.Contains(s.SQL, "Name") {
			found = true
		}
	}
	if !found {
		t.Error("expected ALTER TABLE ... DROP COLUMN Name statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_AddIndex(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
		"CREATE INDEX UsersByName ON Users(Name)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if strings.Contains(s.SQL, "CREATE INDEX UsersByName") {
			found = true
		}
	}
	if !found {
		t.Error("expected CREATE INDEX UsersByName statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_DropIndex(t *testing.T) {
	from, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
		"CREATE INDEX UsersByName ON Users(Name)",
	})
	if err != nil {
		t.Fatal(err)
	}

	to, err := ParseDatabase([]string{
		"CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (UserId)",
	})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, s := range stmts {
		if s.SQL == "DROP INDEX UsersByName" {
			found = true
		}
	}
	if !found {
		t.Error("expected DROP INDEX UsersByName statement")
		for _, s := range stmts {
			t.Logf("  %s: %s", s.Kind, s.SQL)
		}
	}
}

func TestDiff_EmptyToEmpty(t *testing.T) {
	from, err := ParseDatabase([]string{})
	if err != nil {
		t.Fatal(err)
	}
	to, err := ParseDatabase([]string{})
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := Diff(from, to)
	if err != nil {
		t.Fatal(err)
	}

	if len(stmts) != 0 {
		t.Errorf("expected no statements, got %d", len(stmts))
	}
}

func TestDiff_GoldenFromHammer(t *testing.T) {
	// These cases are ported from daichirata/hammer's diff_test.go and kept as
	// exact expectations for the local diff engine's current SQL output.
	tests := []struct {
		name              string
		from              string
		to                string
		want              []Statement
		forbidSQLContains []string
	}{
		{
			name: "drop table",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP TABLE t2`,
				},
			},
		},
		{
			name: "create table",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL: `CREATE TABLE t2 (
  t2_1 INT64 NOT NULL
) PRIMARY KEY (t2_1)`,
				},
			},
		},
		{
			name: "add column (allow null)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE t1 ADD COLUMN t1_2 INT64`,
				},
			},
		},
		{
			name: "add column (not null)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 BOOL NOT NULL,
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE t1 ADD COLUMN t1_2 BOOL NOT NULL DEFAULT (FALSE)`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE t1 ALTER COLUMN t1_2 DROP DEFAULT`,
				},
			},
		},
		{
			name: "set default",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL DEFAULT ("default value"),
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE t1 ALTER COLUMN t1_2 STRING(MAX) NOT NULL DEFAULT ("default value")`,
				},
			},
		},
		{
			name: "set not null and default",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL DEFAULT ("default value"),
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDML,
					SQL:  `UPDATE t1 SET t1_2 = "default value" WHERE t1_2 IS NULL`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE t1 ALTER COLUMN t1_2 STRING(MAX) NOT NULL DEFAULT ("default value")`,
				},
			},
		},
		{
			name: "add generated column",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(1) NOT NULL AS (SUBSTR(t1_1, 1, 1)) STORED,
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE t1 ADD COLUMN t1_2 STRING(1) NOT NULL AS (SUBSTR(t1_1, 1, 1)) STORED`,
				},
			},
		},
		{
			name: "add index",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE INDEX idx_t1_2 ON t1(t1_3);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE INDEX idx_t1_2 ON t1(t1_3)`,
				},
			},
		},
		{
			name: "alter index add stored column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2) STORING (t1_3);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER INDEX idx_t1_1 ADD STORED COLUMN t1_3`,
				},
			},
		},
		{
			name: "add search index",
			from: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 INT64 NOT NULL,
	t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 INT64 NOT NULL,
	t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE SEARCH INDEX idx_t1_3 ON t1(t1_3);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE SEARCH INDEX idx_t1_3 ON t1(t1_3)`,
				},
			},
		},
		{
			name: "add named constraint",
			from: `
CREATE TABLE Singers (
  SingerId INT64 NOT NULL,
  AlbumId INT64 NOT NULL,
) PRIMARY KEY (SingerId);
CREATE TABLE Albums (
  AlbumId INT64 NOT NULL,
) PRIMARY KEY (AlbumId);
`,
			to: `
CREATE TABLE Singers (
  SingerId INT64 NOT NULL,
  AlbumId INT64 NOT NULL,
  CONSTRAINT FK_Albums FOREIGN KEY (AlbumId) REFERENCES Albums (AlbumId),
) PRIMARY KEY (SingerId);
CREATE TABLE Albums (
  AlbumId INT64 NOT NULL,
) PRIMARY KEY (AlbumId);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE Singers ADD CONSTRAINT FK_Albums FOREIGN KEY (AlbumId) REFERENCES Albums (AlbumId)`,
				},
			},
		},
		{
			name: "add row deletion policy",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(id);
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(id), ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY));
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE t1 ADD ROW DELETION POLICY ( OLDER_THAN ( created_at, INTERVAL 30 DAY ))`,
				},
			},
		},
		{
			name: "alter database options",
			from: `
ALTER DATABASE db SET OPTIONS(optimizer_version=3, version_retention_period='3d');
`,
			to: `
ALTER DATABASE db SET OPTIONS(enable_key_visualizer=true);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER DATABASE db SET OPTIONS (enable_key_visualizer = true, optimizer_version = null, version_retention_period = null)`,
				},
			},
		},
		{
			name: "create change stream",
			from: ``,
			to: `
CREATE CHANGE STREAM SomeStream;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE CHANGE STREAM SomeStream`,
				},
			},
		},
		{
			name: "alter change stream watch none to all",
			from: `
CREATE CHANGE STREAM SomeStream;
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER CHANGE STREAM SomeStream SET FOR ALL`,
				},
			},
		},
		{
			name: "alter change stream option",
			from: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '36h', value_capture_type = 'NEW_VALUES', exclude_ttl_deletes = false, exclude_insert = false, exclude_update = false, exclude_delete = false, allow_txn_exclusion = false );
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '5d', value_capture_type = 'NEW_ROW', exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true );
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER CHANGE STREAM SomeStream SET OPTIONS (retention_period = "5d", value_capture_type = "NEW_ROW", exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true)`,
				},
			},
		},
		{
			name: "drop view",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP VIEW v1`,
				},
			},
		},
		{
			name: "create view",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
				},
			},
		},
		{
			name: "replace view",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1 WHERE t1_1 > 0;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE OR REPLACE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
				},
			},
		},
		{
			name: "named schema",
			from: `
CREATE TABLE schema.t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX schema.idx_t1_1 ON schema.t1(t1_1);
`,
			to: `
CREATE TABLE schema.t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP INDEX schema.idx_t1_1`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `ALTER TABLE schema.t1 ADD COLUMN t1_2 INT64`,
				},
			},
		},
		{
			name: "keyword identifier",
			from: `
CREATE TABLE ` + "`Order`" + ` (
  order_1 INT64 NOT NULL,
) PRIMARY KEY(order_1);
`,
			to: `
CREATE TABLE ` + "`Order`" + ` (
  order_1 INT64 NOT NULL,
  order_2 INT64,
) PRIMARY KEY(order_1);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  "ALTER TABLE `Order` ADD COLUMN order_2 INT64",
				},
			},
		},
		{
			name: "create role",
			from: `
CREATE ROLE role1;
`,
			to: `
CREATE ROLE role1;
CREATE ROLE role2;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE ROLE role2`,
				},
			},
		},
		{
			name: "drop role",
			from: `
CREATE ROLE role1;
CREATE ROLE role2;
`,
			to: `
CREATE ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP ROLE role2`,
				},
			},
		},
		{
			name: "grant role",
			from: `
GRANT SELECT ON TABLE T1 TO ROLE role1;
`,
			to: `
GRANT SELECT ON TABLE T1 TO ROLE role1;
GRANT SELECT ON TABLE T2 TO ROLE role2;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT SELECT ON TABLE T2 TO ROLE role2`,
				},
			},
		},
		{
			name: "grant select on view",
			from: `
CREATE ROLE role1;
`,
			to: `
CREATE ROLE role1;
GRANT SELECT ON VIEW V1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT SELECT ON VIEW V1 TO ROLE role1`,
				},
			},
		},
		{
			name: "grant select on change stream",
			from: `
CREATE ROLE role1;
`,
			to: `
CREATE ROLE role1;
GRANT SELECT ON CHANGE STREAM cs1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT SELECT ON CHANGE STREAM cs1 TO ROLE role1`,
				},
			},
		},
		{
			name: "grant execute on table function",
			from: `
CREATE ROLE role1;
`,
			to: `
CREATE ROLE role1;
GRANT EXECUTE ON TABLE FUNCTION tf1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT EXECUTE ON TABLE FUNCTION tf1 TO ROLE role1`,
				},
			},
		},
		{
			name: "grant role with same roles in different order",
			from: `
GRANT SELECT ON TABLE T1 TO ROLE role1, role2;
`,
			to: `
GRANT SELECT ON TABLE T1 TO ROLE role2, role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON TABLE T1 FROM ROLE role1, role2`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT SELECT ON TABLE T1 TO ROLE role2, role1`,
				},
			},
		},
		{
			name: "revoke role",
			from: `
GRANT SELECT ON TABLE T1 TO ROLE role1;
GRANT SELECT ON TABLE T2 TO ROLE role2;
`,
			to: `
GRANT SELECT ON TABLE T1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON TABLE T2 FROM ROLE role2`,
				},
			},
		},
		{
			name: "replace privilege type on same table",
			from: `
GRANT SELECT ON TABLE T1 TO ROLE role1;
`,
			to: `
GRANT INSERT ON TABLE T1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT INSERT ON TABLE T1 TO ROLE role1`,
				},
			},
		},
		{
			name: "grant multiple privileges at once",
			from: ``,
			to: `
GRANT SELECT, INSERT, DELETE ON TABLE T1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT SELECT, INSERT, DELETE ON TABLE T1 TO ROLE role1`,
				},
			},
		},
		{
			name: "revoke select column and grant new columns",
			from: `
GRANT SELECT(col1) ON TABLE T1 TO ROLE role1;
`,
			to: `
GRANT SELECT(col1, col2) ON TABLE T1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT(col1) ON TABLE T1 FROM ROLE role1`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `GRANT SELECT(col1, col2) ON TABLE T1 TO ROLE role1`,
				},
			},
		},
		{
			name: "revoke only keep role",
			from: `
CREATE ROLE role1;
GRANT SELECT ON TABLE T1 TO ROLE role1;
`,
			to: `
CREATE ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				},
			},
		},
		{
			name: "revoke on view before drop view",
			from: `
CREATE ROLE role1;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
GRANT SELECT ON VIEW V1 TO ROLE role1;
`,
			to: `
CREATE ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP VIEW V1`,
				},
			},
		},
		{
			name: "revoke on change stream before drop change stream",
			from: `
CREATE ROLE role1;
CREATE CHANGE STREAM CS1 FOR ALL;
GRANT SELECT ON CHANGE STREAM CS1 TO ROLE role1;
`,
			to: `
CREATE ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP CHANGE STREAM CS1`,
				},
			},
		},
		{
			name: "revoke table grant before drop role",
			from: `
CREATE ROLE role1;
CREATE TABLE T1 (id INT64);
GRANT SELECT ON TABLE T1 TO ROLE role1;
`,
			to: `
CREATE TABLE T1 (id INT64);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP ROLE role1`,
				},
			},
		},
		{
			name: "revoke one grant on unchanged view without replacing view",
			from: `
CREATE ROLE role_keep;
CREATE ROLE role_drop;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
GRANT SELECT ON VIEW V1 TO ROLE role_keep;
GRANT SELECT ON VIEW V1 TO ROLE role_drop;
`,
			to: `
CREATE ROLE role_keep;
CREATE ROLE role_drop;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
GRANT SELECT ON VIEW V1 TO ROLE role_keep;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON VIEW V1 FROM ROLE role_drop`,
				},
			},
		},
		{
			name: "revoke view grant before drop role",
			from: `
CREATE ROLE role1;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
GRANT SELECT ON VIEW V1 TO ROLE role1;
`,
			to: `
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON VIEW V1 FROM ROLE role1`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP ROLE role1`,
				},
			},
		},
		{
			name: "revoke select on unchanged view without replacing view",
			from: `
CREATE ROLE role1;
CREATE ROLE role2;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
GRANT SELECT ON VIEW V1 TO ROLE role1;
GRANT SELECT ON VIEW V1 TO ROLE role2;
`,
			to: `
CREATE ROLE role1;
CREATE ROLE role2;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
GRANT SELECT ON VIEW V1 TO ROLE role1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON VIEW V1 FROM ROLE role2`,
				},
			},
		},
		{
			name: "revoke change stream grant before drop role",
			from: `
CREATE ROLE role1;
CREATE CHANGE STREAM CS1 FOR ALL;
GRANT SELECT ON CHANGE STREAM CS1 TO ROLE role1;
`,
			to: `
CREATE CHANGE STREAM CS1 FOR ALL;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON CHANGE STREAM CS1 FROM ROLE role1`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP ROLE role1`,
				},
			},
		},
		{
			name: "drop role revoke only for resources that remain in target",
			from: `
CREATE ROLE role1;
CREATE TABLE T1 (id INT64);
CREATE TABLE T2 (id INT64);
GRANT SELECT ON TABLE T1 TO ROLE role1;
GRANT SELECT ON TABLE T2 TO ROLE role1;
`,
			to: `
CREATE TABLE T1 (id INT64);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP TABLE T2`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP ROLE role1`,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fromDDL, err := sqlutil.SplitStatements(strings.TrimSpace(tt.from))
			if err != nil {
				t.Fatalf("SplitStatements(from) error = %v", err)
			}

			toDDL, err := sqlutil.SplitStatements(strings.TrimSpace(tt.to))
			if err != nil {
				t.Fatalf("SplitStatements(to) error = %v", err)
			}

			from, err := ParseDatabase(fromDDL)
			if err != nil {
				t.Fatalf("ParseDatabase(from) error = %v", err)
			}

			to, err := ParseDatabase(toDDL)
			if err != nil {
				t.Fatalf("ParseDatabase(to) error = %v", err)
			}

			got, err := Diff(from, to)
			if err != nil {
				t.Fatalf("Diff() error = %v", err)
			}

			for _, forbidden := range tt.forbidSQLContains {
				for _, stmt := range got {
					if strings.Contains(stmt.SQL, forbidden) {
						t.Fatalf("Diff() emitted forbidden SQL %q in statement %q", forbidden, stmt.SQL)
					}
				}
			}

			if diff := gocmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Diff() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDiff_ExtraBeyondHammer(t *testing.T) {
	tests := []struct {
		name              string
		from              string
		to                string
		want              []Statement
		forbidSQLContains []string
	}{
		{
			name: "create schema",
			from: ``,
			to: `
CREATE SCHEMA analytics;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE SCHEMA analytics`,
				},
			},
		},
		{
			name: "drop schema",
			from: `
CREATE SCHEMA analytics;
`,
			to: ``,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP SCHEMA analytics`,
				},
			},
		},
		{
			name: "create sequence",
			from: ``,
			to: `
CREATE SEQUENCE Seq OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE SEQUENCE Seq OPTIONS (sequence_kind = "bit_reversed_positive")`,
				},
			},
		},
		{
			name: "drop sequence",
			from: `
CREATE SEQUENCE Seq OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			to: ``,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP SEQUENCE Seq`,
				},
			},
		},
		{
			name: "create vector index",
			from: `
CREATE TABLE Documents (
  DocId INT64 NOT NULL,
  DocEmbedding ARRAY<FLOAT32>(vector_length=>128) NOT NULL,
) PRIMARY KEY (DocId);
`,
			to: `
CREATE TABLE Documents (
  DocId INT64 NOT NULL,
  DocEmbedding ARRAY<FLOAT32>(vector_length=>128) NOT NULL,
) PRIMARY KEY (DocId);
CREATE VECTOR INDEX DocEmbeddingIndex
ON Documents (DocEmbedding)
OPTIONS(distance_type='COSINE');
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE VECTOR INDEX DocEmbeddingIndex ON Documents (DocEmbedding) OPTIONS (distance_type = "COSINE")`,
				},
			},
		},
		{
			name: "drop vector index",
			from: `
CREATE TABLE Documents (
  DocId INT64 NOT NULL,
  DocEmbedding ARRAY<FLOAT32>(vector_length=>128) NOT NULL,
) PRIMARY KEY (DocId);
CREATE VECTOR INDEX DocEmbeddingIndex
ON Documents (DocEmbedding)
OPTIONS(distance_type='COSINE');
`,
			to: `
CREATE TABLE Documents (
  DocId INT64 NOT NULL,
  DocEmbedding ARRAY<FLOAT32>(vector_length=>128) NOT NULL,
) PRIMARY KEY (DocId);
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP VECTOR INDEX DocEmbeddingIndex`,
				},
			},
		},
		{
			name: "replace vector index by drop create",
			from: `
CREATE TABLE Documents (
  DocId INT64 NOT NULL,
  DocEmbedding ARRAY<FLOAT32>(vector_length=>128) NOT NULL,
) PRIMARY KEY (DocId);
CREATE VECTOR INDEX DocEmbeddingIndex
ON Documents (DocEmbedding)
OPTIONS(distance_type='COSINE');
`,
			to: `
CREATE TABLE Documents (
  DocId INT64 NOT NULL,
  DocEmbedding ARRAY<FLOAT32>(vector_length=>128) NOT NULL,
) PRIMARY KEY (DocId);
CREATE VECTOR INDEX DocEmbeddingIndex
ON Documents (DocEmbedding)
OPTIONS(distance_type='EUCLIDEAN');
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP VECTOR INDEX DocEmbeddingIndex`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE VECTOR INDEX DocEmbeddingIndex ON Documents (DocEmbedding) OPTIONS (distance_type = "EUCLIDEAN")`,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fromDDL, err := sqlutil.SplitStatements(strings.TrimSpace(tt.from))
			if err != nil {
				t.Fatalf("SplitStatements(from) error = %v", err)
			}

			toDDL, err := sqlutil.SplitStatements(strings.TrimSpace(tt.to))
			if err != nil {
				t.Fatalf("SplitStatements(to) error = %v", err)
			}

			from, err := ParseDatabase(fromDDL)
			if err != nil {
				t.Fatalf("ParseDatabase(from) error = %v", err)
			}

			to, err := ParseDatabase(toDDL)
			if err != nil {
				t.Fatalf("ParseDatabase(to) error = %v", err)
			}

			got, err := Diff(from, to)
			if err != nil {
				t.Fatalf("Diff() error = %v", err)
			}

			for _, forbidden := range tt.forbidSQLContains {
				for _, stmt := range got {
					if strings.Contains(stmt.SQL, forbidden) {
						t.Fatalf("Diff() emitted forbidden SQL %q in statement %q", forbidden, stmt.SQL)
					}
				}
			}

			if diff := gocmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Diff() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDiff_ApprovedPlanRegressions(t *testing.T) {
	tests := []struct {
		name              string
		from              string
		to                string
		want              []Statement
		wantErr           bool
		forbidSQLContains []string
	}{
		{
			name: "widen string length without dropping data",
			from: `CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(36)) PRIMARY KEY(UserId);`,
			to:   `CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(UserId);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE Users ALTER COLUMN Name STRING(MAX)`},
			},
			forbidSQLContains: []string{`DROP COLUMN`, `DROP TABLE`},
		},
		{
			name:    "unsupported type change fails instead of drop recreate",
			from:    `CREATE TABLE Users (UserId INT64 NOT NULL, ExternalId INT64) PRIMARY KEY(UserId);`,
			to:      `CREATE TABLE Users (UserId INT64 NOT NULL, ExternalId STRING(36)) PRIMARY KEY(UserId);`,
			wantErr: true,
			forbidSQLContains: []string{
				`DROP COLUMN ExternalId`,
				`DROP TABLE Users`,
			},
		},
		{
			name:    "unsupported indexed column change fails instead of drop recreate",
			from:    `CREATE TABLE Users (UserId INT64 NOT NULL, ExternalId INT64) PRIMARY KEY(UserId); CREATE INDEX UsersByExternalId ON Users(ExternalId);`,
			to:      `CREATE TABLE Users (UserId INT64 NOT NULL, ExternalId STRING(36)) PRIMARY KEY(UserId); CREATE INDEX UsersByExternalId ON Users(ExternalId);`,
			wantErr: true,
			forbidSQLContains: []string{
				`DROP INDEX UsersByExternalId`,
				`DROP COLUMN ExternalId`,
			},
		},
		{
			name: "unsupported sequence kind change fails instead of drop create",
			from: `
CREATE SEQUENCE Seq OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			to: `
CREATE SEQUENCE Seq OPTIONS (sequence_kind = 'skip_range_min_max');
`,
			wantErr: true,
			forbidSQLContains: []string{
				`DROP SEQUENCE Seq`,
				`CREATE SEQUENCE Seq`,
			},
		},
		{
			name: "sequence option changes are altered in place",
			from: `
CREATE SEQUENCE UserSeq OPTIONS (
  sequence_kind = 'bit_reversed_positive',
  skip_range_min = 1,
  skip_range_max = 100
);
`,
			to: `
CREATE SEQUENCE UserSeq OPTIONS (
  sequence_kind = 'bit_reversed_positive',
  skip_range_min = 10,
  skip_range_max = 1000
);
`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER SEQUENCE UserSeq SET OPTIONS (skip_range_min = 10, skip_range_max = 1000)`},
			},
			forbidSQLContains: []string{`DROP SEQUENCE UserSeq`},
		},
		{
			name: "primary key change is the only table recreation trigger",
			from: `CREATE TABLE Users (UserId INT64 NOT NULL, AccountId INT64 NOT NULL, Name STRING(36)) PRIMARY KEY(UserId);`,
			to:   `CREATE TABLE Users (UserId INT64 NOT NULL, AccountId INT64 NOT NULL, Name STRING(36)) PRIMARY KEY(AccountId, UserId);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `DROP TABLE Users`},
				{Kind: sqlutil.KindDDL, SQL: "CREATE TABLE Users (\n  UserId INT64 NOT NULL,\n  AccountId INT64 NOT NULL,\n  Name STRING(36)\n) PRIMARY KEY (AccountId, UserId)"},
			},
		},
		{
			name: "interleave change recreates child after descendants",
			from: `
CREATE TABLE ParentA (ParentId INT64 NOT NULL) PRIMARY KEY(ParentId);
CREATE TABLE ParentB (ParentId INT64 NOT NULL) PRIMARY KEY(ParentId);
CREATE TABLE Child (ParentId INT64 NOT NULL, ChildId INT64 NOT NULL) PRIMARY KEY(ParentId, ChildId), INTERLEAVE IN PARENT ParentA ON DELETE CASCADE;
`,
			to: `
CREATE TABLE ParentA (ParentId INT64 NOT NULL) PRIMARY KEY(ParentId);
CREATE TABLE ParentB (ParentId INT64 NOT NULL) PRIMARY KEY(ParentId);
CREATE TABLE Child (ParentId INT64 NOT NULL, ChildId INT64 NOT NULL) PRIMARY KEY(ParentId, ChildId), INTERLEAVE IN PARENT ParentB ON DELETE NO ACTION;
`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `DROP TABLE Child`},
				{Kind: sqlutil.KindDDL, SQL: "CREATE TABLE Child (\n  ParentId INT64 NOT NULL,\n  ChildId INT64 NOT NULL\n) PRIMARY KEY (ParentId, ChildId),\n  INTERLEAVE IN PARENT ParentB ON DELETE NO ACTION"},
			},
		},
		{
			name: "stable named constraint identity with explicit no action",
			from: `
CREATE TABLE Albums (AlbumId INT64 NOT NULL) PRIMARY KEY(AlbumId);
CREATE TABLE Singers (
  SingerId INT64 NOT NULL,
  AlbumId INT64,
  CONSTRAINT FK_Albums FOREIGN KEY (AlbumId) REFERENCES Albums (AlbumId)
) PRIMARY KEY(SingerId);
`,
			to: `
CREATE TABLE Albums (AlbumId INT64 NOT NULL) PRIMARY KEY(AlbumId);
CREATE TABLE Singers (
  SingerId INT64 NOT NULL,
  AlbumId INT64,
  CONSTRAINT FK_Albums FOREIGN KEY (AlbumId) REFERENCES Albums (AlbumId) ON DELETE NO ACTION
) PRIMARY KEY(SingerId);
`,
			want: nil,
			forbidSQLContains: []string{
				`DROP CONSTRAINT FK_Albums`,
				`ADD CONSTRAINT FK_Albums`,
			},
		},
		{
			name: "unchanged view does not emit create or replace",
			from: `
CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(UserId);
CREATE VIEW ActiveUsers SQL SECURITY INVOKER AS SELECT UserId, Name FROM Users WHERE Name IS NOT NULL;
`,
			to: `
CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY(UserId);
CREATE VIEW ActiveUsers SQL SECURITY INVOKER AS SELECT UserId, Name FROM Users WHERE Name IS NOT NULL;
`,
			want:              nil,
			forbidSQLContains: []string{`CREATE OR REPLACE VIEW ActiveUsers`},
		},
		{
			name: "dropping a view grant only revokes the grant",
			from: `
CREATE ROLE role1;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
GRANT SELECT ON VIEW V1 TO ROLE role1;
`,
			to: `
CREATE ROLE role1;
CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `REVOKE SELECT ON VIEW V1 FROM ROLE role1`,
				},
			},
			forbidSQLContains: []string{`CREATE OR REPLACE VIEW V1`},
		},
		{
			name: "view literal whitespace changes are not normalized away",
			from: `
CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY(UserId);
CREATE VIEW LiteralView SQL SECURITY INVOKER AS SELECT "active  user" AS label FROM Users;
`,
			to: `
CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY(UserId);
CREATE VIEW LiteralView SQL SECURITY INVOKER AS SELECT "active user" AS label FROM Users;
`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `CREATE OR REPLACE VIEW LiteralView SQL SECURITY INVOKER AS SELECT "active user" AS label FROM Users`},
			},
		},
		{
			name: "view replace reissues surviving grants",
			from: `
CREATE ROLE role1;
CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY(UserId);
CREATE VIEW LiteralView SQL SECURITY INVOKER AS SELECT "active  user" AS label FROM Users;
GRANT SELECT ON VIEW LiteralView TO ROLE role1;
`,
			to: `
CREATE ROLE role1;
CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY(UserId);
CREATE VIEW LiteralView SQL SECURITY INVOKER AS SELECT "active user" AS label FROM Users;
GRANT SELECT ON VIEW LiteralView TO ROLE role1;
`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `CREATE OR REPLACE VIEW LiteralView SQL SECURITY INVOKER AS SELECT "active user" AS label FROM Users`},
				{Kind: sqlutil.KindDDL, SQL: `GRANT SELECT ON VIEW LiteralView TO ROLE role1`},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := mustParseDatabaseFromString(t, tt.from)
			to := mustParseDatabaseFromString(t, tt.to)

			got, err := Diff(from, to)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Diff() error = nil, want error; statements = %#v", got)
				}
			} else if err != nil {
				t.Fatalf("Diff() error = %v", err)
			}

			for _, forbidden := range tt.forbidSQLContains {
				for _, stmt := range got {
					if strings.Contains(stmt.SQL, forbidden) {
						t.Fatalf("Diff() emitted forbidden SQL %q in statement %q", forbidden, stmt.SQL)
					}
				}
			}

			if tt.wantErr {
				return
			}
			if diff := gocmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("Diff() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseDatabase_UnsupportedModernDDLReturnsError(t *testing.T) {
	tests := []struct {
		name            string
		ddl             string
		wantSQLFragment string
	}{
		{
			name:            "locality group",
			ddl:             `CREATE LOCALITY GROUP hot OPTIONS (storage = 'ssd');`,
			wantSQLFragment: `CREATE LOCALITY GROUP hot`,
		},
		{
			name: "property graph",
			ddl: `
CREATE TABLE Users (UserId INT64 NOT NULL) PRIMARY KEY(UserId);
CREATE PROPERTY GRAPH UserGraph NODE TABLES (Users);
`,
			wantSQLFragment: `CREATE PROPERTY GRAPH UserGraph`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := sqlutil.SplitStatements(strings.TrimSpace(tt.ddl))
			if err != nil {
				t.Fatalf("SplitStatements() error = %v", err)
			}
			_, err = ParseDatabase(stmts)
			if err == nil {
				t.Fatalf("ParseDatabase() error = nil, want unsupported DDL error")
			}
			if !strings.Contains(err.Error(), tt.wantSQLFragment) {
				t.Fatalf("ParseDatabase() error = %q, want SQL fragment %q", err, tt.wantSQLFragment)
			}
		})
	}
}

func TestDiff_CoverageBranches(t *testing.T) {
	tests := []struct {
		name              string
		from              string
		to                string
		want              []Statement
		wantErr           bool
		forbidSQLContains []string
	}{
		{
			name: "timestamp set not null allow commit true",
			from: `CREATE TABLE t1 (id INT64 NOT NULL, ts TIMESTAMP) PRIMARY KEY(id);`,
			to:   `CREATE TABLE t1 (id INT64 NOT NULL, ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(id);`,
			want: []Statement{
				{Kind: sqlutil.KindDML, SQL: `UPDATE t1 SET ts = TIMESTAMP "0001-01-01T00:00:00Z" WHERE ts IS NULL`},
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE t1 ALTER COLUMN ts TIMESTAMP NOT NULL`},
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE t1 ALTER COLUMN ts SET OPTIONS (allow_commit_timestamp = true)`},
			},
		},
		{
			name: "timestamp clear allow commit and nullable",
			from: `CREATE TABLE t1 (id INT64 NOT NULL, ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(id);`,
			to:   `CREATE TABLE t1 (id INT64 NOT NULL, ts TIMESTAMP) PRIMARY KEY(id);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE t1 ALTER COLUMN ts TIMESTAMP`},
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE t1 ALTER COLUMN ts SET OPTIONS (allow_commit_timestamp = null)`},
			},
		},
		{
			name: "replace row deletion policy",
			from: `CREATE TABLE t1 (id INT64 NOT NULL, created_at TIMESTAMP NOT NULL) PRIMARY KEY(id), ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY));`,
			to:   `CREATE TABLE t1 (id INT64 NOT NULL, created_at TIMESTAMP NOT NULL) PRIMARY KEY(id), ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 60 DAY));`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE t1 REPLACE ROW DELETION POLICY ( OLDER_THAN ( created_at, INTERVAL 60 DAY ))`},
			},
		},
		{
			name: "drop row deletion policy",
			from: `CREATE TABLE t1 (id INT64 NOT NULL, created_at TIMESTAMP NOT NULL) PRIMARY KEY(id), ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY));`,
			to:   `CREATE TABLE t1 (id INT64 NOT NULL, created_at TIMESTAMP NOT NULL) PRIMARY KEY(id);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE t1 DROP ROW DELETION POLICY`},
			},
		},
		{
			name: "drop fk before dropping column",
			from: `CREATE TABLE parent (code STRING(36) NOT NULL) PRIMARY KEY(code); CREATE TABLE child (id INT64 NOT NULL, code STRING(36), CONSTRAINT FK_Parent FOREIGN KEY (code) REFERENCES parent (code)) PRIMARY KEY(id);`,
			to:   `CREATE TABLE parent (code STRING(36) NOT NULL) PRIMARY KEY(code); CREATE TABLE child (id INT64 NOT NULL) PRIMARY KEY(id);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE child DROP CONSTRAINT FK_Parent`},
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE child DROP COLUMN code`},
			},
		},
		{
			name: "drop stored column from existing index",
			from: `CREATE TABLE t1 (id INT64 NOT NULL, email STRING(100), age INT64) PRIMARY KEY(id); CREATE INDEX idx_t1_email ON t1(email) STORING (age);`,
			to:   `CREATE TABLE t1 (id INT64 NOT NULL, email STRING(100), age INT64) PRIMARY KEY(id); CREATE INDEX idx_t1_email ON t1(email);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER INDEX idx_t1_email DROP STORED COLUMN age`},
			},
		},
		{
			name:    "regular-indexed column type change fails instead of drop recreate",
			from:    `CREATE TABLE t1 (id INT64 NOT NULL, c STRING(36)) PRIMARY KEY(id); CREATE INDEX idx_t1_c ON t1(c);`,
			to:      `CREATE TABLE t1 (id INT64 NOT NULL, c INT64 NOT NULL) PRIMARY KEY(id); CREATE INDEX idx_t1_c ON t1(c);`,
			wantErr: true,
			forbidSQLContains: []string{
				`DROP INDEX idx_t1_c`,
				`ALTER TABLE t1 DROP COLUMN c`,
			},
		},
		{
			name:    "search-indexed generated column change fails instead of drop recreate",
			from:    `CREATE TABLE t1 (id INT64 NOT NULL, body STRING(MAX), tok TOKENLIST AS (TOKENIZE_FULLTEXT(body)) HIDDEN) PRIMARY KEY(id); CREATE SEARCH INDEX sidx_t1_tok ON t1(tok);`,
			to:      `CREATE TABLE t1 (id INT64 NOT NULL, body STRING(MAX), tok TOKENLIST AS (TOKENIZE_SUBSTRING(body)) HIDDEN) PRIMARY KEY(id); CREATE SEARCH INDEX sidx_t1_tok ON t1(tok);`,
			wantErr: true,
			forbidSQLContains: []string{
				`DROP SEARCH INDEX sidx_t1_tok`,
				`ALTER TABLE t1 DROP COLUMN tok`,
			},
		},
		{
			name: "revoke on column before drop column",
			from: `CREATE ROLE role1; CREATE TABLE T1 (id INT64, name STRING(100)); GRANT SELECT(name) ON TABLE T1 TO ROLE role1;`,
			to:   `CREATE ROLE role1; CREATE TABLE T1 (id INT64);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER TABLE T1 DROP COLUMN name`},
			},
		},
		{
			name: "recreate table on primary key change",
			from: `CREATE TABLE T1 (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY(id);`,
			to:   `CREATE TABLE T1 (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY(name);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `DROP TABLE T1`},
				{Kind: sqlutil.KindDDL, SQL: "CREATE TABLE T1 (\n  id INT64 NOT NULL,\n  name STRING(100)\n) PRIMARY KEY (name)"},
			},
		},
		{
			name: "recreate table on interleave change",
			from: `CREATE TABLE P1 (pid INT64 NOT NULL) PRIMARY KEY(pid); CREATE TABLE P2 (pid INT64 NOT NULL) PRIMARY KEY(pid); CREATE TABLE C1 (id INT64 NOT NULL) PRIMARY KEY(id), INTERLEAVE IN PARENT P1 ON DELETE CASCADE;`,
			to:   `CREATE TABLE P1 (pid INT64 NOT NULL) PRIMARY KEY(pid); CREATE TABLE P2 (pid INT64 NOT NULL) PRIMARY KEY(pid); CREATE TABLE C1 (id INT64 NOT NULL) PRIMARY KEY(id), INTERLEAVE IN PARENT P2 ON DELETE NO ACTION;`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `DROP TABLE C1`},
				{Kind: sqlutil.KindDDL, SQL: "CREATE TABLE C1 (\n  id INT64 NOT NULL\n) PRIMARY KEY (id),\n  INTERLEAVE IN PARENT P2 ON DELETE NO ACTION"},
			},
		},
		{
			name: "change stream tables to none with option removal",
			from: `CREATE TABLE Singers (id INT64 NOT NULL) PRIMARY KEY(id); CREATE TABLE Albums (id INT64 NOT NULL) PRIMARY KEY(id); CREATE CHANGE STREAM SomeStream FOR Singers(id), Albums OPTIONS (retention_period='7d');`,
			to:   `CREATE TABLE Singers (id INT64 NOT NULL) PRIMARY KEY(id); CREATE TABLE Albums (id INT64 NOT NULL) PRIMARY KEY(id); CREATE CHANGE STREAM SomeStream;`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER CHANGE STREAM SomeStream DROP FOR ALL`},
				{Kind: sqlutil.KindDDL, SQL: `ALTER CHANGE STREAM SomeStream SET OPTIONS (retention_period = null)`},
			},
		},
		{
			name: "change stream tables to different tables",
			from: `CREATE TABLE Singers (id INT64 NOT NULL) PRIMARY KEY(id); CREATE TABLE Albums (id INT64 NOT NULL) PRIMARY KEY(id); CREATE CHANGE STREAM SomeStream FOR Singers(id);`,
			to:   `CREATE TABLE Singers (id INT64 NOT NULL) PRIMARY KEY(id); CREATE TABLE Albums (id INT64 NOT NULL) PRIMARY KEY(id); CREATE CHANGE STREAM SomeStream FOR Albums;`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER CHANGE STREAM SomeStream SET FOR Albums`},
			},
		},
		{
			name: "change stream keeps remaining watched table before drop",
			from: `CREATE TABLE Users (id INT64 NOT NULL) PRIMARY KEY(id); CREATE TABLE Accounts (id INT64 NOT NULL) PRIMARY KEY(id); CREATE CHANGE STREAM cs FOR Users, Accounts;`,
			to:   `CREATE TABLE Accounts (id INT64 NOT NULL) PRIMARY KEY(id); CREATE CHANGE STREAM cs FOR Accounts;`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `ALTER CHANGE STREAM cs SET FOR Accounts`},
				{Kind: sqlutil.KindDDL, SQL: `DROP TABLE Users`},
			},
		},
		{
			name: "search index recreate same name",
			from: `CREATE TABLE t1 (id INT64 NOT NULL, tok TOKENLIST AS (TOKENIZE_FULLTEXT(body)) HIDDEN, body STRING(MAX)) PRIMARY KEY(id); CREATE SEARCH INDEX sidx_t1_tok ON t1(tok);`,
			to:   `CREATE TABLE t1 (id INT64 NOT NULL, tok TOKENLIST AS (TOKENIZE_FULLTEXT(body)) HIDDEN, body STRING(MAX)) PRIMARY KEY(id); CREATE SEARCH INDEX sidx_t1_tok ON t1(tok) STORING (body);`,
			want: []Statement{
				{Kind: sqlutil.KindDDL, SQL: `DROP SEARCH INDEX sidx_t1_tok`},
				{Kind: sqlutil.KindDDL, SQL: `CREATE SEARCH INDEX sidx_t1_tok ON t1(tok) STORING (body)`},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from := mustParseDatabaseFromString(t, tt.from)
			to := mustParseDatabaseFromString(t, tt.to)

			got, err := Diff(from, to)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Diff() error = nil, want error; statements = %#v", got)
				}
			} else if err != nil {
				t.Fatalf("Diff() error = %v", err)
			}

			for _, forbidden := range tt.forbidSQLContains {
				for _, stmt := range got {
					if strings.Contains(stmt.SQL, forbidden) {
						t.Fatalf("Diff() emitted forbidden SQL %q in statement %q", forbidden, stmt.SQL)
					}
				}
			}

			if tt.wantErr {
				return
			}
			if diff := gocmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Diff() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDiff_HelperCoverage(t *testing.T) {
	t.Run("defaultByScalarTypeName", func(t *testing.T) {
		tests := []struct {
			name spanast.ScalarTypeName
			want string
		}{
			{spanast.BoolTypeName, "FALSE"},
			{spanast.Int64TypeName, "0"},
			{spanast.Float64TypeName, "0"},
			{spanast.StringTypeName, `""`},
			{spanast.BytesTypeName, `b""`},
			{spanast.DateTypeName, `DATE "0001-01-01"`},
			{spanast.TimestampTypeName, `TIMESTAMP "0001-01-01T00:00:00Z"`},
			{spanast.NumericTypeName, `NUMERIC "0"`},
			{spanast.JSONTypeName, `JSON "{}"`},
			{spanast.TokenListTypeName, `b""`},
			{spanast.ScalarTypeName("UUID"), `NEW_UUID()`},
		}

		for _, tt := range tests {
			if got := defaultByScalarTypeName(tt.name).SQL(); got != tt.want {
				t.Fatalf("defaultByScalarTypeName(%s) = %q, want %q", tt.name, got, tt.want)
			}
		}
	})

	t.Run("uuidDefaultExpr reuses parsed expression", func(t *testing.T) {
		first := uuidDefaultExpr()
		second := uuidDefaultExpr()
		if first != second {
			t.Fatalf("uuidDefaultExpr() did not reuse the parsed expression instance")
		}
		if got := first.SQL(); got != "NEW_UUID()" {
			t.Fatalf("uuidDefaultExpr().SQL() = %q, want %q", got, "NEW_UUID()")
		}
	})

	t.Run("uuid add not null column uses cast default", func(t *testing.T) {
		from := mustParseDatabaseFromString(t, `CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY(id);`)
		to := mustParseDatabaseFromString(t, `CREATE TABLE t1 (id INT64 NOT NULL, uuid_id UUID NOT NULL) PRIMARY KEY(id);`)
		stmts, err := Diff(from, to)
		if err != nil {
			t.Fatalf("Diff() error = %v", err)
		}
		got := statementsToStrings(stmts)
		want := []string{
			`ALTER TABLE t1 ADD COLUMN uuid_id UUID NOT NULL DEFAULT (NEW_UUID())`,
			`ALTER TABLE t1 ALTER COLUMN uuid_id DROP DEFAULT`,
		}
		if diff := gocmp.Diff(want, got); diff != "" {
			t.Fatalf("uuid add-column statements mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("uuid set not null backfills with new uuid", func(t *testing.T) {
		from := mustParseDatabaseFromString(t, `CREATE TABLE t1 (id INT64 NOT NULL, uuid_id UUID) PRIMARY KEY(id);`)
		to := mustParseDatabaseFromString(t, `CREATE TABLE t1 (id INT64 NOT NULL, uuid_id UUID NOT NULL) PRIMARY KEY(id);`)
		stmts, err := Diff(from, to)
		if err != nil {
			t.Fatalf("Diff() error = %v", err)
		}
		got := statementsToStrings(stmts)
		want := []string{
			`UPDATE t1 SET uuid_id = NEW_UUID() WHERE uuid_id IS NULL`,
			`ALTER TABLE t1 ALTER COLUMN uuid_id UUID NOT NULL`,
		}
		if diff := gocmp.Diff(want, got); diff != "" {
			t.Fatalf("uuid set-not-null statements mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("low level helpers", func(t *testing.T) {
		if got := exprSQL(nil); got != "" {
			t.Fatalf("exprSQL(nil) = %q, want empty string", got)
		}

		if got := optionsSQL(nil); got != "" {
			t.Fatalf("optionsSQL(nil) = %q, want empty string", got)
		}

		if got := normalizeConstraintSQL(`CONSTRAINT fk FOREIGN KEY (c) REFERENCES p (id) ON DELETE NO ACTION`); got != `CONSTRAINT fk FOREIGN KEY (c) REFERENCES p (id)` {
			t.Fatalf("normalizeConstraintSQL() = %q", got)
		}

		normalizeTests := map[string]struct {
			input string
			want  string
		}{
			"double quoted literal":  {input: "DEFAULT (\"a  b\")", want: "DEFAULT (\"a  b\")"},
			"single quoted literal":  {input: "DEFAULT ('a  b')", want: "DEFAULT ('a  b')"},
			"backtick identifier":    {input: "SELECT " + string(rune(0x60)) + "a  b" + string(rune(0x60)) + " FROM Users", want: "SELECT " + string(rune(0x60)) + "a  b" + string(rune(0x60)) + " FROM Users"},
			"backslash quote escape": {input: "DEFAULT ('a\\'  b')", want: "DEFAULT ('a\\'  b')"},
			"doubled quote escape":   {input: "DEFAULT ('a''  b')", want: "DEFAULT ('a''  b')"},
			"raw string literal":     {input: `DEFAULT (r"\n  b")`, want: `DEFAULT (r"\n  b")`},
			"triple quoted literal":  {input: `DEFAULT ("""a  b""")`, want: `DEFAULT ("""a  b""")`},
		}
		for name, tt := range normalizeTests {
			if got := normalizeSQL(tt.input); got != tt.want {
				t.Fatalf("%s: normalizeSQL() = %q, want %q", name, got, tt.want)
			}
		}

		commentTests := map[string]struct {
			input string
			want  string
		}{
			"line comment stripped": {
				input: "SELECT 1 -- ignored\nFROM Users",
				want:  "SELECT 1 FROM Users",
			},
			"block comment stripped": {
				input: "SELECT /* ignored */ 1 FROM Users",
				want:  "SELECT 1 FROM Users",
			},
			"line comment marker inside string": {
				input: `DEFAULT ("-- not a comment") -- ignored`,
				want:  `DEFAULT ("-- not a comment")`,
			},
			"block comment marker inside raw string": {
				input: `DEFAULT (r"/* not a comment */") /* ignored */`,
				want:  `DEFAULT (r"/* not a comment */")`,
			},
		}
		for name, tt := range commentTests {
			if got := normalizeSQL(tt.input); got != tt.want {
				t.Fatalf("%s: normalizeSQL() = %q, want %q", name, got, tt.want)
			}
		}

		if got := normalizeOnDelete(""); got != "NO ACTION" {
			t.Fatalf("normalizeOnDelete(\"\") = %q", got)
		}

		if got := normalizeDirection(""); got != spanast.DirectionAsc {
			t.Fatalf("normalizeDirection(\"\") = %q", got)
		}

		if got := comparableInterleaveIn(nil); got != "" {
			t.Fatalf("comparableInterleaveIn(nil) = %q", got)
		}

		tsDefault := updateDML{
			Table: &spanast.Path{Idents: []*spanast.Ident{{Name: "t1"}}},
			Def: &spanast.ColumnDef{
				Name: &spanast.Ident{Name: "ts"},
				Type: &spanast.ScalarSchemaType{Name: spanast.TimestampTypeName},
			},
		}
		if got := tsDefault.defaultValue().SQL(); got != `TIMESTAMP "0001-01-01T00:00:00Z"` {
			t.Fatalf("updateDML.defaultValue(timestamp) = %q", got)
		}

		arrayDefault := updateDML{
			Table: &spanast.Path{Idents: []*spanast.Ident{{Name: "t1"}}},
			Def: &spanast.ColumnDef{
				Name: &spanast.Ident{Name: "tags"},
				Type: &spanast.ArraySchemaType{Item: &spanast.SizedSchemaType{Name: spanast.StringTypeName, Max: true}},
			},
		}
		if got := arrayDefault.defaultValue().SQL(); got != "ARRAY[]" {
			t.Fatalf("updateDML.defaultValue(array) = %q", got)
		}
	})

	t.Run("generator helpers", func(t *testing.T) {
		db := mustParseDatabaseFromString(t, `
CREATE TABLE parent (
  code STRING(36) NOT NULL,
) PRIMARY KEY(code);
CREATE TABLE child (
  id INT64 NOT NULL,
  code STRING(36),
  tok TOKENLIST AS (TOKENIZE_FULLTEXT(code)) HIDDEN,
  CONSTRAINT FK_Parent FOREIGN KEY (code) REFERENCES parent (code),
) PRIMARY KEY(id);
CREATE INDEX idx_child_code ON child(code) STORING (tok);
CREATE SEARCH INDEX sidx_child_tok ON child(tok);
`)

		g := &generator{
			from:                   db,
			to:                     db,
			droppedConstraintBySQL: map[string]struct{}{},
			droppedIndexByKey:      map[string]struct{}{},
		}

		child := db.Tables["child"]
		if _, ok := g.findIdentByKey(child.Indexes[0].Raw.Storing.Columns, comparableIdentName("tok")); !ok {
			t.Fatalf("findIdentByKey() did not find storing column")
		}

		if got := g.findIndexesByColumn(child.Indexes, comparableIdentName("code")); len(got) != 1 {
			t.Fatalf("findIndexesByColumn() len = %d, want 1", len(got))
		}

		if got := g.findSearchIndexesByColumn(child.SearchIndexes, comparableIdentName("tok")); len(got) != 1 {
			t.Fatalf("findSearchIndexesByColumn() len = %d, want 1", len(got))
		}

		if _, ok := g.findNamedConstraint(child.Constraints, comparableIdentName("FK_Parent")); !ok {
			t.Fatalf("findNamedConstraint() did not find named constraint")
		}

		fromUnnamed := mustParseDatabaseFromString(t, `
CREATE TABLE p (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE c (
  id INT64 NOT NULL,
  pid INT64,
  FOREIGN KEY (pid) REFERENCES p (id),
) PRIMARY KEY(id);
`)
		toUnnamed := mustParseDatabaseFromString(t, `
CREATE TABLE p (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE c (
  id INT64 NOT NULL,
  pid INT64,
  FOREIGN KEY (pid) REFERENCES p (id) ON DELETE NO ACTION,
) PRIMARY KEY(id);
`)

		g2 := &generator{}
		if !g2.constraintEqual(fromUnnamed.Tables["c"].Constraints[0], toUnnamed.Tables["c"].Constraints[0]) {
			t.Fatalf("constraintEqual() should treat omitted and explicit NO ACTION as equal")
		}
		if _, ok := g2.findUnnamedConstraint(fromUnnamed.Tables["c"].Constraints, toUnnamed.Tables["c"].Constraints[0]); !ok {
			t.Fatalf("findUnnamedConstraint() did not match equivalent unnamed constraint")
		}

		drops := g.generateDropNamedConstraint(child.Raw.Name, child.Constraints[0])
		if len(drops) != 1 || drops[0].SQL != `ALTER TABLE child DROP CONSTRAINT FK_Parent` {
			t.Fatalf("generateDropNamedConstraint() = %#v", drops)
		}
		if len(g.generateDropNamedConstraint(child.Raw.Name, child.Constraints[0])) != 0 {
			t.Fatalf("generateDropNamedConstraint() duplicate should return no statements")
		}
		if !g.isDroppedConstraint(child.Constraints[0]) {
			t.Fatalf("isDroppedConstraint() = false, want true")
		}

		g.droppedIndexByKey[child.Indexes[0].Key] = struct{}{}
		if !g.isDroppedIndex(child.Indexes[0].Key) {
			t.Fatalf("isDroppedIndex() = false, want true")
		}

		arrayCol := &spanast.ColumnDef{
			Name:    &spanast.Ident{Name: "tags"},
			Type:    &spanast.ArraySchemaType{Item: &spanast.SizedSchemaType{Name: spanast.StringTypeName, Max: true}},
			NotNull: true,
		}
		if got := g.setDefaultSemantics(arrayCol).DefaultSemantics.SQL(); got != "DEFAULT (ARRAY[])" {
			t.Fatalf("setDefaultSemantics(array) = %q", got)
		}
	})

	t.Run("semantic helpers", func(t *testing.T) {
		dbA := mustParseDatabaseFromString(t, `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  name STRING(100) DEFAULT ("a"),
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(id), ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY));
CREATE CHANGE STREAM cs1 FOR T1(name);
CREATE SEARCH INDEX sidx ON t1(name);
`)
		dbB := mustParseDatabaseFromString(t, `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  name STRING(100) DEFAULT ("a"),
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(id), ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY));
CREATE CHANGE STREAM cs1 FOR T1(name);
CREATE SEARCH INDEX sidx ON t1(name);
`)
		dbC := mustParseDatabaseFromString(t, `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  name STRING(100) DEFAULT ("b"),
  created_at TIMESTAMP NOT NULL,
) PRIMARY KEY(id), ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 60 DAY));
CREATE CHANGE STREAM cs1 FOR T1(id);
CREATE SEARCH INDEX sidx ON t1(name) STORING (id);
`)

		g := &generator{}
		if !g.searchIndexEqual(dbA.Tables["t1"].SearchIndexes[0].Raw, dbB.Tables["t1"].SearchIndexes[0].Raw) {
			t.Fatalf("searchIndexEqual() = false, want true")
		}
		if g.searchIndexEqual(dbA.Tables["t1"].SearchIndexes[0].Raw, dbC.Tables["t1"].SearchIndexes[0].Raw) {
			t.Fatalf("searchIndexEqual() = true, want false")
		}
		if !g.changeStreamForEqual(dbA.ChangeStreams["cs1"].Raw.For, dbB.ChangeStreams["cs1"].Raw.For) {
			t.Fatalf("changeStreamForEqual() = false, want true")
		}
		if g.changeStreamForEqual(dbA.ChangeStreams["cs1"].Raw.For, dbC.ChangeStreams["cs1"].Raw.For) {
			t.Fatalf("changeStreamForEqual() = true, want false")
		}
		if !g.columnDefaultExprEqual(dbA.Tables["t1"].Raw.Columns[1].DefaultSemantics, dbB.Tables["t1"].Raw.Columns[1].DefaultSemantics) {
			t.Fatalf("columnDefaultExprEqual() = false, want true")
		}
		if g.columnDefaultExprEqual(dbA.Tables["t1"].Raw.Columns[1].DefaultSemantics, dbC.Tables["t1"].Raw.Columns[1].DefaultSemantics) {
			t.Fatalf("columnDefaultExprEqual() = true, want false")
		}
		if !g.rowDeletionPolicyEqual(dbA.Tables["t1"].RowDeletionPolicy, dbB.Tables["t1"].RowDeletionPolicy) {
			t.Fatalf("rowDeletionPolicyEqual() = false, want true")
		}
		if g.rowDeletionPolicyEqual(dbA.Tables["t1"].RowDeletionPolicy, dbC.Tables["t1"].RowDeletionPolicy) {
			t.Fatalf("rowDeletionPolicyEqual() = true, want false")
		}

		colWithOptions := mustParseDatabaseFromString(t, `CREATE TABLE t1 (id INT64 NOT NULL, ts TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY(id);`)
		colWithoutOptions := mustParseDatabaseFromString(t, `CREATE TABLE t1 (id INT64 NOT NULL, ts TIMESTAMP) PRIMARY KEY(id);`)
		if !g.optionValueEqual(colWithOptions.Tables["t1"].Raw.Columns[1].Options, colWithOptions.Tables["t1"].Raw.Columns[1].Options, "allow_commit_timestamp") {
			t.Fatalf("optionValueEqual() = false, want true")
		}
		if g.optionValueEqual(colWithOptions.Tables["t1"].Raw.Columns[1].Options, colWithoutOptions.Tables["t1"].Raw.Columns[1].Options, "allow_commit_timestamp") {
			t.Fatalf("optionValueEqual() = true, want false")
		}
	})

	t.Run("privilege helpers", func(t *testing.T) {
		db := mustParseDatabaseFromString(t, `CREATE ROLE role1; GRANT SELECT(col1), INSERT(col2), UPDATE(col3), DELETE ON TABLE T1 TO ROLE role1;`)
		privilege := db.grants[0].Raw.Privilege.(*spanast.PrivilegeOnTable)

		if !hasPrivilegeOnColumn(privilege, &spanast.Ident{Name: "col1"}) {
			t.Fatalf("hasPrivilegeOnColumn(select) = false, want true")
		}
		if !hasPrivilegeOnColumn(privilege, &spanast.Ident{Name: "col2"}) {
			t.Fatalf("hasPrivilegeOnColumn(insert) = false, want true")
		}
		if !hasPrivilegeOnColumn(privilege, &spanast.Ident{Name: "col3"}) {
			t.Fatalf("hasPrivilegeOnColumn(update) = false, want true")
		}
		if hasPrivilegeOnColumn(privilege, &spanast.Ident{Name: "missing"}) {
			t.Fatalf("hasPrivilegeOnColumn(missing) = true, want false")
		}
	})
}

func statementsToStrings(stmts []Statement) []string {
	out := make([]string, len(stmts))
	for i, stmt := range stmts {
		out[i] = stmt.SQL
	}
	return out
}
