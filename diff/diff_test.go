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

	"github.com/google/go-cmp/cmp"

	"github.com/zchee/spanner-manager/sqlutil"
)

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
		name string
		from string
		to   string
		want []Statement
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
					SQL:  `CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT 1`,
				},
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

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Diff() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDiff_ExtraBeyondHammer(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   string
		want []Statement
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
			name: "replace sequence by drop create",
			from: `
CREATE SEQUENCE Seq OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			to: `
CREATE SEQUENCE Seq OPTIONS (sequence_kind = 'skip_range_min_max');
`,
			want: []Statement{
				{
					Kind: sqlutil.KindDDL,
					SQL:  `DROP SEQUENCE Seq`,
				},
				{
					Kind: sqlutil.KindDDL,
					SQL:  `CREATE SEQUENCE Seq OPTIONS (sequence_kind = "skip_range_min_max")`,
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

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Diff() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
