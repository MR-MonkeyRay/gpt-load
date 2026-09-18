package migrations_test

import (
	"testing"

	"gpt-load/internal/storage/migrations"
)

func TestCredentialStateRefreshMigrationRequiresCredentialsTable(t *testing.T) {
	t.Parallel()
	db := openInitialTestDatabase(t)
	if err := migrations.ValidateRecoverable0019(db); err == nil {
		t.Fatal("missing credentials table accepted")
	}
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Validate0019(db); err == nil {
		t.Fatal("missing state refresh schema accepted")
	}
}

// TestCredentialStateRefreshMigrationResumesPartialDDL 覆盖 MySQL DDL 中断恢复：
// 只完成加列时仍可继续，完成后的两次执行保持幂等。
func TestCredentialStateRefreshMigrationResumesPartialDDL(t *testing.T) {
	t.Parallel()
	db := openInitialTestDatabase(t)
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(
		"ALTER TABLE credentials ADD COLUMN turn_state_refreshed_at_ms BIGINT NOT NULL DEFAULT 0",
	).Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateRecoverable0019(db); err != nil {
		t.Fatalf("column-only state rejected: %v", err)
	}
	if err := migrations.Validate0019(db); err == nil {
		t.Fatal("column-only state accepted by full validation")
	}
	for range 2 {
		if err := migrations.Up0019(db); err != nil {
			t.Fatal(err)
		}
		if err := migrations.Validate0019(db); err != nil {
			t.Fatal(err)
		}
	}
	if err := migrations.ValidateRecoverable0019(db); err != nil {
		t.Fatal(err)
	}
}

// TestCredentialStateRefreshMigrationRebuildsEmptyPartialTable 保证残缺但空白的
// 日志表可以重建，而带数据的残缺表会被拒绝，避免静默丢日志。
func TestCredentialStateRefreshMigrationRebuildsEmptyPartialTable(t *testing.T) {
	t.Parallel()
	db := openInitialTestDatabase(t)
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(
		"CREATE TABLE credential_state_refresh_logs (id integer PRIMARY KEY AUTOINCREMENT)",
	).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO credential_state_refresh_logs (id) VALUES (1)").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateRecoverable0019(db); err == nil {
		t.Fatal("incomplete log table with rows accepted")
	}
	if err := db.Exec("DELETE FROM credential_state_refresh_logs").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateRecoverable0019(db); err != nil {
		t.Fatalf("empty partial log table rejected: %v", err)
	}
	if err := migrations.Up0019(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Validate0019(db); err != nil {
		t.Fatal(err)
	}
}
