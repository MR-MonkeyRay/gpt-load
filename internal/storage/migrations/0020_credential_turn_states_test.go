package migrations_test

import (
	"testing"

	"gorm.io/gorm"

	"gpt-load/internal/storage/migrations"
)

// openShippedStateRefreshDatabase 复现「0018 与 0019 已应用」的库：credentials 上的
// 两个单值 state 列与刷新日志表都在，按模型保存的捕获表还不存在。0018 发布时加的是
// credentials.turn_state，本次改动把这一列交给 0020 退休，所以这里手工补上。
func openShippedStateRefreshDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	db := openInitialTestDatabase(t)
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ALTER TABLE credentials ADD COLUMN turn_state VARCHAR(512) NOT NULL DEFAULT ''").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up0018(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up0019(db); err != nil {
		t.Fatal(err)
	}
	if db.Migrator().HasTable("credential_turn_states") {
		t.Fatal("shipped 0019 unexpectedly created the per-model capture table")
	}
	return db
}

// TestAppliedStateRefreshMigrationAcceptsShippedSchema 保证 0019 已应用但仍是发布时
// 形态的库能通过复验，否则旧库在升级前就无法启动。
func TestAppliedStateRefreshMigrationAcceptsShippedSchema(t *testing.T) {
	t.Parallel()
	db := openShippedStateRefreshDatabase(t)
	if err := migrations.ValidateCurrent0019(db); err != nil {
		t.Fatalf("shipped schema rejected: %v", err)
	}
	if err := migrations.Validate0019(db); err != nil {
		t.Fatalf("shipped schema rejected by its own validation: %v", err)
	}
}

// TestCredentialTurnStateMigrationNormalizesShippedSchema 保证升级把发布时的单值结构
// 归一化成按模型保存的结构：捕获表补齐、单值列退休、已有日志保留，并且可以重复执行。
func TestCredentialTurnStateMigrationNormalizesShippedSchema(t *testing.T) {
	t.Parallel()
	db := openShippedStateRefreshDatabase(t)
	if err := db.Exec(`INSERT INTO groups (id, name, channel_id, connection_type, params, models, enabled, created_at_ms, updated_at_ms)
		VALUES (1, 'group', 'codex', 'subscription', '{}', '[]', 1, 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO credentials (id, group_id, data, fingerprint, identity_fingerprint, status, turn_state, turn_state_refreshed_at_ms, created_at_ms, updated_at_ms)
		VALUES (1, 1, 'cipher', 'fingerprint', 'identity', 'active', 'legacy-state', 5, 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO credential_state_refresh_logs
		(group_id, credential_id, status, error_code, turn_state, state_length, attempts, model, input, proxy_url, base_url, duration_ms, created_at_ms)
		VALUES (1, 1, 'succeeded', '', 'captured', 292, 1, 'gpt-6-astra', 'ping', 'direct', '', 812, 7)`).Error; err != nil {
		t.Fatal(err)
	}

	if err := migrations.Up0020(db); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"turn_state", "turn_state_refreshed_at_ms"} {
		if db.Migrator().HasColumn("credentials", column) {
			t.Fatalf("credentials column %s survived the upgrade", column)
		}
	}
	if !db.Migrator().HasTable("credential_turn_states") {
		t.Fatal("per-model capture table is missing after the upgrade")
	}
	var logs int64
	if err := db.Table("credential_state_refresh_logs").Count(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if logs != 1 {
		t.Fatalf("refresh logs after the upgrade = %d, want 1", logs)
	}
	if err := migrations.Validate0020(db); err != nil {
		t.Fatalf("upgraded schema rejected: %v", err)
	}
	// 升级之后再启动时，0019 的复验必须仍然接受已经退休单值列的库。
	if err := migrations.ValidateCurrent0019(db); err != nil {
		t.Fatalf("upgraded schema rejected by applied migration validation: %v", err)
	}
	if err := migrations.Up0020(db); err != nil {
		t.Fatalf("repeated upgrade error = %v", err)
	}
}

// TestCredentialTurnStateMigrationRejectsIncompleteCaptureTable 保证残缺但带数据的捕获
// 表会被拒绝，避免升级在已有数据上静默重建。
func TestCredentialTurnStateMigrationRejectsIncompleteCaptureTable(t *testing.T) {
	t.Parallel()
	db := openShippedStateRefreshDatabase(t)
	if err := db.Exec("CREATE TABLE credential_turn_states (credential_id integer NOT NULL, model varchar(255) NOT NULL)").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO credential_turn_states (credential_id, model) VALUES (1, 'gpt-6-astra')").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateRecoverable0020(db); err == nil {
		t.Fatal("incomplete capture table with rows accepted")
	}
	if err := db.Exec("DELETE FROM credential_turn_states").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateRecoverable0020(db); err != nil {
		t.Fatalf("empty incomplete capture table rejected: %v", err)
	}
	if err := migrations.Up0020(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Validate0020(db); err != nil {
		t.Fatal(err)
	}
}
