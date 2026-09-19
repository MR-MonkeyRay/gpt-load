package storage

import (
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"

	migrationfiles "gpt-load/internal/storage/migrations"
)

func TestMigrationRegistryContainsOrderedMigrations(t *testing.T) {
	wantIDs := []string{
		migrationfiles.ID0001,
		migrationfiles.ID0002,
		migrationfiles.ID0003,
		migrationfiles.ID0004,
		migrationfiles.ID0005,
		migrationfiles.ID0006,
		migrationfiles.ID0007,
		migrationfiles.ID0008,
		migrationfiles.ID0009,
		migrationfiles.ID0010,
		migrationfiles.ID0011,
		migrationfiles.ID0012,
		migrationfiles.ID0013,
		migrationfiles.ID0014,
		migrationfiles.ID0015,
		migrationfiles.ID0016,
		migrationfiles.ID0017,
		migrationfiles.ID0018,
		migrationfiles.ID0019,
		migrationfiles.ID0020,
		migrationfiles.ID0021,
	}
	if len(migrations) != len(wantIDs) {
		t.Fatalf("migration registry length = %d, want %d", len(migrations), len(wantIDs))
	}
	for index, entry := range migrations {
		if entry.ID != wantIDs[index] || entry.Up == nil ||
			entry.Validate == nil || entry.ValidateRecoverable == nil {
			t.Fatalf("migration registry entry %d = %#v", index, entry)
		}
	}
}

func TestMigrationRegistryUsesOneOrderedChainForFreshAndExistingDatabases(t *testing.T) {
	entries, calls := testMigrationRegistry()

	fresh := openInternalMigrationTestDatabase(t)
	if err := applyMigrationRegistry(fresh, entries); err != nil {
		t.Fatalf("migrate fresh database: %v", err)
	}
	if !reflect.DeepEqual(*calls, []string{"0001_test", "0002_test"}) {
		t.Fatalf("fresh migration calls = %v, want [0001_test 0002_test]", *calls)
	}

	existing := openInternalMigrationTestDatabase(t)
	if err := existing.AutoMigrate(&schemaMigration{}); err != nil {
		t.Fatalf("create existing migration ledger: %v", err)
	}
	if err := existing.Create(&schemaMigration{ID: entries[0].ID}).Error; err != nil {
		t.Fatalf("record existing migration: %v", err)
	}
	*calls = nil
	if err := applyMigrationRegistry(existing, entries); err != nil {
		t.Fatalf("migrate existing database: %v", err)
	}
	if !reflect.DeepEqual(*calls, []string{"0002_test"}) {
		t.Fatalf("existing migration calls = %v, want [0002_test]", *calls)
	}
}

func TestApplyMigrationRegistryRejectsOutOfOrderEntries(t *testing.T) {
	entries, _ := testMigrationRegistry()
	entries[0], entries[1] = entries[1], entries[0]

	err := applyMigrationRegistry(openInternalMigrationTestDatabase(t), entries)
	if err == nil || !strings.Contains(err.Error(), "migration registry entry 1") {
		t.Fatalf("applyMigrationRegistry() error = %v, want out-of-order registry rejection", err)
	}
}

// TestAutoMigrateUpgradesShippedStateRefreshSchema 保证停在 0019 的库（单值 state 列 +
// 刷新日志表，没有按模型捕获表）能直接升级到最新：0020 补齐捕获表并退休单值列，0021
// 补上自动刷新选择与用过的模型表，已有数据不动。
func TestAutoMigrateUpgradesShippedStateRefreshSchema(t *testing.T) {
	t.Parallel()
	db := openInternalMigrationTestDatabase(t)
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("first AutoMigrate() error = %v", err)
	}
	// 退回 0019 发布后的形态：删掉 0020、0021 的账本行与它们建的表，恢复两个单值列。
	for _, statement := range []string{
		"DROP TABLE credential_turn_states",
		"DELETE FROM schema_migrations WHERE id = '0020_credential_turn_states'",
		"DROP TABLE credential_used_models",
		"DELETE FROM schema_migrations WHERE id = '0021_credential_state_auto_refresh'",
		"ALTER TABLE credentials DROP COLUMN state_auto_refresh",
		"ALTER TABLE credentials ADD COLUMN turn_state VARCHAR(512) NOT NULL DEFAULT ''",
		"ALTER TABLE credentials ADD COLUMN turn_state_refreshed_at_ms BIGINT NOT NULL DEFAULT 0",
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("rewind to the shipped 0019 schema: %v", err)
		}
	}
	if err := db.Exec(`INSERT INTO groups (id, name, channel_id, connection_type, params, models, enabled, created_at_ms, updated_at_ms)
		VALUES (1, 'group', 'codex', 'subscription', '{}', '[]', 1, 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO credentials (id, group_id, data, fingerprint, identity_fingerprint, status, turn_state, created_at_ms, updated_at_ms)
		VALUES (1, 1, 'cipher', 'fingerprint', 'identity', 'active', 'legacy-state', 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}

	if err := AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate() over the shipped schema error = %v", err)
	}
	if !db.Migrator().HasTable("credential_turn_states") {
		t.Fatal("per-model capture table is missing after the upgrade")
	}
	if !db.Migrator().HasTable("credential_used_models") ||
		!db.Migrator().HasColumn("credentials", "state_auto_refresh") {
		t.Fatal("automatic state refresh schema is missing after the upgrade")
	}
	for _, column := range []string{"turn_state", "turn_state_refreshed_at_ms"} {
		if db.Migrator().HasColumn("credentials", column) {
			t.Fatalf("credentials column %s survived the upgrade", column)
		}
	}
	var state string
	if err := db.Raw("SELECT data FROM credentials WHERE id = 1").Scan(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state != "cipher" {
		t.Fatalf("credential data after the upgrade = %q", state)
	}
	var last string
	if err := db.Table("schema_migrations").Order("id DESC").Limit(1).Pluck("id", &last).Error; err != nil {
		t.Fatal(err)
	}
	if last != migrationfiles.ID0021 {
		t.Fatalf("last migration after the upgrade = %q", last)
	}
	// 升级后的库在下次启动时会重新复验已应用的 0019，而单值列已经不在。
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate() over the upgraded schema error = %v", err)
	}
}

func testMigrationRegistry() ([]migration, *[]string) {
	calls := make([]string, 0, 2)
	entry := func(id string) migration {
		return migration{
			ID: id,
			Up: func(*gorm.DB) error {
				calls = append(calls, id)
				return nil
			},
			Validate:            func(*gorm.DB) error { return nil },
			ValidateRecoverable: func(*gorm.DB) error { return nil },
		}
	}
	return []migration{entry("0001_test"), entry("0002_test")}, &calls
}
