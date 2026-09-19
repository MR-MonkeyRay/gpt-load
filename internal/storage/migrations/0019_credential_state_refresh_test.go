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

// TestCredentialStateRefreshMigrationRepairsEmptyPartialTable 保证残缺但空白的日志表
// 可以重建，而带数据的残缺表会被拒绝，避免静默丢日志。
func TestCredentialStateRefreshMigrationRepairsEmptyPartialTable(t *testing.T) {
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

// TestCredentialTurnStateIsScopedToCredentialAndModel 保证按模型保存的捕获表可建、
// 以 (credential_id, model) 为唯一键，并随凭据级联删除；0019 留下的单值列由同一次
// 升级退休。
func TestCredentialTurnStateIsScopedToCredentialAndModel(t *testing.T) {
	t.Parallel()
	db := openInitialTestDatabase(t)
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up0019(db); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasColumn("credentials", "turn_state_refreshed_at_ms") {
		t.Fatal("0019 did not add the single-value state column")
	}
	if err := migrations.Up0020(db); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"turn_state", "turn_state_refreshed_at_ms"} {
		if db.Migrator().HasColumn("credentials", column) {
			t.Fatalf("credentials column %s was created", column)
		}
	}
	if err := db.Exec(`INSERT INTO groups (id, name, channel_id, connection_type, params, models, enabled, created_at_ms, updated_at_ms)
		VALUES (1, 'group', 'openai', 'subscription', '{}', '[]', 1, 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO credentials (id, group_id, data, fingerprint, identity_fingerprint, status, created_at_ms, updated_at_ms)
		VALUES (1, 1, 'cipher', 'fingerprint', 'identity', 'active', 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO credential_turn_states (credential_id, model, turn_state, refreshed_at_ms, created_at_ms, updated_at_ms)
		VALUES (1, ?, 'state', 5, 5, 5)`
	if err := db.Exec(insert, "gpt-6-astra").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(insert, "gpt-5-codex").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(insert, "gpt-6-astra").Error; err == nil {
		t.Fatal("duplicate (credential, model) pair accepted")
	}
	if err := db.Exec("DELETE FROM credentials WHERE id = 1").Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM credential_turn_states").Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("captures survived the credential: %d rows", count)
	}
}
