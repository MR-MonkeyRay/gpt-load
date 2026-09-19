package migrations_test

import (
	"testing"

	"gpt-load/internal/storage/migrations"
)

// TestCredentialStateAutoRefreshMigrationAddsOptInAndUsedModels 保证升级加上操作者的
// 自动刷新选择（旧行默认关闭），并建立按 (凭据, 模型) 记录的「用过的模型」表：唯一键、
// 非空模型、随凭据级联删除，且可以重复执行。
func TestCredentialStateAutoRefreshMigrationAddsOptInAndUsedModels(t *testing.T) {
	t.Parallel()
	db := openInitialTestDatabase(t)
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO groups (id, name, channel_id, connection_type, params, models, enabled, created_at_ms, updated_at_ms)
		VALUES (1, 'group', 'codex', 'subscription', '{}', '[]', 1, 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO credentials (id, group_id, data, fingerprint, identity_fingerprint, status, created_at_ms, updated_at_ms)
		VALUES (1, 1, 'cipher', 'fingerprint', 'identity', 'active', 1, 1)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.Up0021(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Validate0021(db); err != nil {
		t.Fatal(err)
	}
	var enabled bool
	if err := db.Raw("SELECT state_auto_refresh FROM credentials WHERE id = 1").Scan(&enabled).Error; err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("existing credential opted into automatic state refresh")
	}
	insert := `INSERT INTO credential_used_models (credential_id, model, used_at_ms, created_at_ms, updated_at_ms)
		VALUES (1, ?, 5, 5, 5)`
	if err := db.Exec(insert, "gpt-5-codex").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(insert, "gpt-5-codex").Error; err == nil {
		t.Fatal("duplicate (credential, model) pair accepted")
	}
	if err := db.Exec(insert, "").Error; err == nil {
		t.Fatal("empty model accepted")
	}
	if err := db.Exec("DELETE FROM credentials WHERE id = 1").Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM credential_used_models").Scan(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("used models survived the credential: %d rows", count)
	}
	if err := migrations.Up0021(db); err != nil {
		t.Fatal(err)
	}
}

// TestCredentialStateAutoRefreshMigrationRepairsEmptyPartialTable 保证被中断的建表可以
// 补完（缺的索引与约束在复验前补齐），而带数据的残缺表会被拒绝，避免升级在已有数据上
// 静默重建。
func TestCredentialStateAutoRefreshMigrationRepairsEmptyPartialTable(t *testing.T) {
	t.Parallel()
	db := openInitialTestDatabase(t)
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	// 一次中断的建表：列都在，索引与检查约束还没建上。
	if err := db.Exec(`CREATE TABLE credential_used_models (
		credential_id integer NOT NULL, model varchar(255) NOT NULL,
		used_at_ms integer NOT NULL DEFAULT 0, created_at_ms integer NOT NULL DEFAULT 0,
		updated_at_ms integer NOT NULL DEFAULT 0)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO credential_used_models (credential_id, model) VALUES (1, 'gpt-5-codex')").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateRecoverable0021(db); err == nil {
		t.Fatal("incomplete used model table with rows accepted")
	}
	if err := db.Exec("DELETE FROM credential_used_models").Error; err != nil {
		t.Fatal(err)
	}
	if err := migrations.ValidateRecoverable0021(db); err != nil {
		t.Fatalf("empty partial used model table rejected: %v", err)
	}
	if err := migrations.Up0021(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Validate0021(db); err != nil {
		t.Fatal(err)
	}
}

// TestCredentialStateAutoRefreshMigrationRequiresCredentialsTable 保证升级前的复验会
// 拒绝缺表的库，并且缺列时不会被当成已完成。
func TestCredentialStateAutoRefreshMigrationRequiresCredentialsTable(t *testing.T) {
	t.Parallel()
	db := openInitialTestDatabase(t)
	if err := migrations.ValidateRecoverable0021(db); err == nil {
		t.Fatal("missing credentials table accepted")
	}
	if err := migrations.Up0001(db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Validate0021(db); err == nil {
		t.Fatal("missing automatic state refresh schema accepted")
	}
}
