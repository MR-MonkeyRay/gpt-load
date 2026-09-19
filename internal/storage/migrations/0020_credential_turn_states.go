package migrations

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const ID0020 = "0020_credential_turn_states"

// credentialTurnState0020 冻结按 (凭据, 模型) 保存 codex turn state 的表结构：同一
// 凭据的每个模型各占一行，捕获与重放都以模型为界。
type credentialTurnState0020 struct {
	CredentialID  uint               `gorm:"primaryKey;not null;check:chk_credential_turn_state_credential,credential_id > 0"`
	Model         string             `gorm:"primaryKey;type:varchar(255);not null;check:chk_credential_turn_state_model,model <> ''"`
	TurnState     string             `gorm:"type:varchar(512);not null;default:''"`
	RefreshedAtMS int64              `gorm:"column:refreshed_at_ms;not null;default:0;check:chk_credential_turn_state_refreshed,refreshed_at_ms >= 0"`
	CreatedAtMS   int64              `gorm:"column:created_at_ms;not null;autoCreateTime:milli;check:chk_credential_turn_state_created,created_at_ms >= 0"`
	UpdatedAtMS   int64              `gorm:"column:updated_at_ms;not null;autoUpdateTime:milli;check:chk_credential_turn_state_updated,updated_at_ms >= 0"`
	Credential    *initialCredential `gorm:"foreignKey:CredentialID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
}

func (credentialTurnState0020) TableName() string { return "credential_turn_states" }

// Up0020 把「每凭据单值 state」升级为「每 (凭据, 模型) 多值 state」：建立按模型保存
// 的捕获表，退休 credentials 上的两个单值列，并把刷新日志的 model 列加宽到分组模型
// ID 的上限。旧单值不回填：它属于哪个模型无法证明，宁可不复用也不跨模型复用。
func Up0020(db *gorm.DB) error {
	if err := ValidateRecoverable0020(db); err != nil {
		return err
	}
	if err := db.AutoMigrate(&credentialTurnState0020{}); err != nil {
		return fmt.Errorf("create credential turn state: %w", err)
	}
	if err := retireCredentialTurnStateColumns0020(db); err != nil {
		return err
	}
	if err := widenStateRefreshLogModel0020(db); err != nil {
		return err
	}
	return Validate0020(db)
}

// retireCredentialTurnStateColumns0020 删除 credentials 上的旧单值列。两列都没有
// 约束与索引，三种方言都能直接删列。
func retireCredentialTurnStateColumns0020(db *gorm.DB) error {
	for _, column := range []string{"turn_state", "turn_state_refreshed_at_ms"} {
		if !db.Migrator().HasColumn("credentials", column) {
			continue
		}
		if err := db.Exec("ALTER TABLE credentials DROP COLUMN " + column).Error; err != nil {
			return fmt.Errorf("drop credential column %s: %w", column, err)
		}
	}
	return nil
}

// widenStateRefreshLogModel0020 把刷新日志的 model 列加宽到 255：分组模型 ID 允许 255
// 字节，64 会让 MySQL 与 PostgreSQL 按声明长度拒收长模型 ID，从而静默终止刷新运行。
// SQLite 的列类型只是亲和性声明、不截断存储，因此无需重建。
func widenStateRefreshLogModel0020(db *gorm.DB) error {
	if !db.Migrator().HasTable("credential_state_refresh_logs") {
		return nil
	}
	var statement string
	switch strings.ToLower(db.Dialector.Name()) {
	case "mysql":
		statement = "ALTER TABLE credential_state_refresh_logs MODIFY COLUMN model VARCHAR(255) NOT NULL DEFAULT ''"
	case "postgres":
		statement = "ALTER TABLE credential_state_refresh_logs ALTER COLUMN model TYPE VARCHAR(255)"
	default:
		return nil
	}
	if err := db.Exec(statement).Error; err != nil {
		return fmt.Errorf("widen state refresh log model: %w", err)
	}
	return nil
}

// ValidateRecoverable0020 接受建表与删列分别完成的状态，以支持 MySQL 的 DDL 中断
// 恢复：残缺的捕获表只允许在为空时重建。
func ValidateRecoverable0020(db *gorm.DB) error {
	if !db.Migrator().HasTable("credentials") {
		return fmt.Errorf("credentials table is missing")
	}
	if !db.Migrator().HasTable("credential_state_refresh_logs") {
		return fmt.Errorf("credential state refresh log table is missing")
	}
	model := &credentialTurnState0020{}
	if !db.Migrator().HasTable(model) {
		return nil
	}
	if validateTable0020(db, model) == nil {
		return nil
	}
	var count int64
	if err := db.Model(model).Count(&count).Error; err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("incomplete credential turn state schema contains data")
	}
	return nil
}

// Validate0020 核对升级结果：捕获表结构正确，credentials 不再保留单值列，刷新日志的
// model 列已放宽。
func Validate0020(db *gorm.DB) error {
	if err := validateTable0020(db, &credentialTurnState0020{}); err != nil {
		return err
	}
	for _, column := range []string{"turn_state", "turn_state_refreshed_at_ms"} {
		if db.Migrator().HasColumn("credentials", column) {
			return fmt.Errorf("credential column %s is not retired", column)
		}
	}
	return validateStateRefreshLogModel0020(db)
}

// validateStateRefreshLogModel0020 核对刷新日志 model 列的类型、非空与声明长度。
// SQLite 不按声明长度截断，因此不参与长度核对。
func validateStateRefreshLogModel0020(db *gorm.DB) error {
	columns, err := db.Migrator().ColumnTypes("credential_state_refresh_logs")
	if err != nil {
		return err
	}
	enforcesLength := true
	switch strings.ToLower(db.Dialector.Name()) {
	case "mysql", "postgres":
	default:
		enforcesLength = false
	}
	for _, column := range columns {
		if column.Name() != "model" {
			continue
		}
		if !strings.Contains(strings.ToLower(column.DatabaseTypeName()), "char") {
			return fmt.Errorf("state refresh log model must be varchar")
		}
		if nullable, known := column.Nullable(); known && nullable {
			return fmt.Errorf("state refresh log model must be non-null")
		}
		if length, known := column.Length(); enforcesLength && known && length < 255 {
			return fmt.Errorf("state refresh log model length is %d", length)
		}
		return nil
	}
	return fmt.Errorf("state refresh log model is missing")
}

// validateTable0020 核对冻结表结构与实际表结构：字段齐全、类型与非空一致、没有多余
// 列，声明的索引与检查约束全部存在。
func validateTable0020(db *gorm.DB, model any) error {
	statement := &gorm.Statement{DB: db}
	if err := statement.Parse(model); err != nil {
		return err
	}
	table := statement.Table
	if !db.Migrator().HasTable(model) {
		return fmt.Errorf("%s table is missing", table)
	}
	columns, err := db.Migrator().ColumnTypes(model)
	if err != nil {
		return err
	}
	found := make(map[string]bool)
	for _, column := range columns {
		field := statement.Schema.LookUpField(column.Name())
		if field == nil {
			return fmt.Errorf("%s contains unexpected column %q", table, column.Name())
		}
		found[column.Name()] = true
		integer := field.DataType == "int" || field.DataType == "uint"
		if integer && !strings.Contains(strings.ToLower(column.DatabaseTypeName()), "int") {
			return fmt.Errorf("%s column %s must be integer", table, column.Name())
		}
		if field.NotNull {
			if nullable, known := column.Nullable(); known && nullable {
				return fmt.Errorf("%s column %s must be non-null", table, column.Name())
			}
		}
	}
	for _, field := range statement.Schema.Fields {
		if field.DBName == "" {
			continue
		}
		if !found[field.DBName] {
			return fmt.Errorf("%s column %s is missing", table, field.DBName)
		}
	}
	for _, index := range statement.Schema.ParseIndexes() {
		if !db.Migrator().HasIndex(model, index.Name) {
			return fmt.Errorf("%s index %s is missing", table, index.Name)
		}
	}
	for name := range statement.Schema.ParseCheckConstraints() {
		if !db.Migrator().HasConstraint(model, name) {
			return fmt.Errorf("%s constraint %s is missing", table, name)
		}
	}
	return nil
}
