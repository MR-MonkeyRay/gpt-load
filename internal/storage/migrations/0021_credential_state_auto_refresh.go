package migrations

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const ID0021 = "0021_credential_state_auto_refresh"

// credentialUsedModel0021 冻结「该凭据用过这个上游模型」的记录表结构：一行一个
// (凭据, 模型)，自动 State 刷新只维护这张表里的模型。
type credentialUsedModel0021 struct {
	CredentialID uint               `gorm:"primaryKey;not null;uniqueIndex:idx_credential_used_model,priority:1;check:chk_credential_used_model_credential,credential_id > 0"`
	Model        string             `gorm:"primaryKey;type:varchar(255);not null;uniqueIndex:idx_credential_used_model,priority:2;check:chk_credential_used_model_model,model <> ''"`
	UsedAtMS     int64              `gorm:"column:used_at_ms;not null;default:0;check:chk_credential_used_model_used_at,used_at_ms >= 0"`
	CreatedAtMS  int64              `gorm:"column:created_at_ms;not null;autoCreateTime:milli;check:chk_credential_used_model_created,created_at_ms >= 0"`
	UpdatedAtMS  int64              `gorm:"column:updated_at_ms;not null;autoUpdateTime:milli;check:chk_credential_used_model_updated,updated_at_ms >= 0"`
	Credential   *initialCredential `gorm:"foreignKey:CredentialID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
}

func (credentialUsedModel0021) TableName() string { return "credential_used_models" }

// Up0021 记录操作者的自动 State 刷新选择，并建立「用过的模型」记录表。开启后控制面
// 只为该凭据真实请求过的模型保持 State 有效，因此用过的模型必须独立于捕获值落库。
func Up0021(db *gorm.DB) error {
	if err := ValidateRecoverable0021(db); err != nil {
		return err
	}
	if !db.Migrator().HasColumn("credentials", "state_auto_refresh") {
		if err := db.Exec("ALTER TABLE credentials ADD COLUMN state_auto_refresh BOOLEAN NOT NULL DEFAULT FALSE").Error; err != nil {
			return fmt.Errorf("add credential state auto refresh: %w", err)
		}
	}
	if err := db.AutoMigrate(&credentialUsedModel0021{}); err != nil {
		return fmt.Errorf("create credential used model: %w", err)
	}
	return Validate0021(db)
}

// ValidateRecoverable0021 接受加列、建表分别完成的状态，以支持 MySQL 的 DDL 中断恢复：
// 残缺的记录表只允许在为空时重建。
func ValidateRecoverable0021(db *gorm.DB) error {
	if !db.Migrator().HasTable("credentials") {
		return fmt.Errorf("credentials table is missing")
	}
	if db.Migrator().HasColumn("credentials", "state_auto_refresh") {
		if err := validateStateAutoRefreshColumn0021(db); err != nil {
			return err
		}
	}
	model := &credentialUsedModel0021{}
	if !db.Migrator().HasTable(model) {
		return nil
	}
	if validateCredentialUsedModel0021(db) == nil {
		return nil
	}
	var count int64
	if err := db.Model(model).Count(&count).Error; err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("incomplete credential used model schema contains data")
	}
	return nil
}

// Validate0021 核对升级结果：选择列存在且非空，记录表结构正确。
func Validate0021(db *gorm.DB) error {
	if err := validateStateAutoRefreshColumn0021(db); err != nil {
		return err
	}
	return validateCredentialUsedModel0021(db)
}

// validateStateAutoRefreshColumn0021 核对选择列的类型与非空：布尔列在三种方言里都
// 落在整数亲和性上。
func validateStateAutoRefreshColumn0021(db *gorm.DB) error {
	columns, err := db.Migrator().ColumnTypes("credentials")
	if err != nil {
		return err
	}
	for _, column := range columns {
		if column.Name() != "state_auto_refresh" {
			continue
		}
		if !strings.Contains(strings.ToLower(column.DatabaseTypeName()), "int") &&
			!strings.Contains(strings.ToLower(column.DatabaseTypeName()), "bool") {
			return fmt.Errorf("credential state auto refresh must be boolean")
		}
		if nullable, known := column.Nullable(); !known || nullable {
			return fmt.Errorf("credential state auto refresh must be non-null")
		}
		return nil
	}
	return fmt.Errorf("credential state auto refresh is missing")
}

// validateCredentialUsedModel0021 核对冻结表结构与实际表结构：字段齐全、类型与非空
// 一致、没有多余列，声明的索引与检查约束全部存在。
func validateCredentialUsedModel0021(db *gorm.DB) error {
	model := &credentialUsedModel0021{}
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
