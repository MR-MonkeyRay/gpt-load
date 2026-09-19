package migrations

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const ID0019 = "0019_credential_state_refresh"

// stateRefreshLog0019 冻结本次新增的刷新日志表结构。
type stateRefreshLog0019 struct {
	ID           uint   `gorm:"primaryKey;autoIncrement"`
	GroupID      uint   `gorm:"not null;check:chk_state_refresh_log_group,group_id > 0"`
	CredentialID uint   `gorm:"not null;index:idx_state_refresh_log_credential,priority:1;check:chk_state_refresh_log_credential,credential_id > 0"`
	Status       string `gorm:"type:varchar(16);not null;check:chk_state_refresh_log_status,status IN ('succeeded','failed')"`
	ErrorCode    string `gorm:"type:varchar(32);not null;default:''"`
	TurnState    string `gorm:"type:varchar(512);not null;default:''"`
	StateLength  int    `gorm:"not null;check:chk_state_refresh_log_length,state_length >= 0"`
	Attempts     int    `gorm:"not null;check:chk_state_refresh_log_attempts,attempts >= 0"`
	HTTPStatus   *int
	Model        string `gorm:"type:varchar(64);not null;default:''"`
	Input        string `gorm:"type:varchar(64);not null;default:''"`
	ProxyURL     string `gorm:"type:varchar(255);not null;default:''"`
	BaseURL      string `gorm:"type:varchar(255);not null;default:''"`
	DurationMS   int64  `gorm:"not null;check:chk_state_refresh_log_duration,duration_ms >= 0"`
	CreatedAtMS  int64  `gorm:"column:created_at_ms;not null;index:idx_state_refresh_log_credential,priority:2;index:idx_state_refresh_log_created;check:chk_state_refresh_log_created,created_at_ms >= 0"`
}

func (stateRefreshLog0019) TableName() string { return "credential_state_refresh_logs" }

// Up0019 原子加列记录 turn state 的写入时刻，并建立逐次刷新结果日志表。
func Up0019(db *gorm.DB) error {
	if err := ValidateRecoverable0019(db); err != nil {
		return err
	}
	if !db.Migrator().HasColumn("credentials", "turn_state_refreshed_at_ms") {
		if err := db.Exec("ALTER TABLE credentials ADD COLUMN turn_state_refreshed_at_ms BIGINT NOT NULL DEFAULT 0").Error; err != nil {
			return fmt.Errorf("add credential turn state refreshed at: %w", err)
		}
	}
	if err := db.AutoMigrate(&stateRefreshLog0019{}); err != nil {
		return fmt.Errorf("create credential state refresh log: %w", err)
	}
	return Validate0019(db)
}

// ValidateRecoverable0019 接受加列、建表分别完成的状态，以支持 MySQL 的 DDL 中断恢复。
func ValidateRecoverable0019(db *gorm.DB) error {
	if !db.Migrator().HasTable("credentials") {
		return fmt.Errorf("credentials table is missing")
	}
	if db.Migrator().HasColumn("credentials", "turn_state_refreshed_at_ms") {
		if err := validateTurnStateRefreshedAt0019(db); err != nil {
			return err
		}
	}
	if !db.Migrator().HasTable(&stateRefreshLog0019{}) {
		return nil
	}
	if Validate0019(db) == nil {
		return nil
	}
	var count int64
	if err := db.Model(&stateRefreshLog0019{}).Count(&count).Error; err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("incomplete state refresh log schema contains data")
	}
	statement := &gorm.Statement{DB: db}
	if err := statement.Parse(&stateRefreshLog0019{}); err != nil {
		return err
	}
	columns, err := db.Migrator().ColumnTypes(&stateRefreshLog0019{})
	if err != nil {
		return err
	}
	for _, column := range columns {
		if statement.Schema.LookUpField(column.Name()) == nil {
			return fmt.Errorf("state refresh log contains unexpected column %q", column.Name())
		}
	}
	return nil
}

func Validate0019(db *gorm.DB) error {
	if err := validateTurnStateRefreshedAt0019(db); err != nil {
		return err
	}
	return validateStateRefreshLog0019(db)
}

// ValidateCurrent0019 复验已应用的 0019。0019 发布时加了单值列与刷新日志表，0020
// 随后退休了那一列，因此列还在时按发布时的形态核对，列已退休时只核对日志表。
func ValidateCurrent0019(db *gorm.DB) error {
	if db.Migrator().HasColumn("credentials", "turn_state_refreshed_at_ms") {
		return Validate0019(db)
	}
	return validateStateRefreshLog0019(db)
}

func validateStateRefreshLog0019(db *gorm.DB) error {
	model := &stateRefreshLog0019{}
	if !db.Migrator().HasTable(model) {
		return fmt.Errorf("credential state refresh log table is missing")
	}
	statement := &gorm.Statement{DB: db}
	if err := statement.Parse(model); err != nil {
		return err
	}
	columns, err := db.Migrator().ColumnTypes(model)
	if err != nil {
		return err
	}
	found := make(map[string]bool)
	for _, column := range columns {
		field := statement.Schema.LookUpField(column.Name())
		if field == nil {
			return fmt.Errorf("state refresh log contains unexpected column %q", column.Name())
		}
		found[column.Name()] = true
		integer := field.DataType == "int" || field.DataType == "uint"
		if integer && !strings.Contains(strings.ToLower(column.DatabaseTypeName()), "int") {
			return fmt.Errorf("state refresh log column %s must be integer", column.Name())
		}
		if field.NotNull {
			if nullable, known := column.Nullable(); known && nullable {
				return fmt.Errorf("state refresh log column %s must be non-null", column.Name())
			}
		}
	}
	for _, field := range statement.Schema.Fields {
		if !found[field.DBName] {
			return fmt.Errorf("state refresh log column %s is missing", field.DBName)
		}
	}
	for _, index := range statement.Schema.ParseIndexes() {
		if !db.Migrator().HasIndex(model, index.Name) {
			return fmt.Errorf("state refresh log index %s is missing", index.Name)
		}
	}
	for name := range statement.Schema.ParseCheckConstraints() {
		if !db.Migrator().HasConstraint(model, name) {
			return fmt.Errorf("state refresh log constraint %s is missing", name)
		}
	}
	return nil
}

func validateTurnStateRefreshedAt0019(db *gorm.DB) error {
	columns, err := db.Migrator().ColumnTypes("credentials")
	if err != nil {
		return err
	}
	for _, column := range columns {
		if column.Name() != "turn_state_refreshed_at_ms" {
			continue
		}
		if !strings.Contains(strings.ToLower(column.DatabaseTypeName()), "int") {
			return fmt.Errorf("credential turn state refreshed at must be integer")
		}
		if nullable, known := column.Nullable(); !known || nullable {
			return fmt.Errorf("credential turn state refreshed at must be non-null")
		}
		return nil
	}
	return fmt.Errorf("credential turn state refreshed at is missing")
}
