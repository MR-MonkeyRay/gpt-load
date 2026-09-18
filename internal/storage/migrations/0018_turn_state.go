package migrations

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const ID0018 = "0018_turn_state"

// Up0018 原子加列保存请求侧注入的 codex turn state，旧记录保留空值。
func Up0018(db *gorm.DB) error {
	if err := ValidateRecoverable0018(db); err != nil {
		return err
	}
	if !db.Migrator().HasColumn("credentials", "turn_state") {
		if err := db.Exec("ALTER TABLE credentials ADD COLUMN turn_state VARCHAR(512) NOT NULL DEFAULT ''").Error; err != nil {
			return fmt.Errorf("add credential turn state: %w", err)
		}
	}
	if !db.Migrator().HasColumn("request_logs", "turn_state") {
		if err := db.Exec("ALTER TABLE request_logs ADD COLUMN turn_state VARCHAR(512) NOT NULL DEFAULT ''").Error; err != nil {
			return fmt.Errorf("add request log turn state: %w", err)
		}
	}
	return Validate0018(db)
}

// ValidateRecoverable0018 接受原子加列前后的状态，以支持 MySQL 的 DDL 中断恢复。
func ValidateRecoverable0018(db *gorm.DB) error {
	if !db.Migrator().HasTable("credentials") {
		return fmt.Errorf("credentials table is missing")
	}
	if !db.Migrator().HasTable("request_logs") {
		return fmt.Errorf("request_logs table is missing")
	}
	if db.Migrator().HasColumn("credentials", "turn_state") && db.Migrator().HasColumn("request_logs", "turn_state") {
		return Validate0018(db)
	}
	return nil
}

func Validate0018(db *gorm.DB) error {
	if err := validateTurnStateColumn0018(db, "credentials"); err != nil {
		return err
	}
	return validateTurnStateColumn0018(db, "request_logs")
}

func validateTurnStateColumn0018(db *gorm.DB, table string) error {
	columns, err := db.Migrator().ColumnTypes(table)
	if err != nil {
		return err
	}
	for _, column := range columns {
		if column.Name() != "turn_state" {
			continue
		}
		if !strings.Contains(strings.ToLower(column.DatabaseTypeName()), "char") {
			return fmt.Errorf("%s turn state must be varchar", table)
		}
		if nullable, known := column.Nullable(); !known || nullable {
			return fmt.Errorf("%s turn state must be non-null", table)
		}
		if length, known := column.Length(); known && length != 512 {
			return fmt.Errorf("%s turn state length must be 512", table)
		}
		value, known := column.DefaultValue()
		if !known || (value != "" && value != "''" && value != "''::character varying") {
			return fmt.Errorf("%s turn state must default to empty", table)
		}
		return nil
	}
	return fmt.Errorf("%s turn state is missing", table)
}
