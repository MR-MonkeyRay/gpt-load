package models

// CredentialStateRefreshStatus is the durable outcome of one manual turn-state
// refresh.
type CredentialStateRefreshStatus string

const (
	CredentialStateRefreshSucceeded CredentialStateRefreshStatus = "succeeded"
	CredentialStateRefreshFailed    CredentialStateRefreshStatus = "failed"
)

// CredentialStateRefreshLog is the durable record of one manual turn-state
// refresh. A successful row carries the captured state and the probe request
// that produced it; a failed row carries the classification of the last probe
// failure. Non-complete probe results are never retained as state, only as the
// observed length.
type CredentialStateRefreshLog struct {
	ID           uint                         `gorm:"primaryKey;autoIncrement"`
	GroupID      uint                         `gorm:"not null;check:chk_state_refresh_log_group,group_id > 0"`
	CredentialID uint                         `gorm:"not null;index:idx_state_refresh_log_credential,priority:1;check:chk_state_refresh_log_credential,credential_id > 0"`
	Status       CredentialStateRefreshStatus `gorm:"type:varchar(16);not null;check:chk_state_refresh_log_status,status IN ('succeeded','failed')"`
	ErrorCode    string                       `gorm:"type:varchar(32);not null;default:''"`
	TurnState    string                       `gorm:"type:varchar(512);not null;default:''"`
	StateLength  int                          `gorm:"not null;check:chk_state_refresh_log_length,state_length >= 0"`
	Attempts     int                          `gorm:"not null;check:chk_state_refresh_log_attempts,attempts >= 0"`
	HTTPStatus   *int
	Model        string `gorm:"type:varchar(255);not null;default:''"`
	Input        string `gorm:"type:varchar(64);not null;default:''"`
	ProxyURL     string `gorm:"type:varchar(255);not null;default:''"`
	BaseURL      string `gorm:"type:varchar(255);not null;default:''"`
	DurationMS   int64  `gorm:"not null;check:chk_state_refresh_log_duration,duration_ms >= 0"`
	CreatedAtMS  int64  `gorm:"column:created_at_ms;not null;index:idx_state_refresh_log_credential,priority:2;index:idx_state_refresh_log_created;check:chk_state_refresh_log_created,created_at_ms >= 0"`
}

func (CredentialStateRefreshLog) TableName() string { return "credential_state_refresh_logs" }
