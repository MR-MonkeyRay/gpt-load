package models

// CredentialStateRefreshStatus is the durable outcome of one manual turn-state
// refresh.
type CredentialStateRefreshStatus string

const (
	CredentialStateRefreshSucceeded CredentialStateRefreshStatus = "succeeded"
	CredentialStateRefreshFailed    CredentialStateRefreshStatus = "failed"
)

// CredentialStateRefreshSource distinguishes how one record captured its state:
// a manual refresh probes for it, while a natural capture observes the state an
// upstream returned on a real attempt. A natural capture belongs to no probe
// run, so it carries no probe ordinal.
type CredentialStateRefreshSource string

const (
	CredentialStateRefreshSourceRefresh CredentialStateRefreshSource = "refresh"
	CredentialStateRefreshSourceNatural CredentialStateRefreshSource = "natural"
)

// CredentialStateRefreshLog is the durable record of one turn-state capture. A
// successful row carries the captured state and the request that produced it; a
// failed row carries the classification of the last probe failure. Non-complete
// probe results are never retained as state, only as the observed length.
type CredentialStateRefreshLog struct {
	ID           uint                         `gorm:"primaryKey;autoIncrement"`
	GroupID      uint                         `gorm:"not null;check:chk_state_refresh_log_group,group_id > 0"`
	CredentialID uint                         `gorm:"not null;index:idx_state_refresh_log_credential,priority:1;check:chk_state_refresh_log_credential,credential_id > 0"`
	Status       CredentialStateRefreshStatus `gorm:"type:varchar(16);not null;check:chk_state_refresh_log_status,status IN ('succeeded','failed')"`
	ErrorCode    string                       `gorm:"type:varchar(32);not null;default:''"`
	TurnState    string                       `gorm:"type:varchar(512);not null;default:''"`
	StateLength  int                          `gorm:"not null;check:chk_state_refresh_log_length,state_length >= 0"`
	// Attempts 是这次探测在其刷新运行中的序号，从 1 开始；实时捕获不属于任何刷新
	// 运行，序号为 0，来源也由它判定。
	Attempts    int `gorm:"not null;check:chk_state_refresh_log_attempts,attempts >= 0"`
	HTTPStatus  *int
	Model       string `gorm:"type:varchar(255);not null;default:''"`
	Input       string `gorm:"type:varchar(64);not null;default:''"`
	ProxyURL    string `gorm:"type:varchar(255);not null;default:''"`
	BaseURL     string `gorm:"type:varchar(255);not null;default:''"`
	DurationMS  int64  `gorm:"not null;check:chk_state_refresh_log_duration,duration_ms >= 0"`
	CreatedAtMS int64  `gorm:"column:created_at_ms;not null;index:idx_state_refresh_log_credential,priority:2;index:idx_state_refresh_log_created;check:chk_state_refresh_log_created,created_at_ms >= 0"`
}

func (CredentialStateRefreshLog) TableName() string { return "credential_state_refresh_logs" }

// Source reports how this record captured its state. Only a probe belongs to a
// refresh run and has an ordinal.
func (row CredentialStateRefreshLog) Source() CredentialStateRefreshSource {
	if row.Attempts == 0 {
		return CredentialStateRefreshSourceNatural
	}
	return CredentialStateRefreshSourceRefresh
}
