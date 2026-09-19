package models

// CredentialTurnState is the captured codex turn state of one credential for
// one upstream model. Turn state is bound to an account and a model: a
// credential keeps one row per model it has captured a state for, and a capture
// is replayed only for that model.
type CredentialTurnState struct {
	CredentialID uint   `gorm:"primaryKey;not null;check:chk_credential_turn_state_credential,credential_id > 0"`
	Model        string `gorm:"primaryKey;type:varchar(255);not null;check:chk_credential_turn_state_model,model <> ''"`
	TurnState    string `gorm:"type:varchar(512);not null;default:''"`
	// RefreshedAtMS is the instant TurnState was written; it moves with the value.
	RefreshedAtMS int64       `gorm:"column:refreshed_at_ms;not null;default:0;check:chk_credential_turn_state_refreshed,refreshed_at_ms >= 0"`
	CreatedAtMS   int64       `gorm:"column:created_at_ms;not null;autoCreateTime:milli;check:chk_credential_turn_state_created,created_at_ms >= 0"`
	UpdatedAtMS   int64       `gorm:"column:updated_at_ms;not null;autoUpdateTime:milli;check:chk_credential_turn_state_updated,updated_at_ms >= 0"`
	Credential    *Credential `gorm:"foreignKey:CredentialID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
}

func (CredentialTurnState) TableName() string { return "credential_turn_states" }
