package models

// CredentialUsedModel records one upstream model a credential has actually
// served, together with the instant of its last observed use. Automatic turn
// state refresh keeps the state of exactly these models valid, and only while
// the model keeps being used: a model with no request during its state's
// validity window stops being refreshed until it is requested again.
type CredentialUsedModel struct {
	CredentialID uint   `gorm:"primaryKey;not null;uniqueIndex:idx_credential_used_model,priority:1;check:chk_credential_used_model_credential,credential_id > 0"`
	Model        string `gorm:"primaryKey;type:varchar(255);not null;uniqueIndex:idx_credential_used_model,priority:2;check:chk_credential_used_model_model,model <> ''"`
	// UsedAtMS 是最近一次真实请求使用该 (凭据, 模型) 的时刻。
	UsedAtMS    int64       `gorm:"column:used_at_ms;not null;default:0;check:chk_credential_used_model_used_at,used_at_ms >= 0"`
	CreatedAtMS int64       `gorm:"column:created_at_ms;not null;autoCreateTime:milli;check:chk_credential_used_model_created,created_at_ms >= 0"`
	UpdatedAtMS int64       `gorm:"column:updated_at_ms;not null;autoUpdateTime:milli;check:chk_credential_used_model_updated,updated_at_ms >= 0"`
	Credential  *Credential `gorm:"foreignKey:CredentialID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
}

func (CredentialUsedModel) TableName() string { return "credential_used_models" }
