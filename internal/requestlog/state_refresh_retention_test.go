package requestlog

import (
	"testing"
	"time"

	"gpt-load/internal/storage/models"
)

func TestStateRefreshLogRetentionKeepsRecentRecords(t *testing.T) {
	db := openRequestLogQueryDB(t)
	service := newRequestLogTestService(db)
	now := time.Date(2026, 9, 17, 12, 34, 0, 0, time.UTC)
	cutoff := now.Add(-35 * 24 * time.Hour).UnixMilli()
	for _, at := range []int64{cutoff - 1, cutoff, now.Add(-10 * 24 * time.Hour).UnixMilli()} {
		row := models.CredentialStateRefreshLog{
			GroupID: 1, CredentialID: 1, Status: models.CredentialStateRefreshSucceeded,
			TurnState: "state", StateLength: 5, Attempts: 1, CreatedAtMS: at,
		}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	service.Sweep(t.Context(), now)
	var rows []models.CredentialStateRefreshLog
	if err := db.Order("created_at_ms").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].CreatedAtMS != cutoff {
		t.Fatalf("state refresh log retention: %+v", rows)
	}
}
