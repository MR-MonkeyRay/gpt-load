package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/storage/models"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
	"gpt-load/internal/testutil/turnstatetest"
)

// waitNaturalStateCaptureSettled waits until no live capture of one credential
// is in flight, which is when its durable write has finished either way.
func waitNaturalStateCaptureSettled(t *testing.T, service *Service) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		service.naturalMu.Lock()
		busy := len(service.naturalCaptures)
		service.naturalMu.Unlock()
		if busy == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("live turn state capture did not settle")
}

func naturalCapturesInFlight(service *Service) int {
	service.naturalMu.Lock()
	defer service.naturalMu.Unlock()
	return len(service.naturalCaptures)
}

func observeCompleteTurnState(t *testing.T, fixture serviceFixture, credentialID uint, model, turnState string) {
	t.Helper()
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok {
		t.Fatal("credential is not registered")
	}
	fixture.service.ObserveTurnState(execution.TurnStateObservation{
		CredentialID:       credentialID,
		IdentityGeneration: ref.IdentityGeneration,
		Model:              model,
		TurnState:          turnState,
		ProxyURL:           "direct",
		BaseURL:            "https://chatgpt.com/backend-api/codex",
		ObservedAtMS:       fixture.service.now().UTC().UnixMilli(),
	})
}

func credentialTurnState(t *testing.T, fixture serviceFixture, credentialID uint, model string, at time.Time) string {
	t.Helper()
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok {
		t.Fatal("credential is not registered")
	}
	return ref.TurnStateFor(model, at)
}

// A complete state an upstream returned on a real attempt is retained for its
// credential and model, recorded as a natural capture, and replayed for the next
// hour only.
func TestObserveTurnStateRetainsLiveCapture(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	complete := turnstatetest.Value(0)

	observeCompleteTurnState(t, fixture, credentialID, stateRefreshTestModel, complete)
	waitNaturalStateCaptureSettled(t, fixture.service)

	capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel)
	if capture == nil || capture.TurnState != complete || capture.RefreshedAtMS != now.UnixMilli() {
		t.Fatalf("persisted turn state = %#v", capture)
	}
	// 捕获只属于它自己的模型。
	if other := turnStateCapture(t, fixture, credentialID, stateRefreshOtherModel); other != nil {
		t.Fatalf("capture leaked to another model: %#v", other)
	}

	records := stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel)
	if len(records) != 1 {
		t.Fatalf("capture records = %#v", records)
	}
	record := records[0]
	if record.Status != models.CredentialStateRefreshSucceeded ||
		record.Source() != models.CredentialStateRefreshSourceNatural ||
		record.Attempts != 0 || record.TurnState != complete ||
		record.StateLength != execution.CodexTurnStateLength || record.ErrorCode != "" ||
		record.GroupID != groupID || record.Model != stateRefreshTestModel ||
		record.Input != "" || record.ProxyURL != "direct" ||
		record.BaseURL != "https://chatgpt.com/backend-api/codex" ||
		record.DurationMS != 0 || record.HTTPStatus != nil ||
		record.CreatedAtMS != now.UnixMilli() {
		t.Fatalf("live capture record = %#v", record)
	}

	// 有效期从捕获时刻算起一小时。
	if got := credentialTurnState(t, fixture, credentialID, stateRefreshTestModel, now.Add(59*time.Minute)); got != complete {
		t.Fatalf("state inside its window = %q", got)
	}
	if got := credentialTurnState(t, fixture, credentialID, stateRefreshTestModel, now.Add(61*time.Minute)); got != "" {
		t.Fatalf("state past its window = %q", got)
	}
	if got := credentialTurnState(t, fixture, credentialID, stateRefreshOtherModel, now); got != "" {
		t.Fatalf("state replayed for another model = %q", got)
	}

	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if len(state.Logs) != 1 || state.Logs[0].Source != string(models.CredentialStateRefreshSourceNatural) {
		t.Fatalf("state payload logs = %#v", state.Logs)
	}
	entry := modelState(t, state, stateRefreshTestModel)
	if entry.TurnState != complete || entry.RefreshedAtMS == nil || *entry.RefreshedAtMS != now.UnixMilli() ||
		entry.ExpiresAtMS == nil || *entry.ExpiresAtMS != now.Add(time.Hour).UnixMilli() {
		t.Fatalf("retained state entry = %#v", entry)
	}
}

// A probe capture keeps its own source, so the record list tells a manual
// refresh apart from a live capture.
func TestProbeCaptureKeepsRefreshSource(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	complete := turnstatetest.Value(0)
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		return subscriptionruntime.StateProbeResult{TurnState: complete}, nil
	}

	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)

	records := stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel)
	if len(records) != 1 || records[0].Source() != models.CredentialStateRefreshSourceRefresh || records[0].Attempts != 1 {
		t.Fatalf("probe records = %#v", records)
	}
	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if len(state.Logs) != 1 || state.Logs[0].Source != string(models.CredentialStateRefreshSourceRefresh) {
		t.Fatalf("state payload logs = %#v", state.Logs)
	}
}

// An observation that cannot produce a new capture is dropped before it claims
// any work: a valid state already exists, or the value is incomplete.
func TestObserveTurnStateDropsUnusableObservationsImmediately(t *testing.T) {
	t.Parallel()

	t.Run("valid state already captured", func(t *testing.T) {
		fixture, _, credentialID := newStateRefreshFixture(t)
		now := fixture.service.now()
		live := turnstatetest.Value(1)
		if !fixture.registry.SetCredentialTurnState(credentialID, stateRefreshTestModel, live, now.UTC().UnixMilli()) {
			t.Fatal("SetCredentialTurnState() did not publish the value")
		}

		observeCompleteTurnState(t, fixture, credentialID, stateRefreshTestModel, turnstatetest.Value(2))

		if inFlight := naturalCapturesInFlight(fixture.service); inFlight != 0 {
			t.Fatalf("capture claimed for an already captured model: %d", inFlight)
		}
		if records := stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel); len(records) != 0 {
			t.Fatalf("dropped observation was recorded: %#v", records)
		}
		if got := credentialTurnState(t, fixture, credentialID, stateRefreshTestModel, now); got != live {
			t.Fatalf("existing capture was replaced: %q", got)
		}
	})

	t.Run("incomplete state", func(t *testing.T) {
		fixture, _, credentialID := newStateRefreshFixture(t)

		observeCompleteTurnState(t, fixture, credentialID, stateRefreshTestModel, "incomplete")

		if inFlight := naturalCapturesInFlight(fixture.service); inFlight != 0 {
			t.Fatalf("capture claimed for an incomplete state: %d", inFlight)
		}
		if records := stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel); len(records) != 0 {
			t.Fatalf("incomplete state was recorded: %#v", records)
		}
		if capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel); capture != nil {
			t.Fatalf("incomplete state was retained: %#v", capture)
		}
	})

	// 手动刷新正在跑同一个 (凭据, 模型) 时，捕获由这次运行负责。
	t.Run("refresh run in flight", func(t *testing.T) {
		fixture, groupID, credentialID := newStateRefreshFixture(t)
		blocked := make(chan struct{})
		probeStarted := make(chan struct{}, 1)
		fixture.service.probeSubscriptionTurnState = func(
			ctx context.Context,
			_ channel.ID,
			_ subscriptionruntime.Credential,
			_ subscriptionruntime.Target,
			_ subscriptionruntime.StateProbeRequest,
		) (subscriptionruntime.StateProbeResult, error) {
			select {
			case probeStarted <- struct{}{}:
			default:
			}
			select {
			case <-blocked:
			case <-ctx.Done():
			}
			return subscriptionruntime.StateProbeResult{}, errors.New("probe stopped")
		}
		if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
			t.Fatalf("StartCredentialStateRefresh() error = %v", err)
		}
		select {
		case <-probeStarted:
		case <-time.After(10 * time.Second):
			t.Fatal("refresh run did not start probing")
		}

		observeCompleteTurnState(t, fixture, credentialID, stateRefreshTestModel, turnstatetest.Value(3))

		if inFlight := naturalCapturesInFlight(fixture.service); inFlight != 0 {
			t.Fatalf("capture claimed while a refresh run is in flight: %d", inFlight)
		}
		close(blocked)
		if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
			t.Fatalf("StopCredentialStateRefresh() error = %v", err)
		}
		for _, record := range stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel) {
			if record.Status == models.CredentialStateRefreshSucceeded {
				t.Fatalf("observation was captured during the run: %#v", record)
			}
		}
		if capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel); capture != nil {
			t.Fatalf("observation was retained during the run: %#v", capture)
		}
	})
}

// An observation whose credential is no longer the registered identity is
// dropped before it claims any work: a turn state belongs to the account that
// produced it.
func TestObserveTurnStateDropsCaptureOfAReplacedCredential(t *testing.T) {
	t.Parallel()

	fixture, _, credentialID := newStateRefreshFixture(t)
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok {
		t.Fatal("credential is not registered")
	}
	complete := turnstatetest.Value(0)
	fixture.service.ObserveTurnState(execution.TurnStateObservation{
		CredentialID:       credentialID,
		IdentityGeneration: ref.IdentityGeneration + 1,
		Model:              stateRefreshTestModel,
		TurnState:          complete,
		ObservedAtMS:       fixture.service.now().UTC().UnixMilli(),
	})

	if inFlight := naturalCapturesInFlight(fixture.service); inFlight != 0 {
		t.Fatalf("capture claimed for a replaced credential: %d", inFlight)
	}
	if records := stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel); len(records) != 0 {
		t.Fatalf("capture of a replaced credential was recorded: %#v", records)
	}
	if capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel); capture != nil {
		t.Fatalf("capture of a replaced credential was retained: %#v", capture)
	}
	if got := credentialTurnState(t, fixture, credentialID, stateRefreshTestModel, fixture.service.now()); got != "" {
		t.Fatalf("capture of a replaced credential was published: %q", got)
	}
}

// The durable write itself re-checks the credential identity, so a credential
// replaced while an accepted capture is in flight never inherits its state.
func TestPersistCredentialTurnStateGuardRejectsAReplacedCredential(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok {
		t.Fatal("credential is not registered")
	}
	now := fixture.service.now()
	complete := turnstatetest.Value(0)
	record := models.CredentialStateRefreshLog{
		GroupID: groupID, CredentialID: credentialID,
		Status: models.CredentialStateRefreshSucceeded, TurnState: complete,
		StateLength: len(complete), Model: stateRefreshTestModel,
		CreatedAtMS: now.UTC().UnixMilli(),
	}
	err := fixture.service.persistCredentialTurnState(
		t.Context(), groupID, credentialID, stateRefreshTestModel, complete, now.UTC().UnixMilli(), record,
		fixture.service.credentialIdentityGuard(credentialID, ref.IdentityGeneration+1),
	)
	if !errors.Is(err, errNaturalStateCaptureStale) {
		t.Fatalf("persist error = %v", err)
	}
	if capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel); capture != nil {
		t.Fatalf("capture of a replaced credential was retained: %#v", capture)
	}
	if records := stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel); len(records) != 0 {
		t.Fatalf("capture of a replaced credential was recorded: %#v", records)
	}
	if got := credentialTurnState(t, fixture, credentialID, stateRefreshTestModel, now); got != "" {
		t.Fatalf("capture of a replaced credential was published: %q", got)
	}
}
