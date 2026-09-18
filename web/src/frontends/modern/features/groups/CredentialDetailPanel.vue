<script setup lang="ts">
import { Info, RefreshCw } from '@lucide/vue'
import { useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, onScopeDispose, ref, watch } from 'vue'
import { useMessageSource } from '@modern/app/messages'
import { runningPolling } from '@modern/app/query'
import { useI18n } from 'vue-i18n'
import {
  credentialDetailKey,
  getCredentialDetail,
  getCredentialState,
  startCredentialStateRefresh,
  stopCredentialStateRefresh,
  updateCredential,
} from '@modern/api/credential-actions'
import type { CredentialRow } from '@modern/api/group-detail'
import type { GroupChannel } from '@modern/api/group-create'
import type { GroupRow } from '@modern/api/groups'
import {
  AppBadge,
  AppButton,
  AppCollectionState,
  AppIcon,
  AppNotice,
  AppOverflowText,
  AppSegmentedField,
  AppTextField,
  AppTooltip,
} from '@modern/components/ui'
import { useApiClient } from '@shared/http/client-context'
import { useClock } from '@modern/components/ui/clock'
import { credentialStatus, credentialTime } from './credential-presentation'
import { validProxyURL } from '@modern/app/proxy'
import GroupWorkspacePanel from './GroupWorkspacePanel.vue'
import CredentialAccountInfo from './CredentialAccountInfo.vue'
import CredentialTrends from './CredentialTrends.vue'
import CredentialWindowUsage from './CredentialWindowUsage.vue'

const props = defineProps<{ group: GroupRow; row: CredentialRow; channel?: GroupChannel }>()
const emit = defineEmits<{ close: []; saved: [row: CredentialRow] }>()
const { t, n, locale, te } = useI18n()
const client = useApiClient()
const cache = useQueryClient()
const query = useQuery({
  queryKey: credentialDetailKey(props.group.id, props.row.id),
  queryFn: ({ signal }) => getCredentialDetail(client, props.group.id, props.row.id, signal),
})
const item = computed(() => query.data.value ?? props.row)
const state = computed(() => credentialStatus(item.value))
const saved = ref<CredentialRow>()
const weight = ref('')
const proxyMode = ref('inherit')
const proxyURL = ref('')
const saving = ref(false)
const completed = ref(false)
const attempted = ref(false)
const error = ref('')
const controller = new AbortController()
const dirty = computed(
  () =>
    !completed.value &&
    Boolean(saved.value) &&
    (weight.value !== String(saved.value!.weightManual ?? '') ||
      proxyMode.value !== saved.value!.proxy.mode ||
      Boolean(proxyURL.value)),
)
watch(
  query.data,
  (value) => {
    if (!value || dirty.value || saving.value) return
    saved.value = value
    weight.value = String(value.weightManual ?? '')
    proxyMode.value = value.proxy.mode
    proxyURL.value = ''
  },
  { immediate: true },
)
const weightInvalid = computed(
  () =>
    Boolean(weight.value) &&
    (!/^\d+$/u.test(weight.value) ||
      (Number(weight.value) < 1 && Number(weight.value) !== saved.value?.weight) ||
      Number(weight.value) > 100),
)
const proxyChanged = computed(
  () => proxyMode.value !== saved.value?.proxy.mode || Boolean(proxyURL.value),
)
const proxyInvalid = computed(
  () => proxyChanged.value && proxyMode.value === 'custom' && !validProxyURL(proxyURL.value.trim()),
)
const proxyOptions = computed(() => [
  { value: 'inherit', label: t('credentialCards.proxyMode.inherit') },
  { value: 'direct', label: t('groupCreate.proxyDirect') },
  { value: 'custom', label: t('groupCreate.proxyCustom') },
])
function failure(): string {
  if (!item.value.failures) return '—'
  const key = 'credentialCards.failures.' + item.value.failureCategory
  return [te(key) ? t(key) : item.value.failureCategory, item.value.lastStatusCode]
    .filter(Boolean)
    .join(' · ')
}
async function save(): Promise<void> {
  if (!saved.value || !dirty.value || saving.value) return
  attempted.value = true
  if (weightInvalid.value || proxyInvalid.value) return
  const patch: Parameters<typeof updateCredential>[3] = {}
  if (weight.value !== String(saved.value.weightManual ?? ''))
    patch.weight_manual = weight.value ? Number(weight.value) : null
  if (proxyChanged.value)
    patch.proxy =
      proxyMode.value === 'inherit'
        ? null
        : proxyMode.value === 'direct'
          ? { mode: 'direct' }
          : { mode: 'custom', url: proxyURL.value.trim() }
  saving.value = true
  error.value = ''
  try {
    await cache.cancelQueries({ queryKey: credentialDetailKey(props.group.id, props.row.id) })
    const result = await updateCredential(
      client,
      props.group.id,
      props.row.id,
      patch,
      controller.signal,
    )
    if (controller.signal.aborted) return
    completed.value = true
    emit('saved', result)
    emit('close')
  } catch {
    if (!controller.signal.aborted) error.value = t('groups.edit.saveFailed')
  } finally {
    saving.value = false
  }
}
onScopeDispose(() => controller.abort())

// State 区块：启停后台刷新运行、轮询运行中的记录，并展示已保留 state 的有效期。
const supportsStateRefresh = computed(() => Boolean(props.channel?.stateRefresh))
const stateQueryKey = ['modern', 'credential-state', props.group.id, props.row.id] as const
const stateQuery = useQuery({
  queryKey: stateQueryKey,
  queryFn: ({ signal }) => getCredentialState(client, props.group.id, props.row.id, signal),
  enabled: computed(() => supportsStateRefresh.value),
  // 轮询策略由 app/query.ts 统一提供：服务端快照报告 running 时才回看。
  ...runningPolling,
})
const now = useClock()
const stateRunning = computed(() => stateQuery.data.value?.running ?? false)
const stateExpiresAt = computed(() => stateQuery.data.value?.expiresAt ?? null)
const stateExpired = computed(
  () => stateExpiresAt.value !== null && stateExpiresAt.value <= now.value,
)
const statePending = ref(false)
const stateFeedback = ref('')
const stateController = new AbortController()
function stateProxyLabel(value: string): string {
  if (value === 'direct') return t('credentialCards.state.proxyDirect')
  return value === '' ? t('credentialCards.state.proxyEnvironment') : value
}
function stateFailureLabel(code: string): string {
  const key = `credentialCards.state.failure.${code}`
  return te(key) ? t(key) : code
}
function stateRecordTime(value: number | null): string {
  return value ? credentialTime(value, locale.value) : '—'
}
async function toggleStateRefresh(): Promise<void> {
  if (statePending.value || !supportsStateRefresh.value) return
  const stopping = stateRunning.value
  statePending.value = true
  stateFeedback.value = ''
  try {
    const snapshot = stopping
      ? await stopCredentialStateRefresh(
          client,
          props.group.id,
          props.row.id,
          stateController.signal,
        )
      : await startCredentialStateRefresh(
          client,
          props.group.id,
          props.row.id,
          stateController.signal,
        )
    if (stateController.signal.aborted) return
    cache.setQueryData(stateQueryKey, snapshot)
  } catch {
    if (!stateController.signal.aborted) {
      stateFeedback.value = t(
        stopping ? 'credentialCards.state.stopFailed' : 'credentialCards.state.refreshFailed',
      )
    }
  } finally {
    statePending.value = false
  }
}
onScopeDispose(() => stateController.abort())
useMessageSource(() => (error.value ? { text: error.value, tone: 'danger' } : undefined))
useMessageSource(() =>
  query.isError.value && saved.value
    ? {
        text: t('groupDetail.refreshFailed'),
        tone: 'warning',
        action: { label: t('ui.retry'), run: () => query.refetch() },
      }
    : undefined,
)
</script>
<template>
  <GroupWorkspacePanel
    :title="t('groupDetail.credentialDetails')"
    :description="row.account || row.mask"
    :dirty="dirty"
    :pending="saving"
    :loading="query.isFetching.value"
    :save-disabled="!saved"
    @close="emit('close')"
    @save="save"
  >
    <AppCollectionState v-if="query.isPending.value" :title="t('collection.loading')" loading />
    <AppCollectionState
      v-else-if="query.isError.value && !saved"
      :title="t('groups.edit.loadFailed')"
      error
      ><AppButton @click="query.refetch()">{{ t('ui.retry') }}</AppButton></AppCollectionState
    >
    <template v-else>
      <CredentialTrends
        :subscription="group.connectionType === 'subscription'"
        :group="group.id"
        :credential="row.id"
        :quota-windows="item.observation?.windows ?? []"
      />
      <CredentialWindowUsage
        v-if="item.observation?.windows.length"
        :windows="item.observation.windows"
      />
      <section class="modern-credential-detail-section">
        <div class="modern-credential-detail-title">
          <h3>{{ t('credentialCards.runtime') }}</h3>
          <AppBadge :tone="state.tone" dot>{{ t(state.key) }}</AppBadge>
        </div>
        <dl class="modern-credential-detail-metrics">
          <div>
            <dt>{{ t('credentialCards.dailySuccess') }}</dt>
            <dd :class="{ 'modern-credential-detail-success': item.daily }">
              {{ item.daily ? n(item.daily.successes) : '—' }}
            </dd>
          </div>
          <div>
            <dt>{{ t('credentialCards.dailyFailure') }}</dt>
            <dd
              :class="{ 'modern-credential-detail-failure': item.daily && item.daily.failures > 0 }"
            >
              {{ item.daily ? n(item.daily.failures) : '—' }}
            </dd>
          </div>
          <div>
            <dt>{{ t('credentialCards.healthSuccess') }}</dt>
            <dd>{{ n(item.successes) }}</dd>
          </div>
          <div>
            <dt>{{ t('credentialCards.healthFailure') }}</dt>
            <dd>{{ n(item.failures) }}</dd>
          </div>
          <div>
            <dt>{{ t('groupDetail.failuresInRow') }}</dt>
            <dd>{{ n(item.failuresInRow) }}</dd>
          </div>
          <div>
            <dt>{{ t('groupDetail.lastUsed') }}</dt>
            <dd>{{ credentialTime(item.lastUsed, locale) }}</dd>
          </div>
          <div>
            <dt>{{ t('credentialCards.lastFailure') }}</dt>
            <dd>{{ failure() }}</dd>
          </div>
          <div>
            <dt>{{ t('credentialCards.recovery') }}</dt>
            <dd>
              {{ t('credentialCards.recoveryModes.' + item.recovery.mode)
              }}<span v-if="item.recovery.at">
                · {{ credentialTime(item.recovery.at, locale) }}</span
              >
            </dd>
          </div>
        </dl>
        <p v-if="item.daily && !item.daily.complete" class="modern-credential-detail-hint">
          {{ t('groups.row.partialHelp') }}
        </p>
      </section>
      <section
        v-if="group.connectionType === 'subscription'"
        class="modern-credential-detail-section"
      >
        <h3>{{ t('credentialCards.accountInfo') }}</h3>
        <CredentialAccountInfo :row="item" />
      </section>
      <section v-if="item.modelCooldowns.length" class="modern-credential-detail-section">
        <h3>{{ t('credentialCards.cooldownModels') }}</h3>
        <div
          v-for="cooldown in item.modelCooldowns"
          :key="cooldown.model"
          class="modern-credential-detail-title"
        >
          <AppOverflowText :text="cooldown.model" /><small>{{
            credentialTime(cooldown.until, locale)
          }}</small>
        </div>
      </section>
      <section class="modern-credential-detail-section">
        <div class="modern-credential-detail-settings-title">
          <h3>{{ t('credentialCards.settings') }}</h3>
          <AppTooltip :label="t('credentialCards.autoWeight')">
            <span
              class="modern-credential-detail-help"
              tabindex="0"
              :aria-label="t('credentialCards.autoWeight')"
            >
              <AppIcon :icon="Info" size="sm" />
            </span>
          </AppTooltip>
        </div>
        <div class="modern-credential-detail-routing">
          <AppTextField
            v-model="weight"
            :label="t('groups.edit.weight')"
            :placeholder="t('credentialCards.automatic')"
            size="sm"
            inputmode="numeric"
            :disabled="saving"
            :error="attempted && weightInvalid ? t('groups.edit.weightError') : undefined"
          />
          <AppSegmentedField
            v-if="channel?.proxy"
            v-model="proxyMode"
            :label="t('groupCreate.proxy')"
            :options="proxyOptions"
            size="sm"
            :disabled="saving"
          />
        </div>
        <AppTextField
          v-if="channel?.proxy && proxyMode === 'custom'"
          v-model="proxyURL"
          :label="t('groupCreate.proxyURL')"
          :placeholder="saved?.proxy.mode === 'custom' ? saved.proxy.display : undefined"
          :description="
            saved?.proxy.mode === 'custom' ? t('groupDetail.proxyUnchanged') : undefined
          "
          size="sm"
          :disabled="saving"
          :error="attempted && proxyInvalid ? t('groupCreate.proxyError') : undefined"
          autocomplete="off"
        />
      </section>
      <section v-if="supportsStateRefresh" class="modern-credential-detail-section">
        <div class="modern-credential-detail-title">
          <h3>{{ t('credentialCards.state.title') }}</h3>
          <AppButton
            variant="outline"
            size="xs"
            :icon="RefreshCw"
            :loading="statePending"
            @click="toggleStateRefresh"
          >
            {{
              t(stateRunning ? 'credentialCards.stateStopRefresh' : 'credentialCards.stateRefresh')
            }}
          </AppButton>
        </div>
        <AppNotice v-if="stateFeedback" tone="danger">{{ stateFeedback }}</AppNotice>
        <dl class="modern-credential-detail-metrics">
          <div>
            <dt>{{ t('credentialCards.state.current') }}</dt>
            <dd>
              <code v-if="stateQuery.data.value?.turnState" class="modern-state-value">{{
                stateQuery.data.value.turnState
              }}</code>
              <span v-else>{{ t('credentialCards.state.empty') }}</span>
            </dd>
          </div>
          <div>
            <dt>{{ t('credentialCards.state.recordedAt') }}</dt>
            <dd>{{ stateRecordTime(stateQuery.data.value?.refreshedAt ?? null) }}</dd>
          </div>
          <div>
            <dt>{{ t('credentialCards.state.expiresAt') }}</dt>
            <dd>
              <span v-if="stateExpiresAt !== null" class="modern-state-expiry">
                <span :class="{ 'modern-credential-detail-failure': stateExpired }">{{
                  credentialTime(stateExpiresAt, locale)
                }}</span>
                <AppBadge v-if="stateExpired" tone="danger" size="xs">{{
                  t('credentialCards.state.expired')
                }}</AppBadge>
              </span>
              <span v-else-if="stateQuery.data.value?.turnState">{{
                t('credentialCards.state.expiryUnknown')
              }}</span>
              <span v-else>—</span>
            </dd>
          </div>
        </dl>
        <p v-if="stateQuery.data.value" class="modern-credential-detail-hint">
          {{
            t('credentialCards.state.retained', { length: n(stateQuery.data.value.requiredLength) })
          }}
        </p>
        <div class="modern-state-history">
          <span class="modern-state-history-title">{{ t('credentialCards.state.history') }}</span>
          <p v-if="stateQuery.isPending.value" class="modern-credential-detail-hint">
            {{ t('collection.loading') }}
          </p>
          <p v-else-if="stateQuery.isError.value" class="modern-credential-detail-hint">
            {{ t('credentialCards.state.loadFailed') }}
          </p>
          <p v-else-if="!stateQuery.data.value?.logs.length" class="modern-credential-detail-hint">
            {{ t('credentialCards.state.historyEmpty') }}
          </p>
          <details
            v-for="log in stateQuery.data.value?.logs ?? []"
            :key="log.id"
            class="modern-state-record"
          >
            <summary>
              <AppBadge :tone="log.status === 'succeeded' ? 'success' : 'danger'" size="xs" dot>{{
                t(`credentialCards.state.status.${log.status}`)
              }}</AppBadge>
              <span>{{ credentialTime(log.createdAt, locale) }}</span>
              <span>{{
                t('credentialCards.state.attemptOrdinal', { count: n(log.attempts) })
              }}</span>
              <span>{{
                t('credentialCards.state.stateLength', { length: n(log.stateLength) })
              }}</span>
            </summary>
            <dl class="modern-state-record-grid">
              <div>
                <dt>{{ t('credentialCards.state.model') }}</dt>
                <dd>{{ log.model }}</dd>
              </div>
              <div>
                <dt>{{ t('credentialCards.state.input') }}</dt>
                <dd>{{ log.input }}</dd>
              </div>
              <div>
                <dt>{{ t('credentialCards.state.proxy') }}</dt>
                <dd>{{ stateProxyLabel(log.proxyUrl) }}</dd>
              </div>
              <div>
                <dt>{{ t('credentialCards.state.baseURL') }}</dt>
                <dd>{{ log.baseUrl || t('credentialCards.state.baseURLDefault') }}</dd>
              </div>
              <div>
                <dt>{{ t('credentialCards.state.duration') }}</dt>
                <dd>{{ n(log.durationMs) }} ms</dd>
              </div>
              <div v-if="log.status === 'failed'">
                <dt>{{ t('credentialCards.state.result') }}</dt>
                <dd>{{ stateFailureLabel(log.errorCode) }}</dd>
              </div>
              <div v-if="log.httpStatus !== null">
                <dt>{{ t('credentialCards.state.httpStatus') }}</dt>
                <dd>{{ log.httpStatus }}</dd>
              </div>
              <div v-if="log.turnState">
                <dt>{{ t('credentialCards.state.captured') }}</dt>
                <dd>
                  <code class="modern-state-value">{{ log.turnState }}</code>
                </dd>
              </div>
            </dl>
          </details>
        </div>
      </section>
    </template>
  </GroupWorkspacePanel>
</template>
<style scoped>
.modern-credential-detail-section {
  display: grid;
  gap: var(--modern-space-3);
}
.modern-window-usage + .modern-credential-detail-section,
.modern-credential-detail-section + .modern-credential-detail-section {
  padding-top: var(--modern-space-3);
  border-top: var(--modern-line-width) solid var(--modern-border);
}
.modern-credential-detail-section h3 {
  font-size: var(--modern-font-size-section);
  font-weight: var(--modern-weight-semibold);
}
.modern-credential-detail-title {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: var(--modern-space-3);
  min-width: 0;
  font-size: var(--modern-font-size-secondary);
}
.modern-credential-detail-title small,
.modern-credential-detail-hint {
  color: var(--modern-muted);
  font-size: var(--modern-font-size-small);
}
.modern-credential-detail-metrics {
  display: grid;
  grid-template-columns: max-content minmax(0, 1fr) max-content minmax(0, 1fr);
  column-gap: var(--modern-space-4);
  row-gap: var(--modern-space-1-5);
  line-height: var(--modern-leading-compact);
}
.modern-credential-detail-metrics > div {
  display: grid;
  grid-column: span 2;
  grid-template-columns: subgrid;
  align-items: baseline;
  column-gap: var(--modern-space-2);
  min-width: 0;
}
.modern-credential-detail-metrics dt {
  color: var(--modern-muted);
  font-size: var(--modern-font-size-small);
}
.modern-credential-detail-metrics dd {
  margin: 0;
  min-width: 0;
  font-size: var(--modern-font-size-small);
  font-variant-numeric: tabular-nums;
  overflow-wrap: anywhere;
}
.modern-credential-detail-success {
  color: var(--modern-status-success);
}
.modern-credential-detail-failure {
  color: var(--modern-danger);
}
.modern-credential-detail-settings-title {
  display: flex;
  align-items: center;
  gap: var(--modern-space-2);
}
.modern-credential-detail-help {
  display: inline-flex;
  color: var(--modern-muted);
}
.modern-credential-detail-routing {
  display: grid;
  grid-template-columns: minmax(0, 112px) minmax(0, 1fr);
  align-items: start;
  gap: var(--modern-space-3);
}
.modern-state-value {
  display: block;
  /* 完整 State 可滚动查看，避免 292 字符撑高整个区块。 */
  max-height: 6.5em;
  overflow-y: auto;
  font-family: var(--modern-font-mono);
  font-size: var(--modern-font-size-small);
  overflow-wrap: anywhere;
}
.modern-state-expiry {
  display: inline-flex;
  align-items: center;
  gap: var(--modern-space-1-5);
}
.modern-state-history {
  display: grid;
  gap: var(--modern-space-2);
  min-width: 0;
}
.modern-state-history-title {
  color: var(--modern-muted);
  font-size: var(--modern-font-size-small);
}
.modern-state-record {
  border: var(--modern-line-width) solid var(--modern-border);
  border-radius: var(--modern-radius-control);
  padding: var(--modern-space-2) var(--modern-space-3);
}
.modern-state-record > summary {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--modern-space-1) var(--modern-space-3);
  border-radius: var(--modern-radius-small);
  color: var(--modern-muted);
  font-size: var(--modern-font-size-small);
  font-variant-numeric: tabular-nums;
  cursor: pointer;
}
.modern-state-record > summary:focus-visible {
  outline: var(--modern-focus-width) solid var(--modern-accent);
  outline-offset: var(--modern-space-0-5);
}
.modern-state-record[open] > summary {
  margin-bottom: var(--modern-space-2);
}
.modern-state-record-grid {
  display: grid;
  grid-template-columns: max-content minmax(0, 1fr);
  gap: var(--modern-space-1) var(--modern-space-3);
  font-size: var(--modern-font-size-small);
}
.modern-state-record-grid > div {
  display: grid;
  grid-column: span 2;
  grid-template-columns: subgrid;
  align-items: baseline;
  gap: var(--modern-space-2);
  min-width: 0;
}
.modern-state-record-grid dt {
  color: var(--modern-muted);
}
.modern-state-record-grid dd {
  margin: 0;
  min-width: 0;
  overflow-wrap: anywhere;
}
@container modern-workspace-panel (max-width: 380px) {
  .modern-credential-detail-metrics {
    grid-template-columns: max-content minmax(0, 1fr);
  }
}
@container modern-workspace-panel (max-width: 340px) {
  .modern-credential-detail-routing {
    grid-template-columns: minmax(0, 1fr);
  }
  .modern-credential-detail-routing > :first-child {
    max-width: 112px;
  }
}
</style>
