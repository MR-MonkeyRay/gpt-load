import type { ApiClient } from '@shared/http/client'
import { InvalidResponseError } from '@shared/http/errors'
import type { ProxyOverride } from './group-create'
import { readCredential } from './group-detail'
import { readObservation } from './credential-observation'
import { boolean, integer, list, oneOf, record, text } from './response'

export const credentialDetailKey = (group: number, id: number) =>
  ['modern', 'credential-detail', group, id] as const
export async function getCredentialDetail(
  client: ApiClient,
  group: number,
  id: number,
  signal: AbortSignal,
) {
  const data = record(
    await client.request(`/api/modern/groups/${group}/credentials/${id}`, { signal }),
  )
  const item = readCredential(data.credential)
  if (item.id !== id) throw new InvalidResponseError()
  return { ...item, observation: readObservation(data.observation) ?? item.observation }
}
export async function updateCredential(
  client: ApiClient,
  group: number,
  id: number,
  patch: { weight_manual?: number | null; proxy?: ProxyOverride | null },
  signal: AbortSignal,
) {
  const row = readCredential(
    await client.request(`/api/groups/${group}/credentials/${id}`, {
      method: 'PUT',
      json: patch,
      signal,
    }),
  )
  return Object.hasOwn(patch, 'weight_manual') ? { ...row, weightManual: patch.weight_manual } : row
}

export async function exportAllCredentials(client: ApiClient, group: number, signal: AbortSignal) {
  const data = record(
    await client.request(`/api/groups/${group}/credentials/download-all`, {
      method: 'POST',
      json: {},
      signal,
    }),
  )
  const count = integer(data.credential_count)
  const files = list(data.files).map((value) => {
    const file = record(value)
    const filename = text(file.filename)
    const plain = Object.hasOwn(file, 'content')
    if (
      !(plain ? /^[a-z0-9][a-z0-9._-]{0,191}\.txt$/u : /^[a-z0-9][a-z0-9._-]{0,191}\.json$/u).test(
        filename,
      )
    )
      throw new InvalidResponseError()
    return {
      filename,
      content: plain ? text(file.content) : JSON.stringify(record(file.credential), null, 2),
      type: plain ? 'text/plain;charset=utf-8' : 'application/json;charset=utf-8',
    }
  })
  if (
    files.some((file) => file.type.startsWith('text/'))
      ? files.length !== 1
      : files.length !== count
  )
    throw new InvalidResponseError()
  return { count, files }
}
export async function runCredentialAction(
  client: ApiClient,
  group: number,
  id: number,
  action: 'restore' | 'refresh',
  signal: AbortSignal,
) {
  return readCredential(
    await client.request(`/api/groups/${group}/credentials/${id}/${action}`, {
      method: 'POST',
      json: {},
      signal,
    }),
  )
}
export async function refreshCredentialQuota(
  client: ApiClient,
  group: number,
  id: number,
  signal: AbortSignal,
) {
  return readObservation(
    await client.request(`/api/groups/${group}/credentials/${id}/observation-refresh`, {
      method: 'POST',
      json: {},
      signal,
    }),
  )
}
export interface CredentialStateRefreshRecord {
  id: number
  status: 'succeeded' | 'failed'
  errorCode: string
  turnState: string
  stateLength: number
  // attempts 是这次探测在其刷新运行中的序号，从 1 开始，不是尝试次数。
  attempts: number
  httpStatus: number | null
  model: string
  input: string
  proxyUrl: string
  baseUrl: string
  durationMs: number
  createdAt: number
  // 记录这次 state 的来源：手动刷新探测或实时请求捕获。
  source: 'refresh' | 'natural'
}
// CredentialStateModelState 是一个模型已保留的 state：同一凭据的每个模型各自刷新、互不影响。
export interface CredentialStateModelState {
  model: string
  turnState: string
  stateLength: number
  refreshedAt: number | null
  // expiresAt 是已保留 state 自身携带的有效期；为空表示解析不出有效期。
  expiresAt: number | null
  running: boolean
}
export interface CredentialStateSnapshot {
  requiredLength: number
  // availableModels 是分组已配置的模型，也就是可以刷新的模型。
  availableModels: string[]
  // model 是本次回看的模型：请求未指定时由服务端选出默认模型。
  model: string
  // running 由服务端给出：界面只跟随它切换启停与轮询，不自行计时。
  running: boolean
  // runningModels 是当前全部在跑刷新的模型：界面据此列出刷新任务并逐个取消。
  runningModels: string[]
  states: CredentialStateModelState[]
  logs: CredentialStateRefreshRecord[]
}
function readStateRefreshRecord(value: unknown): CredentialStateRefreshRecord {
  const row = record(value)
  return {
    id: integer(row.id, 1),
    status: oneOf(row.status, ['succeeded', 'failed']),
    errorCode: row.error_code === undefined ? '' : text(row.error_code),
    turnState: text(row.turn_state),
    stateLength: integer(row.state_length),
    attempts: integer(row.attempts),
    httpStatus: row.http_status === undefined ? null : integer(row.http_status, 100),
    model: text(row.model),
    input: text(row.input),
    proxyUrl: text(row.proxy_url),
    baseUrl: text(row.base_url),
    durationMs: integer(row.duration_ms),
    createdAt: integer(row.created_at_ms, 1),
    source: oneOf(row.source, ['refresh', 'natural']),
  }
}
function readStateModelState(value: unknown): CredentialStateModelState {
  const row = record(value)
  return {
    model: text(row.model),
    turnState: text(row.turn_state),
    stateLength: integer(row.state_length),
    refreshedAt:
      row.refreshed_at_ms === null || row.refreshed_at_ms === undefined
        ? null
        : integer(row.refreshed_at_ms, 1),
    expiresAt:
      row.expires_at_ms === null || row.expires_at_ms === undefined
        ? null
        : integer(row.expires_at_ms, 1),
    running: boolean(row.running),
  }
}
function readCredentialStateSnapshot(value: unknown): CredentialStateSnapshot {
  const data = record(value)
  return {
    requiredLength: integer(data.required_length, 1),
    availableModels: list(data.available_models).map((model) => text(model)),
    model: text(data.model),
    running: boolean(data.running),
    runningModels: list(data.running_models).map((model) => text(model)),
    states: list(data.states).map(readStateModelState),
    logs: list(data.logs).map(readStateRefreshRecord),
  }
}
// 一次回看的模型：为空表示交给服务端选默认模型，此时请求不带 model 查询参数。
function stateRefreshQuery(model: string): string {
  return model ? `?${new URLSearchParams({ model })}` : ''
}
// 回看指定模型的 state 与最近刷新记录，未指定模型时由服务端选默认模型。
export async function getCredentialState(
  client: ApiClient,
  group: number,
  id: number,
  model: string,
  signal: AbortSignal,
): Promise<CredentialStateSnapshot> {
  return readCredentialStateSnapshot(
    await client.request(
      `/api/groups/${group}/credentials/${id}/state-refresh${stateRefreshQuery(model)}`,
      { signal },
    ),
  )
}
// 启动指定模型的后台刷新运行：请求立即返回 running 为真的快照，后续结果由轮询补齐。
export async function startCredentialStateRefresh(
  client: ApiClient,
  group: number,
  id: number,
  model: string,
  signal: AbortSignal,
): Promise<CredentialStateSnapshot> {
  return readCredentialStateSnapshot(
    await client.request(`/api/groups/${group}/credentials/${id}/state-refresh`, {
      method: 'POST',
      json: { model },
      signal,
    }),
  )
}
// 停止指定模型的刷新运行：返回的快照已包含这次运行的最后一条记录，running 为假。
export async function stopCredentialStateRefresh(
  client: ApiClient,
  group: number,
  id: number,
  model: string,
  signal: AbortSignal,
): Promise<CredentialStateSnapshot> {
  return readCredentialStateSnapshot(
    await client.request(
      `/api/groups/${group}/credentials/${id}/state-refresh${stateRefreshQuery(model)}`,
      { method: 'DELETE', signal },
    ),
  )
}
export async function revealCredential(
  client: ApiClient,
  group: number,
  id: number,
  signal: AbortSignal,
): Promise<string> {
  const data = record(
    await client.request(`/api/groups/${group}/credentials/${id}/reveal`, {
      method: 'POST',
      signal,
    }),
  )
  if (integer(data.credential_id, 1) !== id) throw new InvalidResponseError()
  const fields = Object.fromEntries(
    Object.entries(record(data.credential)).map(([key, value]) => [key, text(value)]),
  )
  const values = Object.values(fields)
  if (!values.length) throw new InvalidResponseError()
  return values.length === 1 ? values[0]! : JSON.stringify(fields, null, 2)
}
export async function exportCredential(
  client: ApiClient,
  group: number,
  id: number,
  signal: AbortSignal,
) {
  const data = record(
    await client.request(`/api/groups/${group}/credentials/${id}/download`, {
      method: 'POST',
      json: {},
      signal,
    }),
  )
  const filename = text(data.filename)
  if (!/^[a-z0-9][a-z0-9._-]{0,191}\.json$/u.test(filename)) throw new InvalidResponseError()
  return { filename, content: JSON.stringify(record(data.credential), null, 2) }
}
export async function resetCredentialQuota(
  client: ApiClient,
  group: number,
  id: number,
  key: string,
  signal: AbortSignal,
) {
  const data = record(
    await client.request(`/api/groups/${group}/credentials/${id}/reset-credits/consume`, {
      method: 'POST',
      json: {},
      headers: { 'Idempotency-Key': key },
      signal,
    }),
  )
  oneOf(data.status, ['succeeded'])
  return {
    windows: integer(data.windows_reset),
    observation: readObservation(data.observation),
    pending: data.observation_pending === undefined ? false : boolean(data.observation_pending),
  }
}
export interface CredentialTestResult {
  outcome: 'passed' | 'failed' | 'inconclusive'
  latency: number
  model: string
  protocol: string
  reason: string | null
  proof: string | null
}
export async function testCredential(
  client: ApiClient,
  group: number,
  id: number,
  protocol: string,
  model: string,
  signal: AbortSignal,
): Promise<CredentialTestResult> {
  const data = record(
    await client.request(`/api/groups/${group}/credentials/${id}/test`, {
      method: 'POST',
      json: { protocol, model },
      signal,
    }),
  )
  return {
    outcome: oneOf(data.outcome, ['passed', 'failed', 'inconclusive']),
    latency: integer(data.latency_ms),
    model: text(data.model),
    protocol: text(data.protocol),
    reason: data.reason === null ? null : text(data.reason),
    proof:
      boolean(data.can_restore) && data.restore_proof !== null ? text(data.restore_proof) : null,
  }
}
export async function restoreTestedCredential(
  client: ApiClient,
  group: number,
  id: number,
  proof: string,
  signal: AbortSignal,
) {
  return readCredential(
    await client.request(`/api/groups/${group}/credentials/${id}/test/restore`, {
      method: 'POST',
      json: { restore_proof: proof },
      signal,
    }),
  )
}
