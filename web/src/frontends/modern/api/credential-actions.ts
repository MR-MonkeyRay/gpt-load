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
export interface CredentialStateRefresh {
  turn_state: string
  attempts: number
  refreshed_at_ms: number
}
export async function refreshCredentialState(
  client: ApiClient,
  group: number,
  id: number,
  signal: AbortSignal,
): Promise<CredentialStateRefresh> {
  const data = record(
    await client.request(`/api/groups/${group}/credentials/${id}/state-refresh`, {
      method: 'POST',
      json: {},
      signal,
    }),
  )
  const turnState = text(data.turn_state)
  if (!turnState) throw new InvalidResponseError()
  return {
    turn_state: turnState,
    attempts: integer(data.attempts, 1),
    refreshed_at_ms: integer(data.refreshed_at_ms),
  }
}
export interface CredentialStateRefreshRecord {
  id: number
  status: 'succeeded' | 'failed'
  errorCode: string
  turnState: string
  stateLength: number
  attempts: number
  httpStatus: number | null
  model: string
  input: string
  proxyUrl: string
  baseUrl: string
  durationMs: number
  createdAt: number
}
export interface CredentialStateSnapshot {
  turnState: string
  turnStateLength: number
  requiredLength: number
  refreshedAt: number | null
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
  }
}
// 只保留完整长度的 state，读取同时回看最近的刷新记录。
export async function getCredentialState(
  client: ApiClient,
  group: number,
  id: number,
  signal: AbortSignal,
): Promise<CredentialStateSnapshot> {
  const data = record(
    await client.request(`/api/groups/${group}/credentials/${id}/state-refresh`, { signal }),
  )
  return {
    turnState: text(data.turn_state),
    turnStateLength: integer(data.turn_state_length),
    requiredLength: integer(data.required_length, 1),
    refreshedAt:
      data.refreshed_at_ms === null || data.refreshed_at_ms === undefined
        ? null
        : integer(data.refreshed_at_ms, 1),
    logs: list(data.logs).map(readStateRefreshRecord),
  }
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
