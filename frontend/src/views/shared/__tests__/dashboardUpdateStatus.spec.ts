import { describe, expect, it } from 'vitest'
import type { CheckUpdateResponse } from '@/api/admin'
import {
  buildDashboardUpdateErrorStatus,
  describeDashboardUpdateStatus,
} from '../dashboardUpdateStatus'

function updateStatus(overrides: Partial<CheckUpdateResponse> = {}): CheckUpdateResponse {
  return {
    current_version: '0.7.0-rc27',
    latest_version: null,
    has_update: false,
    release_url: null,
    release_notes: null,
    published_at: null,
    error: null,
    ...overrides,
  }
}

describe('dashboardUpdateStatus', () => {
  it('describes loading and latest states', () => {
    expect(describeDashboardUpdateStatus(null)).toBe('检查中')
    expect(describeDashboardUpdateStatus(updateStatus())).toBe('已是最新')
  })

  it('prioritizes update availability over latest-version text', () => {
    expect(describeDashboardUpdateStatus(updateStatus({
      latest_version: 'v0.7.0-rc28',
      has_update: true,
      release_url: 'https://github.com/fawney19/Aether/releases/tag/v0.7.0-rc28',
    }))).toBe('有新版本')
  })

  it('preserves the current version when building an error state', () => {
    const status = buildDashboardUpdateErrorStatus(
      updateStatus({ current_version: '0.7.0-rc28' }),
      new Error('network down')
    )

    expect(status.current_version).toBe('0.7.0-rc28')
    expect(status.has_update).toBe(false)
    expect(status.error).toBe('network down')
  })
})
