import { describe, expect, it } from 'vitest'
import { summarizeOAuthConfigTest } from '../oauthConfigTest'

describe('summarizeOAuthConfigTest', () => {
  it('marks unreachable endpoints and unsupported secret as failed', () => {
    const summary = summarizeOAuthConfigTest({
      authorization_url_reachable: false,
      token_url_reachable: false,
      secret_status: 'unsupported',
      details: 'OAuth 配置测试仅支持 Rust execution runtime',
    })

    expect(summary.severity).toBe('error')
    expect(summary.failures).toEqual([
      'Authorization URL 不可达',
      'Token URL 不可达',
      'Secret 不受支持',
    ])
    expect(summary.message).toBe('测试失败：Authorization URL 不可达，Token URL 不可达，Secret 不受支持')
  })

  it('uses warning when only secret validation is inconclusive', () => {
    const summary = summarizeOAuthConfigTest({
      authorization_url_reachable: true,
      token_url_reachable: true,
      secret_status: 'unknown',
    })

    expect(summary.severity).toBe('warning')
    expect(summary.warnings).toEqual(['Secret 未验证'])
  })

  it('marks fully reachable config with a likely valid secret as successful', () => {
    const summary = summarizeOAuthConfigTest({
      authorization_url_reachable: true,
      token_url_reachable: true,
      secret_status: 'likely_valid',
    })

    expect(summary.severity).toBe('success')
    expect(summary.message).toBe('测试通过')
  })

  it('accepts a configured secret because OAuth secrets are verified during code exchange', () => {
    const summary = summarizeOAuthConfigTest({
      authorization_url_reachable: true,
      token_url_reachable: true,
      secret_status: 'configured',
    })

    expect(summary.severity).toBe('success')
  })
})
