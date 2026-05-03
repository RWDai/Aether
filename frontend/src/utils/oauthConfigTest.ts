import type { OAuthProviderTestResponse } from '@/api/oauth'

export type OAuthConfigTestSeverity = 'success' | 'warning' | 'error'

export interface OAuthConfigTestSummary {
  severity: OAuthConfigTestSeverity
  message: string
  failures: string[]
  warnings: string[]
}

function describeSecretStatus(status: string | undefined): string | null {
  const normalized = (status || '').trim().toLowerCase()
  if (!normalized || normalized === 'likely_valid' || normalized === 'configured') return null
  if (normalized === 'invalid') return 'Secret 无效'
  if (normalized === 'unsupported') return 'Secret 不受支持'
  if (normalized === 'not_provided') return 'Secret 未提供'
  if (normalized === 'unknown') return 'Secret 未验证'
  return `Secret: ${status}`
}

export function summarizeOAuthConfigTest(result: OAuthProviderTestResponse): OAuthConfigTestSummary {
  const failures: string[] = []
  const warnings: string[] = []

  if (!result.authorization_url_reachable) {
    failures.push('Authorization URL 不可达')
  }
  if (!result.token_url_reachable) {
    failures.push('Token URL 不可达')
  }

  const secretStatus = (result.secret_status || '').trim().toLowerCase()
  const secretMessage = describeSecretStatus(result.secret_status)
  if (secretMessage && (secretStatus === 'invalid' || secretStatus === 'unsupported')) {
    failures.push(secretMessage)
  } else if (secretMessage) {
    warnings.push(secretMessage)
  }

  if (failures.length > 0) {
    return {
      severity: 'error',
      message: `测试失败：${failures.join('，')}`,
      failures,
      warnings,
    }
  }

  if (warnings.length > 0) {
    return {
      severity: 'warning',
      message: `测试完成，但有未确认项：${warnings.join('，')}`,
      failures,
      warnings,
    }
  }

  return {
    severity: 'success',
    message: '测试通过',
    failures,
    warnings,
  }
}
