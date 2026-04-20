import { describe, expect, it } from 'vitest'

import { getOAuthOrgBadge } from '../oauthIdentity'

describe('getOAuthOrgBadge', () => {
  it('prefers organization identity over account display name', () => {
    const badge = getOAuthOrgBadge({
      oauth_account_id: 'acct-demo-001',
      oauth_account_name: 'Workspace Alpha',
      oauth_account_user_id: 'user-1__acct-demo-001',
      oauth_organizations: [
        { id: 'org-personal-1234', title: 'Personal', is_default: true },
      ],
    })

    expect(badge).toEqual({
      id: 'org-personal-1234',
      label: 'org:org-pe...1234',
      title: 'name: Workspace Alpha | account_id: acct-demo-001 | account_user_id: user-1__acct-demo-001 | org_id: org-personal-1234 | org_title: Personal',
    })
  })

  it('falls back to short account id when no organization is available', () => {
    const badge = getOAuthOrgBadge({
      oauth_account_id: 'acct-demo-001',
      oauth_account_name: 'Workspace Alpha',
    })

    expect(badge).toEqual({
      id: 'acct-demo-001',
      label: 'acct-dem',
      title: 'name: Workspace Alpha | account_id: acct-demo-001',
    })
  })

  it('does not create an identity badge from account name alone', () => {
    const badge = getOAuthOrgBadge({
      oauth_account_name: 'Free',
    })

    expect(badge).toBeNull()
  })
})
