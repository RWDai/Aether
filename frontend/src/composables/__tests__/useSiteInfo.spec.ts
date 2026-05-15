import { beforeEach, describe, expect, it, vi } from 'vitest'

const apiClientMocks = vi.hoisted(() => ({
  get: vi.fn(),
}))

vi.mock('@/api/client', () => ({
  default: apiClientMocks,
}))

describe('useSiteInfo', () => {
  beforeEach(() => {
    vi.resetModules()
    apiClientMocks.get.mockReset()
  })

  it('loads github link display setting from public site info', async () => {
    apiClientMocks.get.mockResolvedValue({
      data: {
        site_name: 'Custom Aether',
        site_subtitle: 'Gateway',
        show_github_link: false,
      },
    })

    const { useSiteInfo } = await import('../useSiteInfo')
    const { siteName, siteSubtitle, showGithubLink, refreshSiteInfo } = useSiteInfo()
    await refreshSiteInfo()

    expect(siteName.value).toBe('Custom Aether')
    expect(siteSubtitle.value).toBe('Gateway')
    expect(showGithubLink.value).toBe(false)
  })
})
