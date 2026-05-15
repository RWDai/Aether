import { afterEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, h, nextTick, type App } from 'vue'

import SiteInfoSection from '../SiteInfoSection.vue'

vi.mock('@/components/layout', async () => {
  const { defineComponent, h } = await import('vue')
  return {
    CardSection: defineComponent({
      name: 'CardSectionStub',
      props: {
        title: String,
        description: String,
      },
      setup(props, { slots }) {
        return () => h('section', [
          h('h2', props.title),
          h('p', props.description),
          slots.actions?.(),
          slots.default?.(),
        ])
      },
    }),
  }
})

vi.mock('@/components/ui/button.vue', () => ({
  default: defineComponent({
    name: 'ButtonStub',
    setup(_, { slots }) {
      return () => h('button', slots.default?.())
    },
  }),
}))

const mountedApps: Array<{ app: App, root: HTMLElement }> = []

function mountSection(onUpdateShowGithubLink = vi.fn()) {
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp(SiteInfoSection, {
    siteName: 'Aether',
    siteSubtitle: 'AI Gateway',
    showGithubLink: false,
    loading: false,
    hasChanges: true,
    onSave: vi.fn(),
    'onUpdate:siteName': vi.fn(),
    'onUpdate:siteSubtitle': vi.fn(),
    'onUpdate:showGithubLink': onUpdateShowGithubLink,
  })
  app.mount(root)
  mountedApps.push({ app, root })
  return { root, onUpdateShowGithubLink }
}

afterEach(() => {
  for (const { app, root } of mountedApps.splice(0)) {
    app.unmount()
    root.remove()
  }
  document.body.innerHTML = ''
})

describe('SiteInfoSection', () => {
  it('renders and emits the github link display switch', async () => {
    const { root, onUpdateShowGithubLink } = mountSection()
    await nextTick()

    expect(root.textContent).toContain('GitHub 仓库入口')
    const switchButton = root.querySelector('[role="switch"]') as HTMLButtonElement | null
    expect(switchButton?.getAttribute('aria-checked')).toBe('false')

    switchButton?.click()
    expect(onUpdateShowGithubLink).toHaveBeenCalledWith(true)
  })
})
