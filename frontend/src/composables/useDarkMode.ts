import { effectScope, ref, watch, computed } from 'vue'

const THEME_STORAGE_KEY = 'theme'

// 主题模式类型
export type ThemeMode = 'system' | 'light' | 'dark'

// 全局共享的状态
const themeMode = ref<ThemeMode>('system')
const isDark = ref(false)
let initialized = false
let scope: ReturnType<typeof effectScope> | null = null
let mediaQuery: MediaQueryList | null = null

const applyDarkMode = (value: boolean) => {
  if (typeof document === 'undefined') {
    return
  }

  document.documentElement.classList.toggle('dark', value)

  if (document.body) {
    document.body.setAttribute('theme-mode', value ? 'dark' : 'light')
  }
}

const getSystemPreference = (): boolean => {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') {
    return false
  }
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

const getThemeStorage = (): Storage | null => {
  if (typeof window === 'undefined') {
    return null
  }

  const storage = window.localStorage
  if (!storage || typeof storage.getItem !== 'function' || typeof storage.setItem !== 'function') {
    return null
  }

  return storage
}

const readStoredTheme = (): ThemeMode | null => {
  try {
    const value = getThemeStorage()?.getItem(THEME_STORAGE_KEY)
    return value === 'dark' || value === 'light' || value === 'system' ? value : null
  } catch {
    return null
  }
}

const writeStoredTheme = (value: ThemeMode) => {
  try {
    getThemeStorage()?.setItem(THEME_STORAGE_KEY, value)
  } catch {
    // Ignore storage failures in restricted or test-like environments.
  }
}

const updateDarkMode = () => {
  if (themeMode.value === 'system') {
    isDark.value = getSystemPreference()
  } else {
    isDark.value = themeMode.value === 'dark'
  }
  applyDarkMode(isDark.value)
}

const handleSystemChange = (e: MediaQueryListEvent) => {
  if (themeMode.value === 'system') {
    isDark.value = e.matches
    applyDarkMode(isDark.value)
  }
}

const ensureWatcher = () => {
  if (scope) {
    return
  }

  scope = effectScope(true)
  scope.run(() => {
    watch(
      themeMode,
      (value) => {
        updateDarkMode()

        writeStoredTheme(value)
      },
      { flush: 'post' }
    )
  })
}

const initialize = () => {
  if (initialized) {
    return
  }

  initialized = true
  ensureWatcher()

  if (typeof window !== 'undefined') {
    const storedTheme = readStoredTheme()

    if (storedTheme) {
      themeMode.value = storedTheme
    } else {
      // 兼容旧版本存储格式，旧版本直接存储 'dark' 或 'light'
      themeMode.value = 'system'
    }

    // 监听系统主题变化
    if (typeof window.matchMedia === 'function') {
      mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
      mediaQuery.addEventListener('change', handleSystemChange)
    }
  }

  updateDarkMode()
}

export function useDarkMode() {
  initialize()
  ensureWatcher()
  applyDarkMode(isDark.value)

  const setDarkMode = (value: boolean) => {
    themeMode.value = value ? 'dark' : 'light'
  }

  const setThemeMode = (mode: ThemeMode) => {
    themeMode.value = mode
  }

  const toggleDarkMode = () => {
    // 循环切换：system -> light -> dark -> system
    if (themeMode.value === 'system') {
      themeMode.value = 'light'
    } else if (themeMode.value === 'light') {
      themeMode.value = 'dark'
    } else {
      themeMode.value = 'system'
    }
  }

  // 是否为跟随系统模式
  const isSystemMode = computed(() => themeMode.value === 'system')

  return {
    isDark,
    themeMode,
    isSystemMode,
    toggleDarkMode,
    setDarkMode,
    setThemeMode
  }
}
