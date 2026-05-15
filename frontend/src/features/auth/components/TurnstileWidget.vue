<template>
  <div class="space-y-2">
    <div
      ref="containerRef"
      class="min-h-[65px]"
      :class="disabled ? 'pointer-events-none opacity-60' : ''"
    />
    <p
      v-if="errorMessage"
      class="text-xs text-destructive"
    >
      {{ errorMessage }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'

const TURNSTILE_SCRIPT_URL = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit'

let loadTurnstilePromise: Promise<void> | null = null

interface TurnstileApi {
  render: (container: HTMLElement, options: Record<string, unknown>) => string
  reset: (widgetId: string) => void
  remove: (widgetId: string) => void
}

declare global {
  interface Window {
    turnstile?: TurnstileApi
  }
}

const props = withDefaults(defineProps<{
  modelValue?: string
  siteKey: string
  disabled?: boolean
}>(), {
  modelValue: '',
  disabled: false,
})

const emit = defineEmits<{
  'update:modelValue': [value: string]
  error: [message: string]
}>()

const containerRef = ref<HTMLElement | null>(null)
const widgetId = ref<string | null>(null)
const errorMessage = ref('')

function loadTurnstileScript(): Promise<void> {
  if (window.turnstile) {
    return Promise.resolve()
  }
  if (loadTurnstilePromise) {
    return loadTurnstilePromise
  }

  loadTurnstilePromise = new Promise((resolve, reject) => {
    const existing = document.querySelector<HTMLScriptElement>(
      `script[src="${TURNSTILE_SCRIPT_URL}"]`
    )
    if (existing) {
      existing.addEventListener('load', () => resolve(), { once: true })
      existing.addEventListener('error', () => {
        existing.remove()
        loadTurnstilePromise = null
        reject(new Error('turnstile script failed'))
      }, { once: true })
      return
    }

    const script = document.createElement('script')
    script.src = TURNSTILE_SCRIPT_URL
    script.async = true
    script.defer = true
    script.onload = () => resolve()
    script.onerror = () => {
      script.remove()
      loadTurnstilePromise = null
      reject(new Error('turnstile script failed'))
    }
    document.head.appendChild(script)
  })

  return loadTurnstilePromise
}

function clearWidget() {
  if (widgetId.value && window.turnstile) {
    window.turnstile.remove(widgetId.value)
  }
  widgetId.value = null
  emit('update:modelValue', '')
}

async function renderWidget() {
  if (!props.siteKey || !containerRef.value) return
  clearWidget()
  errorMessage.value = ''
  try {
    await loadTurnstileScript()
    await nextTick()
    if (!window.turnstile || !containerRef.value) return
    widgetId.value = window.turnstile.render(containerRef.value, {
      sitekey: props.siteKey,
      callback: (token: string) => {
        errorMessage.value = ''
        emit('update:modelValue', token)
      },
      'expired-callback': () => {
        emit('update:modelValue', '')
      },
      'error-callback': () => {
        const message = '人机验证加载失败，请重试'
        errorMessage.value = message
        emit('update:modelValue', '')
        emit('error', message)
      },
    })
  } catch {
    const message = '人机验证加载失败，请重试'
    errorMessage.value = message
    emit('update:modelValue', '')
    emit('error', message)
  }
}

function reset() {
  emit('update:modelValue', '')
  errorMessage.value = ''
  if (widgetId.value && window.turnstile) {
    window.turnstile.reset(widgetId.value)
    return
  }
  void renderWidget()
}

onMounted(() => {
  void renderWidget()
})

onBeforeUnmount(() => {
  if (widgetId.value && window.turnstile) {
    window.turnstile.remove(widgetId.value)
  }
})

watch(() => props.siteKey, () => {
  void renderWidget()
})

defineExpose({ reset })
</script>
