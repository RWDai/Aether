<template>
  <Dialog
    v-model="isOpen"
    size="md"
    title=""
  >
    <div class="flex flex-col items-center text-center py-2">
      <!-- Logo -->
      <HeaderLogo
        size="h-16 w-16"
        class-name="text-primary"
      />

      <!-- Title -->
      <h2 class="text-xl font-semibold text-foreground mt-4 mb-2">
        发现新版本
      </h2>

      <!-- Version Info -->
      <div class="flex items-center gap-3 mb-4">
        <span class="px-3 py-1.5 rounded-lg bg-muted text-sm font-mono text-muted-foreground">
          v{{ currentVersion }}
        </span>
        <svg
          class="h-4 w-4 text-muted-foreground"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M13 7l5 5m0 0l-5 5m5-5H6"
          />
        </svg>
        <span class="px-3 py-1.5 rounded-lg bg-primary/10 text-sm font-mono font-medium text-primary">
          v{{ latestVersion }}
        </span>
      </div>

      <!-- Description -->
      <p class="text-sm text-muted-foreground max-w-xs">
        新版本已发布，建议更新以获得最新功能和安全修复
      </p>
    </div>

    <template #footer>
      <div class="flex w-full gap-3">
        <Button
          variant="outline"
          class="flex-1"
          @click="handleLater"
        >
          稍后提醒
        </Button>
        <Button
          class="flex-1"
          @click="handleViewRelease"
        >
          查看更新
        </Button>
      </div>
    </template>
  </Dialog>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { Dialog } from '@/components/ui'
import Button from '@/components/ui/button.vue'
import HeaderLogo from '@/components/HeaderLogo.vue'

const props = defineProps<{
  modelValue: boolean
  currentVersion: string
  latestVersion: string
  releaseUrl: string | null
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
}>()

const isOpen = ref(props.modelValue)

watch(() => props.modelValue, (val) => {
  isOpen.value = val
})

watch(isOpen, (val) => {
  emit('update:modelValue', val)
})

function handleLater() {
  // 记录忽略的版本，24小时内不再提醒
  const ignoreKey = 'aether_update_ignore'
  const ignoreData = {
    version: props.latestVersion,
    until: Date.now() + 24 * 60 * 60 * 1000 // 24小时
  }
  localStorage.setItem(ignoreKey, JSON.stringify(ignoreData))
  isOpen.value = false
}

function handleViewRelease() {
  if (props.releaseUrl) {
    window.open(props.releaseUrl, '_blank')
  }
  isOpen.value = false
}
</script>
