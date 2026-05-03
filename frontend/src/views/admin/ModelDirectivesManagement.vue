<template>
  <PageContainer>
    <PageHeader
      title="模型后缀参数"
      description="允许通过模型名后缀覆盖推理参数"
    >
      <template #actions>
        <Button
          variant="outline"
          :disabled="loading"
          @click="loadConfig"
        >
          <RefreshCw
            class="w-4 h-4 mr-2"
            :class="{ 'animate-spin': loading }"
          />
          刷新
        </Button>
      </template>
    </PageHeader>

    <div class="mt-6 space-y-5">
      <Card
        variant="default"
        class="p-6"
      >
        <ModelDirectivesPanel
          :config="modelDirectivesConfig"
          :loading="loading || saving"
          @save="saveConfig"
          @update:config="modelDirectivesConfig = $event"
        />
      </Card>
    </div>
  </PageContainer>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { RefreshCw } from 'lucide-vue-next'
import Button from '@/components/ui/button.vue'
import Card from '@/components/ui/card.vue'
import { PageContainer, PageHeader } from '@/components/layout'
import { adminApi } from '@/api/admin'
import { useToast } from '@/composables/useToast'
import { log } from '@/utils/logger'
import { getErrorMessage } from '@/types/api-error'
import ModelDirectivesPanel from './module-management/ModelDirectivesPanel.vue'
import {
  createDefaultModelDirectivesConfig,
  normalizeModelDirectivesConfig,
  type ModelDirectivesConfig,
} from './module-management/modelDirectivesConfig'

const { success, error } = useToast()

const modelDirectivesConfig = ref<ModelDirectivesConfig>(createDefaultModelDirectivesConfig())
const loading = ref(false)
const saving = ref(false)

async function loadConfig() {
  loading.value = true
  try {
    const response = await adminApi.getSystemConfig('model_directives')
    const normalized = normalizeModelDirectivesConfig(response.value)
    modelDirectivesConfig.value = normalized
  } catch (err) {
    error('获取模型后缀参数配置失败')
    log.error('获取模型后缀参数配置失败:', err)
  } finally {
    loading.value = false
  }
}

async function saveConfig() {
  saving.value = true
  try {
    const normalized = normalizeModelDirectivesConfig(modelDirectivesConfig.value)
    modelDirectivesConfig.value = normalized
    await adminApi.updateSystemConfig(
      'model_directives',
      normalized,
      '模型后缀参数配置'
    )
    success('模型后缀参数配置已保存')
  } catch (err) {
    error(getErrorMessage(err, '保存模型后缀参数配置失败'))
    log.error('保存模型后缀参数配置失败:', err)
  } finally {
    saving.value = false
  }
}

onMounted(() => {
  loadConfig()
})
</script>
