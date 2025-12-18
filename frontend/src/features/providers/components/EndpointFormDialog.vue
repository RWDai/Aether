<template>
  <Dialog
    :model-value="internalOpen"
    :title="isEditMode ? '编辑 API 端点' : '添加 API 端点'"
    :description="isEditMode ? `修改 ${provider?.display_name} 的端点配置` : '为提供商添加新的 API 端点'"
    :icon="isEditMode ? SquarePen : Link"
    size="xl"
    @update:model-value="handleDialogUpdate"
  >
    <form
      class="space-y-6"
      @submit.prevent="handleSubmit"
    >
      <!-- API 配置 -->
      <div class="space-y-4">
        <h3
          v-if="isEditMode"
          class="text-sm font-medium"
        >
          API 配置
        </h3>

        <div class="grid grid-cols-2 gap-4">
          <!-- API 格式 -->
          <div class="space-y-2">
            <Label for="api_format">API 格式 *</Label>
            <template v-if="isEditMode">
              <Input
                id="api_format"
                v-model="form.api_format"
                disabled
                class="bg-muted"
              />
              <p class="text-xs text-muted-foreground">
                API 格式创建后不可修改
              </p>
            </template>
            <template v-else>
              <Select
                v-model="form.api_format"
                v-model:open="selectOpen"
                required
              >
                <SelectTrigger>
                  <SelectValue placeholder="请选择 API 格式" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem
                    v-for="format in apiFormats"
                    :key="format.value"
                    :value="format.value"
                  >
                    {{ format.label }}
                  </SelectItem>
                </SelectContent>
              </Select>
            </template>
          </div>

          <!-- API URL -->
          <div class="space-y-2">
            <Label for="base_url">API URL *</Label>
            <Input
              id="base_url"
              v-model="form.base_url"
              placeholder="https://api.example.com"
              required
            />
          </div>
        </div>

        <!-- 自定义路径 -->
        <div class="space-y-2">
          <Label for="custom_path">自定义请求路径（可选）</Label>
          <Input
            id="custom_path"
            v-model="form.custom_path"
            :placeholder="defaultPathPlaceholder"
          />
        </div>
      </div>

      <!-- 请求配置 -->
      <div class="space-y-4">
        <h3 class="text-sm font-medium">
          请求配置
        </h3>

        <div class="grid grid-cols-3 gap-4">
          <div class="space-y-2">
            <Label for="timeout">超时（秒）</Label>
            <Input
              id="timeout"
              v-model.number="form.timeout"
              type="number"
              placeholder="300"
            />
          </div>

          <div class="space-y-2">
            <Label for="max_retries">最大重试</Label>
            <Input
              id="max_retries"
              v-model.number="form.max_retries"
              type="number"
              placeholder="3"
            />
          </div>

          <div class="space-y-2">
            <Label for="max_concurrent">最大并发</Label>
            <Input
              id="max_concurrent"
              :model-value="form.max_concurrent ?? ''"
              type="number"
              placeholder="无限制"
              @update:model-value="(v) => form.max_concurrent = parseNumberInput(v)"
            />
          </div>
        </div>

        <div class="grid grid-cols-2 gap-4">
          <div class="space-y-2">
            <Label for="rate_limit">速率限制（请求/分钟）</Label>
            <Input
              id="rate_limit"
              :model-value="form.rate_limit ?? ''"
              type="number"
              placeholder="无限制"
              @update:model-value="(v) => form.rate_limit = parseNumberInput(v)"
            />
          </div>
        </div>
      </div>

      <!-- 代理配置 -->
      <div class="space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-sm font-medium">
            代理配置
          </h3>
          <label class="flex items-center gap-2 text-sm">
            <input
              v-model="proxyEnabled"
              type="checkbox"
              class="h-4 w-4 rounded border-gray-300"
            >
            启用代理
          </label>
        </div>

        <div
          v-if="proxyEnabled"
          class="space-y-4 rounded-lg border p-4"
        >
          <div class="space-y-2">
            <Label for="proxy_url">代理 URL *</Label>
            <Input
              id="proxy_url"
              v-model="form.proxy_url"
              placeholder="http://host:port 或 socks5://host:port"
            />
            <p class="text-xs text-muted-foreground">
              支持 HTTP、HTTPS、SOCKS4、SOCKS5 代理
            </p>
          </div>

          <div class="grid grid-cols-2 gap-4">
            <div class="space-y-2">
              <Label for="proxy_username">用户名（可选）</Label>
              <Input
                id="proxy_username"
                v-model="form.proxy_username"
                placeholder="代理认证用户名"
              />
            </div>

            <div class="space-y-2">
              <Label for="proxy_password">密码（可选）</Label>
              <Input
                id="proxy_password"
                v-model="form.proxy_password"
                type="password"
                placeholder="代理认证密码"
              />
            </div>
          </div>
        </div>
      </div>
    </form>

    <template #footer>
      <Button
        type="button"
        variant="outline"
        :disabled="loading"
        @click="handleCancel"
      >
        取消
      </Button>
      <Button
        :disabled="loading || !form.base_url || (!isEditMode && !form.api_format)"
        @click="handleSubmit"
      >
        {{ loading ? (isEditMode ? '保存中...' : '创建中...') : (isEditMode ? '保存修改' : '创建') }}
      </Button>
    </template>
  </Dialog>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import {
  Dialog,
  Button,
  Input,
  Label,
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from '@/components/ui'
import { Link, SquarePen } from 'lucide-vue-next'
import { useToast } from '@/composables/useToast'
import { useFormDialog } from '@/composables/useFormDialog'
import { parseNumberInput } from '@/utils/form'
import { log } from '@/utils/logger'
import {
  createEndpoint,
  updateEndpoint,
  type ProviderEndpoint,
  type ProviderWithEndpointsSummary
} from '@/api/endpoints'
import { adminApi } from '@/api/admin'

const props = defineProps<{
  modelValue: boolean
  provider: ProviderWithEndpointsSummary | null
  endpoint?: ProviderEndpoint | null  // 编辑模式时传入
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'endpointCreated': []
  'endpointUpdated': []
}>()

const { success, error: showError } = useToast()
const loading = ref(false)
const selectOpen = ref(false)
const proxyEnabled = ref(false)

// 内部状态
const internalOpen = computed(() => props.modelValue)

// 表单数据
const form = ref({
  api_format: '',
  base_url: '',
  custom_path: '',
  timeout: 300,
  max_retries: 3,
  max_concurrent: undefined as number | undefined,
  rate_limit: undefined as number | undefined,
  is_active: true,
  // 代理配置
  proxy_url: '',
  proxy_username: '',
  proxy_password: '',
})

// API 格式列表
const apiFormats = ref<Array<{ value: string; label: string; default_path: string; aliases: string[] }>>([])

// 加载API格式列表
const loadApiFormats = async () => {
  try {
    const response = await adminApi.getApiFormats()
    apiFormats.value = response.formats
  } catch (error) {
    log.error('加载API格式失败:', error)
    if (!isEditMode.value) {
      showError('加载API格式失败', '错误')
    }
  }
}

// 根据选择的 API 格式计算默认路径
const defaultPath = computed(() => {
  const format = apiFormats.value.find(f => f.value === form.value.api_format)
  return format?.default_path || '/'
})

// 动态 placeholder
const defaultPathPlaceholder = computed(() => {
  return `留空使用默认路径：${defaultPath.value}`
})

// 组件挂载时加载API格式
onMounted(() => {
  loadApiFormats()
})

// 重置表单
function resetForm() {
  form.value = {
    api_format: '',
    base_url: '',
    custom_path: '',
    timeout: 300,
    max_retries: 3,
    max_concurrent: undefined,
    rate_limit: undefined,
    is_active: true,
    proxy_url: '',
    proxy_username: '',
    proxy_password: '',
  }
  proxyEnabled.value = false
}

// 加载端点数据（编辑模式）
function loadEndpointData() {
  if (!props.endpoint) return

  const proxy = props.endpoint.proxy as { url?: string; username?: string; password?: string } | null

  form.value = {
    api_format: props.endpoint.api_format,
    base_url: props.endpoint.base_url,
    custom_path: props.endpoint.custom_path || '',
    timeout: props.endpoint.timeout,
    max_retries: props.endpoint.max_retries,
    max_concurrent: props.endpoint.max_concurrent || undefined,
    rate_limit: props.endpoint.rate_limit || undefined,
    is_active: props.endpoint.is_active,
    proxy_url: proxy?.url || '',
    proxy_username: proxy?.username || '',
    proxy_password: proxy?.password || '',
  }

  // 如果有代理配置，启用代理开关
  proxyEnabled.value = !!proxy?.url
}

// 使用 useFormDialog 统一处理对话框逻辑
const { isEditMode, handleDialogUpdate, handleCancel } = useFormDialog({
  isOpen: () => props.modelValue,
  entity: () => props.endpoint,
  isLoading: loading,
  onClose: () => emit('update:modelValue', false),
  loadData: loadEndpointData,
  resetForm,
})

// 构建代理配置
function buildProxyConfig() {
  if (!proxyEnabled.value || !form.value.proxy_url) {
    return undefined
  }
  return {
    url: form.value.proxy_url,
    username: form.value.proxy_username || undefined,
    password: form.value.proxy_password || undefined,
  }
}

// 提交表单
const handleSubmit = async () => {
  if (!props.provider && !props.endpoint) return

  loading.value = true
  try {
    const proxyConfig = buildProxyConfig()

    if (isEditMode.value && props.endpoint) {
      // 更新端点
      await updateEndpoint(props.endpoint.id, {
        base_url: form.value.base_url,
        custom_path: form.value.custom_path || undefined,
        timeout: form.value.timeout,
        max_retries: form.value.max_retries,
        max_concurrent: form.value.max_concurrent,
        rate_limit: form.value.rate_limit,
        is_active: form.value.is_active,
        proxy: proxyConfig,
      })

      success('端点已更新', '保存成功')
      emit('endpointUpdated')
    } else if (props.provider) {
      // 创建端点
      await createEndpoint(props.provider.id, {
        provider_id: props.provider.id,
        api_format: form.value.api_format,
        base_url: form.value.base_url,
        custom_path: form.value.custom_path || undefined,
        timeout: form.value.timeout,
        max_retries: form.value.max_retries,
        max_concurrent: form.value.max_concurrent,
        rate_limit: form.value.rate_limit,
        is_active: form.value.is_active,
        proxy: proxyConfig,
      })

      success('端点创建成功', '成功')
      emit('endpointCreated')
      resetForm()
    }

    emit('update:modelValue', false)
  } catch (error: any) {
    const action = isEditMode.value ? '更新' : '创建'
    showError(error.response?.data?.detail || `${action}端点失败`, '错误')
  } finally {
    loading.value = false
  }
}
</script>
