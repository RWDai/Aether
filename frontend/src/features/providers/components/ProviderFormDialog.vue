<template>
  <Dialog
    :model-value="internalOpen"
    :title="isEditMode ? '编辑提供商' : '添加提供商'"
    :description="isEditMode ? '更新提供商配置。API 端点和密钥需在详情页面单独管理。' : '创建新的提供商配置。创建后可以为其添加 API 端点和密钥。'"
    :icon="isEditMode ? SquarePen : Server"
    size="2xl"
    @update:model-value="handleDialogUpdate"
  >
    <form
      class="space-y-6"
      @submit.prevent="handleSubmit"
    >
      <!-- 基本信息 -->
      <div class="space-y-4">
        <h3 class="text-sm font-medium border-b pb-2">
          基本信息
        </h3>

        <!-- 添加模式显示提供商标识 -->
        <div
          v-if="!isEditMode"
          class="space-y-2"
        >
          <Label for="name">提供商标识 *</Label>
          <Input
            id="name"
            v-model="form.name"
            placeholder="例如: openai-primary"
            required
          />
          <p class="text-xs text-muted-foreground">
            唯一ID，创建后不可修改
          </p>
        </div>

        <div class="grid grid-cols-2 gap-4">
          <div class="space-y-2">
            <Label for="display_name">显示名称 *</Label>
            <Input
              id="display_name"
              v-model="form.display_name"
              placeholder="例如: OpenAI 主账号"
              required
            />
          </div>
          <div class="space-y-2">
            <Label for="website">主站链接</Label>
            <Input
              id="website"
              v-model="form.website"
              placeholder="https://..."
              type="url"
            />
          </div>
        </div>

        <div class="space-y-2">
          <Label for="description">描述</Label>
          <Textarea
            id="description"
            v-model="form.description"
            placeholder="提供商描述（可选）"
            rows="2"
          />
        </div>
      </div>

      <!-- 计费与限流 -->
      <div class="space-y-4">
        <h3 class="text-sm font-medium border-b pb-2">
          计费与限流
        </h3>
        <div class="grid grid-cols-2 gap-4">
          <div class="space-y-2">
            <Label>计费类型</Label>
            <Select
              v-model="form.billing_type"
              v-model:open="billingTypeSelectOpen"
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="monthly_quota">
                  月卡额度
                </SelectItem>
                <SelectItem value="pay_as_you_go">
                  按量付费
                </SelectItem>
                <SelectItem value="free_tier">
                  免费套餐
                </SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div class="space-y-2">
            <Label>RPM 限制</Label>
            <Input
              :model-value="form.rpm_limit ?? ''"
              type="number"
              min="0"
              placeholder="不限制请留空"
              @update:model-value="(v) => form.rpm_limit = parseNumberInput(v)"
            />
          </div>
        </div>

        <!-- 月卡配置 -->
        <div
          v-if="form.billing_type === 'monthly_quota'"
          class="grid grid-cols-2 gap-4 p-3 border rounded-lg bg-muted/50"
        >
          <div class="space-y-2">
            <Label class="text-xs">周期额度 (USD)</Label>
            <Input
              :model-value="form.monthly_quota_usd ?? ''"
              type="number"
              step="0.01"
              min="0"
              class="h-9"
              @update:model-value="(v) => form.monthly_quota_usd = parseNumberInput(v, { allowFloat: true })"
            />
          </div>
          <div class="space-y-2">
            <Label class="text-xs">重置周期 (天)</Label>
            <Input
              :model-value="form.quota_reset_day ?? ''"
              type="number"
              min="1"
              max="365"
              class="h-9"
              @update:model-value="(v) => form.quota_reset_day = parseNumberInput(v) ?? 30"
            />
          </div>
          <div class="space-y-2">
            <Label class="text-xs">
              周期开始时间
              <span class="text-red-500">*</span>
            </Label>
            <Input
              v-model="form.quota_last_reset_at"
              type="datetime-local"
              class="h-9"
            />
            <p class="text-xs text-muted-foreground">
              系统会自动统计从该时间点开始的使用量
            </p>
          </div>
          <div class="space-y-2">
            <Label class="text-xs">过期时间</Label>
            <Input
              v-model="form.quota_expires_at"
              type="datetime-local"
              class="h-9"
            />
            <p class="text-xs text-muted-foreground">
              留空表示永久有效
            </p>
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
        :disabled="loading || !form.display_name || (!isEditMode && !form.name)"
        @click="handleSubmit"
      >
        {{ loading ? (isEditMode ? '保存中...' : '创建中...') : (isEditMode ? '保存' : '创建') }}
      </Button>
    </template>
  </Dialog>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import {
  Dialog,
  Button,
  Input,
  Textarea,
  Label,
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from '@/components/ui'
import { Server, SquarePen } from 'lucide-vue-next'
import { useToast } from '@/composables/useToast'
import { useFormDialog } from '@/composables/useFormDialog'
import { createProvider, updateProvider, type ProviderWithEndpointsSummary } from '@/api/endpoints'
import { parseApiError } from '@/utils/errorParser'
import { parseNumberInput } from '@/utils/form'

const props = defineProps<{
  modelValue: boolean
  provider?: ProviderWithEndpointsSummary | null  // 编辑模式时传入
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'providerCreated': []
  'providerUpdated': []
}>()

const { success, error: showError } = useToast()
const loading = ref(false)
const billingTypeSelectOpen = ref(false)

// 内部状态
const internalOpen = computed(() => props.modelValue)

// 表单数据
const form = ref({
  name: '',
  display_name: '',
  description: '',
  website: '',
  // 计费配置
  billing_type: 'pay_as_you_go' as 'monthly_quota' | 'pay_as_you_go' | 'free_tier',
  monthly_quota_usd: undefined as number | undefined,
  quota_reset_day: 30,
  quota_last_reset_at: '',  // 周期开始时间
  quota_expires_at: '',
  rpm_limit: undefined as string | number | undefined,
  provider_priority: 999,
  // 状态配置
  is_active: true,
  rate_limit: undefined as number | undefined,
  concurrent_limit: undefined as number | undefined,
})

// 重置表单
function resetForm() {
  form.value = {
    name: '',
    display_name: '',
    description: '',
    website: '',
    billing_type: 'pay_as_you_go',
    monthly_quota_usd: undefined,
    quota_reset_day: 30,
    quota_last_reset_at: '',
    quota_expires_at: '',
    rpm_limit: undefined,
    provider_priority: 999,
    is_active: true,
    rate_limit: undefined,
    concurrent_limit: undefined,
  }
}

// 加载提供商数据（编辑模式）
function loadProviderData() {
  if (!props.provider) return

  form.value = {
    name: props.provider.name,
    display_name: props.provider.display_name,
    description: props.provider.description || '',
    website: props.provider.website || '',
    billing_type: (props.provider.billing_type as 'monthly_quota' | 'pay_as_you_go' | 'free_tier') || 'pay_as_you_go',
    monthly_quota_usd: props.provider.monthly_quota_usd || undefined,
    quota_reset_day: props.provider.quota_reset_day || 30,
    quota_last_reset_at: props.provider.quota_last_reset_at ?
      new Date(props.provider.quota_last_reset_at).toISOString().slice(0, 16) : '',
    quota_expires_at: props.provider.quota_expires_at ?
      new Date(props.provider.quota_expires_at).toISOString().slice(0, 16) : '',
    rpm_limit: props.provider.rpm_limit ?? undefined,
    provider_priority: props.provider.provider_priority || 999,
    is_active: props.provider.is_active,
    rate_limit: undefined,
    concurrent_limit: undefined,
  }
}

// 使用 useFormDialog 统一处理对话框逻辑
const { isEditMode, handleDialogUpdate, handleCancel } = useFormDialog({
  isOpen: () => props.modelValue,
  entity: () => props.provider,
  isLoading: loading,
  onClose: () => emit('update:modelValue', false),
  loadData: loadProviderData,
  resetForm,
})

// 提交表单
const handleSubmit = async () => {
  // 月卡类型必须设置周期开始时间
  if (form.value.billing_type === 'monthly_quota' && !form.value.quota_last_reset_at) {
    showError('月卡类型必须设置周期开始时间', '验证失败')
    return
  }

  loading.value = true
  try {
    const payload = {
      ...form.value,
      rpm_limit:
        form.value.rpm_limit === undefined || form.value.rpm_limit === ''
          ? null
          : Number(form.value.rpm_limit),
      // 空字符串时不发送
      quota_last_reset_at: form.value.quota_last_reset_at || undefined,
      quota_expires_at: form.value.quota_expires_at || undefined,
    }

    if (isEditMode.value && props.provider) {
      // 更新提供商
      await updateProvider(props.provider.id, payload)
      success('提供商更新成功')
      emit('providerUpdated')
    } else {
      // 创建提供商
      await createProvider(payload)
      success('提供商已创建，请继续添加端点和密钥，或在优先级管理中调整顺序', '创建成功')
      emit('providerCreated')
    }

    emit('update:modelValue', false)
  } catch (error: any) {
    const action = isEditMode.value ? '更新' : '创建'
    showError(parseApiError(error, `${action}提供商失败`), `${action}失败`)
  } finally {
    loading.value = false
  }
}
</script>
