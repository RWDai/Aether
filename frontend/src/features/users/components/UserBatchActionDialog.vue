<template>
  <Dialog
    :model-value="open"
    title="用户批量操作"
    description="按当前选择批量调整用户状态、角色、访问控制和额度"
    size="2xl"
    persistent
    @update:model-value="handleDialogUpdate"
  >
    <div class="space-y-5">
      <div class="rounded-2xl border border-primary/15 bg-gradient-to-br from-primary/10 via-background to-muted/40 p-4 shadow-sm">
        <div class="flex flex-wrap items-start justify-between gap-3">
          <div class="min-w-0 space-y-1">
            <div class="flex items-center gap-2 text-sm font-semibold text-foreground">
              <UsersRound class="h-4 w-4 text-primary" />
              <span>影响用户：{{ impactCount }} 个</span>
            </div>
            <p class="text-xs leading-relaxed text-muted-foreground">
              {{ selectAllFiltered ? '目标为当前筛选条件匹配的全部用户，执行前后端会重新解析。' : '目标为当前已勾选的用户，重复 ID 会自动去重。' }}
            </p>
          </div>
          <Badge variant="secondary" class="shrink-0">
            {{ selectAllFiltered ? '全选筛选结果' : '手动选择' }}
          </Badge>
        </div>

        <div
          v-if="previewLoading"
          class="mt-3 rounded-xl border border-border/60 bg-background/65 px-3 py-2 text-xs text-muted-foreground"
        >
          正在解析影响范围...
        </div>
        <div
          v-else-if="previewItems.length > 0"
          class="mt-3 flex flex-wrap items-center gap-1.5"
        >
          <Badge
            v-for="item in previewItems"
            :key="item.user_id"
            variant="outline"
            class="bg-background/70 text-[11px]"
          >
            {{ item.username }}
          </Badge>
          <span
            v-if="impactCount > previewItems.length"
            class="text-xs text-muted-foreground"
          >
            等 {{ impactCount }} 个用户
          </span>
        </div>
      </div>

      <div class="space-y-2.5">
        <div class="grid gap-2 rounded-xl border border-border/70 bg-muted/20 p-3 sm:grid-cols-[9rem_minmax(0,1fr)] sm:items-start">
          <div>
            <Label class="text-sm font-medium">按分组选择</Label>
            <p class="mt-1 text-[11px] text-muted-foreground">可与直接用户或筛选条件混合</p>
          </div>
          <MultiSelect
            v-model="selectedGroupIds"
            :options="groupOptions"
            :search-threshold="0"
            placeholder="选择一个或多个分组"
            empty-text="暂无用户分组"
          />
        </div>

        <div class="flex items-center justify-between gap-3">
          <Label class="text-sm font-medium">选择批量动作</Label>
          <span class="text-[11px] text-muted-foreground">只会提交当前动作对应的字段</span>
        </div>
        <div class="grid gap-2 md:grid-cols-4">
          <button
            v-for="action in actionOptions"
            :key="action.value"
            type="button"
            :class="actionCardClass(action.value)"
            @click="selectedAction = action.value"
          >
            <span class="flex items-center gap-2">
              <span :class="actionIconClass(action.value)">
                <component :is="action.icon" class="h-4 w-4" />
              </span>
              <span class="font-medium text-foreground">{{ action.label }}</span>
            </span>
            <span class="mt-1 block text-[11px] leading-relaxed text-muted-foreground">
              {{ action.description }}
            </span>
          </button>
        </div>
      </div>

      <div
        v-if="selectedAction === 'update_role'"
        class="space-y-4 rounded-2xl border bg-background p-4 shadow-sm"
      >
        <div class="flex items-start gap-3">
          <div class="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-primary/10 text-primary">
            <UserCog class="h-4 w-4" />
          </div>
          <div class="min-w-0 space-y-1">
            <h4 class="text-sm font-semibold text-foreground">批量修改用户角色</h4>
            <p class="text-xs leading-relaxed text-muted-foreground">
              将所选用户统一调整为同一个角色。管理员角色拥有后台管理权限，请确认选择范围。
            </p>
          </div>
        </div>

        <div class="grid gap-3 rounded-xl border border-border/70 bg-muted/25 p-3 sm:grid-cols-[10rem_minmax(0,1fr)] sm:items-center">
          <div>
            <Label class="text-sm font-medium">目标角色</Label>
            <p class="mt-1 text-[11px] text-muted-foreground">对所有目标用户生效</p>
          </div>
          <Select v-model="targetRole">
            <SelectTrigger class="h-10 w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="user">普通用户</SelectItem>
              <SelectItem value="admin">管理员</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div class="rounded-xl border border-amber-200/70 bg-amber-50/70 px-3 py-2.5 text-xs leading-relaxed text-amber-800 dark:border-amber-900/50 dark:bg-amber-950/30 dark:text-amber-200">
          {{ targetRole === 'admin' ? '提示：设置为管理员会授予用户后台管理能力。' : '提示：设置为普通用户会移除目标用户的管理员权限。' }}
        </div>
      </div>

      <div
        v-if="selectedAction === 'update_access_control'"
        class="space-y-4 rounded-2xl border bg-background p-4 shadow-sm"
      >
        <div class="flex items-start gap-3">
          <div class="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-primary/10 text-primary">
            <ShieldCheck class="h-4 w-4" />
          </div>
          <div class="min-w-0 space-y-1">
            <h4 class="text-sm font-semibold text-foreground">批量设置访问控制与额度</h4>
            <p class="text-xs leading-relaxed text-muted-foreground">
              每个字段都可独立选择“不修改 / 不限制 / 指定列表”。指定列表为空表示全部禁用。
            </p>
          </div>
        </div>

        <div class="grid gap-3">
          <div class="rounded-xl border border-border/70 bg-muted/20 p-3">
            <div class="grid gap-3 lg:grid-cols-[9rem_minmax(0,1fr)] lg:items-start">
              <div>
                <Label class="text-sm font-medium">允许的提供商</Label>
                <p class="mt-1 text-[11px] text-muted-foreground">控制可使用的供应商</p>
              </div>
              <div class="grid gap-2 sm:grid-cols-[9rem_minmax(0,1fr)]">
                <Select v-model="providerMode">
                  <SelectTrigger class="h-9">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="skip">不修改</SelectItem>
                    <SelectItem value="unrestricted">不限制</SelectItem>
                    <SelectItem value="specific">指定列表</SelectItem>
                  </SelectContent>
                </Select>
                <MultiSelect
                  v-model="allowedProviders"
                  :options="providerOptions"
                  :disabled="providerMode !== 'specific'"
                  :search-threshold="0"
                  placeholder="未选择时表示全部禁用"
                  empty-text="暂无可用提供商"
                />
              </div>
            </div>
          </div>

          <div class="rounded-xl border border-border/70 bg-muted/20 p-3">
            <div class="grid gap-3 lg:grid-cols-[9rem_minmax(0,1fr)] lg:items-start">
              <div>
                <Label class="text-sm font-medium">允许的端点</Label>
                <p class="mt-1 text-[11px] text-muted-foreground">控制 API 格式入口</p>
              </div>
              <div class="grid gap-2 sm:grid-cols-[9rem_minmax(0,1fr)]">
                <Select v-model="apiFormatMode">
                  <SelectTrigger class="h-9">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="skip">不修改</SelectItem>
                    <SelectItem value="unrestricted">不限制</SelectItem>
                    <SelectItem value="specific">指定列表</SelectItem>
                  </SelectContent>
                </Select>
                <MultiSelect
                  v-model="allowedApiFormats"
                  :options="apiFormatOptions"
                  :disabled="apiFormatMode !== 'specific'"
                  :search-threshold="0"
                  placeholder="未选择时表示全部禁用"
                  empty-text="暂无可用端点"
                />
              </div>
            </div>
          </div>

          <div class="rounded-xl border border-border/70 bg-muted/20 p-3">
            <div class="grid gap-3 lg:grid-cols-[9rem_minmax(0,1fr)] lg:items-start">
              <div>
                <Label class="text-sm font-medium">允许的模型</Label>
                <p class="mt-1 text-[11px] text-muted-foreground">控制模型白名单</p>
              </div>
              <div class="grid gap-2 sm:grid-cols-[9rem_minmax(0,1fr)]">
                <Select v-model="modelMode">
                  <SelectTrigger class="h-9">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="skip">不修改</SelectItem>
                    <SelectItem value="unrestricted">不限制</SelectItem>
                    <SelectItem value="specific">指定列表</SelectItem>
                  </SelectContent>
                </Select>
                <MultiSelect
                  v-model="allowedModels"
                  :options="modelOptions"
                  :disabled="modelMode !== 'specific'"
                  :search-threshold="0"
                  placeholder="未选择时表示全部禁用"
                  empty-text="暂无可用模型"
                />
              </div>
            </div>
          </div>

          <div class="grid gap-3 md:grid-cols-2">
            <div class="rounded-xl border border-border/70 bg-muted/20 p-3">
              <div class="space-y-2">
                <div>
                  <Label class="text-sm font-medium">速率限制</Label>
                  <p class="mt-1 text-[11px] text-muted-foreground">请求/分钟，0 表示不限速</p>
                </div>
                <div class="grid gap-2 sm:grid-cols-[9rem_minmax(0,1fr)] md:grid-cols-1 xl:grid-cols-[9rem_minmax(0,1fr)]">
                  <Select v-model="rateLimitMode">
                    <SelectTrigger class="h-9">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="skip">不修改</SelectItem>
                      <SelectItem value="inherit">跟随默认</SelectItem>
                      <SelectItem value="custom">指定数值</SelectItem>
                    </SelectContent>
                  </Select>
                  <Input
                    :model-value="rateLimit ?? ''"
                    type="number"
                    min="0"
                    max="10000"
                    class="h-9"
                    :disabled="rateLimitMode !== 'custom'"
                    placeholder="0 = 不限速"
                    @update:model-value="(value) => rateLimit = parseNumberInput(value, { min: 0, max: 10000 })"
                  />
                </div>
              </div>
            </div>

            <div class="rounded-xl border border-border/70 bg-muted/20 p-3">
              <div class="space-y-2">
                <div>
                  <Label class="text-sm font-medium">额度</Label>
                  <p class="mt-1 text-[11px] text-muted-foreground">与单用户编辑保持一致</p>
                </div>
                <Select v-model="quotaMode">
                  <SelectTrigger class="h-9 w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="skip">不修改</SelectItem>
                    <SelectItem value="wallet">按钱包余额限制</SelectItem>
                    <SelectItem value="unlimited">无限额度</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div
        v-if="lastResult"
        class="rounded-xl border bg-muted/20 px-3 py-2 text-xs text-muted-foreground"
      >
        成功 {{ lastResult.success }} 个，失败 {{ lastResult.failed }} 个
        <span v-if="lastResult.failures.length > 0">
          ：{{ lastResult.failures.slice(0, 3).map((item) => `${item.user_id} ${item.reason}`).join('；') }}
        </span>
      </div>
    </div>

    <template #footer>
      <Button
        variant="outline"
        :disabled="executing"
        @click="emit('close')"
      >
        关闭
      </Button>
      <Button
        :disabled="!canExecute"
        @click="executeBatchAction"
      >
        {{ executing ? '执行中...' : executeButtonLabel }}
      </Button>
    </template>
  </Dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch, type Component } from 'vue'
import {
  Ban,
  CheckCircle2,
  ShieldCheck,
  UserCog,
  UsersRound,
} from 'lucide-vue-next'
import {
  Dialog,
  Button,
  Badge,
  Input,
  Label,
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from '@/components/ui'
import { MultiSelect } from '@/components/common'
import { useUsersStore } from '@/stores/users'
import { useToast } from '@/composables/useToast'
import { parseApiError } from '@/utils/errorParser'
import { parseNumberInput } from '@/utils/form'
import { cn } from '@/lib/utils'
import { useUserAccessControlOptions } from '@/features/users/composables/useUserAccessControlOptions'
import type {
  UserBatchAccessControlPayload,
  UserBatchAction,
  UserBatchActionRequest,
  UserBatchActionResponse,
  UserBatchRolePayload,
  UserBatchSelection,
  UserBatchSelectionFilters,
  UserBatchSelectionItem,
  UserRole,
  UserGroup,
} from '@/api/users'

type AccessFieldMode = 'skip' | 'unrestricted' | 'specific'
type RateLimitMode = 'skip' | 'inherit' | 'custom'
type QuotaMode = 'skip' | 'wallet' | 'unlimited'

interface ActionOption {
  value: UserBatchAction
  label: string
  description: string
  icon: Component
}

const props = defineProps<{
  open: boolean
  selectedIds: string[]
  selectAllFiltered: boolean
  selectedCount: number
  filters: UserBatchSelectionFilters
  groups: UserGroup[]
}>()

const emit = defineEmits<{
  close: []
  completed: [result: UserBatchActionResponse]
}>()

const usersStore = useUsersStore()
const { success, warning, error } = useToast()
const {
  providerOptions,
  apiFormatOptions,
  modelOptions,
  loadAccessControlOptions,
} = useUserAccessControlOptions()

const actionOptions: ActionOption[] = [
  {
    value: 'enable',
    label: '启用',
    description: '恢复用户登录与调用',
    icon: CheckCircle2,
  },
  {
    value: 'disable',
    label: '禁用',
    description: '暂停用户访问权限',
    icon: Ban,
  },
  {
    value: 'update_access_control',
    label: '访问控制',
    description: '提供商、端点、模型、限速和额度',
    icon: ShieldCheck,
  },
  {
    value: 'update_role',
    label: '修改角色',
    description: '批量设为普通用户或管理员',
    icon: UserCog,
  },
]

const selectedAction = ref<UserBatchAction>('enable')
const targetRole = ref<UserRole>('user')
const providerMode = ref<AccessFieldMode>('skip')
const apiFormatMode = ref<AccessFieldMode>('skip')
const modelMode = ref<AccessFieldMode>('skip')
const rateLimitMode = ref<RateLimitMode>('skip')
const quotaMode = ref<QuotaMode>('skip')
const selectedGroupIds = ref<string[]>([])
const allowedProviders = ref<string[]>([])
const allowedApiFormats = ref<string[]>([])
const allowedModels = ref<string[]>([])
const rateLimit = ref<number | undefined>(undefined)
const previewLoading = ref(false)
const previewItems = ref<UserBatchSelectionItem[]>([])
const resolvedTotal = ref<number | null>(null)
const executing = ref(false)
const lastResult = ref<UserBatchActionResponse | null>(null)

const groupOptions = computed(() => props.groups.map((group) => ({
  label: `${group.name}${group.is_default ? '（默认）' : ''}`,
  value: group.id,
})))
const hasAnyTarget = computed(() => props.selectedCount > 0 || selectedGroupIds.value.length > 0)
const impactCount = computed(() => resolvedTotal.value ?? props.selectedCount)
const canExecute = computed(() => hasAnyTarget.value && !previewLoading.value && !executing.value)
const selectedActionLabel = computed(() => (
  actionOptions.find((action) => action.value === selectedAction.value)?.label ?? '批量操作'
))
const executeButtonLabel = computed(() => `确认${selectedActionLabel.value}（${impactCount.value}）`)

watch(
  () => props.open,
  (open) => {
    if (!open) return
    resetLocalState()
    void loadAccessControlOptions().catch((err) => {
      error(parseApiError(err, '加载访问控制选项失败'))
    })
    void resolvePreview()
  },
)

watch(
  () => [props.selectedIds, props.selectAllFiltered, props.selectedCount, props.filters] as const,
  () => {
    if (props.open) void resolvePreview()
  },
)

function handleDialogUpdate(value: boolean): void {
  if (!value) emit('close')
}

function resetLocalState(): void {
  selectedAction.value = 'enable'
  targetRole.value = 'user'
  providerMode.value = 'skip'
  apiFormatMode.value = 'skip'
  modelMode.value = 'skip'
  rateLimitMode.value = 'skip'
  quotaMode.value = 'skip'
  selectedGroupIds.value = []
  allowedProviders.value = []
  allowedApiFormats.value = []
  allowedModels.value = []
  rateLimit.value = undefined
  lastResult.value = null
}

function actionCardClass(action: UserBatchAction): string {
  return cn(
    'rounded-xl border p-3 text-left transition-all hover:-translate-y-0.5 hover:border-primary/35 hover:bg-primary/5 hover:shadow-sm focus:outline-none focus:ring-2 focus:ring-primary/30',
    selectedAction.value === action
      ? 'border-primary/60 bg-primary/10 shadow-sm ring-1 ring-primary/20'
      : 'border-border/70 bg-background',
  )
}

function actionIconClass(action: UserBatchAction): string {
  return cn(
    'flex h-7 w-7 items-center justify-center rounded-lg transition-colors',
    selectedAction.value === action
      ? 'bg-primary text-primary-foreground'
      : 'bg-muted text-muted-foreground',
  )
}

function buildSelection(): UserBatchSelection {
  const group_ids = selectedGroupIds.value.length > 0 ? [...selectedGroupIds.value] : undefined
  if (props.selectAllFiltered) {
    return { filters: props.filters, group_ids }
  }
  return { user_ids: [...props.selectedIds], group_ids }
}

async function resolvePreview(): Promise<void> {
  if (!hasAnyTarget.value) {
    resolvedTotal.value = 0
    previewItems.value = []
    return
  }
  previewLoading.value = true
  try {
    const result = await usersStore.resolveBatchSelection(buildSelection())
    resolvedTotal.value = result.total
    previewItems.value = result.items.slice(0, 6)
  } catch (err) {
    resolvedTotal.value = props.selectedCount
    previewItems.value = []
    error(parseApiError(err, '解析用户选择失败'))
  } finally {
    previewLoading.value = false
  }
}

watch(selectedGroupIds, () => {
  if (props.open) void resolvePreview()
})

function buildAccessControlPayload(): UserBatchAccessControlPayload | null {
  const payload: UserBatchAccessControlPayload = {}
  if (providerMode.value === 'unrestricted') payload.allowed_providers = null
  if (providerMode.value === 'specific') payload.allowed_providers = [...allowedProviders.value]
  if (apiFormatMode.value === 'unrestricted') payload.allowed_api_formats = null
  if (apiFormatMode.value === 'specific') payload.allowed_api_formats = [...allowedApiFormats.value]
  if (modelMode.value === 'unrestricted') payload.allowed_models = null
  if (modelMode.value === 'specific') payload.allowed_models = [...allowedModels.value]
  if (rateLimitMode.value === 'inherit') payload.rate_limit = null
  if (rateLimitMode.value === 'custom' && rateLimit.value != null) payload.rate_limit = rateLimit.value
  if (quotaMode.value === 'wallet') payload.unlimited = false
  if (quotaMode.value === 'unlimited') payload.unlimited = true
  return Object.keys(payload).length > 0 ? payload : null
}

function buildRolePayload(): UserBatchRolePayload {
  return { role: targetRole.value }
}

async function executeBatchAction(): Promise<void> {
  if (!canExecute.value) return
  if (selectedAction.value === 'update_access_control' && rateLimitMode.value === 'custom' && rateLimit.value == null) {
    warning('请输入速率限制数值，0 表示不限速')
    return
  }
  const selection = buildSelection()
  let request: UserBatchActionRequest
  if (selectedAction.value === 'update_access_control') {
    const payload = buildAccessControlPayload()
    if (payload === null) {
      warning('请至少选择一个要修改的访问控制或额度字段')
      return
    }
    request = { selection, action: 'update_access_control', payload }
  } else if (selectedAction.value === 'update_role') {
    request = { selection, action: 'update_role', payload: buildRolePayload() }
  } else {
    request = { selection, action: selectedAction.value }
  }

  executing.value = true
  try {
    const result = await usersStore.batchAction(request)
    lastResult.value = result
    const message = `批量操作完成：成功 ${result.success} 个，失败 ${result.failed} 个`
    if (result.failed > 0) {
      warning(message)
    } else {
      success(message)
    }
    emit('completed', result)
  } catch (err) {
    error(parseApiError(err, '批量操作失败'))
  } finally {
    executing.value = false
  }
}
</script>
