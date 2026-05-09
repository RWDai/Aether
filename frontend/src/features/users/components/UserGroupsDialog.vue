<template>
  <Dialog
    :model-value="open"
    title="用户分组"
    description="管理用户组、默认注册组、成员和组级访问控制"
    size="6xl"
    persistent
    @update:model-value="handleDialogUpdate"
  >
    <div class="grid min-h-[560px] gap-4 lg:grid-cols-[17rem_minmax(0,1fr)]">
      <div class="rounded-xl border border-border/70 bg-muted/20 p-3">
        <div class="mb-3 flex items-center justify-between gap-2">
          <Label class="text-sm font-semibold">分组</Label>
          <Button
            size="sm"
            class="h-8 px-2 text-xs"
            @click="startCreate"
          >
            <Plus class="mr-1.5 h-3.5 w-3.5" />
            新建
          </Button>
        </div>

        <div
          v-if="loading"
          class="rounded-lg border border-dashed border-border/70 px-3 py-8 text-center text-xs text-muted-foreground"
        >
          正在加载...
        </div>
        <div
          v-else-if="groups.length === 0"
          class="rounded-lg border border-dashed border-border/70 px-3 py-8 text-center text-xs text-muted-foreground"
        >
          暂无分组
        </div>
        <div
          v-else
          class="space-y-1.5"
        >
          <button
            v-for="group in groups"
            :key="group.id"
            type="button"
            :class="groupButtonClass(group.id)"
            @click="selectGroup(group.id)"
          >
            <span class="min-w-0 flex-1 text-left">
              <span class="flex items-center gap-1.5">
                <span class="truncate text-sm font-medium">{{ group.name }}</span>
                <Badge
                  v-if="group.is_default"
                  variant="secondary"
                  class="h-5 px-1.5 py-0 text-[10px]"
                >
                  默认
                </Badge>
              </span>
              <span class="mt-0.5 block text-[11px] text-muted-foreground">
                优先级 {{ group.priority }}
              </span>
            </span>
            <ChevronRight class="h-4 w-4 shrink-0 text-muted-foreground" />
          </button>
        </div>
      </div>

      <div class="min-w-0 rounded-xl border border-border/70 bg-background p-4">
        <div class="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div class="min-w-0">
            <h4 class="truncate text-base font-semibold text-foreground">
              {{ editingGroupId ? '编辑分组' : '新建分组' }}
            </h4>
            <p class="text-xs text-muted-foreground">
              {{ selectedGroup?.is_default ? '当前为自助注册默认组' : '默认组只影响本地注册和 OAuth 自动创建用户' }}
            </p>
          </div>
          <div class="flex items-center gap-2">
            <Button
              v-if="editingGroupId"
              variant="outline"
              size="sm"
              class="h-8 border-rose-200 px-2 text-xs text-rose-600 hover:bg-rose-50 dark:border-rose-900/60 dark:hover:bg-rose-950/40"
              :disabled="saving"
              @click="deleteSelectedGroup"
            >
              <Trash2 class="mr-1.5 h-3.5 w-3.5" />
              删除
            </Button>
          </div>
        </div>

        <div class="grid gap-5 lg:grid-cols-2">
          <div class="space-y-4">
            <div class="grid gap-3 sm:grid-cols-[minmax(0,1fr)_8rem]">
              <div class="space-y-2">
                <Label class="text-sm font-medium">名称</Label>
                <Input
                  v-model="form.name"
                  class="h-10"
                  placeholder="例如：生产团队"
                />
              </div>
              <div class="space-y-2">
                <Label class="text-sm font-medium">优先级</Label>
                <Input
                  :model-value="form.priority"
                  type="number"
                  class="h-10"
                  @update:model-value="(value) => form.priority = parseNumberInput(value, { min: -10000, max: 10000 }) ?? 0"
                />
              </div>
            </div>

            <div class="flex items-center justify-between gap-3 rounded-lg border border-border/70 bg-muted/20 px-3 py-2">
              <div class="min-w-0">
                <Label class="text-sm font-medium">默认注册组</Label>
                <div class="mt-0.5 text-[11px] text-muted-foreground">
                  本地注册 / OAuth 自动创建
                </div>
              </div>
              <Switch
                v-model="form.is_default"
                class="shrink-0"
              />
            </div>

            <div class="space-y-2">
              <Label class="text-sm font-medium">描述</Label>
              <Textarea
                v-model="form.description"
                class="min-h-20"
                placeholder="可选"
              />
            </div>

            <div class="space-y-2">
              <Label class="text-sm font-medium">成员</Label>
              <MultiSelect
                v-model="memberUserIds"
                :options="userOptions"
                :search-threshold="0"
                placeholder="选择用户"
                empty-text="暂无用户"
                no-results-text="未找到匹配用户"
              />
            </div>
          </div>

          <div class="space-y-4 lg:border-l lg:border-border/60 lg:pl-5">
            <div class="flex items-baseline justify-between gap-2 pb-2 border-b border-border/60">
              <span class="text-sm font-medium">组权限</span>
              <span class="text-[11px] text-muted-foreground">
                用户选择继承时按优先级取首个已配置组
              </span>
            </div>

            <PolicyFieldEditor
              v-model:mode="form.allowed_providers_mode"
              v-model:values="form.allowed_providers"
              label="允许的提供商"
              :options="providerOptions"
            />
            <PolicyFieldEditor
              v-model:mode="form.allowed_api_formats_mode"
              v-model:values="form.allowed_api_formats"
              label="允许的端点"
              :options="apiFormatOptions"
            />
            <PolicyFieldEditor
              v-model:mode="form.allowed_models_mode"
              v-model:values="form.allowed_models"
              label="允许的模型"
              :options="modelOptions"
            />

            <div class="space-y-2">
              <Label class="text-sm font-medium">速率限制 (请求/分钟)</Label>
              <div class="flex items-start gap-2">
                <div class="w-28 shrink-0">
                  <Select v-model="form.rate_limit_mode">
                    <SelectTrigger class="h-10 w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="inherit">不配置</SelectItem>
                      <SelectItem value="system">系统默认</SelectItem>
                      <SelectItem value="custom">指定数值</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div class="min-w-0 flex-1">
                  <Input
                    :model-value="form.rate_limit ?? ''"
                    type="number"
                    min="0"
                    max="10000"
                    class="h-10"
                    :disabled="form.rate_limit_mode !== 'custom'"
                    :placeholder="rateLimitPlaceholder"
                    @update:model-value="(value) => form.rate_limit = parseNumberInput(value, { min: 0, max: 10000 })"
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <template #footer>
      <Button
        variant="outline"
        :disabled="saving"
        @click="emit('close')"
      >
        关闭
      </Button>
      <Button
        :disabled="saving || !form.name.trim()"
        @click="saveGroup"
      >
        {{ saving ? '保存中...' : '保存分组' }}
      </Button>
    </template>
  </Dialog>
</template>

<script setup lang="ts">
import { computed, defineComponent, h, ref, watch } from 'vue'
import { ChevronRight, Plus, Trash2 } from 'lucide-vue-next'
import {
  Badge,
  Button,
  Dialog,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Switch,
  Textarea,
} from '@/components/ui'
import { MultiSelect } from '@/components/common'
import { useUsersStore } from '@/stores/users'
import { useToast } from '@/composables/useToast'
import { useConfirm } from '@/composables/useConfirm'
import { parseApiError } from '@/utils/errorParser'
import { parseNumberInput } from '@/utils/form'
import { cn } from '@/lib/utils'
import { useUserAccessControlOptions } from '@/features/users/composables/useUserAccessControlOptions'
import type {
  ListPolicyMode,
  RateLimitPolicyMode,
  UpsertUserGroupRequest,
  User,
  UserGroup,
} from '@/api/users'

const PolicyFieldEditor = defineComponent({
  name: 'PolicyFieldEditor',
  props: {
    label: { type: String, required: true },
    mode: { type: String as () => ListPolicyMode, required: true },
    values: { type: Array as () => string[], required: true },
    options: { type: Array as () => Array<{ label: string; value: string }>, required: true },
  },
  emits: ['update:mode', 'update:values'],
  setup(props, { emit }) {
    return () => h('div', { class: 'space-y-2' }, [
      h(Label, { class: 'text-sm font-medium' }, () => props.label),
      h('div', { class: 'flex items-start gap-2' }, [
        h('div', { class: 'w-28 shrink-0' }, [
          h(Select, {
            modelValue: props.mode,
            'onUpdate:modelValue': (value: string) => emit('update:mode', value),
          }, () => [
            h(SelectTrigger, { class: 'h-10 w-full' }, () => h(SelectValue)),
            h(SelectContent, null, () => [
              h(SelectItem, { value: 'inherit' }, () => '不配置'),
              h(SelectItem, { value: 'unrestricted' }, () => '不限制'),
              h(SelectItem, { value: 'specific' }, () => '指定列表'),
              h(SelectItem, { value: 'deny_all' }, () => '全部禁用'),
            ]),
          ]),
        ]),
        h('div', { class: 'min-w-0 flex-1' }, [
          h(MultiSelect, {
            modelValue: props.values,
            'onUpdate:modelValue': (value: string[]) => emit('update:values', value),
            options: props.options,
            disabled: props.mode !== 'specific',
            searchThreshold: 0,
            placeholder: listPolicyValuePlaceholder(props.mode),
            emptyText: '暂无选项',
            dropdownMinWidth: '16rem',
          }),
        ]),
      ]),
    ])
  },
})

function listPolicyValuePlaceholder(mode: ListPolicyMode): string {
  switch (mode) {
    case 'inherit':
      return '该组不配置此项'
    case 'unrestricted':
      return '不限制所有选项'
    case 'deny_all':
      return '全部禁用'
    case 'specific':
    default:
      return '未选择时表示全部禁用'
  }
}

const props = defineProps<{
  open: boolean
  users: User[]
}>()

const emit = defineEmits<{
  close: []
  changed: []
}>()

const usersStore = useUsersStore()
const { success, error } = useToast()
const { confirmDanger } = useConfirm()
const {
  providerOptions,
  apiFormatOptions,
  modelOptions,
  loadAccessControlOptions,
} = useUserAccessControlOptions()

const loading = ref(false)
const saving = ref(false)
const groups = ref<UserGroup[]>([])
const defaultGroupId = ref<string | null>(null)
const editingGroupId = ref<string | null>(null)
const memberUserIds = ref<string[]>([])

const form = ref({
  name: '',
  description: '',
  priority: 0,
  is_default: false,
  allowed_providers_mode: 'inherit' as ListPolicyMode,
  allowed_api_formats_mode: 'inherit' as ListPolicyMode,
  allowed_models_mode: 'inherit' as ListPolicyMode,
  allowed_providers: [] as string[],
  allowed_api_formats: [] as string[],
  allowed_models: [] as string[],
  rate_limit_mode: 'inherit' as RateLimitPolicyMode,
  rate_limit: undefined as number | undefined,
})

const selectedGroup = computed(() => groups.value.find((group) => group.id === editingGroupId.value) ?? null)
const rateLimitPlaceholder = computed(() => {
  switch (form.value.rate_limit_mode) {
    case 'inherit':
      return '该组不配置速率'
    case 'system':
      return '使用系统默认'
    case 'custom':
    default:
      return '0 = 不限速'
  }
})
const userOptions = computed(() => props.users.map((user) => ({
  label: `${user.username}${user.email ? ` (${user.email})` : ''}`,
  value: user.id,
})))

watch(
  () => props.open,
  (open) => {
    if (!open) return
    void loadDialogData()
    void loadAccessControlOptions().catch((err) => {
      error(parseApiError(err, '加载访问控制选项失败'))
    })
  },
)

function handleDialogUpdate(value: boolean): void {
  if (!value) emit('close')
}

async function loadDialogData(): Promise<void> {
  loading.value = true
  try {
    const response = await usersStore.listUserGroups()
    groups.value = response.items
    defaultGroupId.value = response.default_group_id ?? null
    if (editingGroupId.value && !groups.value.some((group) => group.id === editingGroupId.value)) {
      editingGroupId.value = null
    }
    const nextGroup = editingGroupId.value
      ? groups.value.find((group) => group.id === editingGroupId.value) ?? null
      : groups.value[0] ?? null
    if (nextGroup) {
      await selectGroup(nextGroup.id)
    } else {
      startCreate()
    }
  } catch (err) {
    error(parseApiError(err, '加载用户分组失败'))
  } finally {
    loading.value = false
  }
}

async function selectGroup(groupId: string): Promise<void> {
  const group = groups.value.find((item) => item.id === groupId)
  if (!group) return
  editingGroupId.value = group.id
  form.value = {
    name: group.name,
    description: group.description ?? '',
    priority: group.priority,
    is_default: group.is_default === true,
    allowed_providers_mode: group.allowed_providers_mode,
    allowed_api_formats_mode: group.allowed_api_formats_mode,
    allowed_models_mode: group.allowed_models_mode,
    allowed_providers: group.allowed_providers ? [...group.allowed_providers] : [],
    allowed_api_formats: group.allowed_api_formats ? [...group.allowed_api_formats] : [],
    allowed_models: group.allowed_models ? [...group.allowed_models] : [],
    rate_limit_mode: group.rate_limit_mode,
    rate_limit: group.rate_limit ?? undefined,
  }
  try {
    const members = await usersStore.listUserGroupMembers(group.id)
    memberUserIds.value = members.map((member) => member.user_id)
  } catch (err) {
    memberUserIds.value = []
    error(parseApiError(err, '加载分组成员失败'))
  }
}

function startCreate(): void {
  editingGroupId.value = null
  form.value = {
    name: '',
    description: '',
    priority: 0,
    is_default: false,
    allowed_providers_mode: 'inherit',
    allowed_api_formats_mode: 'inherit',
    allowed_models_mode: 'inherit',
    allowed_providers: [],
    allowed_api_formats: [],
    allowed_models: [],
    rate_limit_mode: 'inherit',
    rate_limit: undefined,
  }
  memberUserIds.value = []
}

function groupButtonClass(groupId: string): string {
  return cn(
    'flex w-full items-center gap-2 rounded-lg border px-3 py-2 transition-colors',
    editingGroupId.value === groupId
      ? 'border-primary/50 bg-primary/10'
      : 'border-transparent hover:border-border hover:bg-background',
  )
}

function buildPayload(): UpsertUserGroupRequest {
  return {
    name: form.value.name.trim(),
    description: form.value.description.trim() || null,
    priority: form.value.priority,
    allowed_providers_mode: form.value.allowed_providers_mode,
    allowed_api_formats_mode: form.value.allowed_api_formats_mode,
    allowed_models_mode: form.value.allowed_models_mode,
    allowed_providers: form.value.allowed_providers_mode === 'specific'
      ? [...form.value.allowed_providers]
      : null,
    allowed_api_formats: form.value.allowed_api_formats_mode === 'specific'
      ? [...form.value.allowed_api_formats]
      : null,
    allowed_models: form.value.allowed_models_mode === 'specific'
      ? [...form.value.allowed_models]
      : null,
    rate_limit_mode: form.value.rate_limit_mode,
    rate_limit: form.value.rate_limit_mode === 'custom'
      ? (form.value.rate_limit ?? 0)
      : null,
  }
}

async function saveGroup(): Promise<void> {
  if (!form.value.name.trim()) return
  saving.value = true
  try {
    const wasDefault = selectedGroup.value?.is_default === true
    const wantsDefault = form.value.is_default
    const saved = editingGroupId.value
      ? await usersStore.updateUserGroup(editingGroupId.value, buildPayload())
      : await usersStore.createUserGroup(buildPayload())
    await usersStore.replaceUserGroupMembers(saved.id, memberUserIds.value)
    if (wantsDefault) {
      await usersStore.setDefaultUserGroup(saved.id)
    } else if (wasDefault) {
      await usersStore.setDefaultUserGroup(null)
    }
    success('用户分组已保存')
    emit('changed')
    editingGroupId.value = saved.id
    await loadDialogData()
  } catch (err) {
    error(parseApiError(err, '保存用户分组失败'))
  } finally {
    saving.value = false
  }
}

async function deleteSelectedGroup(): Promise<void> {
  if (!selectedGroup.value) return
  const group = selectedGroup.value
  const confirmed = await confirmDanger(
    `确定要删除用户分组 ${group.name} 吗？成员关系会一并清理。`,
    '删除用户分组',
  )
  if (!confirmed) return
  saving.value = true
  try {
    await usersStore.deleteUserGroup(group.id)
    success('用户分组已删除')
    emit('changed')
    editingGroupId.value = null
    await loadDialogData()
  } catch (err) {
    error(parseApiError(err, '删除用户分组失败'))
  } finally {
    saving.value = false
  }
}
</script>
