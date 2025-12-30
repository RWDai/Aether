<template>
  <Dialog
    v-model="isOpen"
    size="lg"
  >
    <div class="space-y-6">
      <!-- Logo 和标题 -->
      <div class="flex flex-col items-center text-center">
        <div class="mb-4 rounded-3xl border border-primary/30 dark:border-[#cc785c]/30 bg-primary/5 dark:bg-transparent p-4 shadow-inner shadow-white/40 dark:shadow-[#cc785c]/10">
          <img
            src="/aether_adaptive.svg"
            alt="Logo"
            class="h-16 w-16"
          >
        </div>
        <h2 class="text-2xl font-semibold text-slate-900 dark:text-white">
          欢迎回来
        </h2>
      </div>

      <!-- Demo 模式提示 -->
      <div
        v-if="isDemo"
        class="rounded-lg border border-primary/20 dark:border-primary/30 bg-primary/5 dark:bg-primary/10 p-4"
      >
        <div class="flex items-start gap-3">
          <div class="flex-shrink-0 text-primary dark:text-primary/90">
            <svg
              class="h-5 w-5"
              viewBox="0 0 20 20"
              fill="currentColor"
            >
              <path
                fill-rule="evenodd"
                d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a.75.75 0 000 1.5h.253a.25.25 0 01.244.304l-.459 2.066A1.75 1.75 0 0010.747 15H11a.75.75 0 000-1.5h-.253a.25.25 0 01-.244-.304l.459-2.066A1.75 1.75 0 009.253 9H9z"
                clip-rule="evenodd"
              />
            </svg>
          </div>
          <div class="flex-1 min-w-0">
            <p class="text-sm font-medium text-foreground">
              演示模式
            </p>
            <p class="mt-1 text-xs text-muted-foreground">
              当前处于演示模式，所有数据均为模拟数据。
            </p>
            <div class="mt-3 space-y-2">
              <button
                type="button"
                class="flex items-center gap-2 text-xs text-muted-foreground hover:text-foreground transition-colors group"
                @click="fillDemoAccount('admin')"
              >
                <span class="inline-flex items-center justify-center w-5 h-5 rounded bg-primary/20 dark:bg-primary/30 text-primary text-[10px] font-bold group-hover:bg-primary/30 dark:group-hover:bg-primary/40 transition-colors">A</span>
                <span>管理员：admin@demo.aether.io / demo123</span>
              </button>
              <button
                type="button"
                class="flex items-center gap-2 text-xs text-muted-foreground hover:text-foreground transition-colors group"
                @click="fillDemoAccount('user')"
              >
                <span class="inline-flex items-center justify-center w-5 h-5 rounded bg-muted text-muted-foreground text-[10px] font-bold group-hover:bg-muted/80 transition-colors">U</span>
                <span>普通用户：user@demo.aether.io / demo123</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- 登录表单 -->
      <form
        class="space-y-4"
        @submit.prevent="handleLogin"
      >
        <div class="space-y-2">
          <Label for="login-email">邮箱</Label>
          <Input
            id="login-email"
            v-model="form.email"
            type="email"
            required
            placeholder="hello@example.com"
            autocomplete="off"
          />
        </div>

        <div class="space-y-2">
          <Label for="login-password">密码</Label>
          <Input
            id="login-password"
            v-model="form.password"
            type="password"
            required
            placeholder="********"
            autocomplete="off"
            @keyup.enter="handleLogin"
          />
        </div>

        <!-- 提示信息 -->
        <p
          v-if="!isDemo && !allowRegistration"
          class="text-xs text-slate-400 dark:text-muted-foreground/80"
        >
          如需开通账户，请联系管理员配置访问权限
        </p>
      </form>

      <!-- 注册链接 -->
      <div
        v-if="allowRegistration"
        class="mt-4 text-center text-sm"
      >
        还没有账户？
        <Button
          variant="link"
          class="h-auto p-0"
          @click="handleSwitchToRegister"
        >
          立即注册
        </Button>
      </div>
    </div>

    <template #footer>
      <Button
        type="button"
        variant="outline"
        class="w-full sm:w-auto border-slate-200 dark:border-slate-600 text-slate-500 dark:text-slate-400 hover:text-primary hover:border-primary/50 hover:bg-primary/5 dark:hover:text-primary dark:hover:border-primary/50 dark:hover:bg-primary/10"
        @click="isOpen = false"
      >
        取消
      </Button>
      <Button
        :disabled="authStore.loading"
        class="w-full sm:w-auto bg-primary hover:bg-primary/90 text-white border-0"
        @click="handleLogin"
      >
        {{ authStore.loading ? '登录中...' : '登录' }}
      </Button>
    </template>
  </Dialog>

  <!-- Register Dialog -->
  <RegisterDialog
    v-model:open="showRegisterDialog"
    :require-email-verification="requireEmailVerification"
    @success="handleRegisterSuccess"
    @switch-to-login="handleSwitchToLogin"
  />
</template>

<script setup lang="ts">
import { ref, watch, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Dialog } from '@/components/ui'
import Button from '@/components/ui/button.vue'
import Input from '@/components/ui/input.vue'
import Label from '@/components/ui/label.vue'
import { useAuthStore } from '@/stores/auth'
import { useToast } from '@/composables/useToast'
import { isDemoMode, DEMO_ACCOUNTS } from '@/config/demo'
import RegisterDialog from './RegisterDialog.vue'
import { authApi } from '@/api/auth'

const props = defineProps<{
  modelValue: boolean
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
}>()

const router = useRouter()
const authStore = useAuthStore()
const { success: showSuccess, warning: showWarning, error: showError } = useToast()

const isOpen = ref(props.modelValue)
const isDemo = computed(() => isDemoMode())
const showRegisterDialog = ref(false)
const requireEmailVerification = ref(false)
const allowRegistration = ref(false) // 由系统配置控制，默认关闭

watch(() => props.modelValue, (val) => {
  isOpen.value = val
  // 打开对话框时重置表单
  if (val) {
    form.value = {
      email: '',
      password: ''
    }
  }
})

watch(isOpen, (val) => {
  emit('update:modelValue', val)
})

const form = ref({
  email: '',
  password: ''
})

function fillDemoAccount(type: 'admin' | 'user') {
  const account = DEMO_ACCOUNTS[type]
  form.value.email = account.email
  form.value.password = account.password
}

async function handleLogin() {
  if (!form.value.email || !form.value.password) {
    showWarning('请输入邮箱和密码')
    return
  }

  const success = await authStore.login(form.value.email, form.value.password)
  if (success) {
    showSuccess('登录成功，正在跳转...')

    // 关闭对话框
    isOpen.value = false

    // 延迟一下让用户看到成功消息
    setTimeout(() => {
      // 根据用户角色跳转到不同的仪表盘
      const targetPath = authStore.user?.role === 'admin' ? '/admin/dashboard' : '/dashboard'
      router.push(targetPath)
    }, 1000)
  } else {
    showError(authStore.error || '登录失败，请检查邮箱和密码')
  }
}

function handleSwitchToRegister() {
  isOpen.value = false
  showRegisterDialog.value = true
}

function handleRegisterSuccess() {
  showRegisterDialog.value = false
  showSuccess('注册成功！请登录')
  isOpen.value = true
}

function handleSwitchToLogin() {
  showRegisterDialog.value = false
  isOpen.value = true
}

// Load registration settings on mount
onMounted(async () => {
  try {
    const settings = await authApi.getRegistrationSettings()
    allowRegistration.value = !!settings.enable_registration
    requireEmailVerification.value = !!settings.require_email_verification
  } catch (error) {
    // If获取失败，保持默认：关闭注册 & 关闭邮箱验证
    allowRegistration.value = false
    requireEmailVerification.value = false
  }
})
</script>
