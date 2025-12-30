<template>
  <Dialog
    v-model:open="isOpen"
    size="lg"
  >
    <DialogContent>
      <!-- Logo -->
      <div class="flex justify-center mb-6">
        <div
          class="w-16 h-16 rounded-full border-2 border-primary/20 flex items-center justify-center bg-primary/5"
        >
          <img
            src="@/assets/logo.svg"
            alt="Logo"
            class="w-10 h-10"
          >
        </div>
      </div>

      <DialogHeader>
        <DialogTitle class="text-center text-2xl">
          注册新账户
        </DialogTitle>
        <DialogDescription class="text-center">
          请填写您的邮箱和个人信息完成注册
        </DialogDescription>
      </DialogHeader>

      <form
        class="space-y-4 mt-4"
        @submit.prevent="handleSubmit"
      >
        <!-- Email -->
        <div class="space-y-2">
          <Label for="register-email">邮箱</Label>
          <Input
            id="register-email"
            v-model="formData.email"
            type="email"
            placeholder="your@email.com"
            required
            :disabled="isLoading || emailVerified"
          />
        </div>

        <!-- Verification Code Section -->
        <div
          v-if="requireEmailVerification"
          class="space-y-2"
        >
          <div class="flex items-center justify-between">
            <Label for="verification-code">验证码</Label>
            <Button
              type="button"
              variant="link"
              size="sm"
              class="h-auto p-0 text-xs"
              :disabled="isLoading || !canSendCode || emailVerified"
              @click="handleSendCode"
            >
              {{ sendCodeButtonText }}
            </Button>
          </div>
          <VerificationCodeInput
            ref="codeInputRef"
            v-model="formData.verificationCode"
            :has-error="verificationError"
            :length="6"
            @complete="handleCodeComplete"
          />
          <p
            v-if="verificationError"
            class="text-xs text-destructive"
          >
            验证码错误，请重新输入
          </p>
          <p
            v-if="emailVerified"
            class="text-xs text-green-600"
          >
            ✓ 邮箱验证成功
          </p>
        </div>

        <!-- Username -->
        <div class="space-y-2">
          <Label for="register-username">用户名</Label>
          <Input
            id="register-username"
            v-model="formData.username"
            type="text"
            placeholder="请输入用户名"
            required
            :disabled="isLoading"
          />
        </div>

        <!-- Password -->
        <div class="space-y-2">
          <Label for="register-password">密码</Label>
          <Input
            id="register-password"
            v-model="formData.password"
            type="password"
            placeholder="至少 8 位字符"
            required
            :disabled="isLoading"
          />
          <p class="text-xs text-muted-foreground">
            密码长度至少 8 位
          </p>
        </div>

        <!-- Confirm Password -->
        <div class="space-y-2">
          <Label for="register-confirm-password">确认密码</Label>
          <Input
            id="register-confirm-password"
            v-model="formData.confirmPassword"
            type="password"
            placeholder="再次输入密码"
            required
            :disabled="isLoading"
          />
        </div>

        <DialogFooter class="gap-2">
          <Button
            type="button"
            variant="outline"
            :disabled="isLoading"
            @click="handleCancel"
          >
            取消
          </Button>
          <Button
            type="submit"
            :disabled="isLoading || !canSubmit"
          >
            <span
              v-if="isLoading"
              class="flex items-center gap-2"
            >
              <span class="animate-spin">⏳</span>
              {{ loadingText }}
            </span>
            <span v-else>注册</span>
          </Button>
        </DialogFooter>
      </form>

      <div class="mt-4 text-center text-sm">
        已有账户？
        <Button
          variant="link"
          class="h-auto p-0"
          @click="handleSwitchToLogin"
        >
          立即登录
        </Button>
      </div>
    </DialogContent>
  </Dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, onUnmounted } from 'vue'
import { authApi } from '@/api/auth'
import { useToast } from '@/composables/useToast'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  Button,
  Input,
  Label
} from '@/components/ui'
import VerificationCodeInput from '@/components/VerificationCodeInput.vue'

interface Props {
  open?: boolean
  requireEmailVerification?: boolean
}

interface Emits {
  (e: 'update:open', value: boolean): void
  (e: 'success'): void
  (e: 'switchToLogin'): void
}

const props = withDefaults(defineProps<Props>(), {
  open: false,
  requireEmailVerification: false
})

const emit = defineEmits<Emits>()
const { showToast, success, error: showError } = useToast()

const codeInputRef = ref<InstanceType<typeof VerificationCodeInput> | null>(null)

const isOpen = computed({
  get: () => props.open,
  set: (value) => emit('update:open', value)
})

const formData = ref({
  email: '',
  username: '',
  password: '',
  confirmPassword: '',
  verificationCode: ''
})

const isLoading = ref(false)
const loadingText = ref('注册中...')
const emailVerified = ref(false)
const verificationError = ref(false)
const codeSentAt = ref<number | null>(null)
const cooldownSeconds = ref(0)
const expireMinutes = ref(30)
const cooldownTimer = ref<number | null>(null)

// Send code cooldown timer
const canSendCode = computed(() => {
  if (!formData.value.email) return false
  if (cooldownSeconds.value > 0) return false
  return true
})

const sendCodeButtonText = computed(() => {
  if (emailVerified.value) return '已验证'
  if (cooldownSeconds.value > 0) return `${cooldownSeconds.value}秒后重试`
  if (codeSentAt.value) return '重新发送验证码'
  return '发送验证码'
})

const canSubmit = computed(() => {
  const hasBasicInfo =
    formData.value.email &&
    formData.value.username &&
    formData.value.password &&
    formData.value.confirmPassword

  if (!hasBasicInfo) return false

  // If email verification is required, check if verified
  if (props.requireEmailVerification && !emailVerified.value) {
    return false
  }

  // Check password match
  if (formData.value.password !== formData.value.confirmPassword) {
    return false
  }

  // Check password length
  if (formData.value.password.length < 8) {
    return false
  }

  return true
})

// Reset form when dialog opens
watch(isOpen, (newValue) => {
  if (newValue) {
    resetForm()
  }
})

// Start cooldown timer
const startCooldown = (seconds: number) => {
  // Clear existing timer if any
  if (cooldownTimer.value !== null) {
    clearInterval(cooldownTimer.value)
  }

  cooldownSeconds.value = seconds
  cooldownTimer.value = window.setInterval(() => {
    cooldownSeconds.value--
    if (cooldownSeconds.value <= 0) {
      if (cooldownTimer.value !== null) {
        clearInterval(cooldownTimer.value)
        cooldownTimer.value = null
      }
    }
  }, 1000)
}

// Cleanup timer on unmount
onUnmounted(() => {
  if (cooldownTimer.value !== null) {
    clearInterval(cooldownTimer.value)
  }
})

const resetForm = () => {
  formData.value = {
    email: '',
    username: '',
    password: '',
    confirmPassword: '',
    verificationCode: ''
  }
  emailVerified.value = false
  verificationError.value = false
  codeSentAt.value = null
  cooldownSeconds.value = 0

  // Clear timer
  if (cooldownTimer.value !== null) {
    clearInterval(cooldownTimer.value)
    cooldownTimer.value = null
  }

  codeInputRef.value?.clear()
}

const handleSendCode = async () => {
  if (!formData.value.email) {
    showError('请输入邮箱')
    return
  }

  // Basic email validation
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
  if (!emailRegex.test(formData.value.email)) {
    showError('请输入有效的邮箱地址', '邮箱格式错误')
    return
  }

  isLoading.value = true
  loadingText.value = '发送中...'

  try {
    const response = await authApi.sendVerificationCode(formData.value.email)

    if (response.success) {
      codeSentAt.value = Date.now()
      if (response.expire_minutes) {
        expireMinutes.value = response.expire_minutes
      }

      success(`请查收邮件，验证码有效期 ${expireMinutes.value} 分钟`, '验证码已发送')

      // Start 60 second cooldown
      startCooldown(60)

      // Focus the verification code input
      setTimeout(() => {
        codeInputRef.value?.focus()
      }, 100)
    } else {
      showError(response.message || '请稍后重试', '发送失败')
    }
  } catch (error: any) {
    showError(error.response?.data?.detail || error.message || '网络错误，请重试', '发送失败')
  } finally {
    isLoading.value = false
  }
}

const handleCodeComplete = async (code: string) => {
  if (!formData.value.email || code.length !== 6) return

  isLoading.value = true
  loadingText.value = '验证中...'
  verificationError.value = false

  try {
    const response = await authApi.verifyEmail(formData.value.email, code)

    if (response.success) {
      emailVerified.value = true
      success('邮箱验证通过，请继续完成注册', '验证成功')
    } else {
      verificationError.value = true
      showError(response.message || '验证码错误', '验证失败')
      // Clear the code input
      codeInputRef.value?.clear()
    }
  } catch (error: any) {
    verificationError.value = true
    showError(error.response?.data?.detail || error.message || '验证码错误，请重试', '验证失败')
    // Clear the code input
    codeInputRef.value?.clear()
  } finally {
    isLoading.value = false
  }
}

const handleSubmit = async () => {
  // Validate password match
  if (formData.value.password !== formData.value.confirmPassword) {
    showError('两次输入的密码不一致', '密码不匹配')
    return
  }

  // Validate password length
  if (formData.value.password.length < 8) {
    showError('密码长度至少 8 位', '密码过短')
    return
  }

  // Check email verification if required
  if (props.requireEmailVerification && !emailVerified.value) {
    showError('请先完成邮箱验证')
    return
  }

  isLoading.value = true
  loadingText.value = '注册中...'

  try {
    const response = await authApi.register({
      email: formData.value.email,
      username: formData.value.username,
      password: formData.value.password
    })

    success(response.message || '欢迎加入！请登录以继续', '注册成功')

    emit('success')
    isOpen.value = false
  } catch (error: any) {
    showError(error.response?.data?.detail || error.message || '注册失败，请重试', '注册失败')
  } finally {
    isLoading.value = false
  }
}

const handleCancel = () => {
  isOpen.value = false
}

const handleSwitchToLogin = () => {
  emit('switchToLogin')
  isOpen.value = false
}
</script>
