<template>
  <div class="verification-code-input">
    <div class="code-inputs flex gap-2">
      <input
        v-for="(digit, index) in digits"
        :key="index"
        :ref="(el) => (inputRefs[index] = el as HTMLInputElement)"
        v-model="digits[index]"
        type="text"
        inputmode="numeric"
        maxlength="1"
        class="code-digit"
        :class="{ error: hasError }"
        @input="handleInput(index, $event)"
        @keydown="handleKeyDown(index, $event)"
        @paste="handlePaste"
      >
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'

interface Props {
  modelValue?: string
  length?: number
  hasError?: boolean
}

interface Emits {
  (e: 'update:modelValue', value: string): void
  (e: 'complete', value: string): void
}

const props = withDefaults(defineProps<Props>(), {
  modelValue: '',
  length: 6,
  hasError: false
})

const emit = defineEmits<Emits>()

const digits = ref<string[]>(Array(props.length).fill(''))
const inputRefs = ref<HTMLInputElement[]>([])

// Watch modelValue changes from parent
watch(
  () => props.modelValue,
  (newValue) => {
    if (newValue.length <= props.length) {
      digits.value = newValue.split('').concat(Array(props.length - newValue.length).fill(''))
    }
  },
  { immediate: true }
)

const updateValue = () => {
  const value = digits.value.join('')
  emit('update:modelValue', value)

  // Emit complete event when all digits are filled
  if (value.length === props.length && /^\d+$/.test(value)) {
    emit('complete', value)
  }
}

const handleInput = (index: number, event: Event) => {
  const input = event.target as HTMLInputElement
  const value = input.value

  // Only allow digits
  if (!/^\d*$/.test(value)) {
    input.value = digits.value[index]
    return
  }

  digits.value[index] = value

  // Auto-focus next input
  if (value && index < props.length - 1) {
    inputRefs.value[index + 1]?.focus()
  }

  updateValue()
}

const handleKeyDown = (index: number, event: KeyboardEvent) => {
  // Handle backspace
  if (event.key === 'Backspace') {
    if (!digits.value[index] && index > 0) {
      // If current input is empty, move to previous and clear it
      inputRefs.value[index - 1]?.focus()
      digits.value[index - 1] = ''
      updateValue()
    } else {
      // Clear current input
      digits.value[index] = ''
      updateValue()
    }
  }
  // Handle arrow keys
  else if (event.key === 'ArrowLeft' && index > 0) {
    inputRefs.value[index - 1]?.focus()
  } else if (event.key === 'ArrowRight' && index < props.length - 1) {
    inputRefs.value[index + 1]?.focus()
  }
}

const handlePaste = (event: ClipboardEvent) => {
  event.preventDefault()
  const pastedData = event.clipboardData?.getData('text') || ''
  const cleanedData = pastedData.replace(/\D/g, '').slice(0, props.length)

  if (cleanedData) {
    digits.value = cleanedData.split('').concat(Array(props.length - cleanedData.length).fill(''))
    updateValue()

    // Focus the next empty input or the last input
    const nextEmptyIndex = digits.value.findIndex((d) => !d)
    const focusIndex = nextEmptyIndex >= 0 ? nextEmptyIndex : props.length - 1
    inputRefs.value[focusIndex]?.focus()
  }
}

// Expose method to clear inputs
const clear = () => {
  digits.value = Array(props.length).fill('')
  inputRefs.value[0]?.focus()
  updateValue()
}

// Expose method to focus first input
const focus = () => {
  inputRefs.value[0]?.focus()
}

defineExpose({
  clear,
  focus
})
</script>

<style scoped>
.code-inputs {
  display: flex;
  justify-content: center;
  align-items: center;
}

.code-digit {
  width: 3rem;
  height: 3.5rem;
  text-align: center;
  font-size: 1.5rem;
  font-weight: 600;
  border: 2px solid hsl(var(--border));
  border-radius: var(--radius);
  background-color: hsl(var(--background));
  color: hsl(var(--foreground));
  transition: all 0.2s;
}

.code-digit:focus {
  outline: none;
  border-color: hsl(var(--primary));
  box-shadow: 0 0 0 3px hsl(var(--primary) / 0.1);
}

.code-digit:hover:not(:focus) {
  border-color: hsl(var(--primary) / 0.5);
}

.code-digit.error {
  border-color: hsl(var(--destructive));
}

.code-digit.error:focus {
  box-shadow: 0 0 0 3px hsl(var(--destructive) / 0.1);
}

/* Prevent spinner buttons on number inputs */
.code-digit::-webkit-outer-spin-button,
.code-digit::-webkit-inner-spin-button {
  -webkit-appearance: none;
  margin: 0;
}

.code-digit[type='number'] {
  -moz-appearance: textfield;
}
</style>
