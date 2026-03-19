const DEFAULT_MODEL_TEST_MESSAGE = 'Hello! This is a test message.'

export function buildDefaultModelTestRequestBody(modelName: string): string {
  return JSON.stringify({
    model: modelName,
    messages: [
      {
        role: 'user',
        content: DEFAULT_MODEL_TEST_MESSAGE,
      },
    ],
    max_tokens: 30,
    temperature: 0.7,
    stream: true,
  }, null, 2)
}

export function parseModelTestRequestBodyDraft(
  draft: string,
): { value: Record<string, unknown> | null; error: string | null } {
  const normalized = draft.trim()
  if (!normalized) {
    return {
      value: null,
      error: '测试请求体不能为空',
    }
  }

  try {
    const parsed = JSON.parse(normalized)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return {
        value: null,
        error: '测试请求体必须是 JSON 对象',
      }
    }
    return {
      value: parsed as Record<string, unknown>,
      error: null,
    }
  } catch (error) {
    return {
      value: null,
      error: error instanceof Error ? error.message : '无效的 JSON',
    }
  }
}
