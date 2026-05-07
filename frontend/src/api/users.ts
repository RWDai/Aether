import apiClient from './client'
import { cachedRequest } from '@/utils/cache'
import type { UserSession as SessionRecord } from '@/types/session'

export interface User {
  id: string // UUID
  username: string
  email: string
  role: 'admin' | 'user'
  is_active: boolean
  unlimited: boolean
  allowed_providers: string[] | null  // 允许使用的提供商 ID 列表
  allowed_api_formats: string[] | null  // 允许使用的 API 格式列表
  allowed_models: string[] | null  // 允许使用的模型名称列表
  rate_limit?: number | null  // null = 跟随系统默认，0 = 不限制
  created_at: string
  updated_at?: string
  last_login_at?: string | null
  request_count?: number
  total_tokens?: number
}

export interface CreateUserRequest {
  username: string
  password: string
  email: string
  role?: 'admin' | 'user'
  initial_gift_usd?: number | null
  unlimited?: boolean
  allowed_providers?: string[] | null
  allowed_api_formats?: string[] | null
  allowed_models?: string[] | null
  rate_limit?: number | null
}

export interface UpdateUserRequest {
  email?: string
  is_active?: boolean
  role?: 'admin' | 'user'
  unlimited?: boolean
  password?: string
  allowed_providers?: string[] | null
  allowed_api_formats?: string[] | null
  allowed_models?: string[] | null
  rate_limit?: number | null
}

export interface UserBatchSelectionFilters {
  search?: string
  role?: 'admin' | 'user'
  is_active?: boolean
}

export interface UserBatchSelection {
  user_ids?: string[]
  filters?: UserBatchSelectionFilters | null
}

export interface UserBatchSelectionItem {
  user_id: string
  username: string
  email?: string | null
  role: 'admin' | 'user'
  is_active: boolean
}

export interface ResolveUserBatchSelectionResponse {
  total: number
  items: UserBatchSelectionItem[]
}

export interface UserBatchAccessControlPayload {
  allowed_providers?: string[] | null
  allowed_api_formats?: string[] | null
  allowed_models?: string[] | null
  rate_limit?: number | null
}

export type UserBatchAction = 'enable' | 'disable' | 'update_access_control'

export interface UserBatchActionRequest {
  selection: UserBatchSelection
  action: UserBatchAction
  payload?: UserBatchAccessControlPayload | null
}

export interface UserBatchActionFailure {
  user_id: string
  reason: string
}

export interface UserBatchActionResponse {
  total: number
  success: number
  failed: number
  failures: UserBatchActionFailure[]
  action?: string
  modified_fields?: string[]
}

export interface ApiKey {
  id: string // UUID
  key?: string  // 完整的 key，只在创建时返回
  key_display?: string  // 脱敏后的密钥显示
  name?: string
  created_at: string
  last_used_at?: string
  expires_at?: string  // 过期时间
  is_active: boolean
  is_locked: boolean  // 管理员锁定标志
  is_standalone: boolean  // 是否为独立余额Key
  rate_limit?: number | null  // 普通Key: 0 = 不限制，历史 null 视为跟随系统默认
  concurrent_limit?: number | null  // 普通Key: 0 = 不限制并发，历史 null 兼容
  total_requests?: number  // 总请求数
  total_cost_usd?: number  // 总费用
}

export interface UpsertUserApiKeyRequest {
  name?: string
  rate_limit?: number | null
  concurrent_limit?: number | null
}

export type UserSession = SessionRecord

export const usersApi = {
  async getAllUsers(options: { cacheTtlMs?: number } = {}): Promise<User[]> {
    const cacheTtlMs = options.cacheTtlMs ?? 0
    return cachedRequest(
      'admin:users:list',
      async () => {
        const response = await apiClient.get<User[]>('/api/admin/users')
        return response.data
      },
      cacheTtlMs,
    )
  },

  async getUser(userId: string): Promise<User> {
    const response = await apiClient.get<User>(`/api/admin/users/${userId}`)
    return response.data
  },

  async createUser(user: CreateUserRequest): Promise<User> {
    const response = await apiClient.post<User>('/api/admin/users', user)
    return response.data
  },

  async updateUser(userId: string, updates: UpdateUserRequest): Promise<User> {
    const response = await apiClient.put<User>(`/api/admin/users/${userId}`, updates)
    return response.data
  },

  async resolveBatchSelection(
    selection: UserBatchSelection
  ): Promise<ResolveUserBatchSelectionResponse> {
    const response = await apiClient.post<ResolveUserBatchSelectionResponse>(
      '/api/admin/users/resolve-selection',
      selection
    )
    return response.data
  },

  async batchAction(request: UserBatchActionRequest): Promise<UserBatchActionResponse> {
    const response = await apiClient.post<UserBatchActionResponse>(
      '/api/admin/users/batch-action',
      request
    )
    return response.data
  },

  async deleteUser(userId: string): Promise<void> {
    await apiClient.delete(`/api/admin/users/${userId}`)
  },

  async getUserApiKeys(userId: string): Promise<ApiKey[]> {
    const response = await apiClient.get<{ api_keys: ApiKey[] }>(`/api/admin/users/${userId}/api-keys`)
    return response.data.api_keys
  },

  async getUserSessions(userId: string): Promise<SessionRecord[]> {
    const response = await apiClient.get<SessionRecord[]>(`/api/admin/users/${userId}/sessions`)
    return response.data
  },

  async revokeUserSession(userId: string, sessionId: string): Promise<{ message: string }> {
    const response = await apiClient.delete<{ message: string }>(`/api/admin/users/${userId}/sessions/${sessionId}`)
    return response.data
  },

  async revokeAllUserSessions(userId: string): Promise<{ message: string; revoked_count: number }> {
    const response = await apiClient.delete<{ message: string; revoked_count: number }>(`/api/admin/users/${userId}/sessions`)
    return response.data
  },

  async createApiKey(
    userId: string,
    data: UpsertUserApiKeyRequest
  ): Promise<ApiKey & { key: string }> {
    const response = await apiClient.post<ApiKey & { key: string }>(
      `/api/admin/users/${userId}/api-keys`,
      data
    )
    return response.data
  },

  async updateApiKey(
    userId: string,
    keyId: string,
    data: UpsertUserApiKeyRequest
  ): Promise<ApiKey & { message: string }> {
    const response = await apiClient.put<ApiKey & { message: string }>(
      `/api/admin/users/${userId}/api-keys/${keyId}`,
      data
    )
    return response.data
  },

  async deleteApiKey(userId: string, keyId: string): Promise<void> {
    await apiClient.delete(`/api/admin/users/${userId}/api-keys/${keyId}`)
  },

  async getFullApiKey(userId: string, keyId: string): Promise<{ key: string }> {
    const response = await apiClient.get<{ key: string }>(
      `/api/admin/users/${userId}/api-keys/${keyId}/full-key`
    )
    return response.data
  },
  // 管理员统计
  async getUsageStats(): Promise<Record<string, unknown>> {
    const response = await apiClient.get<Record<string, unknown>>('/api/admin/usage/stats')
    return response.data
  }
}
