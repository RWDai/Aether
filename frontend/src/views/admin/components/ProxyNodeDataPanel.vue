<template>
  <div class="px-4 sm:px-6 py-4 bg-muted/15 border-t border-border/40">
    <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mb-4">
      <div>
        <div class="flex items-center gap-2">
          <h4 class="text-sm font-semibold">
            节点数据
          </h4>
          <Badge
            variant="outline"
            class="text-[10px] px-1.5 py-0"
          >
            最近 24 小时
          </Badge>
        </div>
        <p class="text-xs text-muted-foreground mt-1">
          {{ loadedText }}
        </p>
      </div>
      <Button
        variant="ghost"
        size="sm"
        class="h-8 px-2 text-xs self-start sm:self-auto"
        :disabled="state?.loading"
        @click="$emit('refresh')"
      >
        <RefreshCw
          class="h-3.5 w-3.5 mr-1"
          :class="state?.loading ? 'animate-spin' : ''"
        />
        刷新
      </Button>
    </div>

    <div
      v-if="state?.loading && !state.metrics"
      class="py-8 flex items-center justify-center gap-2 text-sm text-muted-foreground"
    >
      <Loader2 class="h-4 w-4 animate-spin" />
      加载节点数据...
    </div>

    <div
      v-else-if="state?.error"
      class="rounded-lg border border-destructive/30 bg-destructive/5 px-4 py-3 text-sm text-destructive flex items-start gap-2"
    >
      <AlertTriangle class="h-4 w-4 mt-0.5 shrink-0" />
      <span>{{ state.error }}</span>
    </div>

    <div
      v-else
      class="space-y-4"
    >
      <div class="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-2">
        <div
          v-for="item in summaryStats"
          :key="item.label"
          class="rounded-lg border border-border/50 bg-background/70 px-3 py-2 min-w-0"
        >
          <div class="text-[11px] text-muted-foreground truncate">
            {{ item.label }}
          </div>
          <div
            class="text-sm font-semibold tabular-nums truncate mt-0.5"
            :class="item.tone"
          >
            {{ item.value }}
          </div>
          <div
            v-if="item.hint"
            class="text-[10px] text-muted-foreground truncate mt-0.5"
          >
            {{ item.hint }}
          </div>
        </div>
      </div>

      <div class="grid gap-4 lg:grid-cols-[minmax(0,1.35fr)_minmax(280px,0.9fr)]">
        <section class="rounded-lg border border-border/50 bg-background/70 p-3 min-w-0">
          <div class="flex items-center justify-between gap-3 mb-3">
            <div>
              <h5 class="text-xs font-semibold">
                在线率采样
              </h5>
              <p class="text-[11px] text-muted-foreground mt-0.5">
                1h 桶，颜色随在线率和错误数变化
              </p>
            </div>
            <span class="text-[11px] text-muted-foreground tabular-nums shrink-0">{{ formatNumber(bucketItems.length) }} 点</span>
          </div>
          <div
            v-if="bucketItems.length > 0"
            class="h-20 flex items-end gap-1 overflow-hidden"
          >
            <div
              v-for="bucket in bucketItems"
              :key="bucket.bucket_start_unix_secs"
              class="flex-1 min-w-[4px] rounded-t-sm"
              :class="bucketBarClass(bucket)"
              :style="{ height: bucketBarHeight(bucket) }"
              :title="bucketTitle(bucket)"
            />
          </div>
          <div
            v-else
            class="h-20 rounded-md bg-muted/30 flex items-center justify-center text-xs text-muted-foreground"
          >
            暂无采样数据
          </div>
        </section>

        <section class="rounded-lg border border-border/50 bg-background/70 p-3 min-w-0">
          <div class="flex items-center justify-between gap-2 mb-3">
            <h5 class="text-xs font-semibold">
              最近事件
            </h5>
            <span class="text-[11px] text-muted-foreground tabular-nums">{{ recentEvents.length }}/8</span>
          </div>
          <div
            v-if="recentEvents.length === 0"
            class="h-20 rounded-md bg-muted/30 flex items-center justify-center text-xs text-muted-foreground"
          >
            暂无关键事件
          </div>
          <div
            v-else
            class="space-y-1.5 max-h-28 overflow-y-auto pr-1"
          >
            <div
              v-for="event in recentEvents"
              :key="event.id"
              class="flex items-center gap-2 text-xs min-w-0"
            >
              <Badge
                :variant="eventTypeVariant(event.event_type)"
                class="text-[10px] px-1.5 py-0 shrink-0"
              >
                {{ eventTypeLabel(event.event_type) }}
              </Badge>
              <span class="text-muted-foreground truncate flex-1">{{ eventDetail(event) }}</span>
              <span class="text-[10px] text-muted-foreground/70 tabular-nums shrink-0">{{ formatTime(event.created_at || null) }}</span>
            </div>
          </div>
        </section>
      </div>

      <div class="grid gap-4 lg:grid-cols-2">
        <section class="rounded-lg border border-border/50 bg-background/70 p-3 min-w-0">
          <h5 class="text-xs font-semibold mb-3">
            实时快照
          </h5>
          <div class="grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-2">
            <div
              v-for="item in snapshotItems"
              :key="item.label"
              class="min-w-0"
            >
              <div class="text-[11px] text-muted-foreground truncate">
                {{ item.label }}
              </div>
              <div class="text-xs font-medium tabular-nums truncate mt-0.5">
                {{ item.value }}
              </div>
            </div>
          </div>
        </section>

        <section class="rounded-lg border border-border/50 bg-background/70 p-3 min-w-0">
          <h5 class="text-xs font-semibold mb-3">
            隧道计数器
          </h5>
          <div
            v-if="tunnelMetrics"
            class="grid grid-cols-2 sm:grid-cols-4 gap-x-4 gap-y-2"
          >
            <div
              v-for="item in tunnelCounterItems"
              :key="item.label"
              class="min-w-0"
            >
              <div class="text-[11px] text-muted-foreground truncate">
                {{ item.label }}
              </div>
              <div class="text-xs font-medium tabular-nums truncate mt-0.5">
                {{ item.value }}
              </div>
            </div>
          </div>
          <div
            v-else
            class="rounded-md bg-muted/30 py-4 text-center text-xs text-muted-foreground"
          >
            暂无隧道计数器
          </div>
        </section>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { AlertTriangle, Loader2, RefreshCw } from 'lucide-vue-next'
import { Badge, Button } from '@/components/ui'
import type {
  ProxyNode,
  ProxyNodeEvent,
  ProxyNodeMetricsBucket,
  ProxyNodeMetricsResponse,
} from '@/api/proxy-nodes'

interface ProxyNodeDetailState {
  loading: boolean
  error: string | null
  node: ProxyNode | null
  metrics: ProxyNodeMetricsResponse | null
  events: ProxyNodeEvent[]
  loadedAt: number | null
}

const props = defineProps<{
  node: ProxyNode
  state?: ProxyNodeDetailState | null
}>()

defineEmits<{
  refresh: []
}>()

const detailNode = computed(() => props.state?.node ?? props.node)
const metrics = computed(() => props.state?.metrics ?? null)
const metricsSummary = computed(() => metrics.value?.summary ?? null)
const bucketItems = computed(() => metrics.value?.items.slice(-24) ?? [])
const recentEvents = computed(() => props.state?.events.slice(0, 8) ?? [])
const metadata = computed(() => asRecord(detailNode.value.proxy_metadata))
const tunnelMetrics = computed(() => asRecord(metadata.value?.tunnel_metrics))

const loadedText = computed(() => {
  if (props.state?.loading) return '正在刷新采样数据'
  if (!props.state?.loadedAt) return '展开后自动读取 24 小时指标和最近关键事件'
  return `数据刷新于 ${formatLoadedAt(props.state.loadedAt)}`
})

const summaryStats = computed(() => {
  const summary = metricsSummary.value
  const totalBytes = (summary?.ws_in_bytes_delta ?? 0) + (summary?.ws_out_bytes_delta ?? 0)
  return [
    {
      label: '在线率',
      value: formatPercent(summary?.uptime_ratio ?? null),
      hint: `${formatNumber(summary?.uptime_samples ?? 0)}/${formatNumber(summary?.samples ?? 0)} 样本`,
      tone: uptimeTone(summary?.uptime_ratio ?? null),
    },
    {
      label: '心跳 RTT',
      value: formatMs(summary?.heartbeat_rtt_ms_avg ?? null),
      hint: `峰值 ${formatMs(summary?.heartbeat_rtt_ms_max ?? null)}`,
      tone: '',
    },
    {
      label: '并发峰值',
      value: formatNumber(summary?.active_connections_max ?? 0),
      hint: `均值 ${formatDecimal(summary?.active_connections_avg ?? null)}`,
      tone: '',
    },
    {
      label: '断开',
      value: formatNumber(summary?.disconnects_delta ?? 0),
      hint: '24h delta',
      tone: (summary?.disconnects_delta ?? 0) > 0 ? 'text-yellow-600 dark:text-yellow-400' : '',
    },
    {
      label: '连接错误',
      value: formatNumber(summary?.connect_errors_delta ?? 0),
      hint: '24h delta',
      tone: (summary?.connect_errors_delta ?? 0) > 0 ? 'text-destructive' : '',
    },
    {
      label: 'WS 流量',
      value: formatBytes(totalBytes),
      hint: `${formatNumber((summary?.ws_in_frames_delta ?? 0) + (summary?.ws_out_frames_delta ?? 0))} 帧`,
      tone: '',
    },
  ]
})

const snapshotItems = computed(() => {
  const node = detailNode.value
  return [
    { label: '当前并发', value: formatNumber(node.active_connections ?? 0) },
    { label: '心跳间隔', value: `${node.heartbeat_interval ?? '-'}s` },
    { label: '最后心跳', value: formatTime(node.last_heartbeat_at) },
    { label: '隧道连接', value: node.tunnel_connected ? '已连接' : '未连接' },
    { label: '连接时间', value: formatTime(node.tunnel_connected_at) },
    { label: '容量估算', value: node.estimated_max_concurrency == null ? '-' : formatNumber(node.estimated_max_concurrency) },
    { label: '配置版本', value: `v${node.config_version}` },
    { label: '注册来源', value: node.registered_by || '-' },
    { label: '代理版本', value: stringField(metadata.value, 'version') || '-' },
  ]
})

const tunnelCounterItems = computed(() => [
  { label: '建连尝试', value: formatTunnelNumber('connect_attempts') },
  { label: '建连成功', value: formatTunnelNumber('connect_successes') },
  { label: '建连错误', value: formatTunnelNumber('connect_errors') },
  { label: '断开次数', value: formatTunnelNumber('disconnects') },
  { label: '最近连上', value: formatUnixSecs(numberField(tunnelMetrics.value, 'last_connected_at_unix_secs')) },
  { label: '最近断开', value: formatUnixSecs(numberField(tunnelMetrics.value, 'last_disconnected_at_unix_secs')) },
  { label: '最近在线', value: formatDurationMs(numberField(tunnelMetrics.value, 'last_connected_duration_ms')) },
  { label: '累计在线', value: formatDurationMs(numberField(tunnelMetrics.value, 'connected_duration_total_ms')) },
  { label: '心跳发送', value: formatTunnelNumber('heartbeat_sent') },
  { label: '心跳确认', value: formatTunnelNumber('heartbeat_ack') },
  { label: '最近 RTT', value: formatMs(numberField(tunnelMetrics.value, 'heartbeat_rtt_last_ms')) },
  { label: '平均 RTT', value: formatMs(numberField(tunnelMetrics.value, 'heartbeat_rtt_avg_ms')) },
  { label: 'WS 入站', value: formatBytes(numberField(tunnelMetrics.value, 'ws_in_bytes') ?? 0) },
  { label: 'WS 出站', value: formatBytes(numberField(tunnelMetrics.value, 'ws_out_bytes') ?? 0) },
  { label: '入站帧', value: formatTunnelNumber('ws_in_frames') },
  { label: '出站帧', value: formatTunnelNumber('ws_out_frames') },
])

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  return value as Record<string, unknown>
}

function numberField(record: Record<string, unknown> | null, key: string): number | null {
  const value = record?.[key]
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim() !== '') {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) return parsed
  }
  return null
}

function stringField(record: Record<string, unknown> | null, key: string): string | null {
  const value = record?.[key]
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  return trimmed || null
}

function formatTunnelNumber(key: string) {
  return formatNumber(numberField(tunnelMetrics.value, key) ?? 0)
}

function formatNumber(value: number) {
  if (!Number.isFinite(value)) return '-'
  if (Math.abs(value) >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (Math.abs(value) >= 1_000) return `${(value / 1_000).toFixed(1)}K`
  return String(Math.round(value))
}

function formatDecimal(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '-'
  if (value === 0) return '0'
  if (value < 10) return value.toFixed(1)
  return formatNumber(value)
}

function formatPercent(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '-'
  return `${(value * 100).toFixed(value >= 0.995 || value === 0 ? 0 : 1)}%`
}

function formatMs(value: number | null) {
  if (value == null || !Number.isFinite(value) || value <= 0) return '-'
  if (value >= 1000) return `${(value / 1000).toFixed(1)}s`
  return `${Math.round(value)}ms`
}

function formatDurationMs(value: number | null) {
  if (value == null || !Number.isFinite(value) || value <= 0) return '-'
  const totalSeconds = Math.floor(value / 1000)
  if (totalSeconds < 60) return `${totalSeconds}s`
  const totalMinutes = Math.floor(totalSeconds / 60)
  if (totalMinutes < 60) return `${totalMinutes}m`
  const totalHours = Math.floor(totalMinutes / 60)
  if (totalHours < 24) return `${totalHours}h ${totalMinutes % 60}m`
  const days = Math.floor(totalHours / 24)
  return `${days}d ${totalHours % 24}h`
}

function formatUnixSecs(value: number | null) {
  if (value == null || !Number.isFinite(value) || value <= 0) return '-'
  return formatTime(new Date(value * 1000).toISOString())
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = value
  let unit = 0
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024
    unit += 1
  }
  return `${unit === 0 ? Math.round(size) : size.toFixed(1)} ${units[unit]}`
}

function formatTime(iso: string | null) {
  if (!iso) return '-'
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return '-'
  const diff = (Date.now() - date.getTime()) / 1000
  if (diff >= 0 && diff < 60) return '刚刚'
  if (diff >= 0 && diff < 3600) return `${Math.floor(diff / 60)}分钟前`
  if (diff >= 0 && diff < 86400) return `${Math.floor(diff / 3600)}小时前`
  return date.toLocaleDateString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })
}

function formatLoadedAt(value: number) {
  return new Date(value).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function bucketBarHeight(bucket: ProxyNodeMetricsBucket) {
  if (bucket.samples <= 0 || bucket.uptime_ratio == null) return '10%'
  return `${Math.max(10, Math.round(bucket.uptime_ratio * 100))}%`
}

function bucketBarClass(bucket: ProxyNodeMetricsBucket) {
  if (bucket.samples <= 0) return 'bg-muted'
  if (bucket.error_events_delta > 0 || bucket.connect_errors_delta > 0) return 'bg-destructive/80'
  if ((bucket.uptime_ratio ?? 0) < 0.98 || bucket.disconnects_delta > 0) return 'bg-yellow-500/80'
  return 'bg-primary/80'
}

function bucketTitle(bucket: ProxyNodeMetricsBucket) {
  return [
    formatBucketTime(bucket.bucket_start),
    `在线率 ${formatPercent(bucket.uptime_ratio)}`,
    `RTT ${formatMs(bucket.heartbeat_rtt_ms_avg)}`,
    `断开 ${formatNumber(bucket.disconnects_delta)}`,
    `错误 ${formatNumber(bucket.connect_errors_delta + bucket.error_events_delta)}`,
  ].join('，')
}

function formatBucketTime(value: string | null) {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '-'
  return date.toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit' })
}

function uptimeTone(value: number | null) {
  if (value == null) return ''
  if (value < 0.95) return 'text-destructive'
  if (value < 0.99) return 'text-yellow-600 dark:text-yellow-400'
  return 'text-primary'
}

function eventTypeLabel(type: string) {
  switch (type) {
    case 'connected': return '连接'
    case 'disconnected': return '断开'
    case 'error': return '错误'
    case 'tunnel_err': return '隧道错误'
    default: return type
  }
}

function eventTypeVariant(type: string) {
  switch (type) {
    case 'connected': return 'success' as const
    case 'disconnected':
    case 'error':
    case 'tunnel_err':
      return 'destructive' as const
    default: return 'secondary' as const
  }
}

function eventDetail(event: ProxyNodeEvent) {
  const metadata = asRecord(event.event_metadata)
  return event.detail || stringField(metadata, 'message') || stringField(metadata, 'category') || '-'
}
</script>
