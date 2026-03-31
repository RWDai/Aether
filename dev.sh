#!/bin/bash
# 本地开发启动脚本
set -euo pipefail
clear >/dev/null 2>&1 || true

# 加载 .env 文件
set -a
source .env
set +a

dotenv_has_key() {
    local key="$1"
    rg -q "^[[:space:]]*${key}=" .env
}

# 构建 DATABASE_URL
export DATABASE_URL="postgresql://${DB_USER:-postgres}:${DB_PASSWORD}@${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-aether}"
export REDIS_URL=redis://:${REDIS_PASSWORD}@${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}/0

if ! dotenv_has_key "AETHER_GATEWAY_DATA_POSTGRES_URL"; then
    export AETHER_GATEWAY_DATA_POSTGRES_URL="${DATABASE_URL}"
fi
if ! dotenv_has_key "AETHER_GATEWAY_DATA_REDIS_URL"; then
    export AETHER_GATEWAY_DATA_REDIS_URL="${REDIS_URL}"
fi
if ! dotenv_has_key "AETHER_GATEWAY_DATA_ENCRYPTION_KEY"; then
    export AETHER_GATEWAY_DATA_ENCRYPTION_KEY="${ENCRYPTION_KEY:-}"
fi

# 开发环境连接池低配（节省内存）
export DB_POOL_SIZE=${DB_POOL_SIZE:-5}
export DB_MAX_OVERFLOW=${DB_MAX_OVERFLOW:-5}
export HTTP_MAX_CONNECTIONS=${HTTP_MAX_CONNECTIONS:-20}
export HTTP_KEEPALIVE_CONNECTIONS=${HTTP_KEEPALIVE_CONNECTIONS:-5}

EXECUTOR_PID=""
GATEWAY_PID=""
STARTUP_WAIT_EARLY_EXIT=false

cleanup() {
    if [ -n "${GATEWAY_PID}" ]; then
        echo ""
        echo "=> 停止 aether-gateway..."
        kill "${GATEWAY_PID}" >/dev/null 2>&1 || true
        wait "${GATEWAY_PID}" >/dev/null 2>&1 || true
    fi

    if [ -n "${EXECUTOR_PID}" ]; then
        echo ""
        echo "=> 停止 aether-executor..."
        kill "${EXECUTOR_PID}" >/dev/null 2>&1 || true
        wait "${EXECUTOR_PID}" >/dev/null 2>&1 || true
    fi

}

trap cleanup EXIT

wait_for_startup() {
    local pid="$1"
    local timeout_seconds="$2"
    local service_name="$3"
    shift 3

    STARTUP_WAIT_EARLY_EXIT=false

    local attempts=$((timeout_seconds * 10))
    if [ "${attempts}" -lt 1 ]; then
        attempts=1
    fi

    for ((i = 0; i < attempts; i++)); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi

        if ! kill -0 "${pid}" >/dev/null 2>&1; then
            STARTUP_WAIT_EARLY_EXIT=true
            echo "=> ${service_name} 启动进程已提前退出，请检查上面的日志。"
            return 1
        fi

        sleep 0.1
    done

    if "$@" >/dev/null 2>&1; then
        return 0
    fi

    if ! kill -0 "${pid}" >/dev/null 2>&1; then
        STARTUP_WAIT_EARLY_EXIT=true
        echo "=> ${service_name} 启动进程已提前退出，请检查上面的日志。"
        return 1
    fi

    echo "=> ${service_name} 在 ${timeout_seconds}s 内未通过启动检查。"
    echo "=> 如果这是冷编译或存在并发 cargo 构建，可调大 *_STARTUP_TIMEOUT_SECONDS 后重试。"
    return 1
}

# 本地开发默认约定：
# - Rust aether-gateway 绑定 APP_PORT，作为唯一公开入口
# - ./dev.sh 不再启动 Python 宿主；本地默认只验证 Rust-owned 路径
APP_PORT=${APP_PORT:-8084}
RUST_SERVICE_STARTUP_TIMEOUT_SECONDS=${RUST_SERVICE_STARTUP_TIMEOUT_SECONDS:-120}
EXECUTOR_STARTUP_TIMEOUT_SECONDS=${EXECUTOR_STARTUP_TIMEOUT_SECONDS:-${RUST_SERVICE_STARTUP_TIMEOUT_SECONDS}}
GATEWAY_STARTUP_TIMEOUT_SECONDS=${GATEWAY_STARTUP_TIMEOUT_SECONDS:-${RUST_SERVICE_STARTUP_TIMEOUT_SECONDS}}

should_start_executor=false
if [ "${DEV_START_EXECUTOR:-false}" = "true" ] || [ "${EXECUTOR_BACKEND:-rust}" = "rust" ]; then
    should_start_executor=true
fi

if [ "${should_start_executor}" = "true" ]; then
    if ! command -v cargo >/dev/null 2>&1; then
        echo "=> 未找到 cargo，无法启动 aether-executor。请先安装 Rust toolchain。"
        exit 1
    fi

    if [ -n "${EXECUTOR_TRANSPORT:-}" ] && [ "${EXECUTOR_TRANSPORT}" != "tcp" ]; then
        echo "=> 本地 ./dev.sh 只支持 gateway -> executor 的 TCP 模式。"
        echo "=> 请移除 EXECUTOR_TRANSPORT=${EXECUTOR_TRANSPORT}，或改为 EXECUTOR_TRANSPORT=tcp。"
        exit 1
    fi
    export EXECUTOR_TRANSPORT=tcp
    export EXECUTOR_BASE_URL=${EXECUTOR_BASE_URL:-http://127.0.0.1:5219}
    export EXECUTOR_BIND=${EXECUTOR_BIND:-127.0.0.1:5219}

    echo "=> 启动 aether-executor (tcp: ${EXECUTOR_BIND})..."
    cargo run -q -p aether-executor -- --transport tcp --bind "${EXECUTOR_BIND}" &
    EXECUTOR_PID=$!

    if ! wait_for_startup "${EXECUTOR_PID}" "${EXECUTOR_STARTUP_TIMEOUT_SECONDS}" "aether-executor" curl -sf "${EXECUTOR_BASE_URL}/health"; then
        if [ "${STARTUP_WAIT_EARLY_EXIT}" = "true" ]; then
            EXECUTOR_PID=""
        fi
        exit 1
    fi
fi

if ! command -v cargo >/dev/null 2>&1; then
    echo "=> 未找到 cargo，无法启动 aether-gateway。请先安装 Rust toolchain。"
    exit 1
fi

export AETHER_GATEWAY_BIND=${AETHER_GATEWAY_BIND:-0.0.0.0:${APP_PORT}}
export AETHER_GATEWAY_UPSTREAM=${AETHER_GATEWAY_UPSTREAM:-http://127.0.0.1:9}
export AETHER_GATEWAY_VIDEO_TASK_TRUTH_SOURCE_MODE=${AETHER_GATEWAY_VIDEO_TASK_TRUTH_SOURCE_MODE:-rust-authoritative}
if [ "${should_start_executor}" = "true" ] && [ "${EXECUTOR_TRANSPORT:-}" = "tcp" ]; then
    export AETHER_GATEWAY_EXECUTOR_URL=${AETHER_GATEWAY_EXECUTOR_URL:-${EXECUTOR_BASE_URL}}
fi

GATEWAY_ARGS=(--bind "${AETHER_GATEWAY_BIND}" --upstream "${AETHER_GATEWAY_UPSTREAM}")
if [ -n "${AETHER_GATEWAY_EXECUTOR_URL:-}" ]; then
    GATEWAY_ARGS+=(--executor-url "${AETHER_GATEWAY_EXECUTOR_URL}")
fi

if [ -n "${AETHER_GATEWAY_EXECUTOR_URL:-}" ]; then
    echo "=> 启动 aether-gateway (Rust frontdoor: ${AETHER_GATEWAY_BIND}, upstream=${AETHER_GATEWAY_UPSTREAM}, executor=${AETHER_GATEWAY_EXECUTOR_URL})..."
else
    echo "=> 启动 aether-gateway (Rust frontdoor: ${AETHER_GATEWAY_BIND}, upstream=${AETHER_GATEWAY_UPSTREAM})..."
fi
cargo run -q -p aether-gateway -- "${GATEWAY_ARGS[@]}" &
GATEWAY_PID=$!

if ! wait_for_startup "${GATEWAY_PID}" "${GATEWAY_STARTUP_TIMEOUT_SECONDS}" "aether-gateway" curl -sf "http://127.0.0.1:${APP_PORT}/_gateway/health"; then
    if [ "${STARTUP_WAIT_EARLY_EXIT}" = "true" ]; then
        GATEWAY_PID=""
    fi
    exit 1
fi

echo "=> 启动本地开发服务..."
echo "=> Rust公开入口:     http://localhost:${APP_PORT}"
echo "=> Frontdoor健康检查: http://localhost:${APP_PORT}/_gateway/health"
echo "=> Legacy upstream:  ${AETHER_GATEWAY_UPSTREAM}"
if [ -n "${AETHER_GATEWAY_EXECUTOR_URL:-}" ]; then
    echo "=> Executor API:     ${AETHER_GATEWAY_EXECUTOR_URL}"
fi
echo "=> 数据库: ${DATABASE_URL}"
if [ "${should_start_executor}" = "true" ]; then
    echo "=> Executor 后端: ${EXECUTOR_BACKEND:-rust} (${EXECUTOR_TRANSPORT})"
fi
echo "=> 提示: 未下沉到 Rust 的 legacy 路由会直接失败，除非你手动设置了可用的 upstream/control URL。"
echo ""

wait "${GATEWAY_PID}"
