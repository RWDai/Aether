from __future__ import annotations

import base64

from src.services.request.executor_plan import PreparedExecutionPlan
from src.services.request.executor_plan import (
    ExecutionPlan,
    ExecutionPlanBody,
    ExecutionPlanTimeouts,
    ExecutionProxySnapshot,
    build_execution_plan_body,
    should_bypass_remote_executor_url,
)


def test_execution_plan_to_payload_drops_none_fields() -> None:
    plan = ExecutionPlan(
        request_id="req-1",
        candidate_id=None,
        provider_name="openai",
        provider_id="prov-1",
        endpoint_id="ep-1",
        key_id="key-1",
        method="POST",
        url="https://example.com/v1/chat/completions",
        headers={"content-type": "application/json"},
        body=ExecutionPlanBody(json_body={"model": "gpt-4.1"}),
        stream=False,
        provider_api_format="openai:chat",
        client_api_format="openai:chat",
        model_name="gpt-4.1",
        proxy=ExecutionProxySnapshot(
            enabled=True,
            mode="direct",
            label="no-proxy",
        ),
        timeouts=ExecutionPlanTimeouts(
            connect_ms=10_000,
            total_ms=300_000,
        ),
    )

    payload = plan.to_payload()

    assert "candidate_id" not in payload
    assert payload["body"] == {"json_body": {"model": "gpt-4.1"}}
    assert payload["proxy"] == {
        "enabled": True,
        "mode": "direct",
        "label": "no-proxy",
    }
    assert payload["timeouts"] == {
        "connect_ms": 10_000,
        "total_ms": 300_000,
    }


def test_prepared_execution_plan_remote_eligible_for_non_stream_json() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-4.1"}),
            stream=False,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
        ),
        payload={"model": "gpt-4.1"},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=300.0,
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_allows_upstream_stream() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-4.1", "stream": True}),
            stream=True,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
        ),
        payload={"model": "gpt-4.1", "stream": True},
        headers={"content-type": "application/json"},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=300.0,
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_allows_tunnel_delegate_proxy() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-4.1"}),
            stream=False,
            provider_api_format="gemini:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
            proxy=ExecutionProxySnapshot(
                enabled=True,
                mode="tunnel",
                node_id="node-1",
                label="relay-node",
            ),
        ),
        payload={"model": "gpt-4.1"},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=True,
        provider_type="gemini",
        request_timeout=300.0,
        delegate_config={"tunnel": True, "node_id": "node-1"},
        proxy_config={"node_id": "node-1"},
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_allows_url_proxy_without_delegate() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-4.1"}),
            stream=False,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
            proxy=ExecutionProxySnapshot(
                enabled=True,
                mode="http",
                label="proxy.internal",
                url="http://proxy.internal:8080",
            ),
        ),
        payload={"model": "gpt-4.1"},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=300.0,
        proxy_config={"url": "http://proxy.internal:8080"},
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_allows_conversion_and_envelope() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="gemini",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-4.1"}),
            stream=False,
            provider_api_format="gemini:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
        ),
        payload={"model": "gpt-4.1"},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=True,
        provider_type="gemini",
        request_timeout=300.0,
        envelope=object(),
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_allows_tls_profile() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="claude",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/messages",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "claude-3.7-sonnet"}),
            stream=False,
            provider_api_format="claude:chat",
            client_api_format="claude:chat",
            model_name="claude-3.7-sonnet",
            tls_profile="claude_code_nodejs",
        ),
        payload={"model": "claude-3.7-sonnet"},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=False,
        provider_type="claude_code",
        request_timeout=300.0,
    )

    assert prepared.remote_eligible is True


def test_build_execution_plan_body_encodes_raw_bytes_payload() -> None:
    body = build_execution_plan_body(
        b"raw-payload",
        content_type="text/plain",
    )

    assert body.json_body is None
    assert base64.b64decode(body.body_bytes_b64 or "") == b"raw-payload"


def test_prepared_execution_plan_remote_eligible_allows_gzip_json_body() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-4.1"}),
            stream=False,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
            content_encoding="gzip",
        ),
        payload={"model": "gpt-4.1"},
        headers={"content-type": "application/json"},
        upstream_is_stream=False,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=300.0,
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_allows_raw_body_with_passthrough_encoding() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://example.com/v1/chat/completions",
            headers={"content-type": "text/plain"},
            body=ExecutionPlanBody(body_bytes_b64=base64.b64encode(b"raw").decode("ascii")),
            stream=False,
            provider_api_format="openai:chat",
            client_api_format="openai:chat",
            model_name="gpt-4.1",
            content_encoding="br",
        ),
        payload={"model": "gpt-4.1"},
        headers={"content-type": "text/plain"},
        upstream_is_stream=False,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=300.0,
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_allows_empty_get_body() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="openai",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="GET",
            url="https://example.com/v1/videos/video-1/content",
            headers={},
            body=ExecutionPlanBody(),
            stream=True,
            provider_api_format="openai:video",
            client_api_format="openai:video",
            model_name="sora-2",
        ),
        payload={},
        headers={},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_type="openai",
        request_timeout=300.0,
    )

    assert prepared.remote_eligible is True


def test_prepared_execution_plan_remote_eligible_rejects_codex_cli_transport() -> None:
    prepared = PreparedExecutionPlan(
        contract=ExecutionPlan(
            request_id="req-1",
            candidate_id=None,
            provider_name="codex",
            provider_id="prov-1",
            endpoint_id="ep-1",
            key_id="key-1",
            method="POST",
            url="https://chatgpt.com/backend-api/codex/responses",
            headers={"content-type": "application/json"},
            body=ExecutionPlanBody(json_body={"model": "gpt-5.4", "input": []}),
            stream=True,
            provider_api_format="openai:cli",
            client_api_format="openai:cli",
            model_name="gpt-5.4",
        ),
        payload={"model": "gpt-5.4", "input": []},
        headers={"content-type": "application/json"},
        upstream_is_stream=True,
        needs_conversion=False,
        provider_type="codex",
        request_timeout=30.0,
    )

    assert prepared.remote_eligible is False


def test_should_bypass_remote_executor_url_accepts_backendapi_codex_variant() -> None:
    assert (
        should_bypass_remote_executor_url(
            "https://chatgpt.com/backendapi/codex/responses",
            provider_api_format="openai:cli",
            client_api_format="openai:cli",
        )
        is True
    )
