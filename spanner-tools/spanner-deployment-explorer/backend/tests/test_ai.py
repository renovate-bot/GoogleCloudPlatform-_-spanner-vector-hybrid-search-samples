# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest
from httpx import AsyncClient, ASGITransport, Response

from app.core.config import (
    ROOT_DIR,
    GEMINI_KEY_FILE,
    get_gemini_api_key,
    set_gemini_api_key,
    remove_gemini_api_key,
    get_settings,
)
from app.services.ai_service import AIService
from app.main import create_app


@pytest.fixture(autouse=True)
def clean_gemini_key():
    """Ensure gemini.key does not pollute environment during tests."""
    backup = None
    if GEMINI_KEY_FILE.exists():
        backup = GEMINI_KEY_FILE.read_text(encoding="utf-8")
        GEMINI_KEY_FILE.unlink()
    yield
    if backup is not None:
        GEMINI_KEY_FILE.write_text(backup, encoding="utf-8")
    elif GEMINI_KEY_FILE.exists():
        GEMINI_KEY_FILE.unlink()


def test_gemini_key_is_gitignored_and_not_tracked():
    """Verifies that gemini.key is ignored by Git and never tracked in the repository."""
    result = subprocess.run(
        ["git", "check-ignore", "gemini.key"],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, "gemini.key must be matched by .gitignore"
    assert "gemini.key" in result.stdout

    tracked = subprocess.run(
        ["git", "ls-files", "gemini.key"],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
    )
    assert tracked.stdout.strip() == "", "gemini.key must NEVER be tracked by Git"


def test_ai_throughput_node_calculation():
    """Verifies formula for minimum nodes required for target throughput."""
    service = AIService.get_instance()

    # Multi-Region (2,700 writes/sec per node)
    assert service.calculate_nodes_for_throughput(target_writes_per_sec=20000, is_multi_region=True) == 8
    assert service.calculate_nodes_for_throughput(target_writes_per_sec=2700, is_multi_region=True) == 1
    assert service.calculate_nodes_for_throughput(target_writes_per_sec=2701, is_multi_region=True) == 2

    # Regional (3,500 writes/sec per node)
    assert service.calculate_nodes_for_throughput(target_writes_per_sec=20000, is_multi_region=False) == 6
    assert service.calculate_nodes_for_throughput(target_writes_per_sec=3500, is_multi_region=False) == 1

    # Reads (15k reads multi-region, 22.5k reads regional)
    assert service.calculate_nodes_for_throughput(target_reads_per_sec=100000, is_multi_region=True) == 7
    assert service.calculate_nodes_for_throughput(target_reads_per_sec=100000, is_multi_region=False) == 5


def test_ai_grounding_context_content():
    """Verifies that the grounding prompt contains Spanner topologies, client regions, and capacity rules."""
    service = AIService.get_instance()
    context = service.get_grounding_context()

    assert "eur3" in context
    assert "eur5" in context
    assert "nam3" in context
    assert "AVAILABLE GCP CLIENT REGIONS" in context
    assert "2,700 writes/sec" in context
    assert "3,500 writes/sec" in context


def test_load_prompt_template_and_files_exist():
    """Verifies that prompt markdown files exist on disk and load correctly."""
    service = AIService.get_instance()
    system_prompt = service.load_prompt_template("system_instruction.md")
    assert len(system_prompt) > 0
    assert "Cloud Spanner Architecture & Benchmark Assistant (Experimental)" in system_prompt
    assert "{{GROUNDING_CONTEXT}}" in system_prompt
    assert "{{CURRENT_CONTEXT}}" in system_prompt
    assert "DO NOT MAKE BOLD OR SPECULATIVE LATENCY CLAIMS" in system_prompt

    transcribe_prompt = service.load_prompt_template("transcribe_instruction.md")
    assert len(transcribe_prompt) > 0
    assert "audio transcriber for Google Cloud and Cloud Spanner" in transcribe_prompt


def test_gemini_key_lifecycle():
    """Verifies saving, reading, and deleting gemini.key."""
    assert get_gemini_api_key() is None

    test_key = "AIzaSyTestMockKey12345"
    set_gemini_api_key(test_key)
    assert get_gemini_api_key() == test_key
    assert GEMINI_KEY_FILE.exists()

    remove_gemini_api_key()
    assert get_gemini_api_key() is None
    assert not GEMINI_KEY_FILE.exists()


@pytest.mark.asyncio
async def test_ai_status_endpoint():
    """Verifies /api/v1/ai/status reflects key availability."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Without key
        resp = await client.get("/api/v1/ai/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is True
        assert data["has_key"] is False
        assert data["model"] == "gemini-3.8-flash"

        # With key
        set_gemini_api_key("AIzaSyDummyKeyForTest")
        resp = await client.get("/api/v1/ai/status")
        assert resp.status_code == 200
        assert resp.json()["has_key"] is True


@pytest.mark.asyncio
async def test_ai_chat_without_key_returns_400():
    """Calling chat without key or header must return 400 with a clear message."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "messages": [{"role": "user", "content": "I want to test eur3"}]
        }
        resp = await client.post("/api/v1/ai/chat", json=payload)
        assert resp.status_code == 400
        assert "Gemini API key is not configured" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_ai_service_chat_gemini_parsing():
    """Tests that AIService parses Gemini JSON response correctly."""
    service = AIService.get_instance()
    mock_gemini_response = {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": (
                                '{\n'
                                '  "message": "I configured a benchmark test for eur5 with clients in London and US.",\n'
                                '  "action": "configure_and_size",\n'
                                '  "spanner_config": "eur5",\n'
                                '  "leader_region": "europe-west2",\n'
                                '  "nodes": 8,\n'
                                '  "client_regions": ["europe-west2", "us-central1", "us-east1"],\n'
                                '  "benchmark_name": "Benchmark (eur5 - US RO & Leader)",\n'
                                '  "optional_replicas": ["us-east1"]\n'
                                '}'
                            )
                        }
                    ]
                }
            }
        ]
    }

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = Response(200, json=mock_gemini_response)
        res = await service.chat(
            messages=[{"role": "user", "content": "20k writes in eur5"}],
            api_key_override="AIzaSyMockKey",
        )
        assert res["action"] == "configure_and_size"
        assert res["spanner_config"] == "eur5"
        assert res["nodes"] == 8
        assert "europe-west2" in res["client_regions"]
        assert res["optional_replicas"] == ["us-east1"]


@pytest.mark.asyncio
async def test_ai_system_instruction_optional_replicas_pruning():
    """Verifies that the Gemini system instruction instructs pruning of optional read-only replicas."""
    service = AIService.get_instance()
    mock_gemini_response = {
        "candidates": [
            {
                "content": {
                    "parts": [{"text": '{"message": "ok", "action": "none"}'}]
                }
            }
        ]
    }
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = Response(200, json=mock_gemini_response)
        await service.chat(
            messages=[{"role": "user", "content": "Configure nam3"}],
            api_key_override="AIzaSyMockKey",
        )
        assert mock_post.called
        call_json = mock_post.call_args[1]["json"]
        system_instruction = call_json["system_instruction"]["parts"][0]["text"]

        assert "optional read-only replicas" in system_instruction
        assert "optional_replicas" in system_instruction
        assert "pruned" in system_instruction or "deselected" in system_instruction


@pytest.mark.asyncio
async def test_ai_service_chat_fallback_regex_optional_replicas():
    """Verifies regex fallback parsing correctly extracts optional_replicas and client_regions."""
    service = AIService.get_instance()
    malformed_json_text = (
        'Here is the result:\n'
        '{\n'
        '  "message": "Selecting nam3 with US East read replica only.",\n'
        '  "action": "configure_benchmark",\n'
        '  "spanner_config": "nam3",\n'
        '  "nodes": 5,\n'
        '  "client_regions": ["us-east4", "us-east1"],\n'
        '  "optional_replicas": ["us-west1", "us-east5"]\n'
        '// broken trailing json'
    )
    mock_gemini_response = {
        "candidates": [
            {
                "content": {
                    "parts": [{"text": malformed_json_text}]
                }
            }
        ]
    }
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = Response(200, json=mock_gemini_response)
        res = await service.chat(
            messages=[{"role": "user", "content": "nam3 with US read replica"}],
            api_key_override="AIzaSyMockKey",
        )
        assert res["action"] == "configure_benchmark"
        assert res["spanner_config"] == "nam3"
        assert res["nodes"] == 5
        assert res["client_regions"] == ["us-east4", "us-east1"]
        assert res["optional_replicas"] == ["us-west1", "us-east5"]


@pytest.mark.asyncio
async def test_ai_system_instruction_experimental_and_latency_guardrails():
    """Verifies that the Gemini system instruction explicitly flags experimental status and forbids bold latency claims."""
    service = AIService.get_instance()
    mock_gemini_response = {
        "candidates": [
            {
                "content": {
                    "parts": [{"text": '{"message": "ok", "action": "none"}'}]
                }
            }
        ]
    }
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = Response(200, json=mock_gemini_response)
        await service.chat(
            messages=[{"role": "user", "content": "What is the latency?"}],
            api_key_override="AIzaSyMockKey",
        )
        assert mock_post.called
        call_json = mock_post.call_args[1]["json"]
        system_instruction = call_json["system_instruction"]["parts"][0]["text"]

        assert "Experimental" in system_instruction
        assert "Strict Latency & Performance Claims Guardrail" in system_instruction
        assert "DO NOT MAKE BOLD OR SPECULATIVE LATENCY CLAIMS" in system_instruction
        assert "DO NOT USE SPECIFIC NUMBERS" in system_instruction


@pytest.mark.asyncio
async def test_ai_chat_endpoint_integration():
    """Tests end-to-end /api/v1/ai/chat endpoint."""
    app = create_app()
    set_gemini_api_key("AIzaSyMockValidKey")

    mock_chat_result = {
        "message": "Configured eur5 with 8 nodes.",
        "action": "configure_and_size",
        "spanner_config": "eur5",
        "leader_region": "europe-west2",
        "nodes": 8,
        "client_regions": ["europe-west2", "us-central1"],
    }

    with patch.object(AIService, "chat", new_callable=AsyncMock) as mock_chat:
        mock_chat.return_value = mock_chat_result

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {
                "messages": [
                    {
                        "role": "user",
                        "content": "I want 20k writes in eur3 with clients in leader and US RO regions",
                    }
                ],
                "current_context": {"selected_config": "eur3"},
            }
            resp = await client.post("/api/v1/ai/chat", json=payload)
            assert resp.status_code == 200
            data = resp.json()

            assert data["action"] == "configure_and_size"
            assert data["spanner_config"] == "eur5"
            assert data["nodes"] == 8
            assert "europe-west2" in data["client_regions"]
            assert "Configured eur5" in data["message"]


def test_clean_latex_math():
    raw_sample = (
        r"Target Writes: $50,000 \text{ writes/sec} \div 2,700 = 18.52 \implies \mathbf{19 \text{ nodes}}$"
    )
    cleaned = AIService.clean_latex_math(raw_sample)
    assert r"\text" not in cleaned
    assert r"\mathbf" not in cleaned
    assert r"\div" not in cleaned
    assert r"\implies" not in cleaned
    assert "$" not in cleaned
    assert "Target Writes: 50,000 writes/sec ÷ 2,700 = 18.52 → **19 nodes**" == cleaned


@pytest.mark.asyncio
async def test_ai_transcribe_without_key_returns_400(tmp_path):
    key_file = tmp_path / "gemini.key"
    app = create_app()
    with patch("app.core.config.GEMINI_KEY_FILE", key_file):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/api/v1/ai/transcribe",
                json={"audio_base64": "dummybase64", "audio_mime_type": "audio/webm"},
            )
            assert resp.status_code == 400


@pytest.mark.asyncio
async def test_ai_transcribe_endpoint_success(tmp_path):
    key_file = tmp_path / "gemini.key"
    key_file.write_text("dummy-test-key")
    app = create_app()
    with patch("app.core.config.GEMINI_KEY_FILE", key_file):
        with patch.object(AIService, "transcribe_audio", new_callable=AsyncMock) as mock_transcribe:
            mock_transcribe.return_value = "Configure eur3 with 20k writes per second"
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/v1/ai/transcribe",
                    json={"audio_base64": "dummybase64", "audio_mime_type": "audio/webm"},
                )
                assert resp.status_code == 200
                data = resp.json()
                assert data["text"] == "Configure eur3 with 20k writes per second"
                mock_transcribe.assert_awaited_once_with(
                    audio_base64="dummybase64",
                    mime_type="audio/webm",
                    api_key_override=None,
                )
