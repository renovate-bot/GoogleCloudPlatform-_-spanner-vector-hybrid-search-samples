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

import re
import os
import subprocess
from pathlib import Path
import pytest
from httpx import AsyncClient, ASGITransport

ROOT_DIR = Path(__file__).resolve().parent.parent.parent


def test_app_yaml_exists_and_configured():
    app_yaml_path = ROOT_DIR / "app.yaml"
    assert app_yaml_path.exists(), "app.yaml must exist at the root directory"

    content = app_yaml_path.read_text(encoding="utf-8")

    # Verify custom runtime and flex environment
    assert re.search(r"^runtime:\s*custom\s*$", content, re.MULTILINE), "runtime must be custom"
    assert re.search(r"^env:\s*flex\s*$", content, re.MULTILINE), "env must be flex"

    # Verify manual scaling
    assert re.search(r"instances:\s*1\s*$", content, re.MULTILINE), "instances must be 1"

    # Verify resources
    assert re.search(r"cpu:\s*2\s*$", content, re.MULTILINE), "cpu must be 2"
    assert re.search(r"memory_gb:\s*4\s*$", content, re.MULTILINE), "memory_gb must be 4"
    assert re.search(r"disk_size_gb:\s*10\s*$", content, re.MULTILINE), "disk_size_gb must be 10"

    # Verify environment variables
    assert re.search(r'ENABLE_LOAD_TESTING:\s*"?false"?', content, re.MULTILINE), (
        "ENABLE_LOAD_TESTING must be set to false in app.yaml"
    )
    assert re.search(r'ENABLE_AI:\s*"?false"?', content, re.MULTILINE), (
        "ENABLE_AI must be set to false in app.yaml"
    )
    assert re.search(r'HOST:\s*"0\.0\.0\.0"', content, re.MULTILINE), "HOST must be 0.0.0.0 in app.yaml"
    assert not re.search(r'^\s*PORT:', content, re.MULTILINE), (
        "PORT is reserved by App Engine and must not be defined in app.yaml"
    )



def test_dockerfile_exists_and_configured():
    dockerfile_path = ROOT_DIR / "Dockerfile"
    assert dockerfile_path.exists(), "Dockerfile must exist at the root directory"

    content = dockerfile_path.read_text(encoding="utf-8")

    # Multi-stage build
    assert "node:20-alpine" in content or "node:" in content, "Dockerfile should build frontend in stage 1"
    assert "python:3.12" in content, "Dockerfile should use Python 3.12 runtime in stage 2"
    assert "EXPOSE 8080" in content, "Dockerfile must expose port 8080"
    assert "ENABLE_LOAD_TESTING=false" in content, "Dockerfile should default ENABLE_LOAD_TESTING to false"
    assert "ENABLE_AI=false" in content, "Dockerfile should default ENABLE_AI to false"
    assert "main.py" in content, "Dockerfile CMD should run main.py"


def test_root_requirements_txt():
    root_req_path = ROOT_DIR / "requirements.txt"
    backend_req_path = ROOT_DIR / "backend" / "requirements.txt"

    assert root_req_path.exists(), "requirements.txt must exist at root"
    root_lines = [l.strip() for l in root_req_path.read_text().splitlines() if l.strip() and not l.startswith("#")]
    backend_lines = [l.strip() for l in backend_req_path.read_text().splitlines() if l.strip() and not l.startswith("#")]

    for dep in ["fastapi", "uvicorn", "pydantic", "httpx"]:
        assert any(dep in line for line in root_lines), f"{dep} must be in root requirements.txt"
        assert any(dep in line for line in backend_lines), f"{dep} must be in backend requirements.txt"


def test_main_py_entrypoint():
    main_py_path = ROOT_DIR / "main.py"
    assert main_py_path.exists(), "main.py must exist at the root directory"

    # Verify that app can be imported from root main
    import sys
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))

    import main as root_main
    assert hasattr(root_main, "app"), "main.py must export 'app' FastAPI instance"


@pytest.mark.asyncio
async def test_app_with_load_testing_disabled(monkeypatch):
    monkeypatch.setenv("ENABLE_LOAD_TESTING", "false")

    from app.main import create_app
    app = create_app()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Check system config
        resp = await client.get("/api/v1/system/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["features"]["enable_load_testing"] is False

        # Benchmark endpoint must return 404
        bench_resp = await client.get("/api/v1/benchmark/client-regions")
        assert bench_resp.status_code == 404


@pytest.mark.asyncio
async def test_app_with_ai_disabled(monkeypatch):
    monkeypatch.setenv("ENABLE_AI", "false")

    from app.main import create_app
    app = create_app()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Check system config
        resp = await client.get("/api/v1/system/config")
        assert resp.status_code == 200
        data = resp.json()
        assert data["features"]["enable_ai"] is False

        # AI endpoints must return 404
        ai_resp = await client.get("/api/v1/ai/status")
        assert ai_resp.status_code == 404


def test_zero_pii_in_deployment_artifacts():
    """Verify deployment configurations and scripts do not contain PII or hardcoded project names."""
    files_to_check = [
        ROOT_DIR / "app.yaml",
        ROOT_DIR / "Dockerfile",
        ROOT_DIR / "main.py",
        ROOT_DIR / "requirements.txt",
        ROOT_DIR / ".dockerignore",
        ROOT_DIR / ".gcloudignore",
        ROOT_DIR / "scripts" / "deploy_appengine.sh",
    ]

    # Prohibited patterns indicating specific personal project IDs or emails
    forbidden_terms = [
        "".join(["s", "r", "o", "z"]),
        "".join(["s", "r", "o", "z", "p", "l", "a", "y"]),
        "@gmail.com",
        "user@",
        "/Users/",
        "/home/",
    ]

    for file_path in files_to_check:
        if file_path.exists():
            content = file_path.read_text(encoding="utf-8")
            for term in forbidden_terms:
                assert term not in content.lower(), (
                    f"File {file_path.name} contains forbidden environment/PII term: '{term}'"
                )


def test_benchmarks_json_is_gitignored_and_not_tracked():
    """Verify backend/data/benchmarks.json is excluded by gitignore, dockerignore, gcloudignore and untracked."""
    gitignore = (ROOT_DIR / ".gitignore").read_text(encoding="utf-8")
    assert "backend/data/benchmarks.json" in gitignore, "benchmarks.json must be in .gitignore"

    dockerignore = (ROOT_DIR / ".dockerignore").read_text(encoding="utf-8")
    assert "backend/data/benchmarks.json" in dockerignore, "benchmarks.json must be in .dockerignore"

    gcloudignore = (ROOT_DIR / ".gcloudignore").read_text(encoding="utf-8")
    assert "backend/data/benchmarks.json" in gcloudignore, "benchmarks.json must be in .gcloudignore"

    res = subprocess.run(
        ["git", "ls-files", "backend/data/benchmarks.json"],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert res.stdout.strip() == "", "backend/data/benchmarks.json must not be tracked in git index"


def test_gemini_key_is_excluded_from_deployments():
    """Verify gemini.key is excluded by gitignore, dockerignore, gcloudignore and untracked."""
    gitignore = (ROOT_DIR / ".gitignore").read_text(encoding="utf-8")
    assert "gemini.key" in gitignore, "gemini.key must be in .gitignore"

    dockerignore = (ROOT_DIR / ".dockerignore").read_text(encoding="utf-8")
    assert "gemini.key" in dockerignore, "gemini.key must be in .dockerignore"

    gcloudignore = (ROOT_DIR / ".gcloudignore").read_text(encoding="utf-8")
    assert "gemini.key" in gcloudignore, "gemini.key must be in .gcloudignore"

    res = subprocess.run(
        ["git", "ls-files", "gemini.key"],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert res.stdout.strip() == "", "gemini.key must never be tracked in git index"



def test_deploy_script_dry_run():
    script_path = ROOT_DIR / "scripts" / "deploy_appengine.sh"
    assert script_path.exists()
    assert os.access(script_path, os.X_OK), "deploy_appengine.sh must be executable"

    result = subprocess.run(
        [str(script_path), "--dry-run", "-p", "test-automated-validation-proj"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Deploy script dry-run failed: {result.stderr}"
    assert "test-automated-validation-proj@appspot.gserviceaccount.com" in result.stdout
    assert "gs://staging.test-automated-validation-proj.appspot.com" in result.stdout
    assert "enable_load_testing: false" in result.stdout
    assert "enable_ai: false" in result.stdout


def test_deploy_script_equals_syntax():
    script_path = ROOT_DIR / "scripts" / "deploy_appengine.sh"
    result = subprocess.run(
        [str(script_path), "--dry-run", "--project=test-equals-arg-proj"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Deploy script equals syntax failed: {result.stderr}"
    assert "test-equals-arg-proj@appspot.gserviceaccount.com" in result.stdout
    assert "gs://staging.test-equals-arg-proj.appspot.com" in result.stdout
    assert "enable_load_testing: false" in result.stdout
    assert "enable_ai: false" in result.stdout

