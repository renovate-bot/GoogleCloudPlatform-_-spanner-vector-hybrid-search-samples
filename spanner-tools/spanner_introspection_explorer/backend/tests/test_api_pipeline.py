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

import pytest
from starlette.testclient import TestClient
from backend.app.main import app

def test_api_pipeline_staging():
    client = TestClient(app)
    response = client.get("/api/v1/pipeline/staging")
    assert response.status_code == 200
    data = response.json()
    assert "files" in data

def test_api_pipeline_config():
    client = TestClient(app)
    response = client.get("/api/v1/pipeline/config")
    assert response.status_code == 200
    data = response.json()
    assert "database_file" in data
    assert "staging_dir" in data

def test_api_pipeline_reload_stream_get():
    client = TestClient(app)
    response = client.get("/api/v1/pipeline/reload/stream")
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert "data:" in response.text

def test_api_pipeline_reload_stream_post():
    client = TestClient(app)
    response = client.post("/api/v1/pipeline/reload/stream")
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert "data:" in response.text
