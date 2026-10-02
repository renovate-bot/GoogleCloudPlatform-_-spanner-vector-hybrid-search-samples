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
import app.services.benchmark_service as bs


@pytest.fixture(autouse=True)
def isolate_benchmark_data(tmp_path, monkeypatch):
    """Keep tests away from the real benchmarks.json data store."""
    monkeypatch.setenv("DBEXPLORER_DATA_DIR", str(tmp_path))
    bs._benchmark_service_instance = None
    yield
    bs._benchmark_service_instance = None
