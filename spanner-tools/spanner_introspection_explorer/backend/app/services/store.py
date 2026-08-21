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

import json
import threading
from pathlib import Path
from typing import Any, Dict

class ResourceStore:
    """Thread-safe local JSON persistence for configuration and application state."""
    def __init__(self, data_file: Path):
        self._lock = threading.Lock()
        self._data_file = data_file
        self._data_file.parent.mkdir(parents=True, exist_ok=True)

    def get_all(self) -> Dict[str, Any]:
        with self._lock:
            if not self._data_file.exists():
                return {}
            try:
                with open(self._data_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}

    def get(self, key: str, default: Any = None) -> Any:
        data = self.get_all()
        return data.get(key, default)

    def write(self, data: Dict[str, Any]):
        with self._lock:
            with open(self._data_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    def list(self) -> list:
        data = self.get_all()
        return list(data.values())

    def update(self, key: str, value: Any):
        with self._lock:
            data = {}
            if self._data_file.exists():
                try:
                    with open(self._data_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = {}
            data[key] = value
            with open(self._data_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    def put(self, key: str, value: Any):
        self.update(key, value)

    def delete(self, key: str) -> bool:
        with self._lock:
            data = {}
            if self._data_file.exists():
                try:
                    with open(self._data_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = {}
            if key in data:
                del data[key]
                with open(self._data_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                return True
            return False
