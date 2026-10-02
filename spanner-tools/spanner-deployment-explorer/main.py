#!/usr/bin/env python3
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

"""
Google Cloud Spanner Deployment Explorer
Application Entry Point for Google App Engine and Containerized Runtimes
"""

import os
import sys
from pathlib import Path

# Add backend directory to Python module search path
ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

# Expose FastAPI application instance for ASGI servers (e.g. uvicorn main:app)
from app.main import app  # noqa: E402


def main():
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    reload = os.getenv("RELOAD", "false").lower() in ("true", "1", "yes")

    print(f"🚀 Starting Spanner Deployment Explorer on http://{host}:{port}")
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload,
        reload_dirs=[str(BACKEND_DIR)] if reload else None,
        app_dir=str(BACKEND_DIR),
    )


if __name__ == "__main__":
    main()
