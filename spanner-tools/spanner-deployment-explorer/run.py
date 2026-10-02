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
Google Cloud Spanner Deployment Explorer & Topology Visualizer
Single-Port Launcher & Self-Bootstrapping Startup Script
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "backend"
FRONTEND_DIR = ROOT_DIR / "frontend"
STATIC_DIR = BACKEND_DIR / "app" / "static"
VENV_DIR = ROOT_DIR / "venv"

def get_venv_python() -> Path:
    """Returns the path to the Python interpreter in the local virtual environment."""
    if sys.platform == "win32":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"

def ensure_python_environment():
    """
    Checks if required dependencies (uvicorn, fastapi) are installed.
    If missing, automatically provisions ./venv, installs requirements.txt,
    and re-executes using the virtual environment Python.
    """
    try:
        import uvicorn  # noqa: F401
        import fastapi  # noqa: F401
        # Dependencies already present in the active interpreter
        return
    except ImportError:
        pass

    print("⚡ FastAPI or Uvicorn not found in the current Python environment.")
    venv_python = get_venv_python()

    # Step 1: Create venv if missing
    if not VENV_DIR.exists() or not venv_python.exists():
        print(f"⚙️  Creating Python virtual environment in {VENV_DIR} ...")
        try:
            subprocess.run([sys.executable, "-m", "venv", str(VENV_DIR)], check=True)
            print("✅ Virtual environment created successfully.")
        except Exception as e:
            print(f"❌ Failed to create virtual environment: {e}")
            sys.exit(1)

    # Step 2: Install backend requirements into venv
    requirements_file = BACKEND_DIR / "requirements.txt"
    if requirements_file.exists():
        print(f"📦 Installing Python dependencies from {requirements_file.name} ...")
        try:
            subprocess.run(
                [str(venv_python), "-m", "pip", "install", "--upgrade", "pip"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                [str(venv_python), "-m", "pip", "install", "-r", str(requirements_file)],
                check=True,
            )
            print("✅ Python dependencies installed successfully.")
        except Exception as e:
            print(f"❌ Failed to install dependencies into virtual environment: {e}")
            sys.exit(1)

    # Step 3: Re-execute this launcher with the virtualenv Python
    print(f"🔄 Re-launching application under virtual environment ({venv_python}) ...\n")
    try:
        os.execv(str(venv_python), [str(venv_python)] + sys.argv)
    except Exception as e:
        print(f"❌ Failed to re-execute with virtual environment Python: {e}")
        sys.exit(1)

def check_and_build_frontend():
    """Builds the frontend if static assets are missing."""
    if not (STATIC_DIR / "index.html").exists():
        print("⚡ Static UI assets not found. Checking Node.js tooling to build frontend bundle...")
        if not shutil.which("npm"):
            print("⚠️ 'npm' command not found in PATH. Static frontend cannot be compiled automatically.")
            print("   The backend will run in API-only mode at /api/docs.")
            return

        try:
            print("📦 Installing frontend dependencies (npm install)...")
            subprocess.run(["npm", "install"], cwd=str(FRONTEND_DIR), check=True)
            print("🔨 Compiling production frontend bundle (npm run build)...")
            subprocess.run(["npm", "run", "build"], cwd=str(FRONTEND_DIR), check=True)
            print("✅ Frontend build completed successfully.")
        except Exception as e:
            print(f"⚠️ Could not automatically build frontend ({e}). Backend will run in API-only mode.")

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Google Cloud Spanner Deployment Explorer & Topology Visualizer Launcher"
    )
    parser.add_argument(
        "--deploy-appengine",
        action="store_true",
        help="Deploy the application to Google App Engine Flexible (load testing disabled)",
    )
    parser.add_argument(
        "--docker-build",
        action="store_true",
        help="Build production Docker container image locally (dbexplorer-reloaded)",
    )
    parser.add_argument(
        "-p", "--project",
        type=str,
        default=None,
        help="Google Cloud Project ID for App Engine deployment",
    )
    parser.add_argument(
        "-d", "--dry-run",
        action="store_true",
        help="Perform preflight checks and print commands without deploying",
    )
    parser.add_argument(
        "-v", "--debug",
        action="store_true",
        help="Enable debug verbosity during deployment",
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip interactive confirmation prompts during deployment",
    )

    args = parser.parse_args()

    # Handle App Engine deployment option
    if args.deploy_appengine:
        deploy_script = ROOT_DIR / "scripts" / "deploy_appengine.sh"
        cmd = [str(deploy_script)]
        if args.project:
            cmd.extend(["--project", args.project])
        if args.dry_run:
            cmd.append("--dry-run")
        if args.debug:
            cmd.append("--debug")
        if args.yes:
            cmd.append("--yes")
        sys.exit(subprocess.run(cmd).returncode)

    # Handle local Docker container build option
    if args.docker_build:
        print("🐳 Building Docker container image 'dbexplorer-reloaded' ...")
        cmd = ["docker", "build", "-t", "dbexplorer-reloaded", str(ROOT_DIR)]
        sys.exit(subprocess.run(cmd).returncode)

    print("=" * 72)
    print("  Google Cloud Spanner Deployment Explorer & Topology Visualizer")
    print("=" * 72)

    # Automatically check and install backend dependencies if needed
    ensure_python_environment()

    # Automatically check and build frontend assets if missing
    check_and_build_frontend()

    # Add backend directory to Python path
    sys.path.insert(0, str(BACKEND_DIR))

    # Secure local binding (default 127.0.0.1)
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8080"))
    reload = os.getenv("RELOAD", "true").lower() in ("true", "1", "yes")

    print(f"\n🚀 Starting Web Interface at: http://{host}:{port}")
    print(f"📖 OpenAPI Swagger Docs at:   http://{host}:{port}/api/docs")
    print(f"🌐 REST API Endpoint at:     http://{host}:{port}/api/v1/configs\n")

    import uvicorn
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

