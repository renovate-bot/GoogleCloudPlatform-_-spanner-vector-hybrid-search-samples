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
import os
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field

# Locate root directory (two levels up from app/core)
BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
ROOT_DIR = BACKEND_DIR.parent
CONFIG_PATH = ROOT_DIR / "config.json"
GEMINI_KEY_FILE = ROOT_DIR / "gemini.key"


def get_gemini_api_key() -> Optional[str]:
    """Returns the Gemini API key from gemini.key file or environment variable."""
    if GEMINI_KEY_FILE.exists():
        try:
            content = GEMINI_KEY_FILE.read_text(encoding="utf-8").strip()
            if content:
                return content
        except Exception:
            pass
    return os.environ.get("GEMINI_API_KEY") or os.environ.get("AI_API_KEY")


def set_gemini_api_key(key: str) -> None:
    """Saves the Gemini API key to gemini.key file in project root."""
    GEMINI_KEY_FILE.write_text(key.strip() + "\n", encoding="utf-8")


def remove_gemini_api_key() -> None:
    """Removes the gemini.key file if it exists."""
    if GEMINI_KEY_FILE.exists():
        try:
            GEMINI_KEY_FILE.unlink()
        except Exception:
            pass


class FeatureFlags(BaseModel):
    enable_load_testing: bool = Field(default=True, description="Enable latency load testing suite and UI controls")
    allow_dry_run: bool = Field(default=True, description="Allow physics-based simulated latency testing without cloud provisioning")
    allow_multiple_selections: bool = Field(default=False, description="Allow selecting multiple Spanner configs simultaneously")
    enable_edge_latency: bool = Field(default=True, description="Enable edge latency and distance visualization on topology links")
    enable_ai: bool = Field(default=True, description="Enable Spanner AI Assistant chat and sizing features")
    has_ai_key: bool = Field(default=False, description="True if a Gemini API key is configured and active")
    ai_model: str = Field(default="gemini-3.8-flash", description="Configured Gemini AI model name")


class LoadTestingSettings(BaseModel):
    default_client_regions: List[str] = Field(
        default_factory=list,
        description="Default GCE client regions pre-selected for load testing",
    )
    default_operations: int = Field(default=1000, ge=1, le=10000, description="Default number of benchmark operations")
    default_staleness_seconds: int = Field(default=15, ge=1, le=3600, description="Default staleness for stale read tests")
    gce_machine_type: str = Field(default="e2-standard-2", description="GCE VM machine type for client benchmark runners")
    spanner_nodes: int = Field(default=1, ge=1, le=1, description="Nodes count for test Spanner instance (fixed at 1)")


class AppSettings(BaseModel):
    features: FeatureFlags = Field(default_factory=FeatureFlags)
    load_testing: LoadTestingSettings = Field(default_factory=LoadTestingSettings)


def _load_settings() -> AppSettings:
    raw_data = {}
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except Exception as e:
            print(f"⚠️ Warning: Could not parse {CONFIG_PATH}: {e}. Using defaults.")

    features_data = raw_data.get("features", {})
    load_testing_data = raw_data.get("load_testing", {})

    # Environment variable overrides
    if "ENABLE_LOAD_TESTING" in os.environ:
        features_data["enable_load_testing"] = os.environ["ENABLE_LOAD_TESTING"].lower() in ("true", "1", "yes")
    if "ALLOW_DRY_RUN" in os.environ:
        features_data["allow_dry_run"] = os.environ["ALLOW_DRY_RUN"].lower() in ("true", "1", "yes")
    if "ALLOW_MULTIPLE_SELECTIONS" in os.environ:
        features_data["allow_multiple_selections"] = os.environ["ALLOW_MULTIPLE_SELECTIONS"].lower() in ("true", "1", "yes")
    if "ENABLE_EDGE_LATENCY" in os.environ:
        features_data["enable_edge_latency"] = os.environ["ENABLE_EDGE_LATENCY"].lower() in ("true", "1", "yes")
    if "ENABLE_AI" in os.environ:
        features_data["enable_ai"] = os.environ["ENABLE_AI"].lower() in ("true", "1", "yes")

    # Set AI key status and model
    features_data["has_ai_key"] = bool(get_gemini_api_key())
    features_data["ai_model"] = os.environ.get(
        "GEMINI_MODEL", raw_data.get("ai", {}).get("model", "gemini-3.8-flash")
    )

    if "LOAD_TESTING_GCE_MACHINE_TYPE" in os.environ:
        load_testing_data["gce_machine_type"] = os.environ["LOAD_TESTING_GCE_MACHINE_TYPE"]

    return AppSettings(
        features=FeatureFlags(**features_data),
        load_testing=LoadTestingSettings(**load_testing_data),
    )


_settings_instance: AppSettings = _load_settings()


def get_settings(reload: bool = False) -> AppSettings:
    """Returns the application settings. If reload is True, re-reads config.json from disk."""
    global _settings_instance
    if reload:
        _settings_instance = _load_settings()
    return _settings_instance
