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

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class PreflightCheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    CHECKING = "checking"


class PreflightCheckItem(BaseModel):
    id: str = Field(..., description="Unique identifier for the check")
    name: str = Field(..., description="Human-readable title of the check")
    scope: str = Field(..., description="Category: auth, project, api, spanner, compute, network")
    status: PreflightCheckStatus = Field(..., description="Evaluation outcome: passed, failed, warning")
    message: str = Field(..., description="Detailed explanation of the result")
    remediation_command: Optional[str] = Field(None, description="Command to resolve the failure (e.g., gcloud ...)")
    documentation_url: Optional[str] = Field(None, description="Link to relevant Google Cloud documentation")


class PreflightCheckRequest(BaseModel):
    project_id: str = Field(..., min_length=4, max_length=30, description="Target GCP project ID")
    client_regions: List[str] = Field(..., min_length=1, description="Selected GCE regions from which to execute benchmarks")
    spanner_config: str = Field(..., description="Target Spanner instance configuration")
    execution_mode: str = Field(default="dry_run", description="Execution mode: 'live' or 'dry_run'")
    staging_bucket: Optional[str] = Field(default=None, description="Optional custom GCS staging bucket to validate")


class PreflightCheckResponse(BaseModel):
    all_passed: bool = Field(..., description="True if all critical pre-flight checks passed")
    principal: Optional[str] = Field(None, description="Authenticated caller identity (email or service account)")
    project_id: str = Field(..., description="Target GCP project ID")
    execution_mode: str = Field(..., description="Evaluated execution mode")
    checks: List[PreflightCheckItem] = Field(..., description="List of pre-flight check evaluations")
    summary: str = Field(..., description="Human-readable summary of readiness")
