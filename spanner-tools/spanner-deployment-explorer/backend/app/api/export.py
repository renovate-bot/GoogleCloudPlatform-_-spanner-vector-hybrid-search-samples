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

from fastapi import APIRouter, HTTPException
from app.models.spanner import CliExportRequest, CliExportResponse
from app.services.spanner_service import SpannerService

router = APIRouter(prefix="/export", tags=["CLI & Automation Export"])

@router.post("/cli", response_model=CliExportResponse)
async def export_cli(request: CliExportRequest):
    service = SpannerService()
    detail = service.get_config_detail(request.configname, request.nodes, request.leader_region)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Configuration '{request.configname}' not found")
    return service.generate_cli_export(
        request.configname,
        request.nodes,
        request.leader_region,
        request.optional_replicas,
    )
