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

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from app.models.spanner import SpannerConfigSummary, SpannerConfigDetail, ConfigTreeNode
from app.services.spanner_service import SpannerService

router = APIRouter(prefix="/configs", tags=["Configurations"])

@router.get("", response_model=List[SpannerConfigSummary])
async def list_configs(
    continent: Optional[str] = Query(None, description="Filter by continent region"),
    instance_type: Optional[str] = Query(None, description="Filter by instance type (regional, dual-region, multi-region)"),
    search: Optional[str] = Query(None, description="Search term for config name, location, or region"),
):
    service = SpannerService()
    return service.list_configs(continent=continent, instance_type=instance_type, search=search)

@router.get("/tree", response_model=List[ConfigTreeNode])
async def get_config_tree():
    service = SpannerService()
    return service.get_config_tree()

@router.get("/{configname}", response_model=SpannerConfigDetail)
async def get_config_detail(
    configname: str,
    nodes: int = Query(1, ge=1, le=1000, description="Node count for throughput estimation"),
):
    service = SpannerService()
    detail = service.get_config_detail(configname, nodes=nodes)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Configuration '{configname}' not found")
    return detail
