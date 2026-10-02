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

from fastapi import APIRouter, Query
from app.models.project import ProjectsResponse
from app.services.project_service import get_project_service

router = APIRouter(tags=["Discovery"])


@router.get("/projects", response_model=ProjectsResponse)
async def list_gcp_projects(
    force_refresh: bool = Query(False, description="Bypass cache and force refresh from GCP")
):
    """
    Lists accessible Google Cloud projects with in-memory caching and active project first.
    """
    service = get_project_service()
    return service.list_projects(force_refresh=force_refresh)
