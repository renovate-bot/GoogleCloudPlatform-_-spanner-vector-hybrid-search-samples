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

from fastapi import APIRouter
from app.models.spanner import DeploymentVisualization, VisualizeRequest
from app.services.spanner_service import SpannerService

router = APIRouter(prefix="/deployments", tags=["Deployments"])

@router.post("/visualize", response_model=DeploymentVisualization)
async def visualize_deployments(request: VisualizeRequest):
    service = SpannerService()
    return service.visualize_deployments(
        config_names=request.config_names,
        nodes_map=request.nodes_map,
        leaders_map=request.leaders_map,
        client_regions=request.client_regions,
    )
