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

from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException

from app.core.config import get_settings
from app.models.benchmark import (
    BenchmarkCampaignConfig,
    BenchmarkCampaignStatus,
    BenchmarkCampaignUpdate,
    ClientRegionInfo,
)
from app.models.preflight import (
    PreflightCheckRequest,
    PreflightCheckResponse,
)
from app.services.benchmark_service import get_benchmark_service
from app.services.preflight_service import PreflightService

router = APIRouter(prefix="/benchmark", tags=["Benchmark"])


@router.post("/preflight", response_model=PreflightCheckResponse)
async def run_preflight_check(request: PreflightCheckRequest):
    """
    Executes pre-flight checklist verifying authentication, project state,
    enabled APIs, and required IAM permissions before provisioning.
    """
    settings = get_settings()
    if not settings.features.allow_dry_run and request.execution_mode.lower() in (
        "dry_run",
        "dryrun",
        "simulate",
        "simulation",
    ):
        raise HTTPException(
            status_code=400,
            detail="Simulated dry-run mode is disabled in system configuration.",
        )
    service = PreflightService()
    return await service.execute_preflight(request)


@router.get("/client-regions", response_model=List[ClientRegionInfo])
async def list_client_regions():
    """
    Returns list of supported GCE client benchmark regions with coordinates.
    """
    service = get_benchmark_service()
    return service.list_client_regions()


@router.get("/campaigns", response_model=List[BenchmarkCampaignStatus])
async def list_benchmark_campaigns():
    """
    Lists all configured benchmark campaigns.
    """
    service = get_benchmark_service()
    return service.list_campaigns()


@router.post("/campaigns", response_model=BenchmarkCampaignStatus)
async def create_benchmark_campaign(config: BenchmarkCampaignConfig):
    """
    Creates a new benchmark test record without running it immediately.
    """
    settings = get_settings()
    if not settings.features.allow_dry_run and config.execution_mode == "dry_run":
        raise HTTPException(
            status_code=400,
            detail="Simulated dry-run mode is disabled in system configuration.",
        )
    service = get_benchmark_service()
    return service.create_campaign(config)


@router.get("/campaigns/{campaign_id}", response_model=BenchmarkCampaignStatus)
async def get_benchmark_campaign(campaign_id: str):
    """
    Gets details of a specific benchmark campaign.
    """
    service = get_benchmark_service()
    status = service.get_campaign(campaign_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")
    return status


@router.put("/campaigns/{campaign_id}", response_model=BenchmarkCampaignStatus)
async def update_benchmark_campaign(campaign_id: str, update: BenchmarkCampaignUpdate):
    """
    Updates configuration parameters for a benchmark test.
    """
    settings = get_settings()
    if not settings.features.allow_dry_run and update.execution_mode == "dry_run":
        raise HTTPException(
            status_code=400,
            detail="Simulated dry-run mode is disabled in system configuration.",
        )
    service = get_benchmark_service()
    updated = service.update_campaign(campaign_id, update)
    if not updated:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")
    return updated


@router.delete("/campaigns/{campaign_id}")
async def delete_benchmark_campaign(campaign_id: str):
    """
    Deletes a benchmark campaign and cleans up its provisioned resources.
    """
    service = get_benchmark_service()
    success = await service.delete_campaign(campaign_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")
    return {"status": "deleted", "campaign_id": campaign_id, "success": True}


@router.post("/campaigns/{campaign_id}/run", response_model=BenchmarkCampaignStatus)
async def run_benchmark_campaign(campaign_id: str):
    """
    Launches execution on an existing benchmark test.
    """
    service = get_benchmark_service()
    campaign = service.get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")

    settings = get_settings()
    if not settings.features.allow_dry_run and campaign.execution_mode == "dry_run":
        raise HTTPException(
            status_code=400,
            detail="Simulated dry-run mode is disabled in system configuration.",
        )

    run_status = await service.run_existing_campaign(campaign_id)
    return run_status


@router.post("/cleanup-all")
async def cleanup_all_benchmark_resources():
    """
    Tears down all provisioned cloud resources across all campaigns.
    """
    service = get_benchmark_service()
    return await service.cleanup_all_resources()


@router.post("/start", response_model=BenchmarkCampaignStatus)
async def start_benchmark_campaign(config: BenchmarkCampaignConfig):
    """
    Starts a new latency load test campaign across selected client regions immediately.
    """
    settings = get_settings()
    if not settings.features.allow_dry_run and config.execution_mode == "dry_run":
        raise HTTPException(
            status_code=400,
            detail="Simulated dry-run mode is disabled in system configuration.",
        )
    service = get_benchmark_service()
    return await service.start_campaign(config)


@router.get("/status/{campaign_id}", response_model=BenchmarkCampaignStatus)
async def get_campaign_status(campaign_id: str):
    """
    Retrieves the current progress and telemetry metrics for a campaign.
    """
    service = get_benchmark_service()
    status = service.get_campaign(campaign_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found")
    return status


@router.post("/stop/{campaign_id}")
async def stop_benchmark_campaign(campaign_id: str):
    """
    Stops an active benchmark campaign.
    """
    service = get_benchmark_service()
    stopped = await service.stop_campaign(campaign_id)
    if not stopped:
        raise HTTPException(status_code=404, detail=f"Campaign '{campaign_id}' not found or already stopped")
    return {"status": "stopped", "campaign_id": campaign_id}


@router.post("/cleanup/{campaign_id}")
async def cleanup_campaign_resources(campaign_id: str):
    """
    Tears down all provisioned cloud resources (GCE VMs and Spanner instance).
    """
    service = get_benchmark_service()
    cleaned = await service.cleanup_campaign(campaign_id)
    return {"status": "cleaned", "campaign_id": campaign_id, "success": cleaned}
