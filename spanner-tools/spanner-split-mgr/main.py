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

"""FastAPI application for Spanner Split Points Manager."""
import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import (
    init_db,
    get_all_settings,
    update_settings,
    clear_settings,
    add_local_split,
    get_all_local_splits,
    delete_local_split,
    clear_pending_splits,
)
from models import (
    LocalSplitCreate,
    OperationType,
    SplitStatus,
    SplitPointDisplay,
    EntityType,
    EntitySummary,
    EntityKeySchema,
    RangeSplitRequest,
    RangeSplitResponse,
    RangeValidationResult,
)
from range_utils import (
    validate_range_request,
    generate_range_splits,
)
from spanner_service import get_spanner_service

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

app = FastAPI(
    title="Spanner Split Points Manager",
    description="Manage Google Cloud Spanner split points with local staging",
    version="1.0.0"
)

# Setup templates and static files
BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup."""
    init_db()
    logging.info("Database initialized")


def get_combined_splits(entity_name: Optional[str] = None, entity_type: Optional[EntityType] = None) -> list[SplitPointDisplay]:
    """Get combined view of Spanner and local splits with status.

    Args:
        entity_name: Optional filter by entity name (table or index name)
        entity_type: Optional filter by entity type (TABLE or INDEX)
    """
    spanner_service = get_spanner_service()
    local_splits = get_all_local_splits()

    # Create lookup for local splits
    local_lookup: dict[tuple[str, str], tuple[int, OperationType]] = {}
    for ls in local_splits:
        local_lookup[(ls.table_name, ls.split_value)] = (ls.id, ls.operation_type)

    combined: list[SplitPointDisplay] = []
    seen: set[tuple[str, str]] = set()

    # Add Spanner splits
    if spanner_service.is_configured():
        try:
            spanner_splits = spanner_service.list_splits()
            for sp in spanner_splits:
                # Determine the entity for this split
                split_entity_name = sp.index if sp.index else sp.table
                split_entity_type = EntityType.INDEX if sp.index else EntityType.TABLE

                # Apply filters
                if entity_name and split_entity_name != entity_name:
                    continue
                if entity_type and split_entity_type != entity_type:
                    continue

                key = (sp.table, sp.split_key)
                seen.add(key)

                # Check if this split has a pending delete
                if key in local_lookup and local_lookup[key][1] == OperationType.DELETE:
                    status = SplitStatus.PENDING_DELETE
                    local_id = local_lookup[key][0]
                else:
                    status = SplitStatus.SYNCED
                    local_id = None

                combined.append(SplitPointDisplay(
                    table_name=sp.table,
                    split_value=sp.split_key,
                    status=status,
                    expire_time=sp.expire_time,
                    local_id=local_id,
                    initiator=sp.initiator,
                    index=sp.index
                ))
        except Exception as e:
            logging.error("Error fetching Spanner splits: %s", e)

    # Add pending adds (local splits not in Spanner)
    for ls in local_splits:
        key = (ls.table_name, ls.split_value)
        if key not in seen and ls.operation_type == OperationType.ADD:
            # Determine if this is an index split or table split
            is_index_split = bool(ls.index_name)
            split_entity_name = ls.index_name if is_index_split else ls.table_name
            split_entity_type = EntityType.INDEX if is_index_split else EntityType.TABLE

            # Apply filters
            if entity_name and split_entity_name != entity_name:
                continue
            if entity_type and split_entity_type != entity_type:
                continue

            combined.append(SplitPointDisplay(
                table_name=ls.table_name,
                split_value=ls.index_key if is_index_split else ls.split_value,
                status=SplitStatus.PENDING_ADD,
                local_id=ls.id,
                index=ls.index_name if is_index_split else None,
                index_key=ls.index_key if is_index_split else None,
                table_key=ls.split_value if is_index_split and ls.split_value else None
            ))

    return combined


def get_entity_summaries() -> list[EntitySummary]:
    """Get summary of all entities (tables and indexes) with their split counts.

    This fetches all tables and indexes from INFORMATION_SCHEMA first,
    ensuring entities without splits are still shown in the list.
    """
    spanner_service = get_spanner_service()

    # Initialize entity map with all tables and indexes from INFORMATION_SCHEMA
    entity_map: dict[tuple[str, EntityType], dict] = {}

    if spanner_service.is_configured():
        # Add all tables
        try:
            tables = spanner_service.list_tables()
            for table_name in tables:
                key = (table_name, EntityType.TABLE)
                entity_map[key] = {
                    "entity_name": table_name,
                    "entity_type": EntityType.TABLE,
                    "parent_table": None,
                    "total_splits": 0,
                    "synced_count": 0,
                    "pending_add_count": 0,
                    "pending_delete_count": 0,
                }
        except Exception as e:
            logging.error("Error fetching tables: %s", e)

        # Add all indexes
        try:
            indexes = spanner_service.list_indexes()
            for index_name, parent_table in indexes:
                key = (index_name, EntityType.INDEX)
                entity_map[key] = {
                    "entity_name": index_name,
                    "entity_type": EntityType.INDEX,
                    "parent_table": parent_table,
                    "total_splits": 0,
                    "synced_count": 0,
                    "pending_add_count": 0,
                    "pending_delete_count": 0,
                }
        except Exception as e:
            logging.error("Error fetching indexes: %s", e)

    # Now process splits and update counts
    all_splits = get_combined_splits()

    for split in all_splits:
        # Determine entity name and type
        if split.index:
            entity_name = split.index
            entity_type = EntityType.INDEX
            parent_table = split.table_name
        else:
            entity_name = split.table_name
            entity_type = EntityType.TABLE
            parent_table = None

        key = (entity_name, entity_type)

        # If entity not already in map (e.g., from local pending adds for new entities),
        # add it now
        if key not in entity_map:
            entity_map[key] = {
                "entity_name": entity_name,
                "entity_type": entity_type,
                "parent_table": parent_table,
                "total_splits": 0,
                "synced_count": 0,
                "pending_add_count": 0,
                "pending_delete_count": 0,
            }

        entity_map[key]["total_splits"] += 1

        if split.status == SplitStatus.SYNCED:
            entity_map[key]["synced_count"] += 1
        elif split.status == SplitStatus.PENDING_ADD:
            entity_map[key]["pending_add_count"] += 1
        elif split.status == SplitStatus.PENDING_DELETE:
            entity_map[key]["pending_delete_count"] += 1

    # Convert to list of EntitySummary objects, sorted by type then name
    summaries = [
        EntitySummary(**data)
        for data in entity_map.values()
    ]

    # Sort: Tables first, then Indexes, alphabetically within each group
    summaries.sort(key=lambda x: (x.entity_type.value, x.entity_name))

    return summaries


# Web UI Routes

def _get_connection_info() -> dict:
    """Get current connection info for display in templates."""
    spanner_service = get_spanner_service()
    return {
        "is_configured": spanner_service.is_configured(),
        "project_id": spanner_service.project_id,
        "instance_id": spanner_service.instance_id,
        "database_id": spanner_service.database_id,
    }


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main dashboard page."""
    settings = get_all_settings()
    spanner_service = get_spanner_service()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "settings": settings,
        "is_configured": spanner_service.is_configured(),
        "connection_info": _get_connection_info(),
    })


def _get_env_var_info() -> dict:
    """Get information about environment variables being used for settings."""
    import os
    db_settings = get_all_settings()
    spanner_service = get_spanner_service()

    env_info = {
        "using_env_vars": False,
        "project_id": None,
        "instance_id": None,
        "database_id": None,
    }

    # Check if each setting is coming from env vars (i.e., DB setting is empty but service has a value)
    if not db_settings.project_id and spanner_service.project_id:
        env_info["project_id"] = spanner_service.project_id
        env_info["using_env_vars"] = True

    if not db_settings.instance_id and spanner_service.instance_id:
        env_info["instance_id"] = spanner_service.instance_id
        env_info["using_env_vars"] = True

    if not db_settings.database_id and spanner_service.database_id:
        env_info["database_id"] = spanner_service.database_id
        env_info["using_env_vars"] = True

    return env_info


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """Settings page."""
    settings = get_all_settings()
    env_info = _get_env_var_info()
    spanner_service = get_spanner_service()

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "settings": settings,
        "env_info": env_info,
        "is_configured": spanner_service.is_configured(),
    })


@app.post("/settings", response_class=HTMLResponse)
async def save_settings(
    request: Request,
    project_id: str = Form(""),
    instance_id: str = Form(""),
    database_id: str = Form("")
):
    """Save settings and validate connection."""
    # Save settings first
    update_settings(
        project_id=project_id if project_id else None,
        instance_id=instance_id if instance_id else None,
        database_id=database_id if database_id else None
    )

    # Reset the global spanner service to pick up new settings
    global _spanner_service
    from spanner_service import _spanner_service
    import spanner_service
    spanner_service._spanner_service = None

    # Get fresh service and test connection
    service = get_spanner_service()

    # Only test connection if instance and database are provided
    env_info = _get_env_var_info()
    if instance_id and database_id:
        success, error_message = service.test_connection()
        if success:
            return templates.TemplateResponse("settings.html", {
                "request": request,
                "settings": get_all_settings(),
                "env_info": env_info,
                "is_configured": True,
                "success_message": "Settings saved and connection verified successfully.",
            })
        else:
            return templates.TemplateResponse("settings.html", {
                "request": request,
                "settings": get_all_settings(),
                "env_info": env_info,
                "is_configured": service.is_configured(),
                "error_message": error_message,
            })
    else:
        # Settings saved but no connection to test
        return templates.TemplateResponse("settings.html", {
            "request": request,
            "settings": get_all_settings(),
            "env_info": env_info,
            "is_configured": service.is_configured(),
            "success_message": "Settings saved.",
        })


@app.post("/settings/clear", response_class=HTMLResponse)
async def clear_settings_endpoint(request: Request):
    """Clear all settings and fall back to environment variables."""
    clear_settings()

    # Reset the global spanner service to pick up env vars
    import spanner_service
    spanner_service._spanner_service = None

    env_info = _get_env_var_info()
    service = get_spanner_service()

    # Check if we have env vars to fall back to
    if env_info["using_env_vars"] and service.is_configured():
        success, error_message = service.test_connection()
        if success:
            return templates.TemplateResponse("settings.html", {
                "request": request,
                "settings": get_all_settings(),
                "env_info": env_info,
                "is_configured": True,
                "success_message": "Settings cleared. Now using environment variables.",
            })
        else:
            return templates.TemplateResponse("settings.html", {
                "request": request,
                "settings": get_all_settings(),
                "env_info": env_info,
                "is_configured": True,
                "error_message": f"Settings cleared but connection failed: {error_message}",
            })
    else:
        return templates.TemplateResponse("settings.html", {
            "request": request,
            "settings": get_all_settings(),
            "env_info": env_info,
            "is_configured": service.is_configured(),
            "success_message": "Settings cleared." + (" No environment variables found." if not env_info["using_env_vars"] else ""),
        })


# API Routes (used by Alpine.js frontend)

@app.get("/api/entities")
async def api_list_entities():
    """API: List all entities (tables and indexes) with split counts."""
    return get_entity_summaries()


@app.get("/api/entity-schema")
async def api_get_entity_schema(
    entity_name: str = Query(..., description="Name of the entity (table or index)"),
    entity_type: EntityType = Query(..., description="Type of entity (TABLE or INDEX)")
) -> EntityKeySchema:
    """API: Get key schema information for a table or index.

    Returns the key columns with their types, whether the key is composite or single,
    and for indexes, also returns the parent table's primary key schema.
    """
    spanner_service = get_spanner_service()

    if entity_type == EntityType.TABLE:
        return spanner_service.get_table_key_schema(entity_name)
    else:
        return spanner_service.get_index_key_schema(entity_name)


@app.get("/api/splits")
async def api_list_splits(
    entity_name: Optional[str] = Query(None, description="Filter by entity name"),
    entity_type: Optional[EntityType] = Query(None, description="Filter by entity type (TABLE or INDEX)")
):
    """API: List all split points, optionally filtered by entity."""
    return get_combined_splits(entity_name=entity_name, entity_type=entity_type)


@app.post("/api/splits")
async def api_add_split(split: LocalSplitCreate):
    """API: Add a new local split."""
    result = add_local_split(
        table_name=split.table_name,
        split_value=split.split_value,
        operation_type=split.operation_type,
        index_name=split.index_name,
        index_key=split.index_key
    )
    return result


@app.delete("/api/splits/{split_id}")
async def api_delete_split(split_id: int):
    """API: Delete a local split."""
    if delete_local_split(split_id):
        return {"success": True}
    raise HTTPException(status_code=404, detail="Split not found")


@app.post("/api/splits/clear")
async def api_clear_pending():
    """API: Clear all pending splits."""
    count = clear_pending_splits()
    return {"success": True, "cleared": count}


@app.post("/api/sync")
async def api_sync():
    """API: Sync pending changes to Spanner."""
    spanner_service = get_spanner_service()
    if not spanner_service.is_configured():
        return {"success": False, "message": "Spanner not configured. Please set instance and database in settings."}
    return spanner_service.sync_pending_changes()


@app.get("/api/settings")
async def api_get_settings():
    """API: Get current settings."""
    return get_all_settings()


@app.post("/api/splits/range")
async def api_add_range_splits(request: RangeSplitRequest) -> RangeSplitResponse:
    """API: Add multiple splits using a range specification.

    Generates evenly distributed split values between start and end values.
    Supports INT64 and STRING(UUID) column types for single-column keys.
    """
    spanner_service = get_spanner_service()

    # Get the entity type (table or index)
    entity_name = request.index_name if request.index_name else request.table_name
    entity_type = EntityType.INDEX if request.index_name else EntityType.TABLE

    # Get entity schema for validation
    try:
        if entity_type == EntityType.TABLE:
            schema = spanner_service.get_table_key_schema(entity_name)
        else:
            schema = spanner_service.get_index_key_schema(entity_name)
    except Exception as e:
        return RangeSplitResponse(
            success=False,
            message=f"Failed to get entity schema: {str(e)}",
            errors=[str(e)]
        )

    # Validate the range request
    validation = validate_range_request(schema, request.start_value, request.end_value)

    if not validation.is_valid:
        return RangeSplitResponse(
            success=False,
            message=validation.error_message or "Validation failed",
            errors=[validation.error_message] if validation.error_message else []
        )

    # Generate split values
    try:
        generated_values, warnings = generate_range_splits(
            range_type=validation.range_type,
            start_value=request.start_value,
            end_value=request.end_value,
            num_splits=request.num_splits,
            include_boundaries=request.include_boundaries
        )
    except ValueError as e:
        return RangeSplitResponse(
            success=False,
            message=str(e),
            errors=[str(e)]
        )

    # Add each split to local database
    errors: list[str] = []
    created_count = 0

    for value in generated_values:
        try:
            if request.index_name:
                # Index split
                add_local_split(
                    table_name=request.table_name,
                    split_value="",
                    operation_type=OperationType.ADD,
                    index_name=request.index_name,
                    index_key=value
                )
            else:
                # Table split
                add_local_split(
                    table_name=request.table_name,
                    split_value=value,
                    operation_type=OperationType.ADD
                )
            created_count += 1
        except Exception as e:
            error_msg = f"Failed to add split '{value}': {str(e)}"
            errors.append(error_msg)
            logging.error(error_msg)

    success = created_count > 0 and len(errors) == 0

    return RangeSplitResponse(
        success=success,
        message=f"Created {created_count} of {len(generated_values)} split points",
        generated_values=generated_values,
        splits_created=created_count,
        warnings=warnings,
        errors=errors
    )


@app.get("/api/splits/range/validate")
async def api_validate_range(
    entity_name: str = Query(..., description="Name of the entity (table or index)"),
    entity_type: EntityType = Query(..., description="Type of entity (TABLE or INDEX)"),
    start_value: str = Query(..., description="Start value of the range"),
    end_value: str = Query(..., description="End value of the range")
) -> RangeValidationResult:
    """API: Validate a range split request without creating splits.

    Returns whether the range is valid for the entity and the detected type.
    """
    spanner_service = get_spanner_service()

    # Get entity schema
    try:
        if entity_type == EntityType.TABLE:
            schema = spanner_service.get_table_key_schema(entity_name)
        else:
            schema = spanner_service.get_index_key_schema(entity_name)
    except Exception as e:
        return RangeValidationResult(
            is_valid=False,
            range_type=None,
            error_message=f"Failed to get entity schema: {str(e)}"
        )

    # Validate the range
    return validate_range_request(schema, start_value, end_value)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
