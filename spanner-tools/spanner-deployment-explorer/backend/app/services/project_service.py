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
import logging
import os
import shutil
import subprocess
from typing import List, Optional
from app.models.project import ProjectItem, ProjectsResponse
from app.services.preflight_service import find_gcloud_binary

logger = logging.getLogger("dbexplorer.project_service")


class ProjectDiscoveryService:
    """Discovers accessible Google Cloud projects with in-memory caching."""

    def __init__(self):
        self._projects_cache: Optional[List[ProjectItem]] = None
        self._cached_active_project: Optional[str] = None

    def get_active_project(self) -> Optional[str]:
        """Detects active project from gcloud or environment variables."""
        if self._cached_active_project:
            return self._cached_active_project

        # 1. Environment variable override
        env_proj = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
        if env_proj and env_proj.strip():
            self._cached_active_project = env_proj.strip()
            return self._cached_active_project

        # 2. Query gcloud config
        gcloud_bin = find_gcloud_binary()
        if gcloud_bin and os.path.exists(gcloud_bin):
            try:
                res = subprocess.run(
                    [gcloud_bin, "config", "get-value", "project"],
                    capture_output=True,
                    text=True,
                    timeout=4,
                )
                if res.returncode == 0:
                    val = res.stdout.strip()
                    if val and "(unset)" not in val and not val.startswith("ERROR:"):
                        self._cached_active_project = val
                        return self._cached_active_project
            except Exception as e:
                logger.debug("Failed to get gcloud active project: %s", e)

        return None

    def list_projects(self, force_refresh: bool = False) -> ProjectsResponse:
        """Returns list of accessible projects with caching and active project sorted first."""
        if not force_refresh and self._projects_cache is not None:
            return ProjectsResponse(
                projects=self._projects_cache,
                current_active_project=self.get_active_project(),
            )

        # Clear cached active project on force_refresh
        if force_refresh:
            self._cached_active_project = None

        active = self.get_active_project()
        items: List[ProjectItem] = []
        seen_ids = set()

        # Always add active project first if available
        if active:
            items.append(ProjectItem(project_id=active, display_name=f"{active} (Active Project)"))
            seen_ids.add(active)

        # Query gcloud projects list
        gcloud_bin = find_gcloud_binary()
        if gcloud_bin and os.path.exists(gcloud_bin):
            try:
                res = subprocess.run(
                    [gcloud_bin, "projects", "list", "--limit=50", "--format=json(projectId,name)"],
                    capture_output=True,
                    text=True,
                    timeout=8,
                )
                if res.returncode == 0 and res.stdout.strip():
                    raw_data = json.loads(res.stdout)
                    for p in raw_data:
                        pid = p.get("projectId")
                        name = p.get("name") or pid
                        if pid and pid not in seen_ids:
                            seen_ids.add(pid)
                            display = f"{name} ({pid})" if name and name != pid else pid
                            items.append(ProjectItem(project_id=pid, display_name=display))
            except Exception as e:
                logger.warning("Error fetching GCP projects from gcloud: %s", e)

        self._projects_cache = items
        return ProjectsResponse(
            projects=self._projects_cache,
            current_active_project=active,
        )


_project_service_instance: Optional[ProjectDiscoveryService] = None


def get_project_service() -> ProjectDiscoveryService:
    global _project_service_instance
    if _project_service_instance is None:
        _project_service_instance = ProjectDiscoveryService()
    return _project_service_instance
