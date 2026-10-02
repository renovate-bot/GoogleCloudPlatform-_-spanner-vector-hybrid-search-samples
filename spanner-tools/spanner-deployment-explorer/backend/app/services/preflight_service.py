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
import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple
import httpx

from app.models.preflight import (
    PreflightCheckItem,
    PreflightCheckRequest,
    PreflightCheckResponse,
    PreflightCheckStatus,
)

KNOWN_GCE_REGIONS = {
    # Americas
    "us-central1": "Iowa",
    "us-east1": "South Carolina",
    "us-east4": "Northern Virginia",
    "us-west1": "Oregon",
    "us-west2": "Los Angeles",
    "us-west3": "Salt Lake City",
    "us-west4": "Las Vegas",
    "northamerica-northeast1": "Montréal",
    "northamerica-northeast2": "Toronto",
    "southamerica-east1": "São Paulo",
    "southamerica-west1": "Santiago",
    # Europe
    "europe-west1": "Belgium",
    "europe-west2": "London",
    "europe-west3": "Frankfurt",
    "europe-west4": "Eemshaven",
    "europe-west6": "Zurich",
    "europe-west8": "Milan",
    "europe-west9": "Paris",
    "europe-west10": "Berlin",
    "europe-west12": "Turin",
    "europe-north1": "Finland",
    "europe-central2": "Warsaw",
    "europe-southwest1": "Madrid",
    # Asia Pacific
    "asia-east1": "Taiwan",
    "asia-east2": "Hong Kong",
    "asia-northeast1": "Tokyo",
    "asia-northeast2": "Osaka",
    "asia-northeast3": "Seoul",
    "asia-south1": "Mumbai",
    "asia-south2": "Delhi",
    "asia-southeast1": "Singapore",
    "asia-southeast2": "Jakarta",
    "australia-southeast1": "Sydney",
    "australia-southeast2": "Melbourne",
    # Middle East & Africa
    "me-central1": "Doha",
    "me-central2": "Dammam",
    "me-west1": "Tel Aviv",
    "africa-south1": "Johannesburg",
}


def find_gcloud_binary() -> Optional[str]:
    """Finds gcloud binary in PATH or common user installation paths."""
    found = shutil.which("gcloud")
    if found:
        return found
    common_paths = [
        Path.home() / "Downloads" / "google-cloud-sdk" / "bin" / "gcloud",
        Path.home() / "google-cloud-sdk" / "bin" / "gcloud",
        Path("/opt/homebrew/bin/gcloud"),
        Path("/usr/local/bin/gcloud"),
    ]
    for p in common_paths:
        if p.exists() and os.access(p, os.X_OK):
            return str(p)
    return None


_find_gcloud_binary = find_gcloud_binary


def _get_gcloud_auth_info() -> Tuple[Optional[str], Optional[str]]:
    """
    Attempts to retrieve current active account and access token from gcloud or ADC.
    Returns (account_email, access_token).
    """
    gcloud_bin = _find_gcloud_binary()
    account = None
    token = None

    if gcloud_bin:
        try:
            acc_res = subprocess.run(
                [gcloud_bin, "config", "get-value", "account"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if acc_res.returncode == 0:
                lines = [line.strip() for line in acc_res.stdout.splitlines() if "@" in line]
                if lines:
                    account = lines[0]
        except Exception:
            pass

        # Prioritize ADC token which has cloud-platform scope required by Cloud Resource Manager
        try:
            adc_res = subprocess.run(
                [gcloud_bin, "auth", "application-default", "print-access-token"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if adc_res.returncode == 0 and adc_res.stdout.strip():
                candidate = adc_res.stdout.strip().splitlines()[-1].strip()
                if candidate.startswith("ya29."):
                    token = candidate
        except Exception:
            pass

        if not token:
            try:
                token_res = subprocess.run(
                    [gcloud_bin, "auth", "print-access-token"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if token_res.returncode == 0 and token_res.stdout.strip():
                    candidate = token_res.stdout.strip().splitlines()[-1].strip()
                    if candidate.startswith("ya29."):
                        token = candidate
            except Exception:
                pass

    # Check ADC json file fallback
    if not account:
        adc_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not adc_path:
            adc_path = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
        else:
            adc_path = Path(adc_path)
        if adc_path.exists():
            try:
                with open(adc_path, "r", encoding="utf-8") as f:
                    adc_data = json.load(f)
                    account = adc_data.get("client_email") or adc_data.get("account")
            except Exception:
                pass

    return account, token


class PreflightService:
    """Evaluates readiness and IAM permissions for Cloud Spanner latency load testing."""

    async def execute_preflight(self, request: PreflightCheckRequest) -> PreflightCheckResponse:
        checks: List[PreflightCheckItem] = []
        is_dry_run = request.execution_mode.lower() in ("dry_run", "dryrun", "simulate", "simulation")

        # 1. Syntax validation on Project ID
        project_id_valid = bool(re.match(r"^[a-z0-9-]{4,30}$", request.project_id))

        if is_dry_run:
            return self._execute_dry_run(request, project_id_valid)

        # Live GCP Pre-flight Checks
        account, token = _get_gcloud_auth_info()

        # Check 1: Authentication & Identity
        if token and account:
            checks.append(
                PreflightCheckItem(
                    id="adc_auth",
                    name="Google Cloud Authentication",
                    scope="auth",
                    status=PreflightCheckStatus.PASSED,
                    message=f"Authenticated as {account} with active credentials",
                )
            )
        elif account:
            checks.append(
                PreflightCheckItem(
                    id="adc_auth",
                    name="Google Cloud Authentication",
                    scope="auth",
                    status=PreflightCheckStatus.WARNING,
                    message=f"Discovered account {account}, but could not refresh access token. Re-run login.",
                    remediation_command="gcloud auth application-default login",
                )
            )
        else:
            checks.append(
                PreflightCheckItem(
                    id="adc_auth",
                    name="Google Cloud Authentication",
                    scope="auth",
                    status=PreflightCheckStatus.FAILED,
                    message="No active Google Cloud credentials or ADC found. Please authenticate.",
                    remediation_command="gcloud auth application-default login",
                )
            )

        # Check 2: Project Format & Existence
        if not project_id_valid:
            checks.append(
                PreflightCheckItem(
                    id="project_access",
                    name="Target GCP Project",
                    scope="project",
                    status=PreflightCheckStatus.FAILED,
                    message=f"Project ID '{request.project_id}' is invalid. Must be 4-30 lowercase letters, numbers, or hyphens.",
                )
            )
        else:
            checks.append(
                PreflightCheckItem(
                    id="project_access",
                    name="Target GCP Project",
                    scope="project",
                    status=PreflightCheckStatus.PASSED,
                    message=f"Target project ID '{request.project_id}' is valid",
                )
            )

        # Check 3: Enabled APIs & IAM Permissions (Spanner & Compute Engine)
        if token and project_id_valid:
            async with httpx.AsyncClient(timeout=8.0) as client:
                headers = {"Authorization": f"Bearer {token}"}

                # 3a. Test Service Usage (Spanner, Compute & Storage APIs)
                for svc_name, svc_display, svc_id in [
                    ("spanner.googleapis.com", "Cloud Spanner API", "spanner_api"),
                    ("compute.googleapis.com", "Compute Engine API", "compute_api"),
                    ("storage.googleapis.com", "Cloud Storage API", "storage_api"),
                ]:
                    try:
                        su_url = f"https://serviceusage.googleapis.com/v1/projects/{request.project_id}/services/{svc_name}"
                        su_res = await client.get(su_url, headers=headers)
                        if su_res.status_code == 200:
                            state = su_res.json().get("state")
                            if state == "ENABLED":
                                checks.append(
                                    PreflightCheckItem(
                                        id=svc_id,
                                        name=svc_display,
                                        scope="api",
                                        status=PreflightCheckStatus.PASSED,
                                        message=f"{svc_display} is ENABLED on project {request.project_id}",
                                    )
                                )
                            else:
                                checks.append(
                                    PreflightCheckItem(
                                        id=svc_id,
                                        name=svc_display,
                                        scope="api",
                                        status=PreflightCheckStatus.FAILED,
                                        message=f"{svc_display} ({svc_name}) is {state} in project {request.project_id}.",
                                        remediation_command=f"gcloud services enable {svc_name} --project={request.project_id}",
                                    )
                                )
                        elif su_res.status_code == 403:
                            checks.append(
                                PreflightCheckItem(
                                    id=svc_id,
                                    name=svc_display,
                                    scope="api",
                                    status=PreflightCheckStatus.FAILED,
                                    message=f"Permission denied checking {svc_display} on {request.project_id}.",
                                    remediation_command=f"gcloud services enable {svc_name} --project={request.project_id}",
                                )
                            )
                    except Exception as e:
                        checks.append(
                            PreflightCheckItem(
                                id=svc_id,
                                name=svc_display,
                                scope="api",
                                status=PreflightCheckStatus.WARNING,
                                message=f"Could not connect to verify {svc_display}: {str(e)[:80]}",
                            )
                        )

                # 3b. Test IAM permissions via Cloud Resource Manager
                spanner_perms = [
                    "spanner.instances.create",
                    "spanner.instances.delete",
                    "spanner.databases.create",
                ]
                compute_perms = [
                    "compute.instances.create",
                    "compute.instances.delete",
                ]
                storage_perms = [
                    "storage.buckets.create",
                    "storage.buckets.list",
                ]
                crm_url = f"https://cloudresourcemanager.googleapis.com/v1/projects/{request.project_id}:testIamPermissions"
                granted = set()
                try:
                    resp = await client.post(
                        crm_url,
                        headers=headers,
                        json={"permissions": spanner_perms + compute_perms + storage_perms},
                    )
                    if resp.status_code == 200:
                        granted = set(resp.json().get("permissions", []))
                        missing_spanner = set(spanner_perms) - granted
                        missing_compute = set(compute_perms) - granted

                        if not missing_spanner:
                            checks.append(
                                PreflightCheckItem(
                                    id="spanner_iam",
                                    name="Cloud Spanner Permissions",
                                    scope="spanner",
                                    status=PreflightCheckStatus.PASSED,
                                    message=f"Verified Spanner Admin permissions on {request.project_id}",
                                )
                            )
                        else:
                            checks.append(
                                PreflightCheckItem(
                                    id="spanner_iam",
                                    name="Cloud Spanner Permissions",
                                    scope="spanner",
                                    status=PreflightCheckStatus.FAILED,
                                    message=f"Missing Spanner permissions: {', '.join(sorted(missing_spanner))}",
                                    remediation_command=f"gcloud projects add-iam-policy-binding {request.project_id} --member=\"user:{account}\" --role=\"roles/spanner.admin\"",
                                )
                            )

                        if not missing_compute:
                            checks.append(
                                PreflightCheckItem(
                                    id="compute_iam",
                                    name="Compute Engine Permissions",
                                    scope="compute",
                                    status=PreflightCheckStatus.PASSED,
                                    message=f"Verified Compute Admin permissions on {request.project_id}",
                                )
                            )
                        else:
                            checks.append(
                                PreflightCheckItem(
                                    id="compute_iam",
                                    name="Compute Engine Permissions",
                                    scope="compute",
                                    status=PreflightCheckStatus.FAILED,
                                    message=f"Missing Compute permissions: {', '.join(sorted(missing_compute))}",
                                    remediation_command=f"gcloud projects add-iam-policy-binding {request.project_id} --member=\"user:{account}\" --role=\"roles/compute.instanceAdmin.v1\"",
                                )
                            )
                    elif resp.status_code in (403, 404):
                        detail = resp.json().get("error", {}).get("message", resp.text[:120])
                        checks.append(
                            PreflightCheckItem(
                                id="spanner_iam",
                                name="Cloud Spanner Permissions",
                                scope="spanner",
                                status=PreflightCheckStatus.FAILED,
                                message=f"IAM check failed on project {request.project_id}: {detail}",
                                remediation_command=f"gcloud projects add-iam-policy-binding {request.project_id} --member=\"user:{account}\" --role=\"roles/spanner.admin\"",
                            )
                        )
                    else:
                        checks.append(
                            PreflightCheckItem(
                                id="spanner_iam",
                                name="Cloud Spanner Permissions",
                                scope="spanner",
                                status=PreflightCheckStatus.WARNING,
                                message=f"IAM check returned status {resp.status_code}",
                            )
                        )
                except Exception as e:
                    checks.append(
                        PreflightCheckItem(
                            id="spanner_iam",
                            name="Cloud Spanner Permissions",
                            scope="spanner",
                            status=PreflightCheckStatus.WARNING,
                            message=f"Permission check could not connect: {str(e)[:100]}",
                        )
                    )

                # 3c. Cloud Storage Staging Bucket Usability Check
                custom_bucket = request.staging_bucket.strip() if request.staging_bucket else None
                if custom_bucket:
                    custom_bucket = custom_bucket.replace("gs://", "").rstrip("/")

                if custom_bucket:
                    # Validate custom bucket syntax
                    if not re.match(r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", custom_bucket):
                        checks.append(
                            PreflightCheckItem(
                                id="storage_staging",
                                name="Cloud Storage (Staging)",
                                scope="storage",
                                status=PreflightCheckStatus.FAILED,
                                message=f"Invalid GCS bucket name format: '{custom_bucket}'. Bucket names must be 3-63 lowercase alphanumeric characters, dots, hyphens, or underscores.",
                            )
                        )
                    else:
                        try:
                            b_res = await client.get(
                                f"https://storage.googleapis.com/storage/v1/b/{custom_bucket}",
                                headers=headers,
                            )
                            if b_res.status_code == 200:
                                checks.append(
                                    PreflightCheckItem(
                                        id="storage_staging",
                                        name="Cloud Storage (Staging)",
                                        scope="storage",
                                        status=PreflightCheckStatus.PASSED,
                                        message=f"User-specified staging bucket gs://{custom_bucket} is accessible and ready",
                                    )
                                )
                            elif b_res.status_code == 404:
                                checks.append(
                                    PreflightCheckItem(
                                        id="storage_staging",
                                        name="Cloud Storage (Staging)",
                                        scope="storage",
                                        status=PreflightCheckStatus.FAILED,
                                        message=f"User-specified staging bucket gs://{custom_bucket} does not exist.",
                                        remediation_command=f"gcloud storage buckets create gs://{custom_bucket} --project={request.project_id}",
                                    )
                                )
                            else:
                                checks.append(
                                    PreflightCheckItem(
                                        id="storage_staging",
                                        name="Cloud Storage (Staging)",
                                        scope="storage",
                                        status=PreflightCheckStatus.FAILED,
                                        message=f"Permission denied accessing staging bucket gs://{custom_bucket} (HTTP {b_res.status_code}).",
                                        remediation_command=f"gcloud storage buckets add-iam-policy-binding gs://{custom_bucket} --member=\"user:{account}\" --role=\"roles/storage.objectAdmin\"",
                                    )
                                )
                        except Exception as e:
                            checks.append(
                                PreflightCheckItem(
                                    id="storage_staging",
                                    name="Cloud Storage (Staging)",
                                    scope="storage",
                                    status=PreflightCheckStatus.WARNING,
                                    message=f"Could not verify staging bucket gs://{custom_bucket}: {str(e)[:80]}",
                                )
                            )
                else:
                    # Auto-detection hierarchy:
                    # 1. Check default bucket gs://{project_id}-dbexplorer-staging
                    # 2. Check if user has storage.buckets.create
                    # 3. Check if any existing bucket in project is available
                    default_b = f"{request.project_id}-dbexplorer-staging"
                    try:
                        b_res = await client.get(
                            f"https://storage.googleapis.com/storage/v1/b/{default_b}",
                            headers=headers,
                        )
                        if b_res.status_code == 200:
                            checks.append(
                                PreflightCheckItem(
                                    id="storage_staging",
                                    name="Cloud Storage (Staging)",
                                    scope="storage",
                                    status=PreflightCheckStatus.PASSED,
                                    message=f"Staging bucket gs://{default_b} exists and is ready",
                                )
                            )
                        else:
                            can_create_bucket = "storage.buckets.create" in granted
                            if can_create_bucket:
                                checks.append(
                                    PreflightCheckItem(
                                        id="storage_staging",
                                        name="Cloud Storage (Staging)",
                                        scope="storage",
                                        status=PreflightCheckStatus.PASSED,
                                        message=f"Verified storage permissions. Staging bucket gs://{default_b} will be provisioned on run.",
                                    )
                                )
                            else:
                                list_res = await client.get(
                                    f"https://storage.googleapis.com/storage/v1/b?project={request.project_id}&maxResults=10",
                                    headers=headers,
                                )
                                existing_buckets = []
                                if list_res.status_code == 200:
                                    items = list_res.json().get("items", [])
                                    existing_buckets = [it["name"] for it in items if "name" in it]

                                if existing_buckets:
                                    reused_b = next((b for b in existing_buckets if "staging" in b.lower()), None)
                                    if not reused_b:
                                        reused_b = next((b for b in existing_buckets if "cloudbuild" in b.lower()), existing_buckets[0])
                                    checks.append(
                                        PreflightCheckItem(
                                            id="storage_staging",
                                            name="Cloud Storage (Staging)",
                                            scope="storage",
                                            status=PreflightCheckStatus.PASSED,
                                            message=f"Missing storage.buckets.create, but found existing bucket gs://{reused_b} for runner staging.",
                                        )
                                    )
                                else:
                                    checks.append(
                                        PreflightCheckItem(
                                            id="storage_staging",
                                            name="Cloud Storage (Staging)",
                                            scope="storage",
                                            status=PreflightCheckStatus.FAILED,
                                            message="No staging bucket found and missing storage.buckets.create permission.",
                                            remediation_command=f"gcloud projects add-iam-policy-binding {request.project_id} --member=\"user:{account}\" --role=\"roles/storage.admin\"",
                                        )
                                    )
                    except Exception as e:
                        checks.append(
                            PreflightCheckItem(
                                id="storage_staging",
                                name="Cloud Storage (Staging)",
                                scope="storage",
                                status=PreflightCheckStatus.WARNING,
                                message=f"Could not verify Cloud Storage staging: {str(e)[:80]}",
                            )
                        )

        # Check 4: Client Regions validation
        invalid_regions = [r for r in request.client_regions if r not in KNOWN_GCE_REGIONS]
        if invalid_regions:
            checks.append(
                PreflightCheckItem(
                    id="regional_subnets",
                    name="Client GCE Regions",
                    scope="network",
                    status=PreflightCheckStatus.FAILED,
                    message=f"Unknown or unsupported GCE client regions: {', '.join(invalid_regions)}",
                )
            )
        else:
            region_names = [f"{r} ({KNOWN_GCE_REGIONS[r]})" for r in request.client_regions]
            checks.append(
                PreflightCheckItem(
                    id="regional_subnets",
                    name="Client GCE Regions",
                    scope="network",
                    status=PreflightCheckStatus.PASSED,
                    message=f"Verified {len(request.client_regions)} target GCE client regions: {', '.join(region_names)}",
                )
            )

        all_passed = all(c.status in (PreflightCheckStatus.PASSED, PreflightCheckStatus.WARNING) for c in checks)
        summary = "All pre-flight checks passed! Ready to start load test." if all_passed else "Pre-flight checks failed. Please resolve the issues above."

        return PreflightCheckResponse(
            all_passed=all_passed,
            principal=account,
            project_id=request.project_id,
            execution_mode="live",
            checks=checks,
            summary=summary,
        )

    def _execute_dry_run(self, request: PreflightCheckRequest, project_id_valid: bool) -> PreflightCheckResponse:
        """Generates validated preflight results for dry-run simulation mode."""
        checks: List[PreflightCheckItem] = []
        account, _ = _get_gcloud_auth_info()
        principal = account or "simulated-user@example.com"

        # 1. Auth check
        checks.append(
            PreflightCheckItem(
                id="adc_auth",
                name="Google Cloud Authentication",
                scope="auth",
                status=PreflightCheckStatus.PASSED,
                message=f"Authenticated as {principal} (Simulation mode active)",
            )
        )

        # 2. Project ID Syntax
        if project_id_valid:
            checks.append(
                PreflightCheckItem(
                    id="project_access",
                    name="Target GCP Project",
                    scope="project",
                    status=PreflightCheckStatus.PASSED,
                    message=f"Target project ID '{request.project_id}' syntax valid",
                )
            )
        else:
            checks.append(
                PreflightCheckItem(
                    id="project_access",
                    name="Target GCP Project",
                    scope="project",
                    status=PreflightCheckStatus.FAILED,
                    message=f"Project ID '{request.project_id}' is invalid. Must be 4-30 lowercase letters, numbers, or hyphens.",
                )
            )

        # 3. Spanner API & Permissions
        checks.append(
            PreflightCheckItem(
                id="spanner_iam",
                name="Cloud Spanner Permissions",
                scope="spanner",
                status=PreflightCheckStatus.PASSED,
                message=f"Validated Spanner permissions for config '{request.spanner_config}' (1 node, LatencyKV schema)",
            )
        )

        # 4. Compute Engine API & Permissions
        checks.append(
            PreflightCheckItem(
                id="compute_iam",
                name="Compute Engine Permissions",
                scope="compute",
                status=PreflightCheckStatus.PASSED,
                message="Compute Instance Admin and Direct Access permissions simulated",
            )
        )

        # 5. Cloud Storage Staging
        simulated_bucket = request.staging_bucket or f"{request.project_id}-dbexplorer-staging"
        simulated_bucket = simulated_bucket.replace("gs://", "").strip()
        checks.append(
            PreflightCheckItem(
                id="storage_staging",
                name="Cloud Storage (Staging)",
                scope="storage",
                status=PreflightCheckStatus.PASSED,
                message=f"Staging bucket gs://{simulated_bucket} verified and ready (simulation mode)",
            )
        )

        # 6. Client Regions
        invalid_regions = [r for r in request.client_regions if r not in KNOWN_GCE_REGIONS]
        if invalid_regions:
            checks.append(
                PreflightCheckItem(
                    id="regional_subnets",
                    name="Client GCE Regions",
                    scope="network",
                    status=PreflightCheckStatus.FAILED,
                    message=f"Unknown or unsupported GCE client regions: {', '.join(invalid_regions)}",
                )
            )
        else:
            region_names = [f"{r} ({KNOWN_GCE_REGIONS[r]})" for r in request.client_regions]
            checks.append(
                PreflightCheckItem(
                    id="regional_subnets",
                    name="Client GCE Regions",
                    scope="network",
                    status=PreflightCheckStatus.PASSED,
                    message=f"Validated {len(request.client_regions)} target GCE client regions: {', '.join(region_names)}",
                )
            )

        all_passed = all(c.status == PreflightCheckStatus.PASSED for c in checks)
        summary = (
            "Dry-Run simulation pre-flight checks passed! Ready to run multi-region benchmark simulation."
            if all_passed
            else "Pre-flight checks failed. Please fix configuration errors."
        )

        return PreflightCheckResponse(
            all_passed=all_passed,
            principal=principal,
            project_id=request.project_id,
            execution_mode="dry_run",
            checks=checks,
            summary=summary,
        )
