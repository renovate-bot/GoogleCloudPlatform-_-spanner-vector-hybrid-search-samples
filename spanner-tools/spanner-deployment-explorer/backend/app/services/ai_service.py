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

from pathlib import Path
import math
import json
import logging
import re
from typing import Dict, List, Optional, Any
import httpx

from app.core.config import (
    get_gemini_api_key,
    set_gemini_api_key,
    remove_gemini_api_key,
    get_settings,
)
from app.services.data_loader import DataLoader
from app.services.spanner_service import SpannerService

logger = logging.getLogger(__name__)

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"


class AIService:
    _instance: Optional["AIService"] = None
    _prompt_cache: Dict[str, str] = {}

    @classmethod
    def load_prompt_template(cls, template_name: str) -> str:
        """Loads and caches a markdown prompt template from the backend/app/prompts directory."""
        if template_name in cls._prompt_cache:
            return cls._prompt_cache[template_name]

        template_file = PROMPTS_DIR / template_name
        if template_file.exists():
            try:
                content = template_file.read_text(encoding="utf-8").strip()
                cls._prompt_cache[template_name] = content
                return content
            except Exception as e:
                logger.error(f"Failed to read prompt template {template_file}: {e}")

        logger.warning(f"Prompt template {template_name} not found at {template_file}")
        return ""

    def __init__(
        self,
        data_loader: Optional[DataLoader] = None,
        spanner_service: Optional[SpannerService] = None,
    ):
        self.data_loader = data_loader or DataLoader.get_instance()
        self.spanner_service = spanner_service or SpannerService()
        self._grounding_context: Optional[str] = None

    @classmethod
    def get_instance(cls) -> "AIService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def has_key(self) -> bool:
        """Returns True if a valid Gemini API key is configured."""
        return bool(get_gemini_api_key())

    async def validate_api_key(self, api_key: str) -> bool:
        """Validates the API key with a minimal call to Google Generative Language API."""
        if not api_key or not api_key.strip():
            return False

        clean_key = api_key.strip()
        test_url = f"{GEMINI_BASE_URL}/models?key={clean_key}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.get(test_url)
                return res.status_code == 200
        except Exception as e:
            logger.warning(f"Failed to validate Gemini API key: {e}")
            return False

    def save_api_key(self, api_key: str) -> None:
        """Saves the key to gemini.key."""
        set_gemini_api_key(api_key.strip())

    def delete_api_key(self) -> None:
        """Deletes gemini.key."""
        remove_gemini_api_key()

    def get_grounding_context(self) -> str:
        """Builds a condensed, structured grounding prompt of Spanner configurations and client regions."""
        if self._grounding_context is not None:
            return self._grounding_context

        configs = self.spanner_service.list_configs()
        lines = [
            "AVAILABLE CLOUD SPANNER CONFIGURATIONS:",
        ]

        for cfg in configs:
            leader = cfg.leader_region
            replicas_desc = []
            for r in cfg.replicas:
                r_type = r.replica_type.value if hasattr(r.replica_type, "value") else str(r.replica_type)
                replicas_desc.append(f"{r.region} ({r_type})")

            lines.append(
                f"- {cfg.configname}: Type={cfg.instancetype.value}, Continent={cfg.continentregion}, SLA={cfg.availability_sla}, "
                f"Leader={leader}, Replicas=[{', '.join(replicas_desc)}]"
            )

        client_regions = sorted(list(self.data_loader.geo_lookup.keys()))
        lines.append("\nAVAILABLE GCP CLIENT REGIONS FOR BENCHMARK RUNNERS:")
        lines.append(", ".join(client_regions))

        lines.append("\nTHROUGHPUT & SIZING FORMULAS PER SPANNER NODE:")
        lines.append("- Multi-Region / Dual-Region: 2,700 writes/sec per node, 15,000 reads/sec per node")
        lines.append("- Regional: 3,500 writes/sec per node, 22,500 reads/sec per node")
        lines.append("- Node calculation: ceil(target_writes / writes_per_node) or ceil(target_reads / reads_per_node)")

        self._grounding_context = "\n".join(lines)
        return self._grounding_context

    def calculate_nodes_for_throughput(
        self,
        target_writes_per_sec: Optional[int] = None,
        target_reads_per_sec: Optional[int] = None,
        is_multi_region: bool = True,
    ) -> int:
        """Calculates minimum Spanner nodes needed for target throughput."""
        nodes = 1
        write_cap = 2700 if is_multi_region else 3500
        read_cap = 15000 if is_multi_region else 22500

        if target_writes_per_sec and target_writes_per_sec > 0:
            nodes = max(nodes, math.ceil(target_writes_per_sec / write_cap))
        if target_reads_per_sec and target_reads_per_sec > 0:
            nodes = max(nodes, math.ceil(target_reads_per_sec / read_cap))

        return max(1, min(100, nodes))

    @staticmethod
    def clean_latex_math(text: str) -> str:
        """Sanitizes raw LaTeX math notation into clean human-readable text."""
        if not text:
            return ""
        text = re.sub(r'\\text\{([^}]+)\}', r'\1', text)
        text = re.sub(r'\\mathbf\{([^}]+)\}', r'**\1**', text)
        text = re.sub(r'\\mathit\{([^}]+)\}', r'*\1*', text)
        text = re.sub(r'\\mathrm\{([^}]+)\}', r'\1', text)
        text = text.replace(r'\div', '÷').replace(r'\times', '×').replace(r'\cdot', '·')
        text = text.replace(r'\implies', '→').replace(r'\rightarrow', '→').replace(r'\approx', '≈')
        text = text.replace(r'\leq', '≤').replace(r'\le', '≤').replace(r'\geq', '≥').replace(r'\ge', '≥')
        text = text.replace(r'\lceil', '⌈').replace(r'\rceil', '⌉')
        text = re.sub(r'\$([^$]+)\$', lambda m: m.group(1), text)
        text = re.sub(r'\\[a-zA-Z]+', '', text)
        text = re.sub(r'[ \t]{2,}', ' ', text)
        return text

    async def transcribe_audio(
        self,
        audio_base64: str,
        mime_type: Optional[str] = "audio/webm",
        api_key_override: Optional[str] = None,
    ) -> str:
        """
        Transcribes user voice audio into text using Gemini's dedicated audio transcription model.
        Grounded with Cloud Spanner technical terminology for optimal recognition.
        """
        api_key = api_key_override or get_gemini_api_key()
        if not api_key:
            raise ValueError(
                "Gemini API key is not configured. Please add gemini.key or provide the key in the UI."
            )

        if not audio_base64:
            raise ValueError("No audio data provided.")

        clean_mime = (mime_type or "audio/webm").split(";")[0]

        models_to_try = [
            "gemini-3.5-transcribe",
            "gemini-3.8-flash",
            "gemini-3.5-flash",
            "gemini-flash-latest",
        ]

        transcription_instruction = self.load_prompt_template("transcribe_instruction.md")

        payload = {
            "system_instruction": {
                "parts": [{"text": transcription_instruction}]
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "inline_data": {
                                "mime_type": clean_mime,
                                "data": audio_base64,
                            }
                        },
                        {
                            "text": "Transcribe the spoken audio verbatim."
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": 1024,
            },
        }

        primary_error = None
        last_error = None

        for model in models_to_try:
            url = f"{GEMINI_BASE_URL}/models/{model}:generateContent?key={api_key}"
            max_attempts = 2 if model == models_to_try[0] else 1

            for attempt in range(max_attempts):
                try:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(url, json=payload)
                        if response.status_code == 200:
                            data = response.json()
                            candidates = data.get("candidates", [])
                            if not candidates:
                                return ""
                            parts = candidates[0].get("content", {}).get("parts", [])
                            text = "".join(p.get("text", "") for p in parts).strip()
                            return text
                        elif response.status_code == 404:
                            err_msg = f"Model '{model}' not found (HTTP 404)."
                            if model == models_to_try[0]:
                                primary_error = err_msg
                            last_error = err_msg
                            break
                        elif response.status_code in (429, 503):
                            error_detail = response.text
                            try:
                                msg = response.json().get("error", {}).get("message", error_detail)
                            except Exception:
                                msg = error_detail
                            err_msg = f"Model '{model}' temporarily unavailable ({response.status_code}): {msg}"
                            if model == models_to_try[0]:
                                primary_error = err_msg
                            last_error = err_msg
                            if attempt < max_attempts - 1:
                                import asyncio
                                await asyncio.sleep(1.0)
                                continue
                            break
                        else:
                            error_detail = response.text
                            err_msg = f"Gemini API error ({response.status_code}): {error_detail}"
                            if model == models_to_try[0]:
                                primary_error = err_msg
                            last_error = err_msg
                            break
                except Exception as e:
                    logger.error(f"Exception transcribing with model {model}: {e}")
                    if model == models_to_try[0]:
                        primary_error = str(e)
                    last_error = str(e)
                    break

        raise RuntimeError(primary_error or last_error or "Failed to transcribe audio with Gemini.")

    async def chat(
        self,
        messages: List[Dict[str, str]],
        current_context: Optional[Dict[str, Any]] = None,
        api_key_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Sends conversation to Gemini API with Spanner topology grounding and domain guardrails.
        Returns message response and any structured action (update_sizing, configure_benchmark).
        """
        api_key = api_key_override or get_gemini_api_key()
        if not api_key:
            raise ValueError(
                "Gemini API key is not configured. Please add gemini.key or provide the key in the UI."
            )

        settings = get_settings()
        primary_model = settings.features.ai_model or "gemini-3.8-flash"
        # Fallback to active 2026 Gemini Flash models if primary encounters high demand or 404
        fallback_models = ["gemini-3.7-flash", "gemini-3.5-flash", "gemini-flash-latest"]
        models_to_try = [primary_model] + [m for m in fallback_models if m != primary_model]

        grounding = self.get_grounding_context()
        ui_state_json = json.dumps(current_context or {})

        prompt_template = self.load_prompt_template("system_instruction.md")
        system_instruction = (
            prompt_template.replace("{{GROUNDING_CONTEXT}}", grounding)
            .replace("{{CURRENT_CONTEXT}}", ui_state_json)
        )

        contents = []
        for msg in messages:
            role = "user" if msg.get("role") == "user" else "model"
            contents.append({
                "role": role,
                "parts": [{"text": msg.get("content", "")}]
            })

        payload = {
            "system_instruction": {
                "parts": [{"text": system_instruction}]
            },
            "contents": contents,
            "generationConfig": {
                "response_mime_type": "application/json",
                "temperature": 0.2,
                "maxOutputTokens": 4096,
            },
        }

        primary_error = None
        last_error = None

        for model in models_to_try:
            url = f"{GEMINI_BASE_URL}/models/{model}:generateContent?key={api_key}"
            max_attempts = 2 if model == primary_model else 1

            for attempt in range(max_attempts):
                try:
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(url, json=payload)

                        if response.status_code == 200:
                            if model != primary_model and primary_error:
                                logger.info(
                                    f"Primary model '{primary_model}' had transient issue ({primary_error}); "
                                    f"successfully served request using fallback '{model}'."
                                )
                            data = response.json()
                            candidates = data.get("candidates", [])
                            if not candidates:
                                return {
                                    "message": "No response generated by Gemini. Please try again.",
                                    "action": "none",
                                }
                            parts = candidates[0].get("content", {}).get("parts", [])
                            raw_text = "".join(p.get("text", "") for p in parts)
                            parsed = None
                            try:
                                parsed = json.loads(raw_text)
                            except json.JSONDecodeError:
                                # Resilient recovery if JSON was cut off or malformed
                                last_brace = raw_text.rfind("}")
                                if last_brace != -1:
                                    try:
                                        parsed = json.loads(raw_text[:last_brace + 1])
                                    except Exception:
                                        pass
                                if not parsed:
                                    msg_match = re.search(r'"message"\s*:\s*"((?:[^"\\]|\\.)*)"', raw_text, re.DOTALL)
                                    action_match = re.search(r'"action"\s*:\s*"([^"]+)"', raw_text)
                                    cfg_match = re.search(r'"spanner_config"\s*:\s*"([^"]+)"', raw_text)
                                    nodes_match = re.search(r'"nodes"\s*:\s*(\d+)', raw_text)
                                    opt_match = re.search(r'"optional_replicas"\s*:\s*(\[[^\]]*\])', raw_text)
                                    clients_match = re.search(r'"client_regions"\s*:\s*(\[[^\]]*\])', raw_text)
                                    
                                    parsed_opt = None
                                    if opt_match:
                                        try:
                                            parsed_opt = json.loads(opt_match.group(1))
                                        except Exception:
                                            pass

                                    parsed_clients = None
                                    if clients_match:
                                        try:
                                            parsed_clients = json.loads(clients_match.group(1))
                                        except Exception:
                                            pass

                                    parsed = {
                                        "message": msg_match.group(1).encode().decode('unicode_escape') if msg_match else raw_text,
                                        "action": action_match.group(1) if action_match else "none",
                                        "spanner_config": cfg_match.group(1) if cfg_match else None,
                                        "nodes": int(nodes_match.group(1)) if nodes_match else None,
                                        "client_regions": parsed_clients,
                                        "optional_replicas": parsed_opt,
                                    }

                            if isinstance(parsed, dict) and "message" in parsed:
                                parsed["message"] = self.clean_latex_math(parsed["message"])
                            return parsed
                        elif response.status_code == 404:
                            err_msg = f"Model '{model}' not found (HTTP 404)."
                            logger.warning(err_msg)
                            if model == primary_model:
                                primary_error = err_msg
                            last_error = err_msg
                            break  # Do not retry 404
                        elif response.status_code in (429, 503):
                            error_detail = response.text
                            try:
                                err_json = response.json()
                                msg = err_json.get("error", {}).get("message", error_detail)
                            except Exception:
                                msg = error_detail
                            err_msg = f"Model '{model}' temporarily unavailable ({response.status_code}): {msg}"
                            logger.warning(err_msg)
                            if model == primary_model:
                                primary_error = err_msg
                            last_error = err_msg
                            if attempt < max_attempts - 1:
                                import asyncio
                                await asyncio.sleep(1.0)
                                continue
                            break
                        else:
                            error_detail = response.text
                            err_msg = f"Gemini API error ({response.status_code}): {error_detail}"
                            logger.error(f"Gemini API returned error {response.status_code}: {error_detail}")
                            if model == primary_model:
                                primary_error = err_msg
                            last_error = err_msg
                            break
                except Exception as e:
                    logger.error(f"Exception calling Gemini model {model}: {e}")
                    if model == primary_model:
                        primary_error = str(e)
                    last_error = str(e)
                    break

        raise RuntimeError(primary_error or last_error or "Failed to connect to Google Gemini API.")
