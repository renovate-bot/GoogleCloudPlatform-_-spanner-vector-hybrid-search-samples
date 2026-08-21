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

from .discovery import router as discovery_router
from .data import router as data_router
from .agent import router as agent_router
from .pipeline import router as pipeline_router
from .connections import router as connections_router
from .gcp import router as gcp_router

__all__ = [
    "discovery_router",
    "data_router",
    "agent_router",
    "pipeline_router",
    "connections_router",
    "gcp_router",
]
