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

from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field

class ColumnFilter(BaseModel):
    type: Literal["text", "numeric", "date"] = "text"
    operator: Optional[Literal["contains", "exact", "not_contains", "not_exact"]] = "contains"
    value: Optional[str] = None
    min: Optional[float | str] = None
    max: Optional[float | str] = None
    selected_timestamps: Optional[List[str]] = Field(default_factory=list)

class SortConfig(BaseModel):
    column: Optional[str] = None
    order: Literal["ASC", "DESC"] = "ASC"

class TableQueryRequest(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=10000)
    sort: Optional[SortConfig] = None
    filters: Dict[str, ColumnFilter] = Field(default_factory=dict)
    utc_offset: float = Field(default=0.0)
    global_search: Optional[str] = None

class QueryResultPage(BaseModel):
    items: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int
    total_pages: int
    executed_sql: str
    duration_ms: float
