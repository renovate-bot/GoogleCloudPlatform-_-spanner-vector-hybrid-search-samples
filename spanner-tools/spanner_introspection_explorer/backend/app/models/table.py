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

from typing import List, Dict, Optional, Literal
from pydantic import BaseModel, Field

TableCategory = Literal["Locking", "Query", "Transactions", "Misc"]

class ColumnMetadata(BaseModel):
    name: str
    type: str
    filter_type: Literal["text", "numeric", "date"] = "text"

class TableSummary(BaseModel):
    name: str
    category: TableCategory
    row_count: int
    column_count: int
    is_large: bool = False

class TableMetadata(BaseModel):
    name: str
    category: TableCategory
    row_count: int
    column_count: int
    columns: List[ColumnMetadata] = Field(default_factory=list)

class DatabaseSummary(BaseModel):
    database_file: str
    total_tables: int
    total_rows: int
    categories: Dict[str, List[str]]
    tables: List[TableSummary]

class HistogramBucket(BaseModel):
    bin_index: int
    bin_min: float
    bin_max: float
    count: int

class TopCategory(BaseModel):
    value: str
    display_value: Optional[str] = None
    count: int
    percent: float

class ColumnProfile(BaseModel):
    name: str
    column_type: str
    filter_type: Literal["text", "numeric", "date"]
    null_count: int = 0
    distinct_count: int = 0
    total_count: int = 0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_value: Optional[float] = None
    histogram: List[HistogramBucket] = Field(default_factory=list)
    top_categories: List[TopCategory] = Field(default_factory=list)
    min_date: Optional[str] = None
    max_date: Optional[str] = None

class TableProfilesResponse(BaseModel):
    table: str
    total_rows: int
    profiles: Dict[str, ColumnProfile]
