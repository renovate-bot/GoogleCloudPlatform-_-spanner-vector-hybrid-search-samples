-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE FacetedProducts (
  TenantId STRING(36) NOT NULL,
  ProductId STRING(64) NOT NULL,
  Title STRING(MAX) NOT NULL,
  Description STRING(MAX),
  Category STRING(64),
  Brand STRING(64),
  Colors ARRAY<STRING(32)>,
  Sizes ARRAY<STRING(32)>,
  PriceCents INT64,
  Rating FLOAT64,
  InventoryCount INT64,
  Status STRING(32),
  UpdatedAt TIMESTAMP,
  TitleTokens TOKENLIST AS (
    TOKENIZE_FULLTEXT(Title, token_category=>"title")
  ) HIDDEN,
  DescriptionTokens TOKENLIST AS (
    TOKENIZE_FULLTEXT(Description)
  ) HIDDEN,
  SearchTokens TOKENLIST AS (
    TOKENLIST_CONCAT([TitleTokens, DescriptionTokens])
  ) HIDDEN
) PRIMARY KEY (TenantId, ProductId);

CREATE SEARCH INDEX FacetedProductsByText
ON FacetedProducts(SearchTokens)
STORING (
  Title,
  Category,
  Brand,
  Colors,
  Sizes,
  PriceCents,
  Rating,
  InventoryCount,
  Status,
  UpdatedAt
)
PARTITION BY TenantId;

CREATE INDEX FacetedProductsByTenantCategoryBrand
ON FacetedProducts(TenantId, Category, Brand);
