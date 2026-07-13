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

WITH SearchResults AS (
  SELECT
    ProductId,
    Title,
    Category,
    Brand,
    Colors,
    Sizes,
    PriceCents,
    Rating,
    InventoryCount
  FROM FacetedProducts
  WHERE TenantId = @tenant_id
    AND Status = 'active'
    AND SEARCH(SearchTokens, 'waterproof trail')
),
FilteredResults AS (
  SELECT *
  FROM SearchResults
  WHERE Category = 'footwear'
    AND Brand = 'Summit'
),
ResultPage AS (
  SELECT ARRAY(
    SELECT AS STRUCT ProductId, Title, Category, Brand, PriceCents, Rating
    FROM FilteredResults
    ORDER BY Rating DESC, ProductId ASC
    LIMIT 5
  ) AS ResultRows
),
FilteredMetrics AS (
  SELECT
    COUNT(*) AS ResultCount,
    AVG(PriceCents) AS AvgPriceCents,
    SUM(InventoryCount) AS TotalInventory
  FROM FilteredResults
),
UnfilteredCategoryFacets AS (
  SELECT ARRAY(
    SELECT AS STRUCT Category AS Value, ProductCount
    FROM (
    SELECT Category, COUNT(*) AS ProductCount
    FROM SearchResults
    GROUP BY Category
    )
    ORDER BY ProductCount DESC, Category ASC
  ) AS Buckets
),
UnfilteredBrandFacets AS (
  SELECT ARRAY(
    SELECT AS STRUCT Brand AS Value, ProductCount
    FROM (
    SELECT Brand, COUNT(*) AS ProductCount
    FROM SearchResults
    GROUP BY Brand
    )
    ORDER BY ProductCount DESC, Brand ASC
  ) AS Buckets
)
SELECT
  ResultPage.ResultRows AS Results,
  FilteredMetrics.ResultCount,
  FilteredMetrics.AvgPriceCents,
  FilteredMetrics.TotalInventory,
  UnfilteredCategoryFacets.Buckets AS CategoryFacetsBeforePostFilter,
  UnfilteredBrandFacets.Buckets AS BrandFacetsBeforePostFilter
FROM ResultPage, FilteredMetrics, UnfilteredCategoryFacets, UnfilteredBrandFacets;
