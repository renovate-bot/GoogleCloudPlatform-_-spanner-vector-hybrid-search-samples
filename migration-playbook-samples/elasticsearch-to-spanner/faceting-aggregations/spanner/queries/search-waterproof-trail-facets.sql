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
ResultPage AS (
  SELECT ARRAY(
    SELECT AS STRUCT ProductId, Title, Category, Brand, PriceCents, Rating
    FROM SearchResults
    ORDER BY Rating DESC, ProductId ASC
    LIMIT 5
  ) AS ResultRows
),
CategoryFacets AS (
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
BrandFacets AS (
  SELECT ARRAY(
    SELECT AS STRUCT Brand AS Value, ProductCount
    FROM (
    SELECT Brand, COUNT(*) AS ProductCount
    FROM SearchResults
    GROUP BY Brand
    )
    ORDER BY ProductCount DESC, Brand ASC
  ) AS Buckets
),
ColorFacets AS (
  SELECT ARRAY(
    SELECT AS STRUCT Color AS Value, ProductCount
    FROM (
    SELECT Color, COUNT(*) AS ProductCount
    FROM SearchResults, UNNEST(Colors) AS Color
    GROUP BY Color
    )
    ORDER BY ProductCount DESC, Color ASC
  ) AS Buckets
),
SizeFacets AS (
  SELECT ARRAY(
    SELECT AS STRUCT Size AS Value, ProductCount
    FROM (
    SELECT Size, COUNT(*) AS ProductCount
    FROM SearchResults, UNNEST(Sizes) AS Size
    GROUP BY Size
    )
    ORDER BY ProductCount DESC, Size ASC
  ) AS Buckets
),
PriceRanges AS (
  SELECT ARRAY(
    SELECT AS STRUCT Bucket AS Value, ProductCount
    FROM (
    SELECT
      CASE
        WHEN PriceCents < 10000 THEN 'under_100'
        WHEN PriceCents < 15000 THEN '100_to_150'
        ELSE '150_and_up'
      END AS Bucket,
      CASE
        WHEN PriceCents < 10000 THEN 1
        WHEN PriceCents < 15000 THEN 2
        ELSE 3
      END AS SortOrder,
      COUNT(*) AS ProductCount
    FROM SearchResults
    GROUP BY Bucket, SortOrder
    )
    ORDER BY SortOrder
  ) AS Buckets
),
Metrics AS (
  SELECT
    COUNT(*) AS ResultCount,
    AVG(PriceCents) AS AvgPriceCents,
    SUM(InventoryCount) AS TotalInventory
  FROM SearchResults
)
SELECT
  ResultPage.ResultRows AS Results,
  CategoryFacets.Buckets AS CategoryFacets,
  BrandFacets.Buckets AS BrandFacets,
  ColorFacets.Buckets AS ColorFacets,
  SizeFacets.Buckets AS SizeFacets,
  PriceRanges.Buckets AS PriceRanges,
  Metrics.ResultCount,
  Metrics.AvgPriceCents,
  Metrics.TotalInventory
FROM ResultPage, CategoryFacets, BrandFacets, ColorFacets, SizeFacets, PriceRanges, Metrics;
