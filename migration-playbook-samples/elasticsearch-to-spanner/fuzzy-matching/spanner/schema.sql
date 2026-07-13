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

CREATE TABLE FuzzyProducts (
  TenantId STRING(36) NOT NULL,
  ProductId STRING(64) NOT NULL,
  Title STRING(MAX) NOT NULL,
  Brand STRING(64),
  Status STRING(32),
  TitleNgrams TOKENLIST AS (
    TOKENIZE_SUBSTRING(
      Title,
      ngram_size_min=>2,
      ngram_size_max=>3,
      relative_search_types=>["word_prefix", "word_suffix"]
    )
  ) HIDDEN
) PRIMARY KEY (TenantId, ProductId);

CREATE SEARCH INDEX FuzzyProductsByTitleNgrams
ON FuzzyProducts(TitleNgrams)
STORING (Title, Brand, Status)
PARTITION BY TenantId;
