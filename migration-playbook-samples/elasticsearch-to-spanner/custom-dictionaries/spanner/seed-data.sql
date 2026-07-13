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

INSERT INTO DictionaryProducts (
  TenantId,
  ProductId,
  Title,
  Description,
  Category,
  Status
) VALUES
  (
    'tenant-a',
    'trail-1',
    'Trail running shoe',
    'Lightweight shoe for running on mountain trails.',
    'footwear',
    'active'
  ),
  (
    'tenant-a',
    'jacket-1',
    'Waterproof jacket',
    'Packable shell for wet trail runs.',
    'apparel',
    'active'
  ),
  (
    'tenant-a',
    'pack-1',
    'Trail hydration pack',
    'Low-profile pack for carrying water on long runs.',
    'gear',
    'active'
  ),
  (
    'tenant-a',
    'umbrella-1',
    'Compact umbrella',
    'Small umbrella for everyday commuting.',
    'gear',
    'active'
  ),
  (
    'tenant-a',
    'archived-jacket-1',
    'Rain jacket archive sample',
    'Inactive waterproof layer used to verify status filters.',
    'apparel',
    'inactive'
  ),
  (
    'tenant-b',
    'tenant-b-shoe-1',
    'Running shoe tenant B',
    'Tenant-isolated shoe record.',
    'footwear',
    'active'
  );
