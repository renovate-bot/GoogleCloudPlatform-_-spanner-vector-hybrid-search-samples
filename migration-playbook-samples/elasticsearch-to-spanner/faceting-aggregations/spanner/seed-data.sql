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

INSERT INTO FacetedProducts (
  TenantId,
  ProductId,
  Title,
  Description,
  Category,
  Brand,
  Colors,
  Sizes,
  PriceCents,
  Rating,
  InventoryCount,
  Status,
  UpdatedAt
) VALUES
  (
    'tenant-a',
    'trail-1',
    'Waterproof trail running shoe',
    'Lightweight waterproof trail shoe for wet mountain runs.',
    'footwear',
    'Summit',
    ['black', 'blue'],
    ['9', '10', '11'],
    12999,
    4.7,
    18,
    'active',
    TIMESTAMP '2026-05-01T12:00:00Z'
  ),
  (
    'tenant-a',
    'trail-2',
    'Trail running shoe',
    'Breathable trail shoe for dry singletrack routes.',
    'footwear',
    'Northstar',
    ['green', 'black'],
    ['8', '9', '10'],
    9999,
    4.4,
    25,
    'active',
    TIMESTAMP '2026-05-02T12:00:00Z'
  ),
  (
    'tenant-a',
    'hike-1',
    'Waterproof hiking boot',
    'Durable waterproof boot for rocky trail hikes.',
    'footwear',
    'Summit',
    ['brown', 'black'],
    ['10', '11', '12'],
    17999,
    4.8,
    12,
    'active',
    TIMESTAMP '2026-05-03T12:00:00Z'
  ),
  (
    'tenant-a',
    'jacket-1',
    'Waterproof trail jacket',
    'Packable waterproof shell for trail running and hiking.',
    'apparel',
    'Apex',
    ['blue', 'red'],
    ['S', 'M', 'L'],
    14999,
    4.6,
    9,
    'active',
    TIMESTAMP '2026-05-04T12:00:00Z'
  ),
  (
    'tenant-a',
    'pack-1',
    'Trail hydration pack',
    'Minimal hydration pack for long trail runs.',
    'gear',
    'Northstar',
    ['black'],
    ['one-size'],
    8999,
    4.3,
    20,
    'active',
    TIMESTAMP '2026-05-05T12:00:00Z'
  ),
  (
    'tenant-a',
    'socks-1',
    'Trail running socks',
    'Cushioned socks for trail running in wet weather.',
    'apparel',
    'Metro',
    ['gray', 'black'],
    ['M', 'L'],
    1999,
    4.2,
    50,
    'active',
    TIMESTAMP '2026-05-06T12:00:00Z'
  ),
  (
    'tenant-a',
    'sandal-1',
    'Recovery sandal',
    'Soft sandal for recovery after long trail days.',
    'footwear',
    'Metro',
    ['black'],
    ['9', '10', '11'],
    4999,
    4.1,
    16,
    'active',
    TIMESTAMP '2026-05-07T12:00:00Z'
  ),
  (
    'tenant-a',
    'tent-1',
    'Backpacking tent',
    'Two-person tent for mountain trail trips.',
    'gear',
    'Apex',
    ['green'],
    ['two-person'],
    24999,
    4.9,
    5,
    'active',
    TIMESTAMP '2026-05-08T12:00:00Z'
  );
