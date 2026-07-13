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

INSERT INTO SearchProducts (
  TenantId,
  ProductId,
  Title,
  Description,
  Category,
  Status
) VALUES
  (
    'tenant-a',
    'run-1',
    'Lightweight trail running shoe',
    'Runners use this shoe for running on rocky trails.',
    'footwear',
    'active'
  ),
  (
    'tenant-a',
    'walk-1',
    'City walking sneaker',
    'Comfortable support for daily walks and walking commutes.',
    'footwear',
    'active'
  ),
  (
    'tenant-a',
    'hike-1',
    'Waterproof hiking boot',
    'Built for hikes, hiking trips, and wet mountain paths.',
    'footwear',
    'active'
  ),
  (
    'tenant-a',
    'recover-1',
    'Runner recovery sandal',
    'Soft sandal for recovery after long runs.',
    'footwear',
    'active'
  );
