/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spannergeo;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2CellUnion;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2RegionCoverer;
import com.google.common.geometry.S2Cap;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S1Angle;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for S2 Geometry operations.
 *
 * Key concepts:
 * - S2CellId: a 64-bit identifier for a cell on the Earth's surface.
 *   Cells form a hierarchy (levels 0-30); higher level = smaller cell.
 * - S2RegionCoverer: computes a set of cells that "cover" a region (circle, rect, polygon).
 * - Covering + post-filter: query the index for covering cells, then post-filter
 *   with exact distance to eliminate false positives.
 */
public class S2Util {

    // Cell levels we index at. Multiple levels let us balance precision vs. index size.
    // Level 12 ≈ 3.3km cells, Level 14 ≈ 800m cells, Level 16 ≈ 150m cells
    public static final int[] INDEX_LEVELS = {12, 14, 16};

    // Earth's radius in meters (for Haversine)
    public static final double EARTH_RADIUS_METERS = 6_371_000.0;

    /**
     * Encode a lat/lng point into S2CellIds at the configured index levels.
     * Each returned CellId corresponds to the cell at that level which contains the point.
     */
    public static List<S2CellId> encodeCellIds(double latitude, double longitude) {
        S2LatLng latLng = S2LatLng.fromDegrees(latitude, longitude);
        S2CellId leafCell = S2CellId.fromLatLng(latLng);

        List<S2CellId> cellIds = new ArrayList<>();
        for (int level : INDEX_LEVELS) {
            cellIds.add(leafCell.parent(level));
        }
        return cellIds;
    }

    /**
     * Compute a covering for a circular region (center + radius in meters).
     * Returns a list of S2CellId ranges (min, max pairs). Each range represents
     * a contiguous run of cells in the covering.
     *
     * The covering is used to query the index: for each range, do
     *   WHERE S2CellId BETWEEN range.min AND range.max
     */
    public static List<CellIdRange> computeCovering(double centerLat, double centerLng,
                                                     double radiusMeters) {
        S2LatLng center = S2LatLng.fromDegrees(centerLat, centerLng);

        // Convert radius to an S1Angle (angle subtended at Earth's center)
        S1Angle radiusAngle = S1Angle.radians(radiusMeters / EARTH_RADIUS_METERS);

        // Create a spherical cap (circle on the sphere)
        S2Cap cap = S2Cap.fromAxisAngle(center.toPoint(), radiusAngle);

        // Configure the coverer: balance between precision and number of cells
        S2RegionCoverer coverer = S2RegionCoverer.builder()
                .setMinLevel(INDEX_LEVELS[0])                   // Don't go coarser than our coarsest index level
                .setMaxLevel(INDEX_LEVELS[INDEX_LEVELS.length - 1]) // Don't go finer than our finest
                .setMaxCells(20)                                 // Cap the number of cells to keep query manageable
                .build();

        S2CellUnion covering = coverer.getCovering(cap);
        return toRanges(covering);
    }

    /**
     * Compute a covering for a rectangular region (bounding box).
     */
    public static List<CellIdRange> computeCoveringRect(double minLat, double minLng,
                                                         double maxLat, double maxLng) {
        S2LatLngRect rect = new S2LatLngRect(
                S2LatLng.fromDegrees(minLat, minLng),
                S2LatLng.fromDegrees(maxLat, maxLng));

        S2RegionCoverer coverer = S2RegionCoverer.builder()
                .setMinLevel(INDEX_LEVELS[0])
                .setMaxLevel(INDEX_LEVELS[INDEX_LEVELS.length - 1])
                .setMaxCells(20)
                .build();

        S2CellUnion covering = coverer.getCovering(rect);
        return toRanges(covering);
    }

    /**
     * Convert an S2CellUnion into a list of contiguous CellId ranges.
     * Each cell in the union becomes a range [cell.rangeMin, cell.rangeMax]
     * covering all leaf cells within it.
     */
    private static List<CellIdRange> toRanges(S2CellUnion union) {
        List<CellIdRange> ranges = new ArrayList<>();
        for (S2CellId cellId : union) {
            ranges.add(new CellIdRange(
                    cellId.rangeMin().id(),
                    cellId.rangeMax().id()));
        }
        return ranges;
    }

    /**
     * Haversine distance between two points in meters.
     * Used for post-filtering after the S2 covering query returns candidates.
     */
    public static double haversineDistance(double lat1, double lng1,
                                           double lat2, double lng2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_METERS * c;
    }

    /**
     * A contiguous range of S2 Cell IDs [min, max].
     * Maps directly to a SQL BETWEEN clause.
     */
    public static class CellIdRange {
        private final long min;
        private final long max;

        public CellIdRange(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public long getMin() { return min; }
        public long getMax() { return max; }

        @Override
        public String toString() {
            return String.format("CellIdRange[%d, %d]", min, max);
        }
    }
}
