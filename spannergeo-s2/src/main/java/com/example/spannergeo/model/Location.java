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

package com.example.spannergeo.model;

/**
 * A geo-tagged point of interest. This is the application-level representation
 * of a row in the PointOfInterest table.
 */
public class Location {

    private final String poiId;
    private final String name;
    private final String category;
    private final double latitude;
    private final double longitude;

    public Location(String poiId, String name, String category,
                    double latitude, double longitude) {
        this.poiId = poiId;
        this.name = name;
        this.category = category;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getPoiId()    { return poiId; }
    public String getName()     { return name; }
    public String getCategory() { return category; }
    public double getLatitude()  { return latitude; }
    public double getLongitude() { return longitude; }

    @Override
    public String toString() {
        return String.format("Location{id=%s, name='%s', category='%s', lat=%.6f, lng=%.6f}",
                poiId, name, category, latitude, longitude);
    }
}
