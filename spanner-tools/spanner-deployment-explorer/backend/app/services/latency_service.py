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

"""
GCP Inter-Region Latency & Distance Calculation Service
Provides empirical and physics-based network RTT interpolation and distance formatting.
"""

import csv
import math
from pathlib import Path
from typing import Dict, Optional, Tuple
from pydantic import BaseModel

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
HEATMAP_FILE = DATA_DIR / "heatmap.csv"
PINGMAP_FILE = DATA_DIR / "pingmap.csv"


class EdgeMetrics(BaseModel):
    distance_km: float
    distance_miles: float
    distance_label: str
    latency_rtt_min_ms: Optional[float] = None
    latency_rtt_typical_ms: float
    latency_rtt_max_ms: Optional[float] = None
    latency_label: str
    latency_badge_text: str
    is_measured: bool


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates the great-circle distance between two points in kilometers."""
    r_km = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r_km * c


def km_to_miles(km: float) -> float:
    """Converts kilometers to statute miles."""
    return km * 0.621371


class LatencyService:
    _instance: Optional["LatencyService"] = None

    def __init__(
        self,
        heatmap_path: Optional[Path] = None,
        pingmap_path: Optional[Path] = None,
    ):
        self.heatmap_path = heatmap_path or HEATMAP_FILE
        self.pingmap_path = pingmap_path or PINGMAP_FILE
        self._heatmap_lookup: Dict[Tuple[str, str], float] = {}
        self._ping_lookup: Dict[Tuple[str, str], float] = {}
        self._load_heatmap()
        self._load_pingmap()

    @classmethod
    def get_instance(cls) -> "LatencyService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _load_heatmap(self) -> None:
        """Loads GCP inter-region network latency heatmap matrix (43x43 regions)."""
        if not self.heatmap_path.exists():
            return

        try:
            with open(self.heatmap_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header or len(header) < 2:
                    return
                dest_regions = [r.strip() for r in header[1:]]
                for row in reader:
                    if not row:
                        continue
                    src = row[0].strip()
                    for dest, val_str in zip(dest_regions, row[1:]):
                        val_str = val_str.strip()
                        if val_str:
                            try:
                                val = float(val_str)
                                if val >= 0:
                                    self._heatmap_lookup[(src, dest)] = val
                            except ValueError:
                                pass
        except Exception as e:
            print(f"⚠️ Warning: Could not parse heatmap {self.heatmap_path}: {e}")

    def _load_pingmap(self) -> None:
        """Loads measured GCP ping latency map if available (fallback source)."""
        if not self.pingmap_path.exists():
            return

        try:
            with open(self.pingmap_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    s = row.get("sending_region", "").strip()
                    r = row.get("receiving_region", "").strip()
                    lat_str = row.get("latency", "").strip()
                    if s and r and lat_str:
                        try:
                            val = float(lat_str)
                            if val > 0:  # Ignore sentinels like -9999
                                self._ping_lookup[(s, r)] = val
                        except ValueError:
                            continue
        except Exception as e:
            print(f"⚠️ Warning: Could not parse pingmap {self.pingmap_path}: {e}")

    def calculate_edge_metrics(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        source_region: Optional[str] = None,
        target_region: Optional[str] = None,
    ) -> EdgeMetrics:
        """
        Calculates distance and RTT latency between two regions.
        Uses empirical measured heatmap matrix when available; falls back to calibrated optical propagation model.
        Rounds measured latencies to whole integer milliseconds (e.g. 180.33 -> ~180ms).
        """
        dist_km = round(haversine_distance_km(lat1, lon1, lat2, lon2), 1)
        dist_miles = round(km_to_miles(dist_km), 1)
        dist_label = f"{int(round(dist_miles)):,} mi / {int(round(dist_km)):,} km"

        # 1. Primary check: Heatmap lookup
        measured_latency: Optional[float] = None
        if source_region and target_region:
            measured_latency = self._heatmap_lookup.get((source_region, target_region))
            if measured_latency is None:
                measured_latency = self._heatmap_lookup.get((target_region, source_region))
            # 2. Secondary check: Pingmap lookup
            if measured_latency is None:
                measured_latency = self._ping_lookup.get((source_region, target_region))
                if measured_latency is None:
                    measured_latency = self._ping_lookup.get((target_region, source_region))

        if measured_latency is not None and measured_latency >= 0:
            # Round to nearest integer (e.g. 180.33 -> 180, 0.58 -> 1)
            rounded_val = max(1, int(round(measured_latency)))
            typical_rtt = float(rounded_val)
            min_rtt = typical_rtt
            max_rtt = typical_rtt
            is_measured = True
            latency_label = f"~{rounded_val}ms"
            badge_text = f"~{rounded_val}ms · {dist_label}"
        else:
            # Calibrated fiber optical model fallback
            if dist_km < 30.0:
                typical_rtt = 1.8
                min_rtt = 1.0
                max_rtt = 3.0
            else:
                typical_rtt = round(max(2.5, dist_km * 0.019 + 2.0), 1)
                min_rtt = round(max(1.5, dist_km * 0.014 + 1.2), 1)
                max_rtt = round(max(3.5, dist_km * 0.026 + 3.8), 1)
            is_measured = False
            rounded_val = max(1, int(round(typical_rtt)))
            latency_label = f"~{rounded_val}ms"
            badge_text = f"~{rounded_val}ms · {dist_label}"

        return EdgeMetrics(
            distance_km=dist_km,
            distance_miles=dist_miles,
            distance_label=dist_label,
            latency_rtt_min_ms=min_rtt,
            latency_rtt_typical_ms=typical_rtt,
            latency_rtt_max_ms=max_rtt,
            latency_label=latency_label,
            latency_badge_text=badge_text,
            is_measured=is_measured,
        )
