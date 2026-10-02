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

import csv
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

class DataLoader:
    _instance = None
    _lock = threading.RLock()

    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or DATA_DIR
        self._geo_lookup: Dict[str, Dict[str, Any]] = {}
        self._configs_by_name: Dict[str, List[Dict[str, Any]]] = {}
        self._raw_configs: List[Dict[str, Any]] = []
        self._loaded = False
        self._load_data()

    @classmethod
    def get_instance(cls) -> "DataLoader":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def _load_data(self) -> None:
        with self._lock:
            if self._loaded:
                return

            geo_file = self.data_dir / "geolookup.csv"
            config_file = self.data_dir / "spannerconfig.csv"

            if not geo_file.exists():
                raise FileNotFoundError(f"Missing required geo lookup file: {geo_file}")
            if not config_file.exists():
                raise FileNotFoundError(f"Missing required Spanner config file: {config_file}")

            # Load geo lookup
            with open(geo_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    region = row["region"].strip()
                    self._geo_lookup[region] = {
                        "region": region,
                        "locationname": row["locationname"].strip(),
                        "lat": float(row["lat"]),
                        "lon": float(row["lon"]),
                    }

            # Load Spanner configurations
            with open(config_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    configname = row["configname"].strip()
                    item = {
                        "id": row["id"].strip(),
                        "configname": configname,
                        "region": row["region"].strip(),
                        "type": row["type"].strip().lower(),
                        "instancetype": row["instancetype"].strip().lower(),
                        "continentregion": row["continentregion"].strip(),
                    }
                    self._raw_configs.append(item)
                    if configname not in self._configs_by_name:
                        self._configs_by_name[configname] = []
                    self._configs_by_name[configname].append(item)

            self._loaded = True

    @property
    def geo_lookup(self) -> Dict[str, Dict[str, Any]]:
        return self._geo_lookup

    @property
    def configs_by_name(self) -> Dict[str, List[Dict[str, Any]]]:
        return self._configs_by_name

    @property
    def raw_configs(self) -> List[Dict[str, Any]]:
        return self._raw_configs

    def get_geo(self, region: str) -> Optional[Dict[str, Any]]:
        return self._geo_lookup.get(region)

    def get_config(self, configname: str) -> Optional[List[Dict[str, Any]]]:
        if configname in self._configs_by_name:
            return self._configs_by_name[configname]
        if configname.startswith("regional-"):
            stripped = configname[len("regional-"):]
            if stripped in self._configs_by_name:
                return self._configs_by_name[stripped]
        return None

    def get_gcp_instance_config(self, configname: str) -> str:
        """
        Resolves canonical GCP Spanner instanceConfig identifier.
        Regional configurations in GCP use the 'regional-' prefix (e.g. 'regional-europe-central2'),
        while multi-region and dual-region configurations do not (e.g. 'eur3', 'dual-region-germany1').
        """
        if configname.startswith("regional-") or configname.startswith("dual-region-"):
            return configname
        rows = self.get_config(configname)
        if rows and rows[0].get("instancetype") == "regional":
            return f"regional-{configname}"
        return configname
