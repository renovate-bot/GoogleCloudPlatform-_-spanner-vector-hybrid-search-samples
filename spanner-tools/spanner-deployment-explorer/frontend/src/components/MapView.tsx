// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import React, { useEffect, useState, useRef, useMemo } from 'react';
import {
  MapContainer,
  TileLayer,
  Marker,
  Popup,
  Polyline,
  Tooltip as LeafletTooltip,
  useMap,
} from 'react-leaflet';
import L from 'leaflet';
import {
  Box,
  Typography,
  Chip,
  Paper,
  Stack,
  FormControlLabel,
  Switch,
  IconButton,
  Button,
  Tooltip,
  ToggleButton,
  ToggleButtonGroup,
  Divider,
} from '@mui/material';
import ZoomOutMapIcon from '@mui/icons-material/ZoomOutMap';
import LayersIcon from '@mui/icons-material/Layers';
import MapIcon from '@mui/icons-material/Map';
import PublicIcon from '@mui/icons-material/Public';
import SwapHorizIcon from '@mui/icons-material/SwapHoriz';

import { DeploymentVisualization, TopologyNode, TopologyLink, ReplicaType } from '../types/spanner';
import { Legend } from './Legend';
import { gcpPalette } from '../theme';
import { ALLOW_MULTIPLE_SELECTIONS } from '../config';

interface MapViewProps {
  visualization: DeploymentVisualization | null;
  loading: boolean;
  clientRegions?: string[];
  onLeaderChange?: (configname: string, newLeader: string) => void;
  selectedOptionalReplicasMap?: Record<string, string[]>;
}

const GCE_CLIENT_COORDS: Record<string, { name: string; lat: number; lon: number }> = {
  'us-central1': { name: 'Iowa', lat: 41.2619, lon: -95.8608 },
  'us-east1': { name: 'South Carolina', lat: 33.196, lon: -79.974 },
  'us-east4': { name: 'Northern Virginia', lat: 39.0438, lon: -77.4874 },
  'us-east5': { name: 'Columbus', lat: 39.9612, lon: -82.9988 },
  'us-south1': { name: 'Dallas', lat: 32.7767, lon: -96.797 },
  'us-west1': { name: 'Oregon', lat: 45.5946, lon: -121.1786 },
  'us-west2': { name: 'Los Angeles', lat: 34.0522, lon: -118.2437 },
  'us-west3': { name: 'Salt Lake City', lat: 40.7608, lon: -111.891 },
  'us-west4': { name: 'Las Vegas', lat: 36.1699, lon: -115.1398 },
  'northamerica-northeast1': { name: 'Montréal', lat: 45.5017, lon: -73.5673 },
  'northamerica-northeast2': { name: 'Toronto', lat: 43.6532, lon: -79.3832 },
  'southamerica-east1': { name: 'São Paulo', lat: -23.5505, lon: -46.6333 },
  'southamerica-west1': { name: 'Santiago', lat: -33.4489, lon: -70.6693 },
  'europe-west1': { name: 'Belgium', lat: 50.4542, lon: 3.8232 },
  'europe-west2': { name: 'London', lat: 51.5074, lon: -0.1278 },
  'europe-west3': { name: 'Frankfurt', lat: 50.1109, lon: 8.6821 },
  'europe-west4': { name: 'Eemshaven', lat: 53.4357, lon: 6.7865 },
  'europe-west6': { name: 'Zurich', lat: 47.3769, lon: 8.5417 },
  'europe-west8': { name: 'Milan', lat: 45.4642, lon: 9.19 },
  'europe-west9': { name: 'Paris', lat: 48.8566, lon: 2.3522 },
  'europe-west10': { name: 'Berlin', lat: 52.52, lon: 13.405 },
  'europe-west12': { name: 'Turin', lat: 45.0703, lon: 7.6869 },
  'europe-north1': { name: 'Finland', lat: 60.5693, lon: 27.1878 },
  'europe-central2': { name: 'Warsaw', lat: 52.2297, lon: 21.0122 },
  'europe-southwest1': { name: 'Madrid', lat: 40.4168, lon: -3.7038 },
  'asia-east1': { name: 'Taiwan', lat: 24.0815, lon: 120.5383 },
  'asia-east2': { name: 'Hong Kong', lat: 22.3193, lon: 114.1694 },
  'asia-northeast1': { name: 'Tokyo', lat: 35.6762, lon: 139.6503 },
  'asia-northeast2': { name: 'Osaka', lat: 34.6937, lon: 135.5023 },
  'asia-northeast3': { name: 'Seoul', lat: 37.5665, lon: 126.978 },
  'asia-south1': { name: 'Mumbai', lat: 19.076, lon: 72.8777 },
  'asia-south2': { name: 'Delhi', lat: 28.6139, lon: 77.209 },
  'asia-southeast1': { name: 'Singapore', lat: 1.3521, lon: 103.8198 },
  'asia-southeast2': { name: 'Jakarta', lat: -6.2088, lon: 106.8456 },
  'australia-southeast1': { name: 'Sydney', lat: -33.8688, lon: 151.2093 },
  'australia-southeast2': { name: 'Melbourne', lat: -37.8136, lon: 144.9631 },
  'me-central1': { name: 'Doha', lat: 25.2854, lon: 51.531 },
  'me-central2': { name: 'Dammam', lat: 26.4207, lon: 50.0888 },
  'me-west1': { name: 'Tel Aviv', lat: 32.0853, lon: 34.7818 },
  'africa-south1': { name: 'Johannesburg', lat: -26.2041, lon: 28.0473 },
  'asia-southeast3': { name: 'Bangkok', lat: 13.7563, lon: 100.5018 },
  'europe-north2': { name: 'Stockholm', lat: 59.3293, lon: 18.0686 },
  'northamerica-south1': { name: 'Querétaro', lat: 20.5888, lon: -100.3899 },
  'us-west8': { name: 'Phoenix', lat: 33.4484, lon: -112.0740 },
};

const createClientVmIcon = (regionName: string) => {
  return L.divIcon({
    className: 'gcp-client-vm-marker',
    html: `
      <div style="
        position: relative;
        width: 28px;
        height: 28px;
        background: #7c3aed;
        border: 2px solid #ffffff;
        border-radius: 6px;
        box-shadow: 0 0 0 3px rgba(124, 58, 237, 0.4), 0 3px 6px rgba(0,0,0,0.3);
        display: flex;
        align-items: center;
        justify-content: center;
        color: #ffffff;
        font-size: 13px;
        font-weight: 700;
        cursor: pointer;
      " title="GCE Client VM (${regionName})">
        💻
      </div>
    `,
    iconSize: [28, 28],
    iconAnchor: [14, 14],
    popupAnchor: [0, -16],
  });
};

// Controller to auto-fit map view bounds only when configuration selection changes
const MapBoundsController: React.FC<{
  bounds?: [[number, number], [number, number]] | null;
  configsKey: string;
}> = ({ bounds, configsKey }) => {
  const map = useMap();
  const prevConfigsKeyRef = useRef<string>('');

  useEffect(() => {
    const isCleared = !configsKey || configsKey === '#';
    if (isCleared) {
      if (prevConfigsKeyRef.current !== '') {
        prevConfigsKeyRef.current = '';
        try {
          map.flyTo([25, 0], 2, { animate: true });
        } catch (err) {
          console.error('Error resetting map view:', err);
        }
      }
      return;
    }

    // Only auto-fit bounds when the set of selected configurations or client regions changes
    if (configsKey !== prevConfigsKeyRef.current) {
      prevConfigsKeyRef.current = configsKey;
      if (bounds && bounds.length === 2) {
        try {
          map.fitBounds(bounds, { padding: [60, 60], maxZoom: 6, animate: true });
        } catch (err) {
          console.error('Error fitting bounds:', err);
        }
      }
    }
  }, [bounds, configsKey, map]);

  return null;
};

// Captures the Leaflet Map instance to allow external controls (e.g. manual fit-to-bounds)
const MapInstanceTracker: React.FC<{ onMapReady: (map: L.Map) => void }> = ({ onMapReady }) => {
  const map = useMap();
  useEffect(() => {
    onMapReady(map);
  }, [map, onMapReady]);
  return null;
};

// Observes container resize (e.g. when side pane opens/closes) and triggers map.invalidateSize()
const MapResizeHandler: React.FC = () => {
  const map = useMap();
  useEffect(() => {
    const container = map.getContainer();
    if (!container) return;
    const ro = new ResizeObserver(() => {
      map.invalidateSize();
    });
    ro.observe(container);
    return () => {
      ro.disconnect();
    };
  }, [map]);
  return null;
};

// Custom Marker Icons for GCP Spanner Replica Roles
const createGcpDivIcon = (replicaType: ReplicaType, configName: string, isLeader: boolean) => {
  let bgColor = gcpPalette.replicaRole.rwReplica;
  let iconSvg = `<svg width="12" height="12" viewBox="0 0 24 24" fill="white"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z"/></svg>`;

  if (isLeader || replicaType === 'leader') {
    bgColor = gcpPalette.replicaRole.leader;
    iconSvg = `<svg width="14" height="14" viewBox="0 0 24 24" fill="white"><path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>`;
  } else if (replicaType === 'witness') {
    bgColor = gcpPalette.replicaRole.witness;
    iconSvg = `<svg width="12" height="12" viewBox="0 0 24 24" fill="white"><path d="M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z"/></svg>`;
  } else if (replicaType === 'read only' || replicaType === 'optional read only') {
    bgColor = gcpPalette.replicaRole.readOnly;
    iconSvg = `<svg width="12" height="12" viewBox="0 0 24 24" fill="white"><path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg>`;
  }

  const pulseRing = isLeader
    ? `<div style="position: absolute; width: 34px; height: 34px; border-radius: 50%; border: 2px solid ${bgColor}; animation: pulse 2s infinite; top: -7px; left: -7px; opacity: 0.8;"></div>`
    : '';

  const html = `
    <div style="position: relative; width: 24px; height: 24px;">
      ${pulseRing}
      <div style="
        width: 24px;
        height: 24px;
        background-color: ${bgColor};
        border: 2px solid #ffffff;
        box-shadow: 0 2px 6px rgba(0,0,0,0.35);
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        transition: transform 0.15s ease-in-out;
      " title="${configName}">
        ${iconSvg}
      </div>
    </div>
  `;

  return L.divIcon({
    html,
    className: 'gcp-map-marker',
    iconSize: [24, 24],
    iconAnchor: [12, 12],
    popupAnchor: [0, -14],
  });
};

export const MapView: React.FC<MapViewProps> = ({
  visualization,
  clientRegions = [],
  onLeaderChange,
  selectedOptionalReplicasMap,
}) => {
  const [basemapStyle, setBasemapStyle] = useState<'minimal' | 'osm'>('minimal');
  const [showLabels, setShowLabels] = useState<boolean>(false);
  const [showTopologyLines, setShowTopologyLines] = useState<boolean>(true);
  const [showLegend, setShowLegend] = useState<boolean>(true);
  const [showOptionalReplicas, setShowOptionalReplicas] = useState<boolean>(true);
  const [mapInstance, setMapInstance] = useState<L.Map | null>(null);
  const controlsToolbarRef = useRef<HTMLDivElement | null>(null);

  // Disable map click/scroll propagation on floating controls toolbar so interactions do not drag or zoom the map
  useEffect(() => {
    if (controlsToolbarRef.current) {
      L.DomEvent.disableClickPropagation(controlsToolbarRef.current);
      L.DomEvent.disableScrollPropagation(controlsToolbarRef.current);
    }
  }, []);

  const nodes: TopologyNode[] = visualization?.nodes || [];
  const links: TopologyLink[] = visualization?.links || [];
  const configsKey = (visualization?.configs || []).map((c) => c.configname).sort().join(',');

  // Filter out optional read-only replicas when showOptionalReplicas is false
  // OR when individual optional read-only replicas have been deselected in selectedOptionalReplicasMap
  const displayedNodes = useMemo(() => {
    return nodes.filter((n) => {
      if (n.replica_type === 'optional read only') {
        if (!showOptionalReplicas) return false;
        if (selectedOptionalReplicasMap && selectedOptionalReplicasMap[n.config_name]) {
          return selectedOptionalReplicasMap[n.config_name].includes(n.region);
        }
      }
      return true;
    });
  }, [nodes, showOptionalReplicas, selectedOptionalReplicasMap]);

  const displayedNodeIds = useMemo(() => {
    return new Set(displayedNodes.map((n) => n.id));
  }, [displayedNodes]);

  const [showAllClientLinks, setShowAllClientLinks] = useState<boolean>(false);

  const displayedLinks = useMemo(() => {
    return links.filter(
      (l) => displayedNodeIds.has(l.source_id) && displayedNodeIds.has(l.target_id)
    );
  }, [links, displayedNodeIds]);

  const clientLinks: TopologyLink[] = visualization?.client_links || [];

  // Filter backend client links based on active client regions, displayed nodes, and showAllClientLinks
  const displayedClientLinks = useMemo(() => {
    const clientRegionSet = new Set(clientRegions);
    return clientLinks.filter((l) => {
      if (!l.source_region || !clientRegionSet.has(l.source_region)) return false;
      if (!displayedNodeIds.has(l.target_id)) return false;
      if (!showAllClientLinks && l.link_type !== 'client_to_leader') return false;
      return true;
    });
  }, [clientLinks, clientRegions, displayedNodeIds, showAllClientLinks]);

  // Fallback client links if backend client_links are not yet populated
  const effectiveClientLinks = useMemo(() => {
    if (displayedClientLinks.length > 0) return displayedClientLinks;
    if (clientRegions.length === 0 || displayedNodes.length === 0) return [];

    const leaderNode = displayedNodes.find((n) => n.replica_type === 'leader') || displayedNodes[0];
    const targetNodes = showAllClientLinks ? displayedNodes : (leaderNode ? [leaderNode] : []);

    const fallbackLinks: TopologyLink[] = [];
    for (const regId of clientRegions) {
      const coords = GCE_CLIENT_COORDS[regId];
      if (!coords) continue;
      for (const tNode of targetNodes) {
        const isLeader = tNode.replica_type === 'leader';
        const r_km = 6371.0;
        const dLat = ((tNode.lat - coords.lat) * Math.PI) / 180;
        const dLon = ((tNode.lon - coords.lon) * Math.PI) / 180;
        const a =
          Math.sin(dLat / 2) * Math.sin(dLat / 2) +
          Math.cos((coords.lat * Math.PI) / 180) *
            Math.cos((tNode.lat * Math.PI) / 180) *
            Math.sin(dLon / 2) *
            Math.sin(dLon / 2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        const distKm = Math.round(r_km * c * 10) / 10;
        const distMiles = Math.round(distKm * 0.621371 * 10) / 10;
        const estRtt = Math.max(1, Math.round(distKm < 30 ? 1.8 : distKm * 0.019 + 2.0));

        fallbackLinks.push({
          source_id: `client-${regId}`,
          target_id: tNode.id,
          source_coord: [coords.lat, coords.lon],
          target_coord: [tNode.lat, tNode.lon],
          link_type: isLeader ? 'client_to_leader' : 'client_to_replica',
          config_name: tNode.config_name,
          source_region: regId,
          target_region: tNode.region,
          distance_km: distKm,
          distance_miles: distMiles,
          distance_label: `${Math.round(distMiles).toLocaleString()} mi / ${Math.round(distKm).toLocaleString()} km`,
          latency_rtt_typical_ms: estRtt,
          latency_label: `~${estRtt}ms`,
          latency_badge_text: `~${estRtt}ms · ${Math.round(distMiles).toLocaleString()} mi`,
          is_measured: false,
        });
      }
    }
    return fallbackLinks;
  }, [displayedClientLinks, clientRegions, displayedNodes, showAllClientLinks]);

  // Calculate effective bounds including visible Spanner nodes and client regions
  const effectiveBounds = useMemo(() => {
    const lats: number[] = [];
    const lons: number[] = [];

    for (const node of displayedNodes) {
      if (typeof node.lat === 'number' && typeof node.lon === 'number') {
        lats.push(node.lat);
        lons.push(node.lon);
      }
    }

    for (const regId of clientRegions) {
      const coords = GCE_CLIENT_COORDS[regId];
      if (coords) {
        lats.push(coords.lat);
        lons.push(coords.lon);
      }
    }

    if (lats.length === 0) return null;

    if (lats.length === 1) {
      return [
        [lats[0] - 4, lons[0] - 6],
        [lats[0] + 4, lons[0] + 6],
      ] as [[number, number], [number, number]];
    }

    const minLat = Math.min(...lats);
    const maxLat = Math.max(...lats);
    const minLon = Math.min(...lons);
    const maxLon = Math.max(...lons);

    const latPad = Math.max(1.5, (maxLat - minLat) * 0.1);
    const lonPad = Math.max(1.5, (maxLon - minLon) * 0.1);

    return [
      [Math.max(-85, minLat - latPad), minLon - lonPad],
      [Math.min(85, maxLat + latPad), maxLon + lonPad],
    ] as [[number, number], [number, number]];
  }, [displayedNodes, clientRegions]);

  const optionalKey = useMemo(() => {
    if (!selectedOptionalReplicasMap) return '';
    return Object.entries(selectedOptionalReplicasMap)
      .map(([cfg, regs]) => `${cfg}:${regs.slice().sort().join(',')}`)
      .sort()
      .join(';');
  }, [selectedOptionalReplicasMap]);

  const mapSelectionKey = useMemo(() => {
    return `${configsKey}#${clientRegions.slice().sort().join(',')}#showOpt:${showOptionalReplicas}#allClient:${showAllClientLinks}#optMap:${optionalKey}`;
  }, [configsKey, clientRegions, showOptionalReplicas, showAllClientLinks, optionalKey]);

  const handleFitBounds = () => {
    if (mapInstance && effectiveBounds && effectiveBounds.length === 2) {
      try {
        mapInstance.fitBounds(effectiveBounds, { padding: [60, 60], maxZoom: 6, animate: true });
      } catch (err) {
        console.error('Error fitting bounds:', err);
      }
    }
  };

  const getLinkColor = (linkType: string) => {
    switch (linkType) {
      case 'leader_to_rw':
        return '#1a73e8'; // Google Blue
      case 'leader_to_witness':
      case 'rw_to_witness':
        return '#5f6368'; // Neutral Gray
      case 'leader_to_read_only':
        return '#12b5cb'; // Cyan
      default:
        return '#1a73e8';
    }
  };

  const getLinkDashArray = (linkType: string) => {
    switch (linkType) {
      case 'leader_to_witness':
      case 'rw_to_witness':
        return '6, 6';
      case 'leader_to_read_only':
        return '2, 6';
      default:
        return undefined;
    }
  };

  return (
    <Box sx={{ position: 'relative', width: '100%', height: '100%', minHeight: 480, overflow: 'hidden', borderRadius: 2 }}>
      {/* CSS for pulsating leader marker and clean canvas */}
      <style>
        {`
          @keyframes pulse {
            0% { transform: scale(0.8); opacity: 0.9; }
            50% { transform: scale(1.4); opacity: 0.3; }
            100% { transform: scale(1.8); opacity: 0.8; }
          }
          .leaflet-container {
            width: 100%;
            height: 100%;
            background-color: #f8f9fa;
            font-family: Roboto, "Google Sans", sans-serif;
          }
          .gcp-map-marker:hover div {
            transform: scale(1.2);
          }
          .leaflet-popup-content-wrapper {
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(60,64,67,0.25);
            border: 1px solid #dadce0;
            padding: 0;
            overflow: hidden;
          }
          .leaflet-popup-content {
            margin: 0;
            line-height: 1.4;
          }
        `}
      </style>

      {/* Leaflet Map */}
      <MapContainer
        center={[25, 0]}
        zoom={2}
        minZoom={2}
        maxZoom={14}
        scrollWheelZoom={true}
        style={{ width: '100%', height: '100%' }}
      >
        {/* Minimalist Gray Canvas Basemap (Zero Street Clutter, No API Key Required) */}
        {basemapStyle === 'minimal' ? (
          <>
            <TileLayer
              key="esri-canvas-base"
              attribution="Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ"
              url="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}"
              maxZoom={16}
            />
            {showLabels && (
              <TileLayer
                key="esri-canvas-reference"
                attribution=""
                url="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Reference/MapServer/tile/{z}/{y}/{x}"
                maxZoom={16}
              />
            )}
          </>
        ) : (
          /* Standard OpenStreetMap Tiles (100% Free, Zero API Key) */
          <TileLayer
            key="osm-standard-base"
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            maxZoom={19}
          />
        )}

        <MapBoundsController bounds={effectiveBounds} configsKey={mapSelectionKey} />
        <MapInstanceTracker onMapReady={setMapInstance} />
        <MapResizeHandler />

        {/* Topology Lines */}
        {showTopologyLines &&
          displayedLinks.map((link) => {
            const linkKey = `link-${link.config_name}-${link.source_id}-${link.target_id}-${link.link_type}`;

            return (
              <Polyline
                key={linkKey}
                positions={[link.source_coord, link.target_coord]}
                pathOptions={{
                  color: getLinkColor(link.link_type),
                  weight: 3,
                  opacity: 0.85,
                  dashArray: getLinkDashArray(link.link_type),
                }}
                eventHandlers={{
                  mouseover: (e) => {
                    const layer = e.target;
                    layer.setStyle({ weight: 5, opacity: 1 });
                  },
                  mouseout: (e) => {
                    const layer = e.target;
                    layer.setStyle({ weight: 3, opacity: 0.85 });
                  },
                }}
              >
                {/* Hover Tooltip */}
                <LeafletTooltip sticky direction="top" opacity={0.95}>
                  <Box sx={{ p: 0.5, textAlign: 'center' }}>
                    <Typography variant="caption" sx={{ fontWeight: 700, color: 'text.primary', display: 'block' }}>
                      {link.source_region || 'Source'} ↔ {link.target_region || 'Target'}
                    </Typography>
                    <Typography variant="caption" sx={{ color: 'primary.main', fontWeight: 600, display: 'block' }}>
                      ⚡ {link.latency_label || `~${link.latency_rtt_typical_ms}ms`} · {link.distance_label}
                    </Typography>
                    <Typography variant="caption" sx={{ color: 'text.secondary', fontSize: '9px', display: 'block', mt: 0.2 }}>
                      Click edge for full details
                    </Typography>
                  </Box>
                </LeafletTooltip>

                {/* Click Popup */}
                <Popup className="gcp-map-popup">
                  <Box sx={{ minWidth: 230, p: 0.5 }}>
                    <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }}>
                      <Typography variant="subtitle2" sx={{ fontWeight: 700, color: 'text.primary' }}>
                        {link.source_region || 'Source'} ↔ {link.target_region || 'Target'}
                      </Typography>
                    </Stack>

                    <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1.5 }}>
                      <Chip
                        label={link.config_name}
                        size="small"
                        sx={{ height: 20, fontSize: '0.65rem', fontWeight: 600, backgroundColor: '#e8f0fe', color: '#1a73e8' }}
                      />
                      <Chip
                        label={link.link_type.replace(/_/g, ' ')}
                        size="small"
                        sx={{ height: 20, fontSize: '0.65rem', fontWeight: 500, backgroundColor: '#f1f3f4', color: '#5f6368' }}
                      />
                    </Stack>

                    <Divider sx={{ my: 1 }} />

                    <Box sx={{ mb: 1.5 }}>
                      <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', fontWeight: 500 }}>
                        {link.is_measured ? 'Measured Network Latency (RTT)' : 'Estimated Network Latency (RTT)'}
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 700, color: '#1a73e8' }}>
                        ⚡ {link.latency_label || `~${link.latency_rtt_typical_ms}ms`}
                      </Typography>
                      {!link.is_measured && link.latency_rtt_min_ms != null && link.latency_rtt_max_ms != null && link.latency_rtt_min_ms !== link.latency_rtt_max_ms && (
                        <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', fontSize: '11px', mt: 0.2 }}>
                          Estimated spectrum: {link.latency_rtt_min_ms} – {link.latency_rtt_max_ms} ms
                        </Typography>
                      )}
                    </Box>

                    <Box>
                      <Typography variant="body2" sx={{ fontWeight: 600, color: 'text.primary' }}>
                        📍 {link.distance_label || `${link.distance_miles} mi / ${link.distance_km} km`}
                      </Typography>
                    </Box>
                  </Box>
                </Popup>
              </Polyline>
            );
          })}

        {/* Markers */}
        {displayedNodes.map((node) => {
          const isLeader = node.replica_type === 'leader';
          const icon = createGcpDivIcon(node.replica_type, node.config_name, isLeader);

          return (
            <Marker key={node.id} position={[node.lat, node.lon]} icon={icon}>
              <Popup>
                <Box sx={{ minWidth: 220, p: 2 }}>
                  <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }}>
                    <Typography variant="body2" sx={{ fontWeight: 700, color: 'text.primary' }}>
                      {node.config_name}
                    </Typography>
                    <Chip
                      label={node.replica_type}
                      size="small"
                      sx={{
                        height: 20,
                        fontSize: '0.65rem',
                        fontWeight: 600,
                        backgroundColor:
                          node.replica_type === 'leader'
                            ? '#fce8e6'
                            : node.replica_type === 'witness'
                            ? '#f1f3f4'
                            : '#e8f0fe',
                        color:
                          node.replica_type === 'leader'
                            ? '#d93025'
                            : node.replica_type === 'witness'
                            ? '#5f6368'
                            : '#1a73e8',
                      }}
                    />
                  </Stack>

                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                    {node.location_name}
                  </Typography>
                  <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', mb: 1, fontFamily: 'monospace' }}>
                    {node.region}
                  </Typography>

                  <Box sx={{ p: 1, backgroundColor: '#f8f9fa', borderRadius: 1, border: '1px solid #dadce0' }}>
                    <Stack spacing={0.5}>
                      <Stack direction="row" justifyContent="space-between">
                        <Typography variant="caption" color="text.secondary">
                          SLA Tier:
                        </Typography>
                        <Typography variant="caption" sx={{ fontWeight: 600, color: '#1e8e3e' }}>
                          {node.sla}
                        </Typography>
                      </Stack>
                      <Stack direction="row" justifyContent="space-between">
                        <Typography variant="caption" color="text.secondary">
                          Est. Reads/sec:
                        </Typography>
                        <Typography variant="caption" sx={{ fontWeight: 600 }}>
                          {node.reads_per_sec > 0 ? node.reads_per_sec.toLocaleString() : '0'}
                        </Typography>
                      </Stack>
                      <Stack direction="row" justifyContent="space-between">
                        <Typography variant="caption" color="text.secondary">
                          Est. Writes/sec:
                        </Typography>
                        <Typography variant="caption" sx={{ fontWeight: 600, color: node.writes_per_sec > 0 ? '#d93025' : 'inherit' }}>
                          {node.writes_per_sec > 0 ? node.writes_per_sec.toLocaleString() : '0'}
                        </Typography>
                      </Stack>
                    </Stack>
                  </Box>

                  {/* Swap/Set as leader if candidate R/W replica */}
                  {node.replica_type === 'r/w replica' && onLeaderChange && (
                    <Button
                      size="small"
                      variant="outlined"
                      fullWidth
                      startIcon={<SwapHorizIcon sx={{ fontSize: 16 }} />}
                      onClick={() => onLeaderChange(node.config_name, node.region)}
                      sx={{
                        mt: 1.25,
                        fontSize: '0.72rem',
                        textTransform: 'none',
                        fontWeight: 700,
                        py: 0.35,
                        borderColor: '#fca5a5',
                        color: '#d93025',
                        backgroundColor: '#fff5f5',
                        boxShadow: 'none',
                        '&:hover': {
                          borderColor: '#ef4444',
                          backgroundColor: '#fee2e2',
                          boxShadow: 'none',
                        },
                      }}
                    >
                      Set as Leader Region
                    </Button>
                  )}
                </Box>
              </Popup>
            </Marker>
          );
        })}

        {/* Client VM Benchmark Latency Links */}
        {showTopologyLines &&
          effectiveClientLinks.map((link) => {
            const isLeaderLink = link.link_type === 'client_to_leader';
            const linkKey = `client-link-${link.config_name}-${link.source_id}-${link.target_id}`;

            return (
              <Polyline
                key={linkKey}
                positions={[link.source_coord, link.target_coord]}
                pathOptions={{
                  color: isLeaderLink ? '#7c3aed' : '#9333ea',
                  dashArray: isLeaderLink ? '6, 6' : '3, 6',
                  weight: isLeaderLink ? 3 : 2,
                  opacity: isLeaderLink ? 0.9 : 0.65,
                }}
                eventHandlers={{
                  mouseover: (e) => {
                    const layer = e.target;
                    layer.setStyle({ weight: 5, opacity: 1 });
                  },
                  mouseout: (e) => {
                    const layer = e.target;
                    layer.setStyle({
                      weight: isLeaderLink ? 3 : 2,
                      opacity: isLeaderLink ? 0.9 : 0.65,
                    });
                  },
                }}
              >
                {/* Hover Tooltip */}
                <LeafletTooltip sticky direction="top" opacity={0.95}>
                  <Box sx={{ p: 0.5, textAlign: 'center' }}>
                    <Typography variant="caption" sx={{ fontWeight: 700, color: 'text.primary', display: 'block' }}>
                      Client VM ({link.source_region}) ↔ {isLeaderLink ? 'Leader Region' : 'Replica'} ({link.target_region})
                    </Typography>
                    <Typography variant="caption" sx={{ color: '#7c3aed', fontWeight: 700, display: 'block' }}>
                      ⚡ {link.latency_label || `~${link.latency_rtt_typical_ms}ms`} · {link.distance_label}
                    </Typography>
                    <Typography variant="caption" sx={{ color: 'text.secondary', fontSize: '9px', display: 'block', mt: 0.2 }}>
                      {link.is_measured ? 'Measured GCP Network Latency' : 'Estimated Optical Propagation'}
                    </Typography>
                  </Box>
                </LeafletTooltip>

                {/* Click Popup */}
                <Popup className="gcp-map-popup">
                  <Box sx={{ minWidth: 230, p: 0.5 }}>
                    <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }}>
                      <Typography variant="subtitle2" sx={{ fontWeight: 700, color: 'text.primary' }}>
                        Client Latency Path
                      </Typography>
                    </Stack>

                    <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1.5 }}>
                      <Chip
                        label={`Client: ${link.source_region}`}
                        size="small"
                        sx={{ height: 20, fontSize: '0.65rem', fontWeight: 600, backgroundColor: '#f3e8ff', color: '#7c3aed' }}
                      />
                      <Chip
                        label={isLeaderLink ? 'Leader' : 'Replica'}
                        size="small"
                        sx={{
                          height: 20,
                          fontSize: '0.65rem',
                          fontWeight: 600,
                          backgroundColor: isLeaderLink ? '#fce8e6' : '#e8f0fe',
                          color: isLeaderLink ? '#d93025' : '#1a73e8',
                        }}
                      />
                    </Stack>

                    <Divider sx={{ my: 1 }} />

                    <Box sx={{ mb: 1.5 }}>
                      <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', fontWeight: 500 }}>
                        {link.is_measured ? 'Measured Round-Trip Latency (RTT)' : 'Estimated Optical Propagation RTT'}
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 700, color: '#7c3aed' }}>
                        ⚡ {link.latency_label || `~${link.latency_rtt_typical_ms}ms`}
                      </Typography>
                    </Box>

                    <Box>
                      <Typography variant="body2" sx={{ fontWeight: 600, color: 'text.primary' }}>
                        📍 {link.distance_label || `${link.distance_miles} mi / ${link.distance_km} km`}
                      </Typography>
                      <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', fontSize: '11px', mt: 0.2 }}>
                        Target Spanner Config: {link.config_name} ({link.target_region})
                      </Typography>
                    </Box>
                  </Box>
                </Popup>
              </Polyline>
            );
          })}

        {/* Client VM Benchmark Markers */}
        {clientRegions.map((regionId) => {
          const coords = GCE_CLIENT_COORDS[regionId];
          if (!coords) return null;
          const directLinks = effectiveClientLinks.filter((l) => l.source_region === regionId);

          return (
            <Marker
              key={`client-marker-${regionId}`}
              position={[coords.lat, coords.lon]}
              icon={createClientVmIcon(coords.name)}
              zIndexOffset={1000}
            >
              <Popup className="gcp-map-popup">
                <Box sx={{ p: 0.5, minWidth: 210 }}>
                  <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 0.5 }}>
                    <Chip
                      label="Client VM"
                      size="small"
                      sx={{
                        backgroundColor: '#f3e8ff',
                        color: '#7c3aed',
                        fontWeight: 700,
                        fontSize: '0.7rem',
                      }}
                    />
                    <Chip
                      label="Direct Access"
                      size="small"
                      variant="outlined"
                      color="success"
                      sx={{ fontSize: '0.68rem', height: 20 }}
                    />
                  </Stack>
                  <Typography variant="body2" sx={{ fontWeight: 700 }}>
                    {coords.name}
                  </Typography>
                  <Typography
                    variant="caption"
                    sx={{ color: 'text.secondary', display: 'block', mb: 1, fontFamily: 'monospace' }}
                  >
                    GCE Region: {regionId}
                  </Typography>

                  {/* Direct Spanner Latency Summary */}
                  {directLinks.length > 0 && (
                    <Box sx={{ mt: 1, p: 1, backgroundColor: '#f8f9fa', borderRadius: 1, border: '1px solid #dadce0' }}>
                      <Typography variant="caption" sx={{ fontWeight: 600, color: 'text.secondary', display: 'block', mb: 0.5 }}>
                        Network Latencies to Spanner:
                      </Typography>
                      <Stack spacing={0.5}>
                        {directLinks.map((l) => (
                          <Stack key={`${l.config_name}-${l.target_id}`} direction="row" justifyContent="space-between" alignItems="center">
                            <Typography
                              variant="caption"
                              sx={{
                                fontSize: '0.72rem',
                                color: l.link_type === 'client_to_leader' ? '#d93025' : '#5f6368',
                                fontWeight: l.link_type === 'client_to_leader' ? 700 : 500,
                              }}
                            >
                              {l.link_type === 'client_to_leader' ? '★ Leader' : 'Replica'} ({l.target_region}):
                            </Typography>
                            <Typography variant="caption" sx={{ fontWeight: 700, color: '#7c3aed', fontSize: '0.72rem' }}>
                              {l.latency_label}
                            </Typography>
                          </Stack>
                        ))}
                      </Stack>
                    </Box>
                  )}
                </Box>
              </Popup>
            </Marker>
          );
        })}
      </MapContainer>

      {/* Floating Map Controls & Overlays */}
      <Box
        sx={{
          position: 'absolute',
          top: 12,
          right: 12,
          zIndex: 1000,
          display: 'flex',
          flexDirection: 'column',
          gap: 1,
          alignItems: 'flex-end',
          pointerEvents: 'none',
        }}
      >
        <Paper
          ref={controlsToolbarRef}
          elevation={2}
          onMouseDown={(e) => e.stopPropagation()}
          onPointerDown={(e) => e.stopPropagation()}
          onTouchStart={(e) => e.stopPropagation()}
          onDoubleClick={(e) => e.stopPropagation()}
          onWheel={(e) => e.stopPropagation()}
          sx={{
            p: 1,
            backgroundColor: 'rgba(255, 255, 255, 0.95)',
            backdropFilter: 'blur(4px)',
            border: '1px solid #dadce0',
            borderRadius: 2,
            pointerEvents: 'auto',
          }}
        >
          <Stack spacing={1}>
            {/* Basemap Style Switcher */}
            <Stack direction="row" spacing={1} alignItems="center">
              <Typography variant="caption" sx={{ fontWeight: 600, color: '#5f6368', minWidth: 60 }}>
                Basemap:
              </Typography>
              <ToggleButtonGroup
                size="small"
                value={basemapStyle}
                exclusive
                onChange={(_, val) => {
                  if (val) setBasemapStyle(val);
                }}
                sx={{
                  height: 26,
                  '& .MuiToggleButton-root': {
                    px: 1,
                    py: 0,
                    fontSize: '0.72rem',
                    textTransform: 'none',
                    fontWeight: 500,
                  },
                }}
              >
                <ToggleButton value="minimal">
                  <PublicIcon sx={{ fontSize: 14, mr: 0.5 }} /> Minimal
                </ToggleButton>
                <ToggleButton value="osm">
                  <MapIcon sx={{ fontSize: 14, mr: 0.5 }} /> Standard
                </ToggleButton>
              </ToggleButtonGroup>

              <Tooltip title="Toggle Legend">
                <IconButton
                  size="small"
                  onClick={() => setShowLegend(!showLegend)}
                  color={showLegend ? 'primary' : 'default'}
                >
                  <LayersIcon fontSize="small" />
                </IconButton>
              </Tooltip>

              {effectiveBounds && effectiveBounds.length === 2 && (
                <Tooltip title="Fit map to deployment & clients">
                  <IconButton
                    size="small"
                    onClick={handleFitBounds}
                  >
                    <ZoomOutMapIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              )}
            </Stack>

            <Divider sx={{ my: 0.5 }} />

            {/* Layer Options */}
            <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap">
              <FormControlLabel
                control={
                  <Switch
                    size="small"
                    checked={showOptionalReplicas}
                    onChange={(e) => setShowOptionalReplicas(e.target.checked)}
                  />
                }
                label={
                  <Typography variant="caption" sx={{ fontWeight: 500 }}>
                    Optional Replicas
                  </Typography>
                }
                sx={{ m: 0 }}
              />

              <FormControlLabel
                control={
                  <Switch
                    size="small"
                    checked={showTopologyLines}
                    onChange={(e) => setShowTopologyLines(e.target.checked)}
                  />
                }
                label={
                  <Typography variant="caption" sx={{ fontWeight: 500 }}>
                    Quorum Links
                  </Typography>
                }
                sx={{ m: 0 }}
              />

              {clientRegions.length > 0 && (
                <FormControlLabel
                  control={
                    <Switch
                      size="small"
                      checked={showAllClientLinks}
                      onChange={(e) => setShowAllClientLinks(e.target.checked)}
                    />
                  }
                  label={
                    <Typography variant="caption" sx={{ fontWeight: 600, color: '#7c3aed' }}>
                      Client to All Replicas
                    </Typography>
                  }
                  sx={{ m: 0 }}
                />
              )}

              <FormControlLabel
                control={
                  <Switch
                    size="small"
                    checked={showLegend}
                    onChange={(e) => setShowLegend(e.target.checked)}
                  />
                }
                label={
                  <Typography variant="caption" sx={{ fontWeight: 500 }}>
                    Legend
                  </Typography>
                }
                sx={{ m: 0 }}
              />

              {basemapStyle === 'minimal' && (
                <FormControlLabel
                  control={
                    <Switch
                      size="small"
                      checked={showLabels}
                      onChange={(e) => setShowLabels(e.target.checked)}
                    />
                  }
                  label={
                    <Typography variant="caption" sx={{ fontWeight: 500 }}>
                      Labels
                    </Typography>
                  }
                  sx={{ m: 0 }}
                />
              )}
            </Stack>
          </Stack>
        </Paper>

        {showLegend && <Legend onClose={() => setShowLegend(false)} />}
      </Box>

      {/* Empty State Banner when no configs are selected */}
      {nodes.length === 0 && (
        <Box
          sx={{
            position: 'absolute',
            bottom: 24,
            left: '50%',
            transform: 'translateX(-50%)',
            zIndex: 1000,
            pointerEvents: 'none',
          }}
        >
          <Paper
            elevation={3}
            sx={{
              px: 2.5,
              py: 1.25,
              backgroundColor: 'rgba(32, 33, 36, 0.9)',
              color: '#ffffff',
              borderRadius: 4,
              display: 'flex',
              alignItems: 'center',
              gap: 1.5,
            }}
          >
            <ZoomOutMapIcon sx={{ fontSize: 18, color: '#8ab4f8' }} />
            <Typography variant="body2" sx={{ fontWeight: 500 }}>
              {ALLOW_MULTIPLE_SELECTIONS
                ? 'Select one or more Spanner configurations from the left sidebar to plot their deployment topology'
                : 'Select a Spanner configuration from the left sidebar to plot its deployment topology'}
            </Typography>
          </Paper>
        </Box>
      )}
    </Box>
  );
};
