"""Global spherical distance-band neighbor network for Elwood AQ outlier detection."""

import math

import geopandas as gpd
import numpy as np
from scipy.spatial import KDTree

EARTH_RADIUS_M = 6_371_000.0
MIN_DISTANCE_M = 1.0


def build_spherical_network(
    gdf: gpd.GeoDataFrame,
    threshold_m: float,
    id_column: str,
    max_neighbors: int = 20,
    min_neighbors: int = 0,
) -> dict[str, dict[str, list]]:
    """Build a global great-circle inverse-distance neighbor network from WGS84 points.

    Uses a 3D unit-sphere chord KDTree so the distance threshold (`threshold_m`)
    is enforced uniformly across CONUS, Alaska, Hawaii, Puerto Rico/USVI, Canada,
    and Mexico without planar UTM zone distortion, and clamps co-located (`0 m`)
    sensor pairs to `1.0 m` so weights remain finite JSON floats.
    """
    if gdf.empty:
        return {}

    pts = gdf
    if pts.crs is not None and pts.crs.to_epsg() != 4326:
        pts = pts.to_crs(epsg=4326)

    unit_ids = [str(uid) for uid in pts[id_column].tolist()]
    if len(unit_ids) == 1:
        if min_neighbors <= 0:
            return {unit_ids[0]: {"neighbors": [], "weights": []}}
        return {}

    lons_rad = np.radians(pts.geometry.x.to_numpy(dtype=float))
    lats_rad = np.radians(pts.geometry.y.to_numpy(dtype=float))
    cos_lat = np.cos(lats_rad)
    xyz = np.column_stack(
        (cos_lat * np.cos(lons_rad), cos_lat * np.sin(lons_rad), np.sin(lats_rad))
    )

    half_angle = threshold_m / (2.0 * EARTH_RADIUS_M)
    chord_radius = 2.0 if half_angle >= (math.pi / 2.0) else 2.0 * math.sin(half_angle)

    tree = KDTree(xyz)
    raw_neighbors = tree.query_ball_tree(tree, r=chord_radius)

    network: dict[str, dict[str, list]] = {}
    for idx, candidates in enumerate(raw_neighbors):
        other_indices = [j for j in candidates if j != idx]
        if not other_indices:
            if min_neighbors <= 0:
                network[unit_ids[idx]] = {"neighbors": [], "weights": []}
            continue

        diffs = xyz[other_indices] - xyz[idx]
        chords = np.linalg.norm(diffs, axis=1)
        arc_m = 2.0 * EARTH_RADIUS_M * np.arcsin(np.clip(chords / 2.0, 0.0, 1.0))
        weights = 1.0 / np.maximum(arc_m, MIN_DISTANCE_M)

        pairs = sorted(
            (
                (float(w), unit_ids[j])
                for w, j in zip(weights, other_indices, strict=True)
            ),
            key=lambda pair: (-pair[0], pair[1]),
        )[:max_neighbors]

        if len(pairs) >= min_neighbors:
            network[unit_ids[idx]] = {
                "neighbors": [nid for _, nid in pairs],
                "weights": [w for w, _ in pairs],
            }

    return network
