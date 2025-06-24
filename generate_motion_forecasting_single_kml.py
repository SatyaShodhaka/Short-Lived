#!/usr/bin/env python3
"""
Generate Motion Forecasting HD Maps Single KML

Convert Motion Forecasting HD map data into single KML files per split.
Supports filtering by region and data types.

Updated with robust Detroit geo-fencing using official AV2 coordinate system.
Optimized for processing 250k+ map files with parallel processing.
"""

import json
import os
import simplekml
import argparse
from pathlib import Path
from av2.geometry.utm import convert_city_coords_to_wgs84, CityName
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from shapely.geometry import Point, Polygon
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
from tqdm import tqdm
import time

MOTION_FORECASTING_BASE = "motion_forecasting"

class RobustDetroitGeofence:
    """
    Robust geo-fencing for Detroit region using official city boundaries and AV2 coordinate system.
    """
    def __init__(self):
        self.detroit_polygon_gps = [
            (-83.287, 42.255),
            (-83.287, 42.450),
            (-82.910, 42.450),
            (-82.910, 42.255),
            (-83.287, 42.255)
        ]
        self.detroit_polygon = Polygon(self.detroit_polygon_gps)
        self.detroit_city_code = CityName.DTW
        self.coordinate_cache = {}
    def coordinates_to_gps(self, x: float, y: float, z: float = 0.0) -> Optional[Tuple[float, float, float]]:
        cache_key = (x, y)
        if cache_key in self.coordinate_cache:
            lat, lon = self.coordinate_cache[cache_key]
            return lat, lon, z
        try:
            point_2d = np.array([[x, y]])
            lat_lon = convert_city_coords_to_wgs84(point_2d, self.detroit_city_code.value)[0]
            lat, lon = lat_lon[0], lat_lon[1]
            self.coordinate_cache[cache_key] = (lat, lon)
            if abs(lat) > 0.1 and abs(lon) > 0.1:
                return lat, lon, z
            else:
                return None
        except Exception as e:
            print(f"Warning: Failed to convert coordinates ({x}, {y}): {e}")
            return None
    def is_detroit_by_gps_polygon(self, x: float, y: float, z: float = 0.0) -> bool:
        gps_coords = self.coordinates_to_gps(x, y, z)
        if gps_coords is None:
            return False
        lat, lon, _ = gps_coords
        point = Point(lon, lat)
        return self.detroit_polygon.contains(point)
    def is_detroit_by_coordinate_ranges(self, x: float, y: float) -> bool:
        detroit_bounds = {'min_x': 3000, 'max_x': 13000, 'min_y': 2000, 'max_y': 7000}
        return (detroit_bounds['min_x'] <= x <= detroit_bounds['max_x'] and detroit_bounds['min_y'] <= y <= detroit_bounds['max_y'])
    def is_detroit_by_city_detection(self, x: float, y: float, z: float = 0.0) -> bool:
        try:
            point_2d = np.array([[x, y]])
            lat_lon = convert_city_coords_to_wgs84(point_2d, self.detroit_city_code.value)[0]
            if abs(lat_lon[0]) > 0.1 and abs(lat_lon[1]) > 0.1:
                lat, lon = lat_lon[0], lat_lon[1]
                michigan_bounds = {'min_lat': 41.5, 'max_lat': 43.0, 'min_lon': -84.5, 'max_lon': -82.0}
                return (michigan_bounds['min_lat'] <= lat <= michigan_bounds['max_lat'] and michigan_bounds['min_lon'] <= lon <= michigan_bounds['max_lon'])
            return False
        except Exception:
            return False
    def is_detroit_comprehensive(self, x: float, y: float, z: float = 0.0, method: str = "auto") -> bool:
        if method == "gps_polygon":
            return self.is_detroit_by_gps_polygon(x, y, z)
        elif method == "coordinate_ranges":
            return self.is_detroit_by_coordinate_ranges(x, y)
        elif method == "city_detection":
            return self.is_detroit_by_city_detection(x, y, z)
        else:
            city_detection = self.is_detroit_by_city_detection(x, y, z)
            if city_detection:
                return self.is_detroit_by_gps_polygon(x, y, z)
            return False
    def get_detection_info(self, x: float, y: float, z: float = 0.0) -> Dict[str, Any]:
        gps_coords = self.coordinates_to_gps(x, y, z)
        info = {
            'coordinates': {'x': x, 'y': y, 'z': z},
            'gps_conversion': gps_coords,
            'city_detection': self.is_detroit_by_city_detection(x, y, z),
            'coordinate_ranges': self.is_detroit_by_coordinate_ranges(x, y),
            'gps_polygon': False,
            'comprehensive_result': False
        }
        if gps_coords:
            info['gps_polygon'] = self.is_detroit_by_gps_polygon(x, y, z)
        info['comprehensive_result'] = self.is_detroit_comprehensive(x, y, z)
        return info
    def analyze_scenario_location(self, map_data: Dict[str, Any]) -> Dict[str, Any]:
        sample_coords = []
        detroit_detections = []
        if 'lane_segments' in map_data:
            for i, (lane_id, lane_data) in enumerate(map_data['lane_segments'].items()):
                if i >= 10:
                    break
                if 'left_lane_boundary' in lane_data and lane_data['left_lane_boundary']:
                    first_point = lane_data['left_lane_boundary'][0]
                    x, y, z = first_point['x'], first_point['y'], first_point['z']
                    sample_coords.append((x, y, z))
                    detroit_detections.append(self.is_detroit_comprehensive(x, y, z))
        detroit_count = sum(detroit_detections)
        total_samples = len(detroit_detections)
        is_detroit_scenario = (detroit_count / total_samples) > 0.5 if total_samples > 0 else False
        return {
            'is_detroit': is_detroit_scenario,
            'detroit_confidence': detroit_count / total_samples if total_samples > 0 else 0.0,
            'total_samples': total_samples,
            'detroit_samples': detroit_count,
            'sample_coordinates': sample_coords[:5],
            'sample_detections': detroit_detections[:5]
        }

class OptimizedKMLProcessor:
    """Optimized processor for handling large numbers of map files."""
    
    def __init__(self, detroit_only=False, include_lanes=True, include_drivable=False):
        self.detroit_only = detroit_only
        self.include_lanes = include_lanes
        self.include_drivable = include_drivable
        self.detroit_geofence = RobustDetroitGeofence() if detroit_only else None
        
        # Global coordinate cache for all workers
        self.global_coordinate_cache = {}
        
    def process_scenario_batch(self, scenario_batch: List[Tuple[str, str, Path]]) -> Dict[str, Any]:
        """Process a batch of scenarios and return KML features."""
        batch_features = []
        batch_stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'scenarios_processed': 0
        }
        
        for split, scenario_id, map_file_path in scenario_batch:
            try:
                with open(map_file_path, 'r') as f:
                    map_data = json.load(f)
                
                # Detroit filtering
                if self.detroit_only and self.detroit_geofence:
                    analysis = self.detroit_geofence.analyze_scenario_location(map_data)
                    if not analysis['is_detroit']:
                        continue
                
                # Process features
                scenario_features = self.extract_scenario_features(map_data, split, scenario_id)
                batch_features.extend(scenario_features)
                
                # Update stats
                for feature in scenario_features:
                    if 'lane_segment' in feature['name']:
                        batch_stats['lane_segments'] += 1
                    elif 'crossing' in feature['name']:
                        batch_stats['pedestrian_crossings'] += 1
                    elif 'drivable' in feature['name']:
                        batch_stats['drivable_areas'] += 1
                
                batch_stats['scenarios_processed'] += 1
                
            except Exception as e:
                continue
        
        return {
            'features': batch_features,
            'stats': batch_stats
        }
    
    def extract_scenario_features(self, map_data: Dict[str, Any], split: str, scenario_id: str) -> List[Dict[str, Any]]:
        """Extract KML features from a single scenario."""
        features = []
        
        # Process lane segments
        if self.include_lanes and 'lane_segments' in map_data:
            for lane_id, lane_data in map_data['lane_segments'].items():
                lane_type = lane_data.get('lane_type', 'VEHICLE')
                
                # Left boundary
                if 'left_lane_boundary' in lane_data:
                    left_coords = self.convert_polyline_to_gps(lane_data['left_lane_boundary'])
                    if len(left_coords) >= 2:
                        left_mark_type = lane_data.get('left_lane_mark_type', 'NONE')
                        features.append({
                            'type': 'linestring',
                            'name': f"{split}/{scenario_id[:8]}/L{lane_id}/left",
                            'coords': left_coords,
                            'color': self.get_lane_color(lane_type),
                            'width': self.get_lane_width(left_mark_type),
                            'description': f"Lane {lane_id} - {lane_type} - Left boundary ({left_mark_type})"
                        })
                
                # Right boundary
                if 'right_lane_boundary' in lane_data:
                    right_coords = self.convert_polyline_to_gps(lane_data['right_lane_boundary'])
                    if len(right_coords) >= 2:
                        right_mark_type = lane_data.get('right_lane_mark_type', 'NONE')
                        features.append({
                            'type': 'linestring',
                            'name': f"{split}/{scenario_id[:8]}/L{lane_id}/right",
                            'coords': right_coords,
                            'color': self.get_lane_color(lane_type),
                            'width': self.get_lane_width(right_mark_type),
                            'description': f"Lane {lane_id} - {lane_type} - Right boundary ({right_mark_type})"
                        })
        
        # Process pedestrian crossings
        if self.include_lanes and 'pedestrian_crossings' in map_data:
            for crossing_id, crossing_data in map_data['pedestrian_crossings'].items():
                for edge_name in ['edge1', 'edge2']:
                    if edge_name in crossing_data:
                        edge_coords = self.convert_polyline_to_gps(crossing_data[edge_name])
                        if len(edge_coords) >= 2:
                            features.append({
                                'type': 'linestring',
                                'name': f"{split}/{scenario_id[:8]}/C{crossing_id}/{edge_name}",
                                'coords': edge_coords,
                                'color': simplekml.Color.yellow,
                                'width': 4,
                                'description': f"Pedestrian crossing {crossing_id} - {edge_name}"
                            })
        
        # Process drivable areas
        if self.include_drivable and 'drivable_areas' in map_data:
            for area_id, area_data in map_data['drivable_areas'].items():
                if 'area_boundary' in area_data:
                    boundary_coords = self.convert_polyline_to_gps(area_data['area_boundary'])
                    if len(boundary_coords) >= 3:
                        features.append({
                            'type': 'polygon',
                            'name': f"{split}/{scenario_id[:8]}/A{area_id}",
                            'coords': boundary_coords,
                            'color': simplekml.Color.lightblue,
                            'description': f"Drivable area {area_id}"
                        })
        
        return features
    
    def convert_polyline_to_gps(self, polyline: List[Dict[str, float]]) -> List[Tuple[float, float, float]]:
        """Convert polyline coordinates to GPS with caching."""
        gps_coords = []
        for point in polyline:
            lat, lon, alt = self.get_gps_from_coords(point['x'], point['y'], point['z'])
            if lat != 0.0 or lon != 0.0:
                gps_coords.append((lon, lat, alt))
        return gps_coords
    
    def get_gps_from_coords(self, x: float, y: float, z: float) -> Tuple[float, float, float]:
        """Get GPS coordinates with global caching."""
        cache_key = (x, y)
        if cache_key in self.global_coordinate_cache:
            lat, lon = self.global_coordinate_cache[cache_key]
            return lat, lon, z
        
        if self.detroit_geofence:
            result = self.detroit_geofence.coordinates_to_gps(x, y, z)
            if result:
                lat, lon, _ = result
                self.global_coordinate_cache[cache_key] = (lat, lon)
                return lat, lon, z
        
        # Standard multi-city conversion
        try:
            city_names = [CityName.DTW, CityName.ATX, CityName.MIA, CityName.PAO, CityName.PIT, CityName.WDC]
            for city_name in city_names:
                try:
                    point_2d = np.array([[x, y]])
                    lat_lon = convert_city_coords_to_wgs84(point_2d, city_name)[0]
                    if abs(lat_lon[0]) > 0.1 and abs(lat_lon[1]) > 0.1:
                        lat, lon = lat_lon[0], lat_lon[1]
                        self.global_coordinate_cache[cache_key] = (lat, lon)
                        return lat, lon, z
                except:
                    continue
        except:
            pass
        
        return 0.0, 0.0, z
    
    def get_lane_color(self, lane_type: str) -> str:
        """Get color for lane type."""
        colors = {
            'VEHICLE': simplekml.Color.blue,
            'BIKE': simplekml.Color.green,
            'BUS': simplekml.Color.orange,
            'PEDESTRIAN': simplekml.Color.red
        }
        return colors.get(lane_type, simplekml.Color.blue)
    
    def get_lane_width(self, mark_type: str) -> int:
        """Get width for lane marking type."""
        widths = {
            'SOLID_WHITE': 3,
            'DASHED_WHITE': 2,
            'SOLID_YELLOW': 3,
            'DASHED_YELLOW': 2,
            'DOUBLE_SOLID_YELLOW': 4,
            'SOLID_DASH_YELLOW': 3,
            'DASH_SOLID_YELLOW': 3,
            'NONE': 1
        }
        return widths.get(mark_type, 1)

def process_scenario_batch_worker(args):
    """Worker function for parallel processing."""
    batch_data, detroit_only, include_lanes, include_drivable = args
    processor = OptimizedKMLProcessor(detroit_only, include_lanes, include_drivable)
    return processor.process_scenario_batch(batch_data)

class MotionForecastingSingleKMLGenerator:
    
    def __init__(self, base_dir=MOTION_FORECASTING_BASE, detroit_only=False, include_lanes=True, include_drivable=False, max_workers=None):
        self.base_dir = Path(base_dir)
        self.detroit_only = detroit_only
        self.include_lanes = include_lanes
        self.include_drivable = include_drivable
        self.max_workers = max_workers or min(mp.cpu_count(), 8)  # Limit to 8 workers max
        self.stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'scenarios_processed': 0
        }
        
        # Color schemes for different map elements
        self.colors = {
            'lane_segments': {
                'VEHICLE': simplekml.Color.blue,
                'BIKE': simplekml.Color.green,
                'BUS': simplekml.Color.orange,
                'PEDESTRIAN': simplekml.Color.red
            },
            'pedestrian_crossings': simplekml.Color.yellow,
            'drivable_areas': simplekml.Color.lightblue
        }
        
        # Line styles for different lane markings
        self.lane_mark_styles = {
            'SOLID_WHITE': {'color': simplekml.Color.white, 'width': 3},
            'DASHED_WHITE': {'color': simplekml.Color.white, 'width': 2},
            'SOLID_YELLOW': {'color': simplekml.Color.yellow, 'width': 3},
            'DASHED_YELLOW': {'color': simplekml.Color.yellow, 'width': 2},
            'DOUBLE_SOLID_YELLOW': {'color': simplekml.Color.yellow, 'width': 4},
            'SOLID_DASH_YELLOW': {'color': simplekml.Color.yellow, 'width': 3},
            'DASH_SOLID_YELLOW': {'color': simplekml.Color.yellow, 'width': 3},
            'NONE': {'color': simplekml.Color.gray, 'width': 1}
        }

    def create_split_kml(self, split: str) -> simplekml.Kml:
        """Create single KML file for a specific split using parallel processing."""
        split_dir = self.base_dir / split
        if not split_dir.exists():
            return None
        
        print(f"Processing {split} split with {self.max_workers} workers...")
        
        # Collect all scenario files
        scenario_files = []
        for scenario_dir in split_dir.iterdir():
            if not scenario_dir.is_dir():
                continue
            
            scenario_id = scenario_dir.name
            map_file = scenario_dir / f"log_map_archive_{scenario_id}.json"
            if map_file.exists():
                scenario_files.append((split, scenario_id, map_file))
        
        if not scenario_files:
            print(f"No map files found for {split} split")
            return None
        
        print(f"Found {len(scenario_files)} scenarios to process")
        
        # Create batches for parallel processing
        batch_size = max(1, len(scenario_files) // (self.max_workers * 4))  # 4 batches per worker
        batches = []
        for i in range(0, len(scenario_files), batch_size):
            batch = scenario_files[i:i + batch_size]
            batches.append(batch)
        
        print(f"Created {len(batches)} batches of ~{batch_size} scenarios each")
        
        # Process batches in parallel
        all_features = []
        total_stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'scenarios_processed': 0
        }
        
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(process_scenario_batch_worker, (batch, self.detroit_only, self.include_lanes, self.include_drivable)): batch 
                for batch in batches
            }
            
            # Process completed batches with progress bar
            with tqdm(total=len(batches), desc=f"Processing {split} batches") as pbar:
                for future in as_completed(future_to_batch):
                    try:
                        result = future.result()
                        all_features.extend(result['features'])
                        
                        # Update stats
                        for key in total_stats:
                            total_stats[key] += result['stats'][key]
                        
                        pbar.update(1)
                        
                        # Memory management
                        if len(all_features) > 10000:
                            gc.collect()
                            
                    except Exception as e:
                        print(f"Error processing batch: {e}")
                        pbar.update(1)
        
        processing_time = time.time() - start_time
        print(f"Processed {split} in {processing_time:.1f} seconds ({len(scenario_files)/processing_time:.1f} scenarios/sec)")
        
        # Create KML file
        region_name = "Detroit" if self.detroit_only else "All Regions"
        data_types = []
        if self.include_lanes:
            data_types.append("Lanes+Crossings")
        if self.include_drivable:
            data_types.append("Drivable Areas")
        data_type_str = "+".join(data_types)
        
        kml = simplekml.Kml()
        kml.name = f"Motion Forecasting HD Maps - {region_name} - {data_type_str} - {split.capitalize()}"
        kml.description = f"Motion Forecasting HD Maps for {split} split ({region_name}, {data_type_str})"
        
        # Add features to KML
        print(f"Adding {len(all_features)} features to KML...")
        for feature in all_features:
            if feature['type'] == 'linestring':
                line = kml.newlinestring(
                    name=feature['name'],
                    coords=feature['coords']
                )
                line.style.linestyle.color = feature['color']
                line.style.linestyle.width = feature['width']
                line.description = feature['description']
            elif feature['type'] == 'polygon':
                polygon = kml.newpolygon(
                    name=feature['name'],
                    outerboundaryis=feature['coords']
                )
                polygon.style.polystyle.color = feature['color']
                polygon.style.polystyle.outline = 1
                polygon.style.linestyle.color = simplekml.Color.darkblue
                polygon.style.linestyle.width = 2
                polygon.description = feature['description']
        
        # Update global stats
        for key in self.stats:
            self.stats[key] += total_stats[key]
        
        print(f"Completed {split}: {total_stats['scenarios_processed']} scenarios, {len(all_features)} features")
        return kml if len(all_features) > 0 else None

    def generate_all_kml_files(self):
        """Generate KML files for all splits using optimized processing."""
        print("Processing Motion Forecasting HD Map Files (Optimized)")
        print("=" * 60)
        print(f"Using {self.max_workers} parallel workers")
        print(f"Detroit filtering: {'Enabled' if self.detroit_only else 'Disabled'}")
        print(f"Include lanes: {'Yes' if self.include_lanes else 'No'}")
        print(f"Include drivable areas: {'Yes' if self.include_drivable else 'No'}")
        print("=" * 60)
        
        self.stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'scenarios_processed': 0
        }
        
        total_files_generated = 0
        total_start_time = time.time()
        
        for split in ['train', 'val', 'test']:
            split_kml = self.create_split_kml(split)
            
            if split_kml:
                region_suffix = "detroit" if self.detroit_only else "all_regions"
                data_types = []
                if self.include_lanes:
                    data_types.append("lanes")
                if self.include_drivable:
                    data_types.append("drivable")
                data_suffix = "_".join(data_types)
                
                filename = f"motion_forecasting_maps_{region_suffix}_{data_suffix}_{split}.kml"
                split_kml.save(filename)
                print(f"{split.capitalize()} KML saved: {filename} ({len(split_kml.features)} features)")
                total_files_generated += 1
            else:
                print(f"No data found for {split} split")
        
        total_time = time.time() - total_start_time
        print(f"\n" + "=" * 60)
        print(f"Processing complete in {total_time:.1f} seconds")
        print(f"   Scenarios processed: {self.stats['scenarios_processed']}")
        if self.include_lanes:
            print(f"   Lane segments: {self.stats['lane_segments']}")
            print(f"   Pedestrian crossings: {self.stats['pedestrian_crossings']}")
        if self.include_drivable:
            print(f"   Drivable areas: {self.stats['drivable_areas']}")
        print(f"   KML files generated: {total_files_generated}")
        print(f"   Average processing rate: {self.stats['scenarios_processed']/total_time:.1f} scenarios/sec")

def main():
    """Main function."""
    
    parser = argparse.ArgumentParser(description="Motion Forecasting HD Maps Single KML Generator (Optimized)")
    parser.add_argument("--detroit-only", action="store_true", 
                       help="Only process Detroit region scenarios (default: all regions)")
    parser.add_argument("--mode", choices=["lanes", "drivable"], default="lanes",
                       help="Select which map elements to include: 'lanes' for lane segments and pedestrian crossings, 'drivable' for drivable areas only (default: lanes)")
    parser.add_argument("--workers", type=int, default=None,
                       help="Number of parallel workers (default: auto-detect, max 8)")
    
    args = parser.parse_args()
    
    # Set inclusion flags based on mode
    if args.mode == "lanes":
        include_lanes = True
        include_drivable = False
    elif args.mode == "drivable":
        include_lanes = False
        include_drivable = True
    else:
        include_lanes = True
        include_drivable = False
    
    region_str = "Detroit region" if args.detroit_only else "all regions"
    data_types = []
    if include_lanes:
        data_types.append("lane segments + pedestrian crossings")
    if include_drivable:
        data_types.append("drivable areas")
    data_str = " + ".join(data_types)
    
    print("Motion Forecasting HD Maps Single KML Generator (Optimized)")
    print(f"Processing: {region_str}")
    print(f"Including: {data_str}")
    print(f"Workers: {args.workers or 'auto-detect'}")
    print("=" * 60)
    
    if not os.path.exists(MOTION_FORECASTING_BASE):
        print(f"Error: Motion Forecasting directory '{MOTION_FORECASTING_BASE}' not found!")
        print("Please run the download script first.")
        return
    
    generator = MotionForecastingSingleKMLGenerator(
        MOTION_FORECASTING_BASE, 
        detroit_only=args.detroit_only,
        include_lanes=include_lanes,
        include_drivable=include_drivable,
        max_workers=args.workers
    )
    
    generator.generate_all_kml_files()
    
    if generator.stats['scenarios_processed'] == 0:
        print("\nError: No map files were processed!")
        return
    
    print("\nKML generation complete.")

if __name__ == "__main__":
    main() 