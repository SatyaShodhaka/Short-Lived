#!/usr/bin/env python3
"""
Generate Motion Forecasting HD Maps Single KML

Convert Motion Forecasting HD map data into single KML files per split.
Supports filtering by region and data types.

Updated with robust Detroit geo-fencing using official AV2 coordinate system.
"""

import json
import os
import simplekml
import argparse
from pathlib import Path
from av2.geometry.utm import convert_city_coords_to_wgs84, CityName
import numpy as np
from typing import List, Dict, Any, Tuple
from robust_detroit_geofencing import RobustDetroitGeofence

MOTION_FORECASTING_BASE = "motion_forecasting"

class MotionForecastingSingleKMLGenerator:
    
    def __init__(self, base_dir=MOTION_FORECASTING_BASE, detroit_only=False, include_lanes=True, include_drivable=False):
        self.base_dir = Path(base_dir)
        self.detroit_only = detroit_only
        self.include_lanes = include_lanes
        self.include_drivable = include_drivable
        self.stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'scenarios_processed': 0
        }
        
        # Initialize robust Detroit geo-fencing
        self.detroit_geofence = RobustDetroitGeofence() if detroit_only else None
        
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
    
    def is_detroit_coordinates(self, x: float, y: float) -> bool:
        """Check if coordinates are in Detroit region using robust geo-fencing."""
        if self.detroit_geofence:
            return self.detroit_geofence.is_detroit_comprehensive(x, y)
        else:
            # No Detroit filtering enabled
            return True
    
    def get_gps_from_coords(self, x: float, y: float, z: float) -> Tuple[float, float, float]:
        """Convert coordinates to GPS coordinates using robust approach."""
        if self.detroit_geofence:
            # Use robust geo-fencing coordinate conversion
            result = self.detroit_geofence.coordinates_to_gps(x, y, z)
            if result:
                return result
        
        # Standard multi-city coordinate conversion
        try:
            city_names = [CityName.DTW, CityName.ATX, CityName.MIA, CityName.PAO, CityName.PIT, CityName.WDC]
            
            for city_name in city_names:
                try:
                    point_2d = np.array([[x, y]])
                    lat_lon = convert_city_coords_to_wgs84(point_2d, city_name)[0]
                    if abs(lat_lon[0]) > 0.1 and abs(lat_lon[1]) > 0.1:
                        return lat_lon[0], lat_lon[1], z
                except:
                    continue
            
            return 0.0, 0.0, z
        except:
            return 0.0, 0.0, z
    
    def convert_polyline_to_gps(self, polyline: List[Dict[str, float]]) -> List[Tuple[float, float, float]]:
        """Convert a polyline from coordinates to GPS coordinates."""
        gps_coords = []
        for point in polyline:
            lat, lon, alt = self.get_gps_from_coords(point['x'], point['y'], point['z'])
            if lat != 0.0 or lon != 0.0:
                gps_coords.append((lon, lat, alt))
        return gps_coords
    
    def add_lane_segments(self, kml: simplekml.Kml, lane_segments: Dict[str, Any], split: str, scenario_id: str) -> int:
        """Add lane segments to the KML."""
        features_added = 0
        for lane_id, lane_data in lane_segments.items():
            try:
                lane_type = lane_data.get('lane_type', 'VEHICLE')
                
                if 'left_lane_boundary' in lane_data:
                    left_coords = self.convert_polyline_to_gps(lane_data['left_lane_boundary'])
                    if len(left_coords) >= 2:
                        left_mark_type = lane_data.get('left_lane_mark_type', 'NONE')
                        style = self.lane_mark_styles.get(left_mark_type, self.lane_mark_styles['NONE'])
                        
                        left_line = kml.newlinestring(
                            name=f"{split}/{scenario_id[:8]}/L{lane_id}/left",
                            coords=left_coords
                        )
                        left_line.style.linestyle.color = style['color']
                        left_line.style.linestyle.width = style['width']
                        left_line.description = f"Lane {lane_id} - {lane_type} - Left boundary ({left_mark_type})"
                        features_added += 1
                
                if 'right_lane_boundary' in lane_data:
                    right_coords = self.convert_polyline_to_gps(lane_data['right_lane_boundary'])
                    if len(right_coords) >= 2:
                        right_mark_type = lane_data.get('right_lane_mark_type', 'NONE')
                        style = self.lane_mark_styles.get(right_mark_type, self.lane_mark_styles['NONE'])
                        
                        right_line = kml.newlinestring(
                            name=f"{split}/{scenario_id[:8]}/L{lane_id}/right",
                            coords=right_coords
                        )
                        right_line.style.linestyle.color = style['color']
                        right_line.style.linestyle.width = style['width']
                        right_line.description = f"Lane {lane_id} - {lane_type} - Right boundary ({right_mark_type})"
                        features_added += 1
                
                self.stats['lane_segments'] += 1
                
            except Exception as e:
                continue
        
        return features_added
    
    def add_pedestrian_crossings(self, kml: simplekml.Kml, pedestrian_crossings: Dict[str, Any], split: str, scenario_id: str) -> int:
        """Add pedestrian crossings to the KML."""
        features_added = 0
        for crossing_id, crossing_data in pedestrian_crossings.items():
            try:
                if 'edge1' in crossing_data:
                    edge1_coords = self.convert_polyline_to_gps(crossing_data['edge1'])
                    if len(edge1_coords) >= 2:
                        edge1_line = kml.newlinestring(
                            name=f"{split}/{scenario_id[:8]}/C{crossing_id}/edge1",
                            coords=edge1_coords
                        )
                        edge1_line.style.linestyle.color = self.colors['pedestrian_crossings']
                        edge1_line.style.linestyle.width = 4
                        edge1_line.description = f"Pedestrian crossing {crossing_id} - Edge 1"
                        features_added += 1
                
                if 'edge2' in crossing_data:
                    edge2_coords = self.convert_polyline_to_gps(crossing_data['edge2'])
                    if len(edge2_coords) >= 2:
                        edge2_line = kml.newlinestring(
                            name=f"{split}/{scenario_id[:8]}/C{crossing_id}/edge2",
                            coords=edge2_coords
                        )
                        edge2_line.style.linestyle.color = self.colors['pedestrian_crossings']
                        edge2_line.style.linestyle.width = 4
                        edge2_line.description = f"Pedestrian crossing {crossing_id} - Edge 2"
                        features_added += 1
                
                self.stats['pedestrian_crossings'] += 1
                
            except Exception as e:
                continue
        
        return features_added
    
    def add_drivable_areas(self, kml: simplekml.Kml, drivable_areas: Dict[str, Any], split: str, scenario_id: str) -> int:
        """Add drivable areas to the KML."""
        features_added = 0
        for area_id, area_data in drivable_areas.items():
            try:
                if 'area_boundary' in area_data:
                    boundary_coords = self.convert_polyline_to_gps(area_data['area_boundary'])
                    if len(boundary_coords) >= 3:
                        polygon = kml.newpolygon(
                            name=f"{split}/{scenario_id[:8]}/A{area_id}",
                            outerboundaryis=boundary_coords
                        )
                        polygon.style.polystyle.color = self.colors['drivable_areas']
                        polygon.style.polystyle.outline = 1
                        polygon.style.linestyle.color = simplekml.Color.darkblue
                        polygon.style.linestyle.width = 2
                        polygon.description = f"Drivable area {area_id}"
                        features_added += 1
                        
                        self.stats['drivable_areas'] += 1
                
            except Exception as e:
                continue
        
        return features_added
    
    def process_map_file(self, kml: simplekml.Kml, map_file_path: Path, split: str, scenario_id: str) -> int:
        """Process a single map file and add its elements to the KML."""
        features_added = 0
        
        try:
            with open(map_file_path, 'r') as f:
                map_data = json.load(f)
            
            # Check if Detroit filtering is enabled
            if self.detroit_only and self.detroit_geofence:
                # Use robust geo-fencing to analyze scenario location
                analysis = self.detroit_geofence.analyze_scenario_location(map_data)
                
                if not analysis['is_detroit']:
                    return 0
            
            # Process lane segments and pedestrian crossings together
            if self.include_lanes:
                if 'lane_segments' in map_data:
                    features_added += self.add_lane_segments(kml, map_data['lane_segments'], split, scenario_id)
                
                if 'pedestrian_crossings' in map_data:
                    features_added += self.add_pedestrian_crossings(kml, map_data['pedestrian_crossings'], split, scenario_id)
            
            # Process drivable areas separately
            if self.include_drivable:
                if 'drivable_areas' in map_data:
                    features_added += self.add_drivable_areas(kml, map_data['drivable_areas'], split, scenario_id)
            
            self.stats['scenarios_processed'] += 1
            
        except Exception as e:
            pass
        
        return features_added
    
    def create_split_kml(self, split: str) -> simplekml.Kml:
        """Create single KML file for a specific split."""
        split_dir = self.base_dir / split
        if not split_dir.exists():
            return None
        
        print(f"Processing {split} split...")
        
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
        
        total_features = 0
        scenario_count = 0
        
        for scenario_dir in split_dir.iterdir():
            if not scenario_dir.is_dir():
                continue
            
            scenario_id = scenario_dir.name
            map_file = scenario_dir / f"log_map_archive_{scenario_id}.json"
            if not map_file.exists():
                continue
            
            scenario_count += 1
            if scenario_count % 100 == 0:
                print(f"   Processed {scenario_count} scenarios...")
            
            features_added = self.process_map_file(kml, map_file, split, scenario_id)
            total_features += features_added
        
        print(f"   Completed {split}: {scenario_count} scenarios, {total_features} features")
        return kml if total_features > 0 else None
    
    def generate_all_kml_files(self):
        """Generate KML files for all splits."""
        print("Processing Motion Forecasting HD Map Files")
        print("=" * 50)
        
        self.stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'scenarios_processed': 0
        }
        
        total_files_generated = 0
        
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
        
        print(f"\nProcessing complete:")
        print(f"   Scenarios processed: {self.stats['scenarios_processed']}")
        if self.include_lanes:
            print(f"   Lane segments: {self.stats['lane_segments']}")
            print(f"   Pedestrian crossings: {self.stats['pedestrian_crossings']}")
        if self.include_drivable:
            print(f"   Drivable areas: {self.stats['drivable_areas']}")
        print(f"   KML files generated: {total_files_generated}")

def main():
    """Main function."""
    
    parser = argparse.ArgumentParser(description="Motion Forecasting HD Maps Single KML Generator")
    parser.add_argument("--detroit-only", action="store_true", 
                       help="Only process Detroit region scenarios (default: all regions)")
    parser.add_argument("--include-drivable", action="store_true",
                       help="Include drivable areas (default: lanes+crossings only)")
    parser.add_argument("--lanes-only", action="store_true",
                       help="Only include lane segments and pedestrian crossings")
    
    args = parser.parse_args()
    
    include_lanes = not args.lanes_only or True
    include_drivable = args.include_drivable
    
    if not args.include_drivable and not args.lanes_only:
        include_lanes = True
        include_drivable = False
    
    region_str = "Detroit region" if args.detroit_only else "all regions"
    data_types = []
    if include_lanes:
        data_types.append("lane segments + pedestrian crossings")
    if include_drivable:
        data_types.append("drivable areas")
    data_str = " + ".join(data_types)
    
    print("Motion Forecasting HD Maps Single KML Generator")
    print(f"Processing: {region_str}")
    print(f"Including: {data_str}")
    print("=" * 60)
    
    if not os.path.exists(MOTION_FORECASTING_BASE):
        print(f"Error: Motion Forecasting directory '{MOTION_FORECASTING_BASE}' not found!")
        print("Please run the download script first.")
        return
    
    generator = MotionForecastingSingleKMLGenerator(
        MOTION_FORECASTING_BASE, 
        detroit_only=args.detroit_only,
        include_lanes=include_lanes,
        include_drivable=include_drivable
    )
    
    generator.generate_all_kml_files()
    
    if generator.stats['scenarios_processed'] == 0:
        print("\nError: No map files were processed!")
        return
    
    print("\nKML generation complete.")

if __name__ == "__main__":
    main() 