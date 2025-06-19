#!/usr/bin/env python3
"""
Generate Detroit HD Maps KML

Convert Detroit HD map data (lane segments, drivable areas, pedestrian crossings)
into KML format for visualization in Google Earth or other mapping applications.
"""

import json
import os
import simplekml
from pathlib import Path
from av2.geometry.utm import convert_city_coords_to_wgs84
import numpy as np
from typing import List, Dict, Any, Tuple

DETROIT_CITY_NAME = "DTW"
LOGS_BASE = "detroit_logs"  # Use existing detroit_logs directory

class DetroitHDMapKMLGenerator:
    def __init__(self, base_dir=LOGS_BASE):
        self.base_dir = Path(base_dir)
        self.stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'logs_processed': 0
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
    
    def get_gps_from_city_coords(self, x: float, y: float, z: float) -> Tuple[float, float, float]:
        """Convert city coordinates to GPS coordinates."""
        point_2d = np.array([[x, y]])
        lat_lon = convert_city_coords_to_wgs84(point_2d, DETROIT_CITY_NAME)[0]
        return lat_lon[0], lat_lon[1], z
    
    def convert_polyline_to_gps(self, polyline: List[Dict[str, float]]) -> List[Tuple[float, float, float]]:
        """Convert a polyline from city coordinates to GPS coordinates."""
        gps_coords = []
        for point in polyline:
            lat, lon, alt = self.get_gps_from_city_coords(point['x'], point['y'], point['z'])
            gps_coords.append((lon, lat, alt))  # KML expects (lon, lat, alt)
        return gps_coords
    
    def add_lane_segments(self, kml: simplekml.Kml, lane_segments: Dict[str, Any], split: str, log_id: str) -> int:
        """Add lane segments to the KML. Returns number of features added."""
        features_added = 0
        for lane_id, lane_data in lane_segments.items():
            try:
                # Get lane type and color
                lane_type = lane_data.get('lane_type', 'VEHICLE')
                color = self.colors['lane_segments'].get(lane_type, simplekml.Color.blue)
                
                # Add left lane boundary
                if 'left_lane_boundary' in lane_data:
                    left_coords = self.convert_polyline_to_gps(lane_data['left_lane_boundary'])
                    if len(left_coords) >= 2:
                        left_mark_type = lane_data.get('left_lane_mark_type', 'NONE')
                        style = self.lane_mark_styles.get(left_mark_type, self.lane_mark_styles['NONE'])
                        
                        left_line = kml.newlinestring(
                            name=f"{split}/{log_id[:8]}/L{lane_id}/left",
                            coords=left_coords
                        )
                        left_line.style.linestyle.color = style['color']
                        left_line.style.linestyle.width = style['width']
                        left_line.description = f"Lane {lane_id} - {lane_type} - Left boundary ({left_mark_type})"
                        features_added += 1
                
                # Add right lane boundary
                if 'right_lane_boundary' in lane_data:
                    right_coords = self.convert_polyline_to_gps(lane_data['right_lane_boundary'])
                    if len(right_coords) >= 2:
                        right_mark_type = lane_data.get('right_lane_mark_type', 'NONE')
                        style = self.lane_mark_styles.get(right_mark_type, self.lane_mark_styles['NONE'])
                        
                        right_line = kml.newlinestring(
                            name=f"{split}/{log_id[:8]}/L{lane_id}/right",
                            coords=right_coords
                        )
                        right_line.style.linestyle.color = style['color']
                        right_line.style.linestyle.width = style['width']
                        right_line.description = f"Lane {lane_id} - {lane_type} - Right boundary ({right_mark_type})"
                        features_added += 1
                
                self.stats['lane_segments'] += 1
                
            except Exception as e:
                print(f"      ⚠️  Error processing lane {lane_id}: {e}")
        
        return features_added
    
    def add_pedestrian_crossings(self, kml: simplekml.Kml, pedestrian_crossings: Dict[str, Any], split: str, log_id: str) -> int:
        """Add pedestrian crossings to the KML. Returns number of features added."""
        features_added = 0
        for crossing_id, crossing_data in pedestrian_crossings.items():
            try:
                # Add edge1
                if 'edge1' in crossing_data:
                    edge1_coords = self.convert_polyline_to_gps(crossing_data['edge1'])
                    if len(edge1_coords) >= 2:
                        edge1_line = kml.newlinestring(
                            name=f"{split}/{log_id[:8]}/C{crossing_id}/edge1",
                            coords=edge1_coords
                        )
                        edge1_line.style.linestyle.color = self.colors['pedestrian_crossings']
                        edge1_line.style.linestyle.width = 4
                        edge1_line.description = f"Pedestrian crossing {crossing_id} - Edge 1"
                        features_added += 1
                
                # Add edge2
                if 'edge2' in crossing_data:
                    edge2_coords = self.convert_polyline_to_gps(crossing_data['edge2'])
                    if len(edge2_coords) >= 2:
                        edge2_line = kml.newlinestring(
                            name=f"{split}/{log_id[:8]}/C{crossing_id}/edge2",
                            coords=edge2_coords
                        )
                        edge2_line.style.linestyle.color = self.colors['pedestrian_crossings']
                        edge2_line.style.linestyle.width = 4
                        edge2_line.description = f"Pedestrian crossing {crossing_id} - Edge 2"
                        features_added += 1
                
                self.stats['pedestrian_crossings'] += 1
                
            except Exception as e:
                print(f"      ⚠️  Error processing crossing {crossing_id}: {e}")
        
        return features_added
    
    def add_drivable_areas(self, kml: simplekml.Kml, drivable_areas: Dict[str, Any], split: str, log_id: str) -> int:
        """Add drivable areas to the KML. Returns number of features added."""
        features_added = 0
        for area_id, area_data in drivable_areas.items():
            try:
                if 'area_boundary' in area_data:
                    boundary_coords = self.convert_polyline_to_gps(area_data['area_boundary'])
                    if len(boundary_coords) >= 3:  # Need at least 3 points for a polygon
                        # Create polygon
                        polygon = kml.newpolygon(
                            name=f"{split}/{log_id[:8]}/A{area_id}",
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
                print(f"      ⚠️  Error processing drivable area {area_id}: {e}")
        
        return features_added
    
    def process_map_file(self, kml: simplekml.Kml, map_file_path: Path, split: str, log_id: str) -> int:
        """Process a single map file and add its elements to the KML. Returns number of features added."""
        features_added = 0
        try:
            with open(map_file_path, 'r') as f:
                map_data = json.load(f)
            
            # Process lane segments
            if 'lane_segments' in map_data:
                print(f"      🛣️  Processing {len(map_data['lane_segments'])} lane segments...")
                features_added += self.add_lane_segments(kml, map_data['lane_segments'], split, log_id)
            
            # Process pedestrian crossings
            if 'pedestrian_crossings' in map_data:
                print(f"      🚶 Processing {len(map_data['pedestrian_crossings'])} pedestrian crossings...")
                features_added += self.add_pedestrian_crossings(kml, map_data['pedestrian_crossings'], split, log_id)
            
            # Process drivable areas
            if 'drivable_areas' in map_data:
                print(f"      🚗 Processing {len(map_data['drivable_areas'])} drivable areas...")
                features_added += self.add_drivable_areas(kml, map_data['drivable_areas'], split, log_id)
            
            self.stats['logs_processed'] += 1
            
        except Exception as e:
            print(f"      ❌ Error processing map file {map_file_path}: {e}")
        
        return features_added
    
    def create_split_kml(self, split: str) -> List[simplekml.Kml]:
        """Create KML files for a specific split, splitting into chunks if needed."""
        split_dir = self.base_dir / split
        if not split_dir.exists():
            return []
        
        print(f"\n📁 Processing {split} split...")
        
        # Create first KML chunk
        kml_chunks = [simplekml.Kml()]
        kml_chunks[0].name = f"Detroit HD Maps - {split.capitalize()} - Part 1"
        kml_chunks[0].description = f"Detroit HD Maps for {split} split (Part 1)"
        
        current_chunk = 0
        feature_count = 0
        max_features_per_chunk = 9500  # Leave some buffer below 10,000
        
        for log_dir in split_dir.iterdir():
            if not log_dir.is_dir():
                continue
            
            log_id = log_dir.name
            map_dir = log_dir / 'map'
            
            if not map_dir.exists():
                continue
            
            # Find vector map files (JSON files with DTW in name)
            for map_file in map_dir.glob("log_map_archive_*____DTW_city_*.json"):
                print(f"   📄 Processing {split}/{log_id[:8]} - {map_file.name}")
                
                # Check if we need a new chunk
                if feature_count >= max_features_per_chunk:
                    current_chunk += 1
                    kml_chunks.append(simplekml.Kml())
                    kml_chunks[current_chunk].name = f"Detroit HD Maps - {split.capitalize()} - Part {current_chunk + 1}"
                    kml_chunks[current_chunk].description = f"Detroit HD Maps for {split} split (Part {current_chunk + 1})"
                    feature_count = 0
                    print(f"      📦 Starting new chunk {current_chunk + 1}...")
                
                # Process the file and count features added
                features_added = self.process_map_file(kml_chunks[current_chunk], map_file, split, log_id)
                feature_count += features_added
        
        return kml_chunks
    
    def generate_all_kml_files(self):
        """Generate KML files for all splits."""
        print("🗺️  Processing Detroit HD Map Files")
        print("=" * 50)
        
        # Reset stats
        self.stats = {
            'lane_segments': 0,
            'pedestrian_crossings': 0,
            'drivable_areas': 0,
            'logs_processed': 0
        }
        
        # Create KML for each split
        for split in ['train', 'val', 'test']:
            split_kml_chunks = self.create_split_kml(split)
            
            for kml_chunk in split_kml_chunks:
                if kml_chunk.features:
                    split_filename = f"detroit_hd_maps_{split}_part_{split_kml_chunks.index(kml_chunk) + 1}.kml"
                    kml_chunk.save(split_filename)
                    print(f"🗺️  {split.capitalize()} KML saved: {split_filename} ({len(kml_chunk.features)} features)")
                else:
                    print(f"⚠️  No data found for {split} split")
        
        # Create a summary KML with sample data
        self.create_summary_kml()
        
        print(f"\n📊 FINAL SUMMARY:")
        print(f"   Logs processed: {self.stats['logs_processed']}")
        print(f"   Lane segments: {self.stats['lane_segments']}")
        print(f"   Pedestrian crossings: {self.stats['pedestrian_crossings']}")
        print(f"   Drivable areas: {self.stats['drivable_areas']}")
    
    def create_summary_kml(self):
        """Create a summary KML with representative samples."""
        summary_kml = simplekml.Kml()
        summary_kml.name = "Detroit HD Maps - Summary"
        summary_kml.description = f"""
        Detroit HD Maps Summary
        Generated from {self.stats['logs_processed']} logs
        
        Statistics:
        - Lane segments: {self.stats['lane_segments']}
        - Pedestrian crossings: {self.stats['pedestrian_crossings']}
        - Drivable areas: {self.stats['drivable_areas']}
        
        Color Legend:
        - Blue lines: Vehicle lanes
        - Green lines: Bike lanes
        - Orange lines: Bus lanes
        - Yellow lines: Pedestrian crossings
        - Light blue polygons: Drivable areas
        """
        
        # Process a few sample files from each split
        sample_count = 0
        for split in ['train', 'val', 'test']:
            split_dir = self.base_dir / split
            if not split_dir.exists():
                continue
            
            # Take first 2 logs from each split
            for log_dir in list(split_dir.iterdir())[:2]:
                if not log_dir.is_dir():
                    continue
                
                log_id = log_dir.name
                map_dir = log_dir / 'map'
                
                if not map_dir.exists():
                    continue
                
                for map_file in map_dir.glob("log_map_archive_*____DTW_city_*.json"):
                    if sample_count >= 5:  # Limit to 5 sample files
                        break
                    
                    print(f"   📄 Adding sample: {split}/{log_id[:8]} - {map_file.name}")
                    self.process_map_file(summary_kml, map_file, split, log_id)
                    sample_count += 1
                
                if sample_count >= 5:
                    break
        
        summary_filename = "detroit_hd_maps_summary.kml"
        summary_kml.save(summary_filename)
        print(f"🗺️  Summary KML saved: {summary_filename}")

def main():
    """Main function."""
    
    print("🗺️  Detroit HD Maps KML Generator")
    print("Convert HD map data to KML format for visualization")
    print("=" * 60)
    
    # Check if detroit_logs directory exists
    if not os.path.exists(LOGS_BASE):
        print(f"❌ Detroit logs directory '{LOGS_BASE}' not found!")
        print("💡 Please run the download script first:")
        print("   python download_all_detroit_logs.py")
        return
    
    # Initialize generator
    generator = DetroitHDMapKMLGenerator(LOGS_BASE)
    
    # Generate all KML files
    generator.generate_all_kml_files()
    
    if generator.stats['logs_processed'] == 0:
        print("\n❌ No map files were processed!")
        print("💡 Make sure you have downloaded Detroit log data first.")
        return
    
    # Final summary
    print(f"\n" + "="*60)
    print("🎉 KML GENERATION COMPLETE")
    print("="*60)
    print(f"✅ Successfully processed {generator.stats['logs_processed']} logs")
    print(f"🛣️  Generated {generator.stats['lane_segments']} lane segments")
    print(f"🚶 Generated {generator.stats['pedestrian_crossings']} pedestrian crossings")
    print(f"🚗 Generated {generator.stats['drivable_areas']} drivable areas")
    
    print(f"\n📁 Generated KML files:")
    print(f"   - detroit_hd_maps_train_part_1.kml (training data)")
    print(f"   - detroit_hd_maps_val_part_1.kml (validation data)")
    print(f"   - detroit_hd_maps_test_part_1.kml (test data)")
    print(f"   - detroit_hd_maps_summary.kml (representative sample)")
    
    print(f"\n🗺️  Open these files in Google Earth or other KML viewers to visualize the HD maps!")

if __name__ == "__main__":
    main() 