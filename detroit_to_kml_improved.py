#!/usr/bin/env python3
"""
Improved Detroit Drivable Areas to KML

Better coordinate transformation based on actual data analysis.
"""

import json
import simplekml
from pathlib import Path
import math

def analyze_coordinate_statistics(map_file):
    """Analyze the coordinate statistics to understand the data better."""
    
    with open(map_file, 'r') as f:
        map_data = json.load(f)
    
    drivable_areas = map_data.get('drivable_areas', {})
    
    all_x = []
    all_y = []
    
    for area_data in drivable_areas.values():
        boundary_points = area_data.get('area_boundary', [])
        for point in boundary_points:
            all_x.append(point['x'])
            all_y.append(point['y'])
    
    stats = {
        'x_min': min(all_x), 'x_max': max(all_x), 'x_center': sum(all_x) / len(all_x),
        'y_min': min(all_y), 'y_max': max(all_y), 'y_center': sum(all_y) / len(all_y),
        'x_span': max(all_x) - min(all_x),
        'y_span': max(all_y) - min(all_y)
    }
    
    print(f"📊 Coordinate Statistics:")
    print(f"   X: {stats['x_min']:.1f} to {stats['x_max']:.1f} (center: {stats['x_center']:.1f}, span: {stats['x_span']:.1f}m)")
    print(f"   Y: {stats['y_min']:.1f} to {stats['y_max']:.1f} (center: {stats['y_center']:.1f}, span: {stats['y_span']:.1f}m)")
    
    return stats

def coordinate_transform_improved(x, y, stats):
    """
    Improved coordinate transformation using actual data statistics.
    
    Strategy: Map the center of our data to a known Detroit location,
    then scale appropriately.
    """
    
    # Known Detroit locations for reference:
    # Downtown Detroit: ~42.3314° N, 83.0458° W
    # Let's assume our data center corresponds to a Detroit area
    
    # Use the actual center of our data as the reference point
    local_center_x = stats['x_center']  # ~10125m
    local_center_y = stats['y_center']  # ~3540m
    
    # Map to a reasonable Detroit location (adjust as needed)
    # This is still approximate but more realistic
    reference_lat = 42.3314  # Detroit downtown area
    reference_lon = -83.0458
    
    # Scale factors (meters to degrees)
    lat_scale = 1.0 / 111320  # ~111.32 km per degree latitude
    lon_scale = 1.0 / (111320 * math.cos(math.radians(reference_lat)))
    
    # Transform relative to the center
    dx = x - local_center_x  # Offset from center in meters
    dy = y - local_center_y  # Offset from center in meters
    
    # Convert to lat/lon
    lat = reference_lat + dy * lat_scale
    lon = reference_lon + dx * lon_scale
    
    return lat, lon

def create_detroit_kml_improved(map_file, output_file="detroit_drivable_areas_improved.kml"):
    """Create improved KML file with better coordinate transformation."""
    
    print(f" Creating improved KML from Detroit data...")
    
    if not Path(map_file).exists():
        print(f"Map file not found: {map_file}")
        return None
    
    # First, analyze the coordinate statistics
    stats = analyze_coordinate_statistics(map_file)
    
    # Load Detroit map data
    with open(map_file, 'r') as f:
        map_data = json.load(f)
    
    drivable_areas = map_data.get('drivable_areas', {})
    
    if not drivable_areas:
        print("No drivable areas found in map data")
        return None
    
    print(f"📊 Processing {len(drivable_areas)} drivable areas with improved transformation...")
    
    # Create KML document
    kml = simplekml.Kml()
    kml.document.name = "Detroit Drivable Areas - Improved Transform"
    kml.document.description = f"""
    Detroit drivable areas with improved coordinate transformation.
    
    Dataset: Argoverse 2 Sensor Dataset
    Log ID: 14896a70-a440-34d0-b68e-fd9882557da6
    City: Detroit (DTW)
    
    Coordinate Transform: Improved mapping based on data center
    Local Center: ({stats['x_center']:.1f}, {stats['y_center']:.1f})
    Data Span: {stats['x_span']:.1f}m × {stats['y_span']:.1f}m
    """
    
    # Create folder for drivable areas
    folder = kml.newfolder(name="Drivable Areas")
    
    # Process each drivable area
    for i, (area_id, area_data) in enumerate(drivable_areas.items()):
        boundary_points = area_data.get('area_boundary', [])
        
        if len(boundary_points) < 3:
            continue
        
        print(f"   Processing area {i+1}/{len(drivable_areas)} (ID: {area_id}, {len(boundary_points)} points)")
        
        # Convert coordinates using improved transformation
        coords = []
        for point in boundary_points:
            lat, lon = coordinate_transform_improved(point['x'], point['y'], stats)
            coords.append((lon, lat, 0))
        
        # Close the polygon
        coords.append(coords[0])
        
        # Create polygon
        pol = folder.newpolygon(name=f"Area {area_id}")
        pol.outerboundaryis = coords
        
        # Style - make them more visible
        pol.style.polystyle.color = simplekml.Color.changealphaint(150, simplekml.Color.blue)
        pol.style.polystyle.outline = 1
        pol.style.linestyle.color = simplekml.Color.red
        pol.style.linestyle.width = 3
        
        # Add description with coordinate info
        center_x = sum(pt['x'] for pt in boundary_points) / len(boundary_points)
        center_y = sum(pt['y'] for pt in boundary_points) / len(boundary_points)
        center_lat, center_lon = coordinate_transform_improved(center_x, center_y, stats)
        
        pol.description = f"""
        Area ID: {area_id}
        Points: {len(boundary_points)}
        Local Center: ({center_x:.1f}, {center_y:.1f})
        Transformed: ({center_lat:.6f}, {center_lon:.6f})
        """
    
    # Add reference points for known Detroit locations
    ref_folder = kml.newfolder(name="Reference Points")
    
    # Detroit landmarks for reference
    landmarks = [
        ("Detroit Downtown", 42.3314, -83.0458),
        ("Detroit Metro Airport", 42.2124, -83.3534),
        ("Belle Isle", 42.3401, -82.9851),
        ("Ford Field", 42.3400, -83.0456)
    ]
    
    for name, lat, lon in landmarks:
        pnt = ref_folder.newpoint(name=name)
        pnt.coords = [(lon, lat)]
        pnt.style.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/pushpin/grn-pushpin.png'
        pnt.style.iconstyle.scale = 1.2
    
    # Save KML file
    kml.save(output_file)
    print(f"✅ Improved KML file created: {output_file}")
    
    return output_file

def create_multiple_transformation_attempts(map_file):
    """Create multiple KML files with different transformation approaches."""
    
    print(f"\nCreating multiple transformation attempts...")
    
    stats = analyze_coordinate_statistics(map_file)
    
    with open(map_file, 'r') as f:
        map_data = json.load(f)
    
    drivable_areas = map_data.get('drivable_areas', {})
    
    # Different transformation approaches
    transforms = [
        {
            'name': 'downtown_centered',
            'ref_lat': 42.3314, 'ref_lon': -83.0458,
            'description': 'Centered on downtown Detroit'
        },
        {
            'name': 'shifted_north',
            'ref_lat': 42.3514, 'ref_lon': -83.0458,
            'description': 'Shifted north of downtown'
        },
        {
            'name': 'shifted_east',
            'ref_lat': 42.3314, 'ref_lon': -83.0258,
            'description': 'Shifted east of downtown'
        }
    ]
    
    for transform in transforms:
        output_file = f"detroit_attempt_{transform['name']}.kml"
        
        kml = simplekml.Kml()
        kml.document.name = f"Detroit Areas - {transform['description']}"
        
        folder = kml.newfolder(name="Drivable Areas")
        
        for area_id, area_data in drivable_areas.items():
            boundary_points = area_data.get('area_boundary', [])
            if len(boundary_points) < 3:
                continue
            
            coords = []
            for point in boundary_points:
                # Custom transformation for this attempt
                dx = point['x'] - stats['x_center']
                dy = point['y'] - stats['y_center']
                
                lat_scale = 1.0 / 111320
                lon_scale = 1.0 / (111320 * math.cos(math.radians(transform['ref_lat'])))
                
                lat = transform['ref_lat'] + dy * lat_scale
                lon = transform['ref_lon'] + dx * lon_scale
                
                coords.append((lon, lat, 0))
            
            coords.append(coords[0])
            
            pol = folder.newpolygon(name=f"Area {area_id}")
            pol.outerboundaryis = coords
            pol.style.polystyle.color = simplekml.Color.changealphaint(120, simplekml.Color.green)
            pol.style.linestyle.color = simplekml.Color.yellow
            pol.style.linestyle.width = 2
        
        kml.save(output_file)
        print(f"   Created: {output_file}")
    
    print(f"\n💡 Try opening each file in Google Earth to see which looks most realistic!")

def main():
    """Main function."""
    
    print("Detroit KML - Improved Coordinate Transformation")
    print("=" * 60)
    
    map_file = "detroit_map_found.json"
    
    if not Path(map_file).exists():
        print(f"Detroit map file not found: {map_file}")
        return
    
    # Create improved version
    output_file = create_detroit_kml_improved(map_file)
    
    # Create multiple attempts with different reference points
    create_multiple_transformation_attempts(map_file)
    
    print(f"\n Created multiple KML files with different transformations!")
    print(f"📱 Try each one in Google Earth:")
    print(f"   1. detroit_drivable_areas_improved.kml")
    print(f"   2. detroit_attempt_downtown_centered.kml")
    print(f"   3. detroit_attempt_shifted_north.kml") 
    print(f"   4. detroit_attempt_shifted_east.kml")
    print(f"\n The goal is to find which one aligns best with actual Detroit roads!")

if __name__ == "__main__":
    main() 