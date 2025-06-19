import os
import pyarrow.feather as feather
import simplekml
from av2.geometry.utm import convert_city_coords_to_wgs84
import numpy as np

DETROIT_CITY_NAME = "DTW"
LOGS_BASE = "detroit_logs"
OUTPUT_KML = "detroit_ego_trajectories"


def get_gps_from_city_coords(x, y, z):
    point_2d = np.array([[x, y]])
    lat_lon = convert_city_coords_to_wgs84(point_2d, DETROIT_CITY_NAME)[0]
    return lat_lon[0], lat_lon[1], z


def main():
    kml = simplekml.Kml()
    for split in ['test']:
        split_dir = os.path.join(LOGS_BASE, split)
        if not os.path.exists(split_dir):
            continue
        for log_id in os.listdir(split_dir):
            log_dir = os.path.join(split_dir, log_id)
            pose_path = os.path.join(log_dir, "city_SE3_egovehicle.feather")
            if not os.path.exists(pose_path):
                continue
            poses = feather.read_feather(pose_path)
            coords = []
            for i, row in poses.iterrows():
                x, y, z = row['tx_m'], row['ty_m'], row['tz_m']
                lat, lon, alt = get_gps_from_city_coords(x, y, z)
                coords.append((lon, lat, alt))
            if coords:
                ls = kml.newlinestring(name=f"{split}/{log_id}", coords=coords)
                ls.style.linestyle.color = simplekml.Color.red if split == 'train' else simplekml.Color.green if split == 'val' else simplekml.Color.blue
                ls.style.linestyle.width = 2
    kml.save(OUTPUT_KML)
    print(f"KML file saved as {OUTPUT_KML}")

if __name__ == "__main__":
    main() 