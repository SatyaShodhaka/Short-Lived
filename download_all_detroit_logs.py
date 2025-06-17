#!/usr/bin/env python3
"""
Download All Detroit Logs

Download all Detroit (DTW) logs from train, val, and test splits.
Organize them in a structured directory format.
"""

import subprocess
import re
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class DetroitLogDownloader:
    def __init__(self, base_dir="detroit_logs"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        
        # Create split directories
        for split in ['train', 'val', 'test']:
            (self.base_dir / split).mkdir(exist_ok=True)
        
        self.found_logs = {'train': [], 'val': [], 'test': []}
        self.download_stats = {'downloaded': 0, 'failed': 0, 'skipped': 0}
    
    def find_detroit_logs_in_split(self, split):
        """Find all Detroit logs in a specific split."""
        
        print(f"\nSearching for Detroit logs in {split} split...")
        
        try:
            # Get list of all logs
            print(f"   Getting log list for {split}...")
            cmd = f"s5cmd ls s3://argoverse/datasets/av2/sensor/{split}/"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
            
            if result.returncode != 0:
                print(f"   Failed to list logs in {split}")
                return []
            
            # Extract log IDs
            log_ids = [] 
            for line in result.stdout.strip().split('\n'):
                if 'DIR' in line:
                    # Extract UUID from "DIR uuid/"
                    match = re.search(r'DIR\s+([a-f0-9-]+)/', line)
                    if match:
                        log_ids.append(match.group(1))
            
            print(f"   Found {len(log_ids)} total logs in {split}")
            
            # Process all logs to find Detroit data
            detroit_logs = []
            total_logs = len(log_ids)
            
            for i, log_id in enumerate(log_ids):
                if i % 100 == 0:  # Progress update every 100 logs
                    print(f"   Checking log {i+1}/{total_logs}...")
                
                # Check if this log has Detroit map data
                map_cmd = f"s5cmd ls s3://argoverse/datasets/av2/sensor/{split}/{log_id}/map/ | grep DTW"
                map_result = subprocess.run(map_cmd, shell=True, capture_output=True, text=True, timeout=30)
                
                if map_result.returncode == 0 and map_result.stdout.strip():
                    detroit_logs.append(log_id)
                    print(f"   Found Detroit log: {log_id}")
            
            print(f"   Found {len(detroit_logs)} Detroit logs in {split}")
            return detroit_logs
            
        except subprocess.TimeoutExpired:
            print(f"   Timeout while searching {split}")
            return []
        except Exception as e:
            print(f"   Error searching {split}: {e}")
            return []
    
    def find_all_detroit_logs(self):
        """Find Detroit logs across all splits."""
        
        print("Finding Detroit Logs Across All Splits")
        print("=" * 50)
        
        for split in ['test', 'val', 'train']:  # Start with smaller splits
            self.found_logs[split] = self.find_detroit_logs_in_split(split)
        
        total_found = sum(len(logs) for logs in self.found_logs.values())
        print(f"\nDISCOVERY SUMMARY:")
        for split, logs in self.found_logs.items():
            print(f"   {split}: {len(logs)} Detroit logs")
        print(f"   Total: {total_found} Detroit logs found")
        
        return total_found > 0
    
    def download_log_data(self, split, log_id):
        """Download essential data for a single Detroit log."""
        
        log_dir = self.base_dir / split / log_id
        log_dir.mkdir(exist_ok=True)
        
        files_to_download = [
            {
                'name': 'pose',
                'path': f'{log_id}/city_SE3_egovehicle.feather',
                'local': log_dir / 'city_SE3_egovehicle.feather'
            }
        ]
        
        # Find and download the Detroit map file
        try:
            # List map files to find the DTW one
            map_cmd = f"s5cmd ls s3://argoverse/datasets/av2/sensor/{split}/{log_id}/map/"
            map_result = subprocess.run(map_cmd, shell=True, capture_output=True, text=True, timeout=30)
            
            if map_result.returncode == 0:
                for line in map_result.stdout.strip().split('\n'):
                    if 'log_map_archive' in line and 'DTW' in line:
                        filename = line.split()[-1]
                        files_to_download.append({
                            'name': 'map',
                            'path': f'{log_id}/map/{filename}',
                            'local': log_dir / f'map_{filename}'
                        })
                        break
        except:
            pass
        
        # Download each file
        downloaded = 0
        for file_info in files_to_download:
            s3_path = f"s3://argoverse/datasets/av2/sensor/{split}/{file_info['path']}"
            local_path = file_info['local']
            
            if local_path.exists():
                print(f"      Skipping {file_info['name']} (already exists)")
                downloaded += 1
                continue
            
            try:
                cmd = f"s5cmd cp {s3_path} {local_path}"
                result = subprocess.run(cmd, shell=True, timeout=300)  # 5 min timeout
                
                if result.returncode == 0 and local_path.exists():
                    print(f"      Downloaded {file_info['name']}")
                    downloaded += 1
                else:
                    print(f"      Failed to download {file_info['name']}")
                    
            except subprocess.TimeoutExpired:
                print(f"      Timeout downloading {file_info['name']}")
            except Exception as e:
                print(f"      Error downloading {file_info['name']}: {e}")
        
        return downloaded > 0
    
    def download_all_detroit_data(self, max_workers=3):
        """Download data for all found Detroit logs."""
        
        all_downloads = []
        for split, log_ids in self.found_logs.items():
            for log_id in log_ids:
                all_downloads.append((split, log_id))
        
        if not all_downloads:
            print("No Detroit logs to download")
            return
        
        print(f"\nDownloading data for {len(all_downloads)} Detroit logs...")
        print(f"Using {max_workers} parallel downloads")
        
        def download_worker(split_log):
            split, log_id = split_log
            print(f"   Downloading {split}/{log_id[:8]}...")
            
            try:
                success = self.download_log_data(split, log_id)
                if success:
                    self.download_stats['downloaded'] += 1
                    return f"Success: {split}/{log_id[:8]}"
                else:
                    self.download_stats['failed'] += 1
                    return f"Failed: {split}/{log_id[:8]}"
            except Exception as e:
                self.download_stats['failed'] += 1
                return f"Error: {split}/{log_id[:8]}: {e}"
        
        # Download with thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_log = {executor.submit(download_worker, dl): dl for dl in all_downloads}
            
            for future in as_completed(future_to_log):
                result = future.result()
                print(f"      {result}")
    
    def create_download_manifest(self):
        """Create a manifest file with download information."""
        
        manifest = {
            'download_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'found_logs': self.found_logs,
            'download_stats': self.download_stats,
            'directory_structure': {}
        }
        
        # Scan downloaded files
        for split in ['train', 'val', 'test']:
            split_dir = self.base_dir / split
            if split_dir.exists():
                manifest['directory_structure'][split] = {}
                
                for log_dir in split_dir.iterdir():
                    if log_dir.is_dir():
                        log_id = log_dir.name
                        files = [f.name for f in log_dir.iterdir() if f.is_file()]
                        manifest['directory_structure'][split][log_id] = {
                            'files': files,
                            'file_count': len(files),
                            'total_size_bytes': sum(f.stat().st_size for f in log_dir.iterdir() if f.is_file())
                        }
        
        manifest_file = self.base_dir / 'download_manifest.json'
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"Manifest saved: {manifest_file}")
        return manifest_file

def main():
    """Main function."""
    
    print("Detroit Logs Downloader - Argoverse 2")
    print("Download all Detroit logs from train, val, test splits")
    print("=" * 60)
    
    # Initialize downloader
    downloader = DetroitLogDownloader("detroit_logs")
    
    # Find all Detroit logs
    found = downloader.find_all_detroit_logs()
    
    if not found:
        print("\nNo Detroit logs found!")
        print("This might be because:")
        print("   1. Detroit logs are in the unsampled portion")
        print("   2. Different city code is used")
        print("   3. Network timeouts during search")
        return
    
    # Ask user confirmation before downloading
    total_logs = sum(len(logs) for logs in downloader.found_logs.values())
    print(f"\nProceed with downloading {total_logs} Detroit logs?")
    print("   This will download pose files and map data for each log.")
    print("   Estimated size: ~1-5MB per log")
    
    response = input("   Continue? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("   Download cancelled by user.")
        return
    
    # Download all data
    print(f"\nStarting download...")
    downloader.download_all_detroit_data(max_workers=3)
    
    # Create manifest
    manifest_file = downloader.create_download_manifest()
    
    # Summary
    print(f"\n" + "="*60)
    print("DOWNLOAD COMPLETE")
    print("="*60)
    print(f"Successfully downloaded: {downloader.download_stats['downloaded']}")
    print(f"Failed downloads: {downloader.download_stats['failed']}")
    print(f"Data saved in: {downloader.base_dir}")
    print(f"Manifest: {manifest_file}")
    
    print(f"\nDirectory structure:")
    print(f"   detroit_logs/")
    for split in ['train', 'val', 'test']:
        split_dir = downloader.base_dir / split
        if split_dir.exists():
            log_dirs = [d for d in split_dir.iterdir() if d.is_dir()]
            print(f"   ├── {split}/ ({len(log_dirs)} logs)")
            for log_dir in log_dirs[:2]:  # Show first 2
                files = [f for f in log_dir.iterdir() if f.is_file()]
                print(f"   │   ├── {log_dir.name}/ ({len(files)} files)")
            if len(log_dirs) > 2:
                print(f"   │   └── ... and {len(log_dirs)-2} more")
    
    print(f"\nNext step: Run the KML generation script!")
    print(f"   python generate_detroit_kml_from_logs.py")

if __name__ == "__main__":
    main() 