#!/usr/bin/env python3
"""
Download Motion Forecasting HD Maps

Download HD map JSON files from Argoverse 2 Motion Forecasting dataset.
Organize them in motion_forecasting/{split}/{log_id}/ structure.
"""

import subprocess
import re
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from tqdm import tqdm

class MotionForecastingDownloader:
    def __init__(self, base_dir="motion_forecasting"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        
        # Create split directories
        for split in ['train', 'val', 'test']:
            (self.base_dir / split).mkdir(exist_ok=True)
        
        self.found_scenarios = {'train': [], 'val': [], 'test': []}
        self.download_stats = {'downloaded': 0, 'failed': 0, 'skipped': 0}
    
    def find_scenarios_in_split(self, split):
        """Find all scenarios in a specific split."""
        
        print(f"\nFinding scenarios in {split} split...")
        
        try:
            # Get list of all scenarios
            print(f"   Getting scenario list for {split}...")
            cmd = f"aws s3 ls --no-sign-request s3://argoverse/datasets/av2/motion-forecasting/{split}/"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
            
            if result.returncode != 0:
                print(f"   Failed to list scenarios in {split}")
                return []
            
            # Extract scenario IDs
            scenario_ids = [] 
            for line in result.stdout.strip().split('\n'):
                if 'PRE' in line:
                    # Extract UUID from "PRE uuid/"
                    match = re.search(r'PRE\s+([a-f0-9-]+)/', line)
                    if match:
                        scenario_ids.append(match.group(1))
            
            print(f"   Found {len(scenario_ids)} scenarios in {split}")
            return scenario_ids
            
        except subprocess.TimeoutExpired:
            print(f"   Timeout while searching {split}")
            return []
        except Exception as e:
            print(f"   Error searching {split}: {e}")
            return []
    
    def find_all_scenarios(self, splits_to_process=['test']):
        """Find scenarios across specified splits."""
        
        print("Finding Motion Forecasting Scenarios")
        print("=" * 50)
        
        for split in splits_to_process:
            self.found_scenarios[split] = self.find_scenarios_in_split(split)
        
        total_found = sum(len(scenarios) for scenarios in self.found_scenarios.values())
        print(f"\nDiscovery Summary:")
        for split, scenarios in self.found_scenarios.items():
            if scenarios:  # Only show splits that have scenarios
                print(f"   {split}: {len(scenarios)} scenarios")
        print(f"   Total: {total_found} scenarios found")
        
        return total_found > 0
    
    def download_scenario_map(self, split, scenario_id):
        """Download HD map JSON file for a single scenario."""
        
        scenario_dir = self.base_dir / split / scenario_id
        scenario_dir.mkdir(exist_ok=True)
        
        # HD map file to download
        map_filename = f"log_map_archive_{scenario_id}.json"
        local_path = scenario_dir / map_filename
        
        # Skip if already exists
        if local_path.exists():
            self.download_stats['skipped'] += 1
            return True
        
        # Download the JSON map file
        s3_path = f"s3://argoverse/datasets/av2/motion-forecasting/{split}/{scenario_id}/{map_filename}"
        
        try:
            cmd = f"aws s3 cp --no-sign-request {s3_path} {local_path}"
            result = subprocess.run(cmd, shell=True, timeout=300)  # 5 min timeout
            
            if result.returncode == 0 and local_path.exists():
                self.download_stats['downloaded'] += 1
                return True
            else:
                self.download_stats['failed'] += 1
                return False
                
        except subprocess.TimeoutExpired:
            self.download_stats['failed'] += 1
            return False
        except Exception as e:
            self.download_stats['failed'] += 1
            return False
    
    def download_all_maps(self, max_workers=5):
        """Download HD map files for all found scenarios."""
        
        all_downloads = []
        for split, scenario_ids in self.found_scenarios.items():
            for scenario_id in scenario_ids:
                all_downloads.append((split, scenario_id))
        
        if not all_downloads:
            print("No scenarios to download")
            return
        
        print(f"\nDownloading HD maps for {len(all_downloads)} scenarios...")
        print(f"Using {max_workers} parallel downloads")
        print("=" * 60)
        
        def download_worker(split_scenario):
            split, scenario_id = split_scenario
            return self.download_scenario_map(split, scenario_id)
        
        # Download with thread pool and progress bar
        start_time = time.time()
        with tqdm(total=len(all_downloads), desc="Downloading HD maps", unit="files") as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_scenario = {executor.submit(download_worker, dl): dl for dl in all_downloads}
                
                for future in as_completed(future_to_scenario):
                    split, scenario_id = future_to_scenario[future]
                    success = future.result()
                    
                    # Update progress bar with detailed info
                    elapsed = time.time() - start_time
                    rate = pbar.n / elapsed if elapsed > 0 else 0
                    pbar.set_postfix({
                        'Downloaded': self.download_stats['downloaded'],
                        'Skipped': self.download_stats['skipped'], 
                        'Failed': self.download_stats['failed'],
                        'Rate': f"{rate:.1f}/s"
                    })
                    pbar.update(1)
    
    def create_download_manifest(self):
        """Create a manifest file with download information."""
        
        manifest = {
            'download_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'dataset': 'motion_forecasting',
            'found_scenarios': self.found_scenarios,
            'download_stats': self.download_stats,
            'directory_structure': {}
        }
        
        # Scan downloaded files
        for split in ['train', 'val', 'test']:
            split_dir = self.base_dir / split
            if split_dir.exists():
                manifest['directory_structure'][split] = {}
                
                for scenario_dir in split_dir.iterdir():
                    if scenario_dir.is_dir():
                        scenario_id = scenario_dir.name
                        files = [f.name for f in scenario_dir.iterdir() if f.is_file()]
                        manifest['directory_structure'][split][scenario_id] = {
                            'files': files,
                            'file_count': len(files),
                            'total_size_bytes': sum(f.stat().st_size for f in scenario_dir.iterdir() if f.is_file())
                        }
        
        manifest_file = self.base_dir / 'download_manifest.json'
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"\nManifest saved: {manifest_file}")
        return manifest_file

def main():
    """Main function."""
    
    print("Motion Forecasting HD Maps Downloader - Argoverse 2")
    print("Download HD map JSON files from Motion Forecasting dataset")
    print("=" * 70)
    
    # Initialize downloader
    downloader = MotionForecastingDownloader("motion_forecasting")
    
    # Start with test split only
    splits_to_process = ['test']
    print(f"Processing splits: {', '.join(splits_to_process)}")
    
    # Find all scenarios
    found = downloader.find_all_scenarios(splits_to_process)
    
    if not found:
        print("\nNo scenarios found!")
        return
    
    # Show download info and start immediately
    total_scenarios = sum(len(scenarios) for scenarios in downloader.found_scenarios.values())
    print(f"\nStarting download of {total_scenarios} HD map files...")
    print("   Downloading JSON map files (~50-200KB each)")
    print(f"   Estimated total size: ~{(total_scenarios * 100) / 1024:.1f}MB")
    
    # Download all maps
    downloader.download_all_maps(max_workers=8)  # Increased workers for smaller files
    
    # Create manifest
    manifest_file = downloader.create_download_manifest()
    
    # Summary
    print(f"\n" + "="*70)
    print("DOWNLOAD COMPLETE")
    print("="*70)
    print(f"Successfully downloaded: {downloader.download_stats['downloaded']}")
    print(f"Skipped (already existed): {downloader.download_stats['skipped']}")
    print(f"Failed downloads: {downloader.download_stats['failed']}")
    print(f"Data saved in: {downloader.base_dir}")
    print(f"Manifest: {manifest_file}")
    
    print(f"\nDirectory structure:")
    print(f"   motion_forecasting/")
    for split in ['test', 'val', 'train']:
        split_dir = downloader.base_dir / split
        if split_dir.exists():
            scenario_dirs = [d for d in split_dir.iterdir() if d.is_dir()]
            if scenario_dirs:
                print(f"   ├── {split}/ ({len(scenario_dirs)} scenarios)")
                for scenario_dir in scenario_dirs[:2]:  # Show first 2
                    files = [f for f in scenario_dir.iterdir() if f.is_file()]
                    print(f"   │   ├── {scenario_dir.name}/ ({len(files)} files)")
                if len(scenario_dirs) > 2:
                    print(f"   │   └── ... and {len(scenario_dirs)-2} more")

if __name__ == "__main__":
    main() 