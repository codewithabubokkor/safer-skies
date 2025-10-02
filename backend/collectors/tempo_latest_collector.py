#!/usr/bin/env python3
"""
🛰️ TEMPO LATEST DATA COLLECTOR
Fresh, clean collector for the most recent TEMPO satellite data

Features:
- Targets latest data (within last hour if available)
- Works for any location worldwide
- NASA-compliant filtering for all 3 gases (NO2, HCHO, O3)
- Stream processing without downloads
- Clean, focused implementation

Usage:
    collector = TempoLatestCollector()
    data = collector.get_latest_data(lat=40.7128, lon=-74.0060)
"""

import h5py
import s3fs
import requests
import numpy as np
from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, List, Optional, Tuple
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TempoLatestCollector:
    """Clean TEMPO collector focused on latest data retrieval"""
    
    def __init__(self):
        """Initialize with NASA credentials"""
        self.bearer_token = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImFidWJva2tvci5jc2UiLCJleHAiOjE3NjAxMTM3MTEsImlhdCI6MTc1NDkyOTcxMSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.rJhkWn4bxNeWipNFNUgQu4qZelDQ47czJBtgWPbGIg7Yujny2c19d6QFfEGWTIDOCiwDhLde9RsrNH2W_JVk190fuekdiBPMUXMH5gnn-OO0eiB4QO5kN2nVKCin6jZPo7HLAXvshs92Z6VXXpj3mKVjAPlxA3R0keR93R0gVl0bKYyjkps5AUA93qDKKS5iBh1-Azil5aKeIqmSWDG6iHyp6bIAoznrt5hkEqkLU0BYsVmWNpMHty0Legv7ZEit_zR414LlUpY-hyVa8hhgnJM1pNyR4bnHvqSnZh8rvpXNt6_qQHAR0RvgrgoFXqRMpy6-tVd3XyqdEkLgaVZccQ'
        self.s3_cred_endpoint = 'https://data.asdc.earthdata.nasa.gov/s3credentials'
        self.s3 = None
        
    def setup_s3_credentials(self) -> bool:
        """Setup NASA S3 credentials"""
        try:
            headers = {'Authorization': f'Bearer {self.bearer_token}'}
            response = requests.get(self.s3_cred_endpoint, headers=headers)
            response.raise_for_status()
            
            creds = response.json()
            self.s3 = s3fs.S3FileSystem(
                key=creds['accessKeyId'],
                secret=creds['secretAccessKey'],
                token=creds['sessionToken'],
                client_kwargs={'region_name': 'us-west-2'}
            )
            
            logger.info("✅ NASA S3 credentials configured")
            return True
            
        except Exception as e:
            logger.error(f"❌ S3 credential setup failed: {e}")
            return False
    
    def find_latest_files(self, hours_back: int = 2) -> Dict[str, Optional[str]]:
        """
        Find the most recent TEMPO files for all gases
        
        Args:
            hours_back: How many hours back to search (default 2)
            
        Returns:
            Dict with gas types as keys and file paths as values
        """
        files = {"NO2": None, "HCHO": None, "O3TOT": None}
        
        for hours in range(hours_back):
            check_time = datetime.now() - timedelta(hours=hours)
            date_str = check_time.strftime('%Y.%m.%d')
            
            for gas in files.keys():
                if files[gas] is not None:  # Already found
                    continue
                    
                try:
                    s3path = f's3://asdc-prod-protected/TEMPO/TEMPO_{gas}_L3_V03/{date_str}/*.nc'
                    found_files = self.s3.glob(s3path)
                    
                    if found_files:
                        latest_file = sorted(found_files)[-1]
                        files[gas] = latest_file
                        
                        age_hours = hours + (check_time.hour - datetime.now().hour) / 24
                        logger.info(f"📁 Found {gas}: {latest_file.split('/')[-1]} (~{age_hours:.1f}h old)")
                        
                except Exception as e:
                    logger.debug(f"No {gas} files for {date_str}: {e}")
                    continue
            
            if all(files.values()):
                break
        
        return files
    
    def extract_gas_data(self, file_path: str, gas: str, lat: float, lon: float) -> Optional[Dict]:
        """
        Extract data for a specific gas using NASA-compliant filtering
        
        Args:
            file_path: S3 path to TEMPO file
            gas: Gas type (NO2, HCHO, O3TOT)
            lat, lon: Target coordinates
            
        Returns:
            Extracted data dict or None if extraction fails
        """
        try:
            logger.info(f"🛰️ Processing {gas} from {file_path.split('/')[-1]}")
            
            with self.s3.open(file_path, 'rb') as f:
                with h5py.File(f, 'r') as h5_file:
                    
                    try:
                        if 'geolocation' in h5_file:
                            lats = h5_file['geolocation']['latitude'][:]
                            lons = h5_file['geolocation']['longitude'][:]
                        else:
                            lats = h5_file['latitude'][:]
                            lons = h5_file['longitude'][:]
                    except KeyError:
                        lats = h5_file['latitude'][:]
                        lons = h5_file['longitude'][:]
                    
                    lat_idx = np.argmin(np.abs(lats - lat))
                    lon_idx = np.argmin(np.abs(lons - lon))
                    closest_lat = lats[lat_idx] 
                    closest_lon = lons[lon_idx]
                    
                    product = h5_file['product']
                    support_data = h5_file['support_data']
                    
                    # Gas-specific dataset configuration
                    if gas == "NO2":
                        main_dataset = product['vertical_column_troposphere']
                        quality_dataset = product['main_data_quality_flag']
                        cloud_dataset = support_data['eff_cloud_fraction']
                        units = "molecules/cm²"
                        conversion_factor = 3.5
                    elif gas == "HCHO":
                        main_dataset = product['vertical_column']
                        quality_dataset = product['main_data_quality_flag']
                        cloud_dataset = support_data['eff_cloud_fraction']
                        units = "molecules/cm²"
                        conversion_factor = 2.8
                    elif gas == "O3TOT":
                        main_dataset = product['column_amount_o3']
                        quality_dataset = None
                        cloud_dataset = product['radiative_cloud_frac']
                        units = "DU"
                        conversion_factor = 40.0 / 300.0
                        qa_stats = h5_file['qa_statistics']
                        samples_dataset = qa_stats['num_column_samples']
                        min_dataset = qa_stats['min_column_sample']
                        max_dataset = qa_stats['max_column_sample']
                    else:
                        logger.error(f"❌ Unsupported gas: {gas}")
                        return None
                    
                    gas_value = main_dataset[0, lat_idx, lon_idx]
                    cloud_fraction = cloud_dataset[0, lat_idx, lon_idx]
                    
                    # Gas-specific quality data
                    if gas == "O3TOT":
                        quality_flag = None
                        num_samples = samples_dataset[0, lat_idx, lon_idx]
                        min_sample = min_dataset[0, lat_idx, lon_idx]
                        max_sample = max_dataset[0, lat_idx, lon_idx]
                        sample_range = max_sample - min_sample
                        logger.info(f"📊 {gas}: {gas_value:.1f} {units}, Samples: {num_samples}, Range: {sample_range:.1f} DU, Cloud: {cloud_fraction:.3f}")
                    else:
                        quality_flag = quality_dataset[0, lat_idx, lon_idx]
                        logger.info(f"📊 {gas}: {gas_value:.2e} {units}, QA: {quality_flag}, Cloud: {cloud_fraction:.3f}")
                    
                    logger.info(f"📍 Target: {lat:.3f}°N, {lon:.3f}°W → Closest: {closest_lat:.3f}°N, {closest_lon:.3f}°W")
                    
                    filter_reason = None
                    
                    if gas == "O3TOT":
                        # O3-specific NASA filtering
                        if np.isnan(num_samples) or num_samples < 5:
                            filter_reason = f"Insufficient samples ({num_samples} < 5)"
                        elif np.isnan(sample_range) or sample_range > 20:
                            filter_reason = f"Inconsistent data (range={sample_range:.1f} DU > 20 DU)"
                        elif np.isnan(cloud_fraction) or cloud_fraction > 0.2:
                            filter_reason = f"Cloudy conditions ({cloud_fraction:.3f} > 0.2)"
                        elif np.isnan(gas_value) or gas_value <= 0:
                            filter_reason = f"Invalid {gas} measurement"
                    else:
                        if np.isnan(quality_flag) or quality_flag != 0:
                            filter_reason = f"Quality flag issue (QA={quality_flag})"
                        elif np.isnan(cloud_fraction) or cloud_fraction >= 0.2:
                            filter_reason = f"Cloudy conditions ({cloud_fraction:.3f} ≥ 0.2)"
                        elif np.isnan(gas_value) or gas_value <= 0:
                            filter_reason = f"Invalid {gas} measurement"
                    
                    if filter_reason is None:
                        if gas == "O3TOT":
                            surface_ppb = gas_value * conversion_factor
                            logger.info(f"✅ VALID {gas}: {gas_value:.1f} DU → {surface_ppb:.1f} ppb")
                        else:
                            surface_ppb = (gas_value / 1e16) * conversion_factor
                            logger.info(f"✅ VALID {gas}: {gas_value:.2e} {units} → {surface_ppb:.1f} ppb")
                        
                        return {
                            "gas": gas,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "file_path": file_path,
                            "target_lat": lat,
                            "target_lon": lon,
                            "actual_lat": float(closest_lat),
                            "actual_lon": float(closest_lon),
                            "raw_value": float(gas_value),
                            "units": units,
                            "surface_ppb": round(surface_ppb, 2),
                            "quality_flag": int(quality_flag) if quality_flag is not None else None,
                            "cloud_fraction": float(cloud_fraction),
                            "nasa_quality": "passed",
                            "data_valid": True
                        }
                    else:
                        logger.warning(f"❌ {gas} filtered: {filter_reason}")
                        return {
                            "gas": gas,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "file_path": file_path,
                            "target_lat": lat,
                            "target_lon": lon,
                            "actual_lat": float(closest_lat),
                            "actual_lon": float(closest_lon),
                            "quality_flag": int(quality_flag) if quality_flag is not None else None,
                            "cloud_fraction": float(cloud_fraction) if not np.isnan(cloud_fraction) else None,
                            "filter_reason": filter_reason,
                            "nasa_quality": "failed",
                            "data_valid": False
                        }
                        
        except Exception as e:
            logger.error(f"❌ Error processing {gas}: {e}")
            return None
    
    def get_latest_data(self, lat: float, lon: float, max_hours: int = 2) -> Dict:
        """
        Get the latest TEMPO data for all gases at a specific location
        
        Args:
            lat, lon: Target coordinates
            max_hours: Maximum hours back to search (default 2)
            
        Returns:
            Complete results dict with all available data
        """
        logger.info(f"🛰️ TEMPO LATEST DATA COLLECTOR")
        logger.info(f"📍 Location: {lat:.4f}°N, {lon:.4f}°W")
        logger.info(f"🕐 Searching last {max_hours} hours")
        
        if not self.setup_s3_credentials():
            return {"error": "S3 credentials setup failed"}
        
        files = self.find_latest_files(max_hours)
        
        results = {
            "location": {"lat": lat, "lon": lon},
            "search_hours": max_hours,
            "processing_time": datetime.now(timezone.utc).isoformat(),
            "data": {}
        }
        
        for gas, file_path in files.items():
            if file_path:
                gas_data = self.extract_gas_data(file_path, gas, lat, lon)
                results["data"][gas] = gas_data if gas_data else {"error": f"{gas} processing failed"}
            else:
                results["data"][gas] = {"error": f"No {gas} files found in last {max_hours} hours"}
        
        return results

def main():
    """Example usage - test with NYC coordinates"""
    collector = TempoLatestCollector()
    
    # Test locations
    locations = [
        {"name": "NYC", "lat": 40.7128, "lon": -74.0060},
        {"name": "LA", "lat": 34.0522, "lon": -118.2437},
        {"name": "Chicago", "lat": 41.8781, "lon": -87.6298}
    ]
    
    for location in locations:
        print(f"\n{'='*60}")
        print(f"Testing: {location['name']}")
        print(f"{'='*60}")
        
        data = collector.get_latest_data(location['lat'], location['lon'], max_hours=2)
        
        print("\n📊 RESULTS:")
        print(json.dumps(data, indent=2))
        
        # Quick summary
        print(f"\n📈 SUMMARY for {location['name']}:")
        for gas, result in data.get("data", {}).items():
            if isinstance(result, dict) and result.get("data_valid"):
                print(f"  ✅ {gas}: {result['surface_ppb']} ppb")
            else:
                reason = result.get("filter_reason", result.get("error", "Unknown"))
                print(f"  ❌ {gas}: {reason}")
        
        break  # Test only first location for demo

if __name__ == "__main__":
    main()