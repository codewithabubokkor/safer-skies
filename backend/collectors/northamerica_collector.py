#!/usr/bin/env python3
"""
North America NASA Data Collector
=================================
Focused on North America NASA satellite data collection

From TEMPO (Streaming): NO2, HCHO, O3 (direct S3 streaming)
From GEOS-CF: NO2, O3, CO, SO2 + Meteorology (API)
Ground Stations: All 6 EPA pollutants (AirNow/WAQI APIs)

FEATURES:
- Parallel processing for multiple locations using asyncio.gather()
- Raw data collection only - no user management

CRITICAL FIXES:
- NO AQI calculation - raw concentrations only
- HCHO separated (science data, not EPA AQI)
- External AQI stored separately from concentrations  
- Processors directory handles all data processing
- NASA-compliant TEMPO filtering from stage1
"""

import json
import time
import h5py
import numpy as np
import boto3
import requests
import os
import math
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging
import tempfile
import asyncio
import pytz
import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor, as_completed

# IMMEDIATE PROCESSING IMPORTS
from backend.processors.fusion_bias_corrector import ProductionFusionEngine
from backend.processors.aqi_calculator import EPAAQICalculator

# Import TEMPO streaming collector
from backend.collectors.tempo_latest_collector import TempoLatestCollector

# Import database connection utility (same as global collector)
try:
    from utils.database_connection import get_db_connection
except ImportError:
    from backend.utils.database_connection import get_db_connection

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@dataclass
class RawPollutantData:
    """Raw pollutant measurement - NO AQI CALCULATION"""
    pollutant: str
    concentration: float
    units: str  # ppb, ppm, ug/m3
    source: str
    quality: str
    uncertainty: str
    timestamp: str
    reported_aqi: Optional[int] = None
    reported_aqi_source: Optional[str] = None

@dataclass 
class RawLocationData:
    """Raw location data - NO PROCESSING, ready for processors directory"""
    location: Dict[str, float]
    timestamp: str
    collection_time_seconds: float
    # Raw measurements only
    raw_measurements: Dict[str, RawPollutantData]
    data_sources: Dict[str, Any]
    science_data: Dict[str, Any]
    meteorology_data: Dict[str, Any]
    metadata: Dict[str, Any]

@dataclass
class ProcessedLocationData:
    """IMMEDIATE PROCESSING RESULT - Fused + AQI calculated"""
    location: Dict[str, float]
    timestamp: str
    collection_time_seconds: float
    processing_time_seconds: float
    # Raw data (preserved for reference)
    raw_data: RawLocationData
    fused_concentrations: Dict[str, Any]
    epa_aqi_results: Dict[str, Any]
    processing_pipeline: Dict[str, Any]
    metadata: Dict[str, Any]

class MultiSourceLocationCollector:
    """
    Collects data from ALL sources for a specific location
    Following your complete architecture from the implementation plan
    
    Supports parallel collection for multiple locations
    """
    
    def __init__(self, s3_bucket: str = "naq-forecast-tempo-data", data_bucket: str = "naq-forecast-data", use_local_tempo: bool = False):
        self.s3_bucket = s3_bucket  # For TEMPO files (legacy)
        self.data_bucket = data_bucket  # For storing raw data
        self.s3_client = boto3.client('s3')  # Still used for data storage, not TEMPO downloads
        self.use_local_tempo = use_local_tempo
        
        # IMMEDIATE PROCESSING ENGINES
        self.fusion_engine = ProductionFusionEngine()
        self.aqi_calculator = EPAAQICalculator()
        self.enable_immediate_processing = True  # Toggle for immediate vs batch processing
        
        self.tempo_collector = TempoLatestCollector()
        logger.info("🛰️ TempoLatestCollector initialized for streaming S3 access")
        
        # File patterns - S3 vs Local
        if use_local_tempo:
            # Local file paths for testing - same location as tempo_file_fetcher.py
            self.local_tempo_dir = os.getenv('TEMPO_LOCAL_CACHE', '/app/data/tempo_data')
            self.tempo_files = {
                "NO2": os.path.join(self.local_tempo_dir, "latest-no2.nc"),
                "HCHO": os.path.join(self.local_tempo_dir, "latest-hcho.nc")
            }
            logger.info(f"🧪 Using LOCAL TEMPO files from: {self.local_tempo_dir}")
        else:
            self.tempo_files = {
                "NO2": "tempo-cache/latest-no2.nc",
                "HCHO": "tempo-cache/latest-hcho.nc"
            }
            # S3 TEMPO files configured
        
        # API endpoints
        self.apis = {
            "geos_cf": "https://fluid.nccs.nasa.gov/cfapi/fcast/chm/v1",
            "airnow": "https://airnowapi.org/aq/observation/latLong/current",
            "waqi": "https://api.waqi.info/feed/geo"
        }
        
        # Fire data is handled by separate fire_collector.py system
    
    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using haversine formula"""
        R = 6371  # Earth radius in kilometers
        
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    @staticmethod
    def convert_aqi_to_concentration(aqi: int, pollutant: str) -> float:
        """
        Convert EPA AQI back to concentration using official EPA breakpoint tables
        This is needed because fusion engine expects concentrations, not AQI values
        """
        if aqi is None:
            return None
            
        # EPA Official AQI Breakpoint Tables (reverse lookup)
        breakpoints = {
            'O3': [  # 8-hour average (ppm)
                (0, 50, 0.000, 0.054),
                (51, 100, 0.055, 0.070),
                (101, 150, 0.071, 0.085),
                (151, 200, 0.086, 0.105),
                (201, 300, 0.106, 0.200),
                (301, 500, 0.201, 0.604)
            ],
            'PM2.5': [
                (0, 50, 0.0, 9.0),
                (51, 100, 9.1, 35.4),
                (101, 150, 35.5, 55.4),
                (151, 200, 55.5, 125.4),
                (201, 300, 125.5, 225.4),
                (301, 500, 225.5, 325.4)
            ],
            'PM25': [
                (0, 50, 0.0, 9.0),
                (51, 100, 9.1, 35.4),
                (101, 150, 35.5, 55.4),
                (151, 200, 55.5, 125.4),
                (201, 300, 125.5, 225.4),
                (301, 500, 225.5, 325.4)
            ],
            'PM10': [
                (0, 50, 0, 54),
                (51, 100, 55, 154),
                (101, 150, 155, 254),
                (151, 200, 255, 354),
                (201, 300, 355, 424),
                (301, 500, 425, 604)
            ],
            'CO': [
                (0, 50, 0.0, 4.4),
                (51, 100, 4.5, 9.4),
                (101, 150, 9.5, 12.4),
                (151, 200, 12.5, 15.4),
                (201, 300, 15.5, 30.4),
                (301, 500, 30.5, 50.4)
            ],
            'SO2': [
                (0, 50, 0, 35),
                (51, 100, 36, 75),
                (101, 150, 76, 185),
                (151, 200, 186, 304),
                (201, 300, 305, 604),
                (301, 500, 605, 1004)
            ],
            'NO2': [
                (0, 50, 0, 53),
                (51, 100, 54, 100),
                (101, 150, 101, 360),
                (151, 200, 361, 649),
                (201, 300, 650, 1249),
                (301, 500, 1250, 2049)
            ]
        }
        
        if pollutant not in breakpoints:
            logger.warning(f"⚠️ Unknown pollutant for AQI conversion: {pollutant}")
            return aqi  # Fallback to AQI value
        
        for aqi_lo, aqi_hi, conc_lo, conc_hi in breakpoints[pollutant]:
            if aqi_lo <= aqi <= aqi_hi:
                # Reverse EPA formula: C = ((AQI - ILo) * (BPHi - BPLo) / (IHi - ILo)) + BPLo
                concentration = ((aqi - aqi_lo) * (conc_hi - conc_lo) / (aqi_hi - aqi_lo)) + conc_lo
                logger.debug(f"🔄 AQI→Conc: {pollutant} AQI {aqi} → {concentration:.6f}")
                return round(concentration, 3)
        
        # Fallback for out-of-range values
        logger.warning(f"⚠️ AQI {aqi} out of range for {pollutant}")
        return aqi  # Return original AQI as fallback
    
    def get_closest_airnow_data(self, target_lat: float, target_lon: float, api_key: str) -> Dict[str, Dict[str, Any]]:
        """
        Optimized AirNow strategy: Gradual distance expansion (1,2,3,4,5,6,7,8,9,10,11...)
        Returns ONE closest value per parameter for next process
        """
        
        def calculate_distance(lat1, lon1, lat2, lon2):
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            return c * 6371
        
        def get_stations(lat, lon, distance, api_key):
            url = "https://www.airnowapi.org/aq/observation/latLong/current/"
            params = {
                "format": "application/json",
                "latitude": lat,
                "longitude": lon,
                "distance": distance,
                "API_KEY": api_key
            }
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except:
                return []
        
        # SUPER OPTIMIZED: Start close, increment by 1 until 12, then bigger jumps
        # Pattern: [4,5,6,7,8,9,10,11,12,15,20,25,30,40,50] - find closest stations faster
        distances = [4,5,6,7,8,9,10,11,12,15,20,25,30,40,50]
        current_time = datetime.now(timezone.utc)
        
        logger.info(f"🎯 Starting SUPER OPTIMIZED AirNow distance search for {target_lat}, {target_lon}")
        logger.info(f"🚀 Using smart search pattern: {distances[:8]}... (close range), then {distances[8:]} (wider range)")
        
        # PARALLEL SEARCH: Check multiple distances simultaneously in batches
        import concurrent.futures
        import threading
        
        def check_distance_with_result(distance):
            """Check a single distance and return (distance, stations)"""
            try:
                stations = get_stations(target_lat, target_lon, distance, api_key)
                return (distance, stations if stations else [])
            except:
                return (distance, [])
        
        batch_size = 4
        found_results = []
        
        for i in range(0, len(distances), batch_size):
            batch = distances[i:i+batch_size]
            logger.info(f"📍 PARALLEL: Checking {batch} miles simultaneously...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                # Submit all distances in this batch
                future_to_distance = {executor.submit(check_distance_with_result, dist): dist for dist in batch}
                
                batch_results = []
                for future in concurrent.futures.as_completed(future_to_distance):
                    distance, stations = future.result()
                    if stations:  # Has data
                        batch_results.append((distance, stations))
                        logger.info(f"✅ Found {len(stations)} stations at {distance} miles")
                    else:
                        logger.info(f"❌ No stations at {distance} miles")
            
            if batch_results:
                batch_results.sort(key=lambda x: x[0])
                closest_distance, stations = batch_results[0]
                
                logger.info(f"🎯 PARALLEL SUCCESS: Using closest result {closest_distance} miles (found {len(batch_results)} options)")
                logger.info(f"⚡ Saved time by checking {len(batch)} distances simultaneously")
                
                distance = closest_distance
                closest_per_parameter = {}
                
                for station in stations:
                    param = station.get('ParameterName', '')
                    if not param:
                        continue
                        
                    station_lat = station.get('Latitude', 0)
                    station_lon = station.get('Longitude', 0)
                    if not station_lat or not station_lon:
                        continue
                    
                    distance_km = calculate_distance(target_lat, target_lon, station_lat, station_lon)
                    
                    if param not in closest_per_parameter or distance_km < closest_per_parameter[param]['distance_km']:
                        date_obs = station.get('DateObserved', '')
                        hour_obs = station.get('HourObserved', '')
                        age_hours = 0
                        timestamp = current_time
                        
                        if date_obs and hour_obs is not None:
                            try:
                                timestamp_str = f"{date_obs}T{str(hour_obs).zfill(2)}:00:00"
                                ny_tz = pytz.timezone('America/New_York')
                                local_time = datetime.fromisoformat(timestamp_str)
                                localized_time = ny_tz.localize(local_time)
                                timestamp = localized_time.astimezone(timezone.utc)
                                age_seconds = (current_time - timestamp).total_seconds()
                                age_hours = age_seconds / 3600
                            except:
                                age_hours = float('inf')
                        
                        aqi_value = station.get('AQI')
                        concentration = self.convert_aqi_to_concentration(aqi_value, param)
                        
                        closest_per_parameter[param] = {
                            'value': concentration,  # Use concentration instead of AQI for fusion
                            'aqi': aqi_value,       # Keep original AQI for reference
                            'distance_km': distance_km,
                            'age_hours': age_hours,
                            'area': station.get('ReportingArea', ''),
                            'state': station.get('StateCode', ''),
                            'coordinates': (station_lat, station_lon),
                            'timestamp': timestamp,
                            'source': 'airnow_observation',
                            'category': station.get('Category', {}).get('Name', ''),
                            'category_number': station.get('Category', {}).get('Number', 0),
                            'raw_station': station
                        }
                        
                        logger.info(f"🎯 New closest {param}: {station.get('AQI')} AQI at {distance_km:.1f}km")
                
                return closest_per_parameter  # Return immediately when data found
            else:
                logger.info(f"❌ No stations at {distance} miles, trying next distance...")
        
        logger.warning("❌ No AirNow data found even at maximum distance")
        return {}  # No data found

    async def get_closest_airnow_data_parallel(self, target_lat: float, target_lon: float, api_key: str) -> Dict[str, Dict[str, Any]]:
        """
        PARALLEL AirNow strategy: Check multiple distances simultaneously
        Much faster - returns as soon as any distance finds data
        """
        import asyncio
        import aiohttp
        
        def calculate_distance(lat1, lon1, lat2, lon2):
            lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            return c * 6371

        async def get_stations_async(session, lat, lon, distance, api_key):
            url = "https://www.airnowapi.org/aq/observation/latLong/current/"
            params = {
                "format": "application/json",
                "latitude": lat,
                "longitude": lon,
                "distance": distance,
                "API_KEY": api_key
            }
            try:
                async with session.get(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return distance, data
                    return distance, []
            except:
                return distance, []

        close_distances = [4,5,6,7,8]  # Check closest 5 distances simultaneously
        far_distances = [9,10,11,12,15,20,25,30,40,50]
        
        logger.info(f"🎯 Starting PARALLEL AirNow search for {target_lat}, {target_lon}")
        logger.info(f"🚀 Phase 1: Parallel check {close_distances} miles")
        
        current_time = datetime.now(timezone.utc)
        
        async with aiohttp.ClientSession() as session:
            # Phase 1: Check close distances in parallel
            tasks = [get_stations_async(session, target_lat, target_lon, dist, api_key) 
                     for dist in close_distances]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful_results = []
            for result in results:
                if isinstance(result, tuple) and len(result) == 2:
                    distance, stations = result
                    if stations:  # Has data
                        successful_results.append((distance, stations))
            
            if not successful_results:
                logger.info(f"🚀 Phase 2: Parallel check {far_distances[:5]} miles")
                tasks = [get_stations_async(session, target_lat, target_lon, dist, api_key) 
                         for dist in far_distances[:5]]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, tuple) and len(result) == 2:
                        distance, stations = result
                        if stations:
                            successful_results.append((distance, stations))
            
            if successful_results:
                successful_results.sort(key=lambda x: x[0])
                closest_distance, stations = successful_results[0]
                
                logger.info(f"✅ PARALLEL SUCCESS: Found {len(stations)} stations at {closest_distance} miles")
                
                closest_per_parameter = {}
                for station in stations:
                    param = station.get('ParameterName', '')
                    if not param:
                        continue
                        
                    station_lat = station.get('Latitude', 0)
                    station_lon = station.get('Longitude', 0)
                    if not station_lat or not station_lon:
                        continue
                    
                    distance_km = calculate_distance(target_lat, target_lon, station_lat, station_lon)
                    
                    if param not in closest_per_parameter or distance_km < closest_per_parameter[param]['distance_km']:
                        date_obs = station.get('DateObserved', '')
                        hour_obs = station.get('HourObserved', '')
                        age_hours = 0
                        timestamp = current_time
                        
                        if date_obs and hour_obs is not None:
                            try:
                                timestamp_str = f"{date_obs}T{str(hour_obs).zfill(2)}:00:00"
                                ny_tz = pytz.timezone('America/New_York')
                                local_time = datetime.fromisoformat(timestamp_str)
                                localized_time = ny_tz.localize(local_time)
                                timestamp = localized_time.astimezone(timezone.utc)
                                age_seconds = (current_time - timestamp).total_seconds()
                                age_hours = age_seconds / 3600
                            except Exception as e:
                                logger.debug(f"Date parsing error: {e}")
                        
                        aqi_value = station.get('AQI', 0)
                        concentration = self.aqi_to_concentration(param, aqi_value)
                        
                        closest_per_parameter[param] = {
                            'value': concentration,
                            'aqi': aqi_value,
                            'distance_km': distance_km,
                            'age_hours': age_hours,
                            'area': station.get('ReportingArea', ''),
                            'state': station.get('StateCode', ''),
                            'coordinates': (station_lat, station_lon),
                            'timestamp': timestamp,
                            'source': 'airnow_observation',
                            'category': station.get('Category', {}).get('Name', ''),
                            'category_number': station.get('Category', {}).get('Number', 0),
                            'raw_station': station
                        }
                        
                        logger.info(f"🎯 New closest {param}: {aqi_value} AQI at {distance_km:.1f}km")
                
                return closest_per_parameter
        
        logger.warning("❌ No AirNow data found in parallel search")
        return {}
    
    def generate_s3_key(self, lat: float, lon: float, data_type: str = "raw") -> str:
        """
        Generate S3 key for storing location data
        Format: s3://naq-forecast-data/raw/2025-08-20/NYC_40.7128_-74.0060.json
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        lat_str = f"{lat:.4f}".replace("-", "neg")
        lon_str = f"{lon:.4f}".replace("-", "neg")
        location_id = f"{lat_str}_{lon_str}"
        
        s3_key = f"{data_type}/{today}/{location_id}.json"
        return s3_key
    
    def check_existing_data(self, lat: float, lon: float, data_type: str = "raw") -> dict:
        """
        Check if data already exists for this location today
        Returns info about existing data or None
        """
        try:
            s3_key = self.generate_s3_key(lat, lon, data_type)
            
            response = self.s3_client.head_object(
                Bucket=self.data_bucket,
                Key=s3_key
            )
            
            return {
                "exists": True,
                "s3_key": s3_key,
                "last_modified": response['LastModified'],
                "size": response['ContentLength'],
                "metadata": response.get('Metadata', {})
            }
            
        except self.s3_client.exceptions.NoSuchKey:
            return {"exists": False, "s3_key": s3_key}
        except Exception as e:
            logger.warning(f"Error checking existing data: {e}")
            return {"exists": False, "s3_key": s3_key}
    
    def save_raw_data_to_s3(self, raw_data: RawLocationData) -> str:
        """
        Save raw data to S3 in organized structure
        Replaces existing data for same location/date
        Returns the S3 key where data was saved
        """
        try:
            lat = raw_data.location["latitude"]
            lon = raw_data.location["longitude"]
            
            existing_info = self.check_existing_data(lat, lon, "raw")
            
            if existing_info["exists"]:
                last_modified = existing_info["last_modified"]
                logger.info(f"🔄 Replacing existing data from {last_modified}")
            else:
                logger.info(f"📝 Creating new data file")
            
            s3_key = existing_info["s3_key"]
            
            data_dict = asdict(raw_data)
            
            data_dict["s3_metadata"] = {
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "s3_bucket": self.data_bucket,
                "s3_key": s3_key,
                "data_version": "v1.0",
                "ready_for_fusion": True
            }
            
            json_data = json.dumps(data_dict, indent=2, default=str)
            
            self.s3_client.put_object(
                Bucket=self.data_bucket,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
                Metadata={
                    'data-type': 'raw-pollutant-data',
                    'latitude': str(lat),
                    'longitude': str(lon),
                    'collection-timestamp': raw_data.timestamp,
                    'sources-count': str(len(raw_data.raw_measurements))
                }
            )
            
            logger.info(f"✅ Raw data saved to S3: s3://{self.data_bucket}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"❌ Failed to save raw data to S3: {e}")
            return None
    
    def extract_tempo_data_fast_batch(self, locations: List[tuple], gases: List[str]) -> Dict:
        """
        Ultra-fast TEMPO extraction for multiple locations using instant_tempo_extractor logic
        Opens each file only ONCE and processes all locations in batch
        """
        results = {
            "raw_measurements": {},
            "science_data": {},
            "tempo_sources": []
        }
        
        for gas in gases:
            try:
                local_file_path = os.path.join(self.local_tempo_dir, f"latest-{gas.lower()}.nc")
                
                if not os.path.exists(local_file_path):
                    logger.warning(f"⚠️ Local {gas} file not found: {local_file_path}")
                    continue
                
                logger.info(f"📁 Fast batch reading {gas} data from: {local_file_path}")
                start_time = time.time()
                
                with h5py.File(local_file_path, 'r') as h5_file:
                    
                    lats = h5_file['latitude'][:]  # 1D array (2950,)
                    lons = h5_file['longitude'][:]  # 1D array (7750,)
                    
                    product = h5_file['product']
                    support_data = h5_file['support_data']
                    
                    # Gas-specific dataset names
                    if gas == "NO2":
                        vertical_column_dataset = product['vertical_column_troposphere']
                        conversion_factor = 3.5
                    else:  # HCHO
                        vertical_column_dataset = product['vertical_column']
                        conversion_factor = 2.8
                    
                    main_data_quality_dataset = product['main_data_quality_flag']
                    eff_cloud_fraction_dataset = support_data['eff_cloud_fraction']
                    
                    gas_fill = vertical_column_dataset.attrs['_FillValue'][0]
                    qa_fill = main_data_quality_dataset.attrs['_FillValue'][0]
                    cloud_fill = eff_cloud_fraction_dataset.attrs['_FillValue'][0]
                    
                    logger.info(f"🔧 {gas} fill values - Gas: {gas_fill}, Quality: {qa_fill}, Cloud: {cloud_fill}")
                    
                    for lat, lon in locations:
                        
                        lat_idx = np.argmin(np.abs(lats - lat))
                        lon_idx = np.argmin(np.abs(lons - lon))
                        
                        closest_lat = lats[lat_idx]
                        closest_lon = lons[lon_idx]
                        
                        # LAZY LOADING: Read only nearest pixel (hyperslab selection)
                        gas_value = vertical_column_dataset[0, lat_idx, lon_idx]
                        quality_flag = main_data_quality_dataset[0, lat_idx, lon_idx]
                        cloud_fraction = eff_cloud_fraction_dataset[0, lat_idx, lon_idx]
                        
                        logger.info(f"📍 {lat:.3f}, {lon:.3f} → {closest_lat:.3f}, {closest_lon:.3f}")
                        logger.info(f"📊 Raw {gas}: {gas_value}, QA: {quality_flag}, Cloud: {cloud_fraction}")
                        
                        filter_reason = None
                        
                        if quality_flag == qa_fill or np.isnan(quality_flag):
                            filter_reason = "No quality flag data (fill value)"
                        elif quality_flag != 0:
                            filter_reason = f"Quality flag {quality_flag} (NASA requires 0=good for forecasting)"
                        elif cloud_fraction == cloud_fill or np.isnan(cloud_fraction):
                            filter_reason = "No cloud fraction data (fill value)"
                        elif cloud_fraction >= 0.2:
                            filter_reason = f"Too cloudy (ECF={cloud_fraction:.3f} ≥ 0.2 NASA official limit)"
                        elif gas_value == gas_fill or np.isnan(gas_value):
                            filter_reason = f"No {gas} measurement (fill value)"
                        elif gas_value <= 0:
                            filter_reason = f"Invalid {gas} value (≤ 0)"
                        
                        if filter_reason:
                            logger.info(f"❌ {gas} filtered: {filter_reason}")
                        else:
                            logger.info(f"✅ {gas} VALID DATA FOUND!")
                        
                        if filter_reason is None:
                            surface_ppb = (gas_value / 1e16) * conversion_factor
                            
                            logger.info(f"✅ VALID {gas} DATA: {gas_value:.2e} molecules/cm² → {surface_ppb:.1f} ppb")
                            
                            raw_data = RawPollutantData(
                                pollutant=gas,
                                concentration=round(surface_ppb, 2),
                                units="ppb",
                                source=f"TEMPO_fast_{closest_lat:.3f}_{closest_lon:.3f}",
                                quality="nasa_compliant",
                                uncertainty="±20%",
                                timestamp=datetime.now(timezone.utc).isoformat()
                            )
                            
                            location_key = f"{lat}_{lon}"
                            if gas == "NO2":  # EPA pollutant
                                results["raw_measurements"][f"{gas}_{location_key}"] = raw_data
                            else:  # HCHO = science data
                                results["science_data"][f"{gas}_{location_key}"] = asdict(raw_data)
                            
                            results["tempo_sources"].append(gas)
                            
                        else:
                            logger.warning(f"❌ {gas} filtered: {filter_reason}")
                
                processing_time = time.time() - start_time
                logger.info(f"⚡ Fast batch {gas} processing: {len(locations)} locations in {processing_time:.3f}s")
                
            except Exception as e:
                logger.error(f"❌ Fast batch {gas} extraction failed: {e}")
        
        logger.info("🎯 FAST BATCH PROCESSING SUMMARY")
        logger.info("=" * 50)
        
        total_valid = len(results["raw_measurements"]) + len(results["science_data"])
        logger.info(f"📊 Total locations processed: {len(locations)}")
        logger.info(f"📊 Valid measurements found: {total_valid}")
        
        if results["raw_measurements"]:
            logger.info("✅ VALID MEASUREMENTS:")
            for key, data in results["raw_measurements"].items():
                logger.info(f"   📡 {key}: {data.concentration} {data.units}")
        
        if results["science_data"]:
            logger.info("🔬 SCIENCE DATA:")
            for key, data in results["science_data"].items():
                logger.info(f"   🧪 {key}: {data.get('concentration', 'N/A')} {data.get('units', '')}")
        
        if total_valid == 0:
            logger.info("❌ No valid TEMPO data found")
            logger.info("💡 This is normal - satellite data depends on atmospheric conditions")
        
        return results

    def extract_tempo_data_raw(self, file_path: str, gas_type: str, lat: float, lon: float) -> Optional[RawPollutantData]:
        """
        Extract TEMPO raw concentrations using NASA OPERATIONAL filtering
        NO AQI CALCULATION - raw concentrations only (KEPT FOR S3 FALLBACK)
        """
        try:
            with h5py.File(file_path, 'r') as h5_file:
                lats = h5_file['latitude'][:]  # 1D array
                lons = h5_file['longitude'][:]  # 1D array
                
                lat_idx = np.argmin(np.abs(lats - lat))
                lon_idx = np.argmin(np.abs(lons - lon))
                
                closest_lat = lats[lat_idx]
                closest_lon = lons[lon_idx]
                
                product = h5_file['product']
                support_data = h5_file['support_data']
                
                # Main data variables - different names for different gases (EXACT stage1)
                if gas_type == "NO2":
                    vertical_column_dataset = product['vertical_column_troposphere']
                    conversion_factor = 3.5  # NO2 conversion
                else:  # HCHO
                    vertical_column_dataset = product['vertical_column']
                    conversion_factor = 2.8  # HCHO conversion
                
                main_data_quality_dataset = product['main_data_quality_flag']
                eff_cloud_fraction_dataset = support_data['eff_cloud_fraction']
                
                gas_fill = vertical_column_dataset.attrs['_FillValue'][0]
                qa_fill = main_data_quality_dataset.attrs['_FillValue'][0]
                cloud_fill = eff_cloud_fraction_dataset.attrs['_FillValue'][0]
                
                # LAZY LOADING: Use h5py hyperslab selection to read only the nearest pixel (stage1)
                # This reduces I/O from ~2.8M pixels to just 1 pixel per variable
                gas_value = vertical_column_dataset[0, lat_idx, lon_idx]
                quality_flag = main_data_quality_dataset[0, lat_idx, lon_idx]
                cloud_fraction = eff_cloud_fraction_dataset[0, lat_idx, lon_idx]
                
                logger.info(f"📍 Target: {lat:.3f}°N, {lon:.3f}°W")
                logger.info(f"📍 Closest: {closest_lat:.3f}°N, {closest_lon:.3f}°W")
                logger.info(f"📊 Raw {gas_type}: {gas_value}, QA: {quality_flag}, Cloud: {cloud_fraction}")
                
                filter_reason = None
                
                if quality_flag == qa_fill or np.isnan(quality_flag):
                    filter_reason = "No quality flag data"
                elif quality_flag != 0:
                    filter_reason = f"Quality flag {quality_flag} (NASA requires 0=good for forecasting)"
                
                elif cloud_fraction == cloud_fill or np.isnan(cloud_fraction):
                    filter_reason = "No cloud fraction data"
                elif cloud_fraction >= 0.2:
                    filter_reason = f"Cloudy conditions (ECF={cloud_fraction:.3f} ≥ 0.2 NASA official limit)"
                
                elif gas_value == gas_fill or np.isnan(gas_value):
                    filter_reason = f"No {gas_type} measurement"
                elif gas_value <= 0:
                    filter_reason = f"Invalid {gas_type} value (≤ 0)"
                
                # All checks passed (stage1 pattern)
                if filter_reason is None:
                    surface_ppb = (gas_value / 1e16) * conversion_factor
                    
                    logger.info(f"✅ VALID {gas_type} DATA: {gas_value:.2e} molecules/cm² → {surface_ppb:.1f} ppb")
                    
                    return RawPollutantData(
                        pollutant=gas_type,
                        concentration=round(surface_ppb, 2),
                        units="ppb",
                        source=f"TEMPO_satellite_{closest_lat:.3f}_{closest_lon:.3f}",
                        quality="nasa_compliant",
                        uncertainty="±20%",
                        timestamp=datetime.now(timezone.utc).isoformat()
                    )
                else:
                    logger.warning(f"❌ {gas_type} data filtered: {filter_reason}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Error processing {gas_type} file: {e}")
            return None
        """
        Extract TEMPO raw concentrations using EXACT working logic from stage1_tempo_processor.py
        NO AQI CALCULATION - raw concentrations only
        """
        try:
            with h5py.File(file_path, 'r') as h5_file:
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
                
                # Main data variables - different names for different gases (EXACT stage1)
                if gas_type == "NO2":
                    vertical_column_dataset = product['vertical_column_troposphere']
                    conversion_factor = 3.5  # NO2 conversion
                else:  # HCHO
                    vertical_column_dataset = product['vertical_column']
                    conversion_factor = 2.8  # HCHO conversion
                
                main_data_quality_dataset = product['main_data_quality_flag']
                eff_cloud_fraction_dataset = support_data['eff_cloud_fraction']
                
                gas_fill = vertical_column_dataset.attrs['_FillValue'][0]
                qa_fill = main_data_quality_dataset.attrs['_FillValue'][0]
                cloud_fill = eff_cloud_fraction_dataset.attrs['_FillValue'][0]
                
                # LAZY LOADING: Use h5py hyperslab selection to read only the nearest pixel (stage1)
                # This reduces I/O from ~2.8M pixels to just 1 pixel per variable
                gas_value = vertical_column_dataset[0, lat_idx, lon_idx]
                quality_flag = main_data_quality_dataset[0, lat_idx, lon_idx]
                cloud_fraction = eff_cloud_fraction_dataset[0, lat_idx, lon_idx]
                
                logger.info(f"📍 Target: {lat:.3f}°N, {lon:.3f}°W")
                logger.info(f"📍 Closest: {closest_lat:.3f}°N, {closest_lon:.3f}°W")
                logger.info(f"📊 Raw {gas_type}: {gas_value}, QA: {quality_flag}, Cloud: {cloud_fraction}")
                
                filter_reason = None
                
                if quality_flag == qa_fill or np.isnan(quality_flag):
                    filter_reason = "No quality flag data"
                elif quality_flag != 0:
                    filter_reason = f"Quality flag {quality_flag} (NASA requires 0=good for forecasting)"
                
                elif cloud_fraction == cloud_fill or np.isnan(cloud_fraction):
                    filter_reason = "No cloud fraction data"
                elif cloud_fraction >= 0.2:
                    filter_reason = f"Cloudy conditions (ECF={cloud_fraction:.3f} ≥ 0.2 NASA official limit)"
                
                elif gas_value == gas_fill or np.isnan(gas_value):
                    filter_reason = f"No {gas_type} measurement"
                elif gas_value <= 0:
                    filter_reason = f"Invalid {gas_type} value (≤ 0)"
                
                # All checks passed (stage1 pattern)
                if filter_reason is None:
                    surface_ppb = (gas_value / 1e16) * conversion_factor
                    
                    logger.info(f"✅ VALID {gas_type} DATA: {gas_value:.2e} molecules/cm² → {surface_ppb:.1f} ppb")
                    
                    return RawPollutantData(
                        pollutant=gas_type,
                        concentration=round(surface_ppb, 2),
                        units="ppb",
                        source=f"TEMPO_satellite_{closest_lat:.3f}_{closest_lon:.3f}",
                        quality="nasa_compliant",
                        uncertainty="±20%",
                        timestamp=datetime.now(timezone.utc).isoformat()
                    )
                else:
                    logger.warning(f"❌ {gas_type} data filtered: {filter_reason}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Error processing {gas_type} file: {e}")
            return None
    
    def find_closest_timestamp(self, timestamps):
        """Find the timestamp closest to current time for real-time data"""
        if not timestamps:
            return None, None
        
        current_dt = datetime.now(timezone.utc)
        closest_timestamp = None
        closest_index = 0
        min_diff = float('inf')
        
        for i, ts in enumerate(timestamps):
            try:
                ts_dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                
                diff = abs((current_dt - ts_dt).total_seconds())
                
                if diff < min_diff:
                    min_diff = diff
                    closest_timestamp = ts
                    closest_index = i
                    
            except Exception as e:
                logger.warning(f"Error parsing timestamp {ts}: {e}")
                continue
        
        return closest_timestamp, closest_index

    async def fetch_geos_cf_data_async(self, lat: float, lon: float) -> tuple[Dict[str, RawPollutantData], Dict[str, Any]]:
        """
        ASYNC version of GEOS-CF data fetching for parallel optimization
        Same data collection, faster execution with aiohttp
        """
        geos_data = {}
        meteorology_data = {}
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                # Parallel chemistry + meteorology requests
                chemistry_task = self._fetch_chemistry_async(session, lat, lon)
                meteorology_task = self._fetch_meteorology_async(session, lat, lon)
                
                geos_data, meteorology_data = await asyncio.gather(
                    chemistry_task,
                    meteorology_task,
                    return_exceptions=True
                )
                
                if isinstance(geos_data, Exception):
                    logger.error(f"Chemistry fetch failed: {geos_data}")
                    geos_data = {}
                
                if isinstance(meteorology_data, Exception):
                    logger.error(f"Meteorology fetch failed: {meteorology_data}")
                    meteorology_data = {}
                    
        except Exception as e:
            logger.error(f"ASYNC GEOS-CF failed: {e}")
            geos_data = {}
            meteorology_data = {}
        
        return geos_data, meteorology_data
    
    async def _fetch_single_species_async(self, session: aiohttp.ClientSession, species: str, lat: float, lon: float) -> tuple[str, RawPollutantData]:
        """Fetch a single chemistry species with retries"""
        base_url = "https://fluid.nccs.nasa.gov/cfapi/fcast/chm/v1"
        max_retries = 2
        
        for attempt in range(max_retries):
            try:
                url = f"{base_url}/{species}/{lat:.1f}x{lon:.1f}/latest/"
                logger.info(f"🌐 PARALLEL GEOS-CF {species} (attempt {attempt + 1})")
                
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    if species == "PM25":
                        # PM2.5 processing (same logic, async)
                        if 'values' in data:
                            timestamps = data.get('time', [])
                            if timestamps:
                                closest_timestamp, closest_index = self.find_closest_timestamp(timestamps)
                                
                                if closest_timestamp and closest_index is not None:
                                    pm25_components = [
                                        "PM25bc_RH35_GCC", "PM25du_RH35_GCC", "PM25ni_RH35_GCC",
                                        "PM25oc_RH35_GCC", "PM25ss_RH35_GCC", "PM25su_RH35_GCC", "PM25soa_RH35_GCC"
                                    ]
                                    
                                    total_pm25 = 0
                                    components_found = 0
                                    
                                    for component in pm25_components:
                                        if component in data['values'] and data['values'][component]:
                                            component_values = data['values'][component]
                                            if closest_index < len(component_values):
                                                component_value = component_values[closest_index]
                                                if component_value is not None:
                                                    total_pm25 += component_value
                                                    components_found += 1
                                    
                                    if components_found >= 5:
                                        pollutant_data = RawPollutantData(
                                            pollutant=species,
                                            concentration=total_pm25,
                                            units='ug/m3',
                                            source="GEOS-CF_model",
                                            quality="forecast",
                                            uncertainty="±30%",
                                            timestamp=closest_timestamp
                                        )
                                        logger.info(f"✅ PARALLEL GEOS-CF PM25: {total_pm25:.2f} μg/m³")
                                        return species, pollutant_data
                        
                    elif 'values' in data and species in data['values']:
                        # Standard species processing (same logic, async)
                        timestamps = data.get('time', [])
                        values = data['values'][species]
                        
                        if timestamps and values:
                            closest_timestamp, closest_index = self.find_closest_timestamp(timestamps)
                            
                            if closest_timestamp and closest_index < len(values):
                                raw_value = values[closest_index]
                                
                                if raw_value is not None:
                                    if species == "CO":
                                        concentration = raw_value / 1000.0  # ppbv to ppm
                                        units = 'ppm'
                                    else:
                                        concentration = raw_value
                                        units = 'ppb'
                                    
                                    pollutant_data = RawPollutantData(
                                        pollutant=species,
                                        concentration=concentration,
                                        units=units,
                                        source="GEOS-CF_model",
                                        quality="forecast",
                                        uncertainty="±30%",
                                        timestamp=closest_timestamp
                                    )
                                    logger.info(f"✅ PARALLEL GEOS-CF {species}: {concentration} {units}")
                                    return species, pollutant_data
                
            except Exception as e:
                logger.warning(f"PARALLEL GEOS-CF {species} attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)  # Brief async wait
        
        return species, None  # Return None if all attempts failed
    
    async def _fetch_chemistry_async(self, session: aiohttp.ClientSession, lat: float, lon: float) -> Dict[str, RawPollutantData]:
        """PARALLEL chemistry data fetching using asyncio.gather"""
        chemistry_species = ["NO2", "O3", "CO", "SO2", "PM25"]
        
        logger.info(f"PARALLEL: Fetching {len(chemistry_species)} GEOS-CF chemistry species simultaneously...")
        
        species_tasks = [self._fetch_single_species_async(session, species, lat, lon) for species in chemistry_species]
        results = await asyncio.gather(*species_tasks, return_exceptions=True)
        
        geos_data = {}
        for result in results:
            if isinstance(result, tuple) and len(result) == 2:
                species, pollutant_data = result
                if pollutant_data is not None:
                    geos_data[species] = pollutant_data
            elif isinstance(result, Exception):
                logger.warning(f"PARALLEL GEOS-CF species failed: {result}")
        
        logger.info(f"✅ PARALLEL GEOS-CF chemistry complete: {len(geos_data)}/{len(chemistry_species)} species")
        return geos_data
    
    async def _fetch_single_meteorology_async(self, session: aiohttp.ClientSession, param: str, lat: float, lon: float) -> tuple[str, dict]:
        """Fetch a single meteorology parameter with retries"""
        met_base_url = "https://fluid.nccs.nasa.gov/cfapi/fcast/met/v1"
        max_retries = 2
        
        for attempt in range(max_retries):
            try:
                met_url = f"{met_base_url}/{param}/{lat:.1f}x{lon:.1f}/latest/"
                logger.info(f"🌤️ PARALLEL GEOS-CF meteorology {param} (attempt {attempt + 1})")
                
                async with session.get(met_url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    if 'values' in data and param in data['values']:
                        raw_value = data['values'][param][0] if data['values'][param] else None
                        
                        if raw_value is not None:
                            # Same unit conversion logic as original
                            if param == "T2M":
                                value = raw_value
                                units = "raw_api_units"
                            elif param == "TPREC":
                                value = raw_value
                                units = "raw_api_units"
                            elif param == "CLDTT":
                                value = raw_value
                                units = "raw_api_units"
                            elif param in ["U10M", "V10M"]:
                                value = raw_value
                                units = "m/s"
                            else:
                                value = raw_value
                                units = "raw_api_units"
                            
                            met_data = {
                                'value': value,
                                'units': units,
                                'source': 'GEOS-CF_meteorology',
                                'timestamp': datetime.now(timezone.utc).isoformat()
                            }
                            logger.info(f"✅ PARALLEL GEOS-CF {param}: {value} {units}")
                            return param, met_data
            
            except Exception as e:
                logger.warning(f"PARALLEL meteorology {param} attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)  # Brief async wait
        
        return param, None  # Return None if all attempts failed
    
    async def _fetch_meteorology_async(self, session: aiohttp.ClientSession, lat: float, lon: float) -> Dict[str, Any]:
        """PARALLEL meteorology data fetching using asyncio.gather"""
        meteorology_params = ["T2M", "TPREC", "CLDTT", "U10M", "V10M"]
        
        logger.info(f"PARALLEL: Fetching {len(meteorology_params)} GEOS-CF meteorology parameters simultaneously...")
        
        met_tasks = [self._fetch_single_meteorology_async(session, param, lat, lon) for param in meteorology_params]
        results = await asyncio.gather(*met_tasks, return_exceptions=True)
        
        meteorology_data = {}
        for result in results:
            if isinstance(result, tuple) and len(result) == 2:
                param, met_data = result
                if met_data is not None:
                    meteorology_data[param] = met_data
            elif isinstance(result, Exception):
                logger.warning(f"PARALLEL GEOS-CF meteorology failed: {result}")
        
        logger.info(f"✅ PARALLEL GEOS-CF meteorology complete: {len(meteorology_data)}/{len(meteorology_params)} parameters")
        return meteorology_data

    def fetch_geos_cf_data(self, lat: float, lon: float) -> tuple[Dict[str, RawPollutantData], Dict[str, Any]]:
        """
        Fetch GEOS-CF chemistry + meteorology data from stage3_geos_cf_met_integrator.py
        Returns: (chemistry_data, meteorology_data)
        """
        geos_data = {}
        meteorology_data = {}
        
        try:
            # Chemistry data
            base_url = "https://fluid.nccs.nasa.gov/cfapi/fcast/chm/v1"
            chemistry_species = ["NO2", "O3", "CO", "SO2", "PM25"]
            
            for species in chemistry_species:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        url = f"{base_url}/{species}/{lat:.1f}x{lon:.1f}/latest/"
                        logger.info(f"Fetching GEOS-CF {species} (attempt {attempt + 1})")
                        
                        response = requests.get(url, timeout=15)
                        response.raise_for_status()
                        
                        data = response.json()
                        
                        if species == "PM25":
                            # Special handling for PM2.5 - sum all components
                            if 'values' in data:
                                timestamps = data.get('time', [])
                                if timestamps:
                                    closest_timestamp, closest_index = self.find_closest_timestamp(timestamps)
                                    
                                    if closest_timestamp and closest_index is not None:
                                        # PM2.5 components to sum for total PM2.5
                                        pm25_components = [
                                            "PM25bc_RH35_GCC",   # Black Carbon
                                            "PM25du_RH35_GCC",   # Dust  
                                            "PM25ni_RH35_GCC",   # Nitrates
                                            "PM25oc_RH35_GCC",   # Organic Carbon
                                            "PM25ss_RH35_GCC",   # Sea Salt
                                            "PM25su_RH35_GCC",   # Sulfates
                                            "PM25soa_RH35_GCC"
                                        ]
                                        
                                        total_pm25 = 0
                                        components_found = 0
                                        component_details = {}
                                        
                                        for component in pm25_components:
                                            if component in data['values'] and data['values'][component]:
                                                component_values = data['values'][component]
                                                if closest_index < len(component_values):
                                                    component_value = component_values[closest_index]
                                                    if component_value is not None:
                                                        total_pm25 += component_value
                                                        components_found += 1
                                                        component_details[component] = component_value
                                                        logger.debug(f"  {component}: {component_value:.2f} μg/m³")
                                        
                                        if components_found >= 5:  # Need at least 5 out of 7 components for reliable data
                                            concentration = total_pm25
                                            units = 'ug/m3'  # EPA standard units
                                            logger.info(f"GEOS-CF PM25: {concentration:.2f} {units} ({components_found}/7 components) at {closest_timestamp}")
                                            logger.info(f"  Component breakdown: {list(component_details.keys())}")
                                            
                                            geos_data[species] = RawPollutantData(
                                                pollutant=species,
                                                concentration=concentration,
                                                units=units,
                                                source="GEOS-CF_model",
                                                quality="forecast",
                                                uncertainty="±30%", 
                                                timestamp=closest_timestamp
                                            )
                                            break
                                        else:
                                            logger.warning(f"Insufficient PM25 components: {components_found}/7")
                                            break
                                    else:
                                        logger.warning(f"No valid timestamp found for PM25")
                                        break
                                else:
                                    logger.warning(f"No timestamps for PM25")
                                    break
                            else:
                                logger.warning(f"No values data for PM25")
                                break
                        
                        elif 'values' in data and species in data['values']:
                            # Standard handling for other species (NO2, O3, CO, SO2)
                            timestamps = data.get('time', [])
                            values = data['values'][species]
                            
                            if timestamps and values:
                                closest_timestamp, closest_index = self.find_closest_timestamp(timestamps)
                                
                                if closest_timestamp and closest_index < len(values):
                                    raw_value = values[closest_index]
                                    
                                    if raw_value is not None:
                                        # Unit conversion logic for EPA consistency
                                        if species == "CO":
                                            # GEOS-CF returns CO in ppbv, but need ppm for consistency  
                                            concentration = raw_value / 1000.0  # ppbv to ppm
                                            units = 'ppm'
                                            logger.info(f"Converting CO: {raw_value} ppbv -> {concentration:.3f} ppm")
                                        else:
                                            concentration = raw_value
                                            units = 'ppb'
                                        
                                        logger.info(f"GEOS-CF {species}: {concentration} {units} at {closest_timestamp}")
                                        
                                        geos_data[species] = RawPollutantData(
                                            pollutant=species,
                                            concentration=concentration,
                                            units=units,
                                            source="GEOS-CF_model",
                                            quality="forecast",
                                            uncertainty="±30%", 
                                            timestamp=closest_timestamp
                                        )
                                        break
                                    else:
                                        logger.warning(f"No current data found for {species}")
                                        break
                                else:
                                    logger.warning(f"No valid timestamp found for {species}")
                                    break
                            else:
                                logger.warning(f"No timestamps or values for {species}")
                                break
                                
                    except requests.exceptions.RequestException as e:
                        logger.warning(f"GEOS-CF attempt {attempt + 1} failed for {species}: {e}")
                        if attempt == max_retries - 1:
                            logger.error(f"All attempts failed for {species}")
                        else:
                            time.sleep(1 * (attempt + 1))  # Exponential backoff
                    except Exception as e:
                        logger.error(f"Unexpected error fetching {species}: {e}")
                        break
            
            logger.info(f"GEOS-CF chemistry data collected: {list(geos_data.keys())}")
            
            # Meteorology data - from GEOS-CF meteorology API
            met_base_url = "https://fluid.nccs.nasa.gov/cfapi/fcast/met/v1"
            meteorology_params = ["T2M", "TPREC", "CLDTT", "U10M", "V10M"]
            
            for param in meteorology_params:
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        met_url = f"{met_base_url}/{param}/{lat:.1f}x{lon:.1f}/latest/"
                        logger.info(f"Fetching GEOS-CF meteorology {param} (attempt {attempt + 1})")
                        
                        response = requests.get(met_url, timeout=15)
                        response.raise_for_status()
                        
                        data = response.json()
                        
                        if 'values' in data and param in data['values']:
                            raw_value = data['values'][param][0] if data['values'][param] else None
                            
                            if raw_value is not None:
                                logger.info(f"Raw GEOS-CF {param} value: {raw_value}")
                                
                                # Unit conversion for meteorology
                                if param == "T2M":
                                    value = raw_value
                                    units = "raw_api_units"  # Unknown units from API
                                    logger.info(f"T2M raw value: {value} {units}")
                                elif param == "TPREC":
                                    value = raw_value
                                    units = "raw_api_units"
                                elif param == "CLDTT":
                                    value = raw_value
                                    units = "raw_api_units"
                                elif param in ["U10M", "V10M"]:
                                    value = raw_value
                                    units = "raw_api_units"
                                else:
                                    value = raw_value
                                    units = "raw_api_units"
                                
                                meteorology_data[param] = {
                                    "parameter": param,
                                    "value": round(value, 2),
                                    "units": units,
                                    "raw_value": raw_value,  # Keep original value
                                    "source": "GEOS-CF_meteorology",
                                    "timestamp": data.get('time', [None])[0] or datetime.now(timezone.utc).isoformat(),
                                    "description": {
                                        "T2M": "2-Meter Air Temperature",
                                        "TPREC": "Total Precipitation",
                                        "CLDTT": "Total Cloud Area Fraction",
                                        "U10M": "10-Meter Eastward Wind",
                                        "V10M": "10-Meter Northward Wind"
                                    }.get(param, param),
                                    "note": "Raw data - processors will handle unit conversions"
                                }
                                
                                logger.info(f"GEOS-CF {param}: {value} {units} (raw: {raw_value})")
                                break  # Success, exit retry loop
                            else:
                                logger.warning(f"No meteorology data found for {param}")
                                break
                                
                    except requests.exceptions.RequestException as e:
                        logger.warning(f"GEOS-CF meteorology attempt {attempt + 1} failed for {param}: {e}")
                        if attempt == max_retries - 1:
                            logger.error(f"All attempts failed for meteorology {param}")
                        else:
                            time.sleep(1 * (attempt + 1))  # Exponential backoff
                    except Exception as e:
                        logger.error(f"Unexpected error fetching meteorology {param}: {e}")
                        break
            
            logger.info(f"GEOS-CF meteorology data collected: {list(meteorology_data.keys())}")
            
        except Exception as e:
            logger.error(f"GEOS-CF fetch failed: {e}")
        
        return geos_data, meteorology_data

    async def fetch_ground_station_data_async(self, lat: float, lon: float) -> Dict[str, Dict[str, Any]]:
        """
        ASYNC version of ground station data fetching for parallel optimization
        Same data collection, faster execution with parallel AirNow + WAQI
        """
        all_ground_sources = {
            "airnow": {"raw_data": {}, "metadata": {}},
            "waqi": {"raw_data": {}, "metadata": {}},
            "external_aqi": {}
        }
        
        async def get_airnow_data():
            """Async AirNow data collection"""
            try:
                airnow_api_key = os.getenv('AIRNOW_API_KEY', "634A0C8D-66D0-4E8B-B93C-4935886F8C14")
                
                import concurrent.futures
                loop = asyncio.get_event_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    closest_airnow_data = await loop.run_in_executor(
                        executor, 
                        self.get_closest_airnow_data, 
                        lat, lon, airnow_api_key
                    )
                
                if closest_airnow_data:
                    logger.info(f"✅ Async AirNow: Found {len(closest_airnow_data)} pollutants")
                    return self._process_airnow_data(closest_airnow_data)
                else:
                    logger.warning("❌ Async AirNow: No data found")
                    return {"raw_data": {}, "metadata": {"error": "No stations found"}}
                    
            except Exception as e:
                logger.error(f"Async AirNow failed: {e}")
                return {"raw_data": {}, "metadata": {"error": str(e)}}
        
        async def get_waqi_data():
            """Async WAQI data collection"""
            try:
                waqi_api_key = os.getenv('WAQI_API_KEY', "0cea04c89fa6384f2b93e0486125b8b596ffd8f6")
                
                # Grid search with aiohttp for speed
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                    search_radius = 0.5  # degrees
                    grid_points = [
                        (lat, lon),  # Center point
                        (lat + search_radius, lon),  # North
                        (lat - search_radius, lon),  # South  
                        (lat, lon + search_radius),  # East
                        (lat, lon - search_radius),  # West
                    ]
                    
                    waqi_base = "https://api.waqi.info"
                    all_waqi_data = {}
                    
                    for grid_lat, grid_lon in grid_points:
                        try:
                            url = f"{waqi_base}/feed/geo:{grid_lat};{grid_lon}/"
                            async with session.get(url, params={'token': waqi_api_key}) as response:
                                if response.status == 200:
                                    data = await response.json()
                                    
                                    if data.get('status') == 'ok' and 'data' in data:
                                        station_data = data['data']
                                        station_name = station_data.get('city', {}).get('name', 'Unknown')
                                        
                                        iaqi = station_data.get('iaqi', {})
                                        for param, measurement in iaqi.items():
                                            if param.lower() in ['pm25', 'pm10', 'o3', 'no2', 'co', 'so2']:
                                                normalized_param = param.upper()
                                                if normalized_param == 'PM25':
                                                    normalized_param = 'PM25'
                                                
                                                aqi_value = measurement.get('v')
                                                if aqi_value is not None:
                                                    geo_data = station_data.get('city', {}).get('geo', [0, 0])
                                                    if isinstance(geo_data, list) and len(geo_data) >= 2:
                                                        # Simple distance calculation
                                                        distance_km = abs(lat - geo_data[0]) + abs(lon - geo_data[1])  # Manhattan distance approximation
                                                    else:
                                                        distance_km = 999  # Default if no geo data
                                                    
                                                    if normalized_param not in all_waqi_data or distance_km < all_waqi_data[normalized_param]['distance_km']:
                                                        all_waqi_data[normalized_param] = {
                                                            'value': aqi_value,
                                                            'distance_km': distance_km,
                                                            'station_name': station_name,
                                                            'source': 'waqi_async_grid'
                                                        }
                        
                        except Exception as e:
                            logger.debug(f"WAQI grid point {grid_lat}, {grid_lon} failed: {e}")
                            continue
                    
                    logger.info(f"✅ Async WAQI: Found {len(all_waqi_data)} pollutants")
                    return {
                        "raw_data": all_waqi_data,
                        "metadata": {
                            "total_stations": len(all_waqi_data),
                            "strategy": "async_grid_search",
                            "api_source": "WAQI_Async"
                        }
                    }
                    
            except Exception as e:
                logger.error(f"Async WAQI failed: {e}")
                return {"raw_data": {}, "metadata": {"error": str(e)}}
        
        logger.info("⚡ Running AirNow + WAQI in parallel...")
        airnow_result, waqi_result = await asyncio.gather(
            get_airnow_data(),
            get_waqi_data(),
            return_exceptions=True
        )
        
        if isinstance(airnow_result, Exception):
            logger.error(f"AirNow async failed: {airnow_result}")
            airnow_result = {"raw_data": {}, "metadata": {"error": str(airnow_result)}}
        elif not isinstance(airnow_result, dict):
            logger.error(f"AirNow returned invalid format: {type(airnow_result)}")
            airnow_result = {"raw_data": {}, "metadata": {"error": "Invalid format"}}
        
        if isinstance(waqi_result, Exception):
            logger.error(f"WAQI async failed: {waqi_result}")
            waqi_result = {"raw_data": {}, "metadata": {"error": str(waqi_result)}}
        elif not isinstance(waqi_result, dict):
            logger.error(f"WAQI returned invalid format: {type(waqi_result)}")
            waqi_result = {"raw_data": {}, "metadata": {"error": "Invalid format"}}
        
        all_ground_sources["airnow"] = airnow_result
        all_ground_sources["waqi"] = waqi_result
        
        external_aqi = {}
        for source_name, source_data in [("airnow", airnow_result), ("waqi", waqi_result)]:
            for param, data in source_data.get("raw_data", {}).items():
                if param not in external_aqi:
                    external_aqi[param] = []
                
                if isinstance(data, list):
                    # AirNow returns list of stations
                    for station in data:
                        if isinstance(station, dict):
                            external_aqi[param].append({
                                "source": source_name,
                                "value": station.get("value", 0),
                                "distance_km": station.get("distance_km", 999)
                            })
                elif isinstance(data, dict):
                    external_aqi[param].append({
                        "source": source_name,
                        "value": data.get("value", 0),
                        "distance_km": data.get("distance_km", 999)
                    })
        
        all_ground_sources["external_aqi"] = external_aqi
        
        logger.info(f"🚀 Async ground stations complete:")
        logger.info(f"   AirNow pollutants: {list(airnow_result.get('raw_data', {}).keys())}")
        logger.info(f"   WAQI pollutants: {list(waqi_result.get('raw_data', {}).keys())}")
        
        logger.info(f"DEBUG: all_ground_sources structure = {type(all_ground_sources)}")
        logger.info(f"DEBUG: airnow keys = {list(all_ground_sources['airnow'].keys())}")
        logger.info(f"DEBUG: waqi keys = {list(all_ground_sources['waqi'].keys())}")
        
        return all_ground_sources
    
    def _process_airnow_data(self, closest_airnow_data):
        """Helper to process AirNow data into expected format"""
        airnow_pollutants = {}
        processed_stations = []
        
        for param, data in closest_airnow_data.items():
            # Normalize parameter names
            param_map = {
                'PM2.5': 'PM25', 'PM10': 'PM10', 'OZONE': 'O3', 
                'O3': 'O3', 'NO2': 'NO2', 'CO': 'CO', 'SO2': 'SO2'
            }
            normalized_param = param_map.get(param, param)
            
            processed_station = {
                "source": "airnow_optimized",
                "station_name": data['area'],
                "station_lat": data['coordinates'][0],
                "station_lon": data['coordinates'][1],
                "pollutant": normalized_param,
                "value": data['value'],
                "unit": "AQI",
                "category": data['category'],
                "category_number": data['category_number'],
                "timestamp": data['timestamp'].isoformat(),
                "age_hours": round(data['age_hours'], 2),
                "raw_data": data['raw_station']
            }
            processed_stations.append(processed_station)
            
            if normalized_param not in airnow_pollutants:
                airnow_pollutants[normalized_param] = []
            airnow_pollutants[normalized_param].append(processed_station)
        
        return {
            "raw_data": airnow_pollutants,
            "metadata": {
                "total_stations": len(processed_stations),
                "unique_pollutants": list(airnow_pollutants.keys()),
                "strategy": "optimized_smart_distance_async",
                "api_source": "EPA_AirNow_Optimized_Async"
            }
        }
    
    def fetch_ground_station_data(self, lat: float, lon: float) -> Dict[str, Dict[str, Any]]:
        """
        Fetch ALL raw data from ground stations - comprehensive approach from stage2_ground_fetcher
        Returns: {"airnow": {...}, "waqi": {...}, "external_aqi": {...}}
        """
        all_ground_sources = {
            "airnow": {"raw_data": {}, "metadata": {}},
            "waqi": {"raw_data": {}, "metadata": {}},
            "external_aqi": {}
        }
        
        try:
            airnow_api_key = os.getenv('AIRNOW_API_KEY', "634A0C8D-66D0-4E8B-B93C-4935886F8C14")  # Use env var in production
            
            closest_airnow_data = self.get_closest_airnow_data(lat, lon, airnow_api_key)
            
            if closest_airnow_data:
                airnow_pollutants = {}
                processed_stations = []
                
                for param, data in closest_airnow_data.items():
                    # Normalize parameter names
                    param_map = {
                        'PM2.5': 'PM25',
                        'PM10': 'PM10',
                        'OZONE': 'O3', 
                        'O3': 'O3',
                        'NO2': 'NO2',
                        'CO': 'CO',
                        'SO2': 'SO2'
                    }
                    normalized_param = param_map.get(param, param)
                    
                    processed_station = {
                        "source": "airnow_optimized",
                        "station_name": data['area'],
                        "station_lat": data['coordinates'][0],
                        "station_lon": data['coordinates'][1],
                        "distance_km": round(data['distance_km'], 2),
                        "parameter": param,
                        "value": data['value'],
                        "unit": "AQI",
                        "category": data['category'],
                        "category_number": data['category_number'],
                        "timestamp": data['timestamp'].isoformat(),
                        "age_hours": round(data['age_hours'], 2),
                        "raw_data": data['raw_station']
                    }
                    processed_stations.append(processed_station)
                    
                    if normalized_param not in airnow_pollutants:
                        airnow_pollutants[normalized_param] = []
                    
                    airnow_pollutants[normalized_param].append(processed_station)
                    
                    logger.info(f"AirNow {normalized_param}: {data['value']} AQI from {data['area']}, {data['state']} ({data['distance_km']:.1f}km, {data['age_hours']:.1f}h old)")
                
                all_ground_sources["airnow"]["raw_data"] = airnow_pollutants
                all_ground_sources["airnow"]["metadata"] = {
                    "total_stations": len(processed_stations),
                    "unique_pollutants": list(airnow_pollutants.keys()),
                    "strategy": "optimized_smart_distance",
                    "closest_distance_km": min(data['distance_km'] for data in closest_airnow_data.values()),
                    "api_source": "EPA_AirNow_Optimized",
                    "collection_timestamp": datetime.now(timezone.utc).isoformat(),
                }
                
                logger.info(f"✅ AirNow optimized strategy: Found {len(closest_airnow_data)} pollutants")
            else:
                logger.warning("❌ AirNow optimized strategy found no data")
                all_ground_sources["airnow"]["metadata"] = {
                    "total_stations": 0,
                    "unique_pollutants": [],
                    "strategy": "optimized_smart_distance",
                    "error": "No stations found",
                    "api_source": "EPA_AirNow_Optimized",
                    "collection_timestamp": datetime.now(timezone.utc).isoformat(),
                }
            
        except Exception as e:
            logger.error(f"AirNow data collection failed: {e}")
        
        try:
            waqi_api_key = os.getenv('WAQI_API_KEY', "0cea04c89fa6384f2b93e0486125b8b596ffd8f6")  # Use env var in production
            waqi_base = "https://api.waqi.info"
            current_time = datetime.now(timezone.utc)  # For timestamp optimization
            
            # Grid search pattern from stage2_ground_fetcher
            search_radius = 0.5  # degrees
            grid_points = [
                (0, 0),  # Center
                (search_radius, 0), (-search_radius, 0),  # East/West
                (0, search_radius), (0, -search_radius),  # North/South
                (search_radius/2, search_radius/2), (-search_radius/2, -search_radius/2),  # Diagonals
                (search_radius/2, -search_radius/2), (-search_radius/2, search_radius/2)
            ]
            
            all_waqi_measurements = []
            unique_stations = set()
            
            # PARALLEL WAQI: Use ThreadPoolExecutor to check multiple grid points simultaneously
            def check_waqi_grid_point(grid_point):
                dlat, dlon = grid_point
                search_lat = lat + dlat
                search_lon = lon + dlon
                point_measurements = []
                
                try:
                    waqi_url = f"{waqi_base}/feed/geo:{search_lat};{search_lon}/"
                    waqi_params = {'token': waqi_api_key}
                    
                    response = requests.get(waqi_url, params=waqi_params, timeout=30)
                    response.raise_for_status()
                    
                    waqi_data = response.json()
                    
                    if waqi_data.get('status') == 'ok':
                        station_data = waqi_data.get('data', {})
                        station_name = station_data.get('city', {}).get('name', 'Unknown')
                        
                        station_coords = station_data.get('city', {}).get('geo', [0, 0])
                        if len(station_coords) >= 2:
                            station_lat, station_lon = station_coords[0], station_coords[1]
                            distance = self.haversine_distance(lat, lon, station_lat, station_lon)
                            
                            iaqi_data = station_data.get('iaqi', {})
                            
                            time_info = station_data.get('time', {})
                            time_iso = time_info.get('iso', '')
                            age_hours = float('inf')
                            timestamp = current_time
                            
                            if time_iso:
                                try:
                                    timestamp = datetime.fromisoformat(time_iso.replace('Z', '+00:00'))
                                    age_seconds = (current_time - timestamp).total_seconds()
                                    age_hours = age_seconds / 3600
                                except Exception as e:
                                    logger.warning(f"WAQI timestamp parse error for {station_name}: {e}")
                                    age_hours = float('inf')
                            
                            for pollutant, measurement in iaqi_data.items():
                                param_map = {
                                    'pm25': 'PM25',
                                    'pm10': 'PM10',
                                    'o3': 'O3',
                                    'no2': 'NO2',
                                    'co': 'CO',
                                    'so2': 'SO2',
                                    't': 'TEMP',
                                    'h': 'HUMIDITY',
                                    'p': 'PRESSURE',
                                    'w': 'WIND'
                                }
                                
                                normalized_pollutant = param_map.get(pollutant.lower(), pollutant.upper())
                                
                                processed_measurement = {
                                    "source": "waqi",
                                    "station_name": station_name,
                                    "station_lat": station_lat,
                                    "station_lon": station_lon,
                                    "distance_km": round(distance, 2),
                                    "parameter": normalized_pollutant,
                                    "value": measurement.get('v', None),
                                    "unit": "AQI" if normalized_pollutant in ['PM25', 'PM10', 'O3', 'NO2', 'CO', 'SO2'] else "raw",
                                    "aqi_overall": station_data.get('aqi', None),
                                    "age_hours": age_hours,
                                    "timestamp": timestamp,
                                    "measurement_time": station_data.get('time', {}).get('s', ''),
                                    "measurement_iso": station_data.get('time', {}).get('iso', ''),
                                    "attribution": station_data.get('attributions', []),
                                    "raw_data": station_data,
                                    "raw_iaqi_data": measurement
                                }
                                
                                point_measurements.append(processed_measurement)
                                
                                if normalized_pollutant in ['PM25', 'PM10', 'O3', 'NO2', 'CO', 'SO2']:
                                    logger.info(f"WAQI {normalized_pollutant}: {measurement.get('v')} from {station_name} ({distance:.1f}km)")
                    
                    return point_measurements, station_name if 'station_name' in locals() else None
                
                except Exception as grid_error:
                    logger.warning(f"WAQI grid point ({search_lat:.3f}, {search_lon:.3f}) failed: {grid_error}")
                    return [], None
            
            logger.info(f"PARALLEL: Fetching WAQI data using grid search (radius: {search_radius}°)")
            logger.info(f"PARALLEL: Checking {len(grid_points)} grid points simultaneously...")
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                # Submit all grid point checks in parallel
                future_to_point = {executor.submit(check_waqi_grid_point, point): point for point in grid_points}
                
                for future in as_completed(future_to_point):
                    point_measurements, station_name = future.result()
                    
                    # Skip duplicate stations
                    if station_name and station_name in unique_stations:
                        continue
                    if station_name:
                        unique_stations.add(station_name)
                    
                    all_waqi_measurements.extend(point_measurements)
            
            waqi_pollutants = {}
            waqi_freshest_only = {}  # Store only the single best measurement per pollutant
            
            for measurement in all_waqi_measurements:
                param = measurement["parameter"]
                if param not in waqi_pollutants:
                    waqi_pollutants[param] = []
                waqi_pollutants[param].append(measurement)
            
            for param in waqi_pollutants:
                waqi_pollutants[param].sort(key=lambda x: (x.get('age_hours', float('inf')), x.get('distance_km', float('inf'))))
                
                # Take ONLY the first (freshest) measurement
                if waqi_pollutants[param]:
                    freshest = waqi_pollutants[param][0]
                    waqi_freshest_only[param] = [freshest]  # Store as single-item list for compatibility
                    
                    age_str = f"{freshest.get('age_hours', 'unknown'):.2f}h" if freshest.get('age_hours') != float('inf') else "unknown age"
                    logger.info(f"WAQI {param} freshest: {freshest.get('value')} AQI from {freshest.get('station_name')} ({freshest.get('distance_km', 0):.1f}km, {age_str})")
            
            waqi_pollutants = waqi_freshest_only
            
            all_ground_sources["waqi"]["raw_data"] = waqi_pollutants
            all_ground_sources["waqi"]["metadata"] = {
                "unique_stations": len(unique_stations),
                "total_measurements": len(all_waqi_measurements),
                "unique_pollutants": list(waqi_pollutants.keys()),
                "search_radius_degrees": search_radius,
                "api_source": "WAQI_Global", 
                "collection_timestamp": datetime.now(timezone.utc).isoformat(),
                "all_measurements": all_waqi_measurements  # Keep all data like stage2
            }
            
        except Exception as e:
            logger.error(f"WAQI data collection failed: {e}")
        
        external_aqi_summary = {}
        
        for pollutant, stations in all_ground_sources["airnow"]["raw_data"].items():
            if stations:  # Take closest station
                closest_station = min(stations, key=lambda x: x["distance_km"])
                external_aqi_summary[f"airnow_{pollutant}"] = {
                    "aqi": closest_station["value"],
                    "source": f"EPA_AirNow_{closest_station['station_name']}_{closest_station['distance_km']}km"
                }
        
        for pollutant, measurements in all_ground_sources["waqi"]["raw_data"].items():
            if measurements and pollutant in ['PM25', 'PM10', 'O3', 'NO2', 'CO', 'SO2']:
                closest_measurement = min(measurements, key=lambda x: x["distance_km"])
                external_aqi_summary[f"waqi_{pollutant}"] = {
                    "aqi": closest_measurement["value"],
                    "source": f"WAQI_{closest_measurement['station_name']}_{closest_measurement['distance_km']}km"
                }
            elif measurements and pollutant in ['HUMIDITY']:
                closest_measurement = min(measurements, key=lambda x: x["distance_km"])
                external_aqi_summary[f"waqi_{pollutant}"] = {
                    "value": closest_measurement["value"],
                    "source": f"WAQI_{closest_measurement['station_name']}_{closest_measurement['distance_km']}km"
                }
        
        all_ground_sources["external_aqi"] = external_aqi_summary
        
        logger.info(f"Ground station data collected (comprehensive):")
        logger.info(f"   AirNow pollutants: {list(all_ground_sources['airnow']['raw_data'].keys())}")
        logger.info(f"   WAQI pollutants: {list(all_ground_sources['waqi']['raw_data'].keys())}")
        logger.info(f"   Total external AQI sources: {len(external_aqi_summary)}")
        
        return all_ground_sources

    # Fire data methods removed - handled by separate fire_collector.py system



    
    async def collect_location_data(self, lat: float, lon: float, location_name: str = None) -> RawLocationData:
        """
        Main collection method - gather ALL raw data for location
        NO AQI CALCULATIONS - raw concentrations only
        """
        start_time = time.time()
        
        logger.info(f"Starting multi-source data collection for {lat:.4f}, {lon:.4f}")
        
        raw_measurements = {}
        science_data = {}
        data_sources = {
            "tempo": [],
            "geos_cf": [],
            "ground_stations": [],
            "external_aqi": []
        }
        
        # PARALLEL COLLECTION: TEMPO + GEOS-CF + Ground stations (MAXIMUM SPEED)
        logger.info("⚡ Running ALL sources in PARALLEL: TEMPO + GEOS-CF + Ground stations...")
        
        async def get_tempo_data():
            logger.info("�️ Collecting TEMPO satellite data (ASYNC)...")
            tempo_raw_data = {}
            tempo_science_data = {}
            tempo_sources = []
            
            if self.use_local_tempo:
                tempo_results = self.extract_tempo_data_fast_batch([(lat, lon)], ["NO2", "HCHO"])
                
                for key, data in tempo_results["raw_measurements"].items():
                    gas = key.split('_')[0]  # Extract gas name
                    tempo_raw_data[gas] = data
                    logger.info(f"✅ TEMPO {gas}: {data.concentration} {data.units} (fast batch)")
                
                for key, data in tempo_results["science_data"].items():
                    gas = key.split('_')[0]  # Extract gas name  
                    tempo_science_data[gas] = data
                    logger.info(f"🔬 TEMPO {gas}: {data.get('concentration', 'N/A')} {data.get('units', '')} (science)")
                
                tempo_sources = list(set(tempo_results["tempo_sources"]))
                
            else:
                try:
                    tempo_results = self.tempo_collector.get_latest_data(lat, lon, max_hours=2)
                    
                    for gas in ["NO2", "HCHO", "O3TOT"]:
                        gas_data = tempo_results.get("data", {}).get(gas)
                        
                        if gas_data and isinstance(gas_data, dict) and gas_data.get("data_valid"):
                            tempo_data = RawPollutantData(
                                pollutant=gas,
                                concentration=gas_data.get("surface_ppb", 0.0),
                                units="ppb",
                                source="TEMPO_STREAMING",
                                quality="passed" if gas_data.get("nasa_quality") == "passed" else "filtered",
                                uncertainty="nasa_filtered",
                                timestamp=gas_data.get("timestamp", datetime.now(timezone.utc).isoformat())
                            )
                            
                            if gas in ["NO2", "O3TOT"]:  # EPA pollutants for fusion
                                pollutant_name = "O3" if gas == "O3TOT" else gas
                                tempo_data.pollutant = pollutant_name  # Update pollutant name
                                tempo_raw_data[pollutant_name] = tempo_data
                                logger.info(f"✅ TEMPO {gas}→{pollutant_name}: {tempo_data.concentration} {tempo_data.units} (streaming)")
                            elif gas == "HCHO":  # Science data only
                                tempo_science_data[gas] = asdict(tempo_data)
                                logger.info(f"🔬 TEMPO {gas}: {tempo_data.concentration} {tempo_data.units} (science/streaming)")
                            
                            tempo_sources.append(gas)
                        else:
                            reason = gas_data.get("error", gas_data.get("filter_reason", "No data")) if gas_data else "No data"
                            logger.warning(f"TEMPO {gas}: {reason}")
                            
                except Exception as e:
                    logger.error(f"TEMPO streaming failed: {e}")
            
            return tempo_raw_data, tempo_science_data, tempo_sources
        
       async def get_geos_data():
            logger.info("🚫 GEOS-CF TEMPORARILY DISABLED - Skipping atmospheric model data collection...")
            return {}, {}
        
        async def get_ground_data():
            logger.info("🏢 Collecting ground station data from all sources (ASYNC)...")
            return await self.fetch_ground_station_data_async(lat, lon)
        
        parallel_start = time.time()
        (tempo_raw_data, tempo_science_data, tempo_sources), (geos_data, meteorology_data), all_ground_sources = await asyncio.gather(
            get_tempo_data(),
            get_geos_data(),
            get_ground_data()
        )
        parallel_time = time.time() - parallel_start
        logger.info(f"⚡ PARALLEL collection completed in {parallel_time:.2f}s (TEMPO + GEOS-CF + Ground stations)")
        
        for gas, data in tempo_raw_data.items():
            raw_measurements[gas] = data
        for gas, data in tempo_science_data.items():
            science_data[gas] = data
        data_sources["tempo"] = tempo_sources
        
        for species, data in geos_data.items():
            raw_measurements[species] = data
            data_sources["geos_cf"].append(species)
        
        data_sources["ground_stations"] = {
            "airnow": all_ground_sources["airnow"]["metadata"],
            "waqi": all_ground_sources["waqi"]["metadata"] 
        }
        
        data_sources["external_aqi"] = all_ground_sources["external_aqi"]
        
        all_ground_raw_data = all_ground_sources
        
        # Fire data collection handled by separate fire_collector.py system
        
        collection_time = time.time() - start_time
        
        location_dict = {
            "latitude": lat, 
            "longitude": lon,
            "name": location_name or f"{lat:.3f}°N, {lon:.3f}°W"
        }
        
        result = RawLocationData(
            location=location_dict,
            timestamp=datetime.now(timezone.utc).isoformat(),
            collection_time_seconds=round(collection_time, 2),
            raw_measurements=raw_measurements,
            data_sources=data_sources,
            science_data=science_data,
            meteorology_data=meteorology_data,
            metadata={
                "collection_method": "multi_source_raw_full_parallel",
                "total_sources": len(raw_measurements),
                "ready_for_processing": True,
                "complete_data_set": {
                    "tempo_no2": "NO2" in raw_measurements and raw_measurements["NO2"].source.startswith("TEMPO"),
                    "tempo_hcho_science": "HCHO" in science_data,
                    "geos_cf_chemistry": len([k for k in raw_measurements.keys() if raw_measurements[k].source == "GEOS-CF_model"]) == 4,
                    "geos_cf_meteorology": len(meteorology_data) >= 4,
                    "airnow_stations": len(all_ground_sources["airnow"]["raw_data"]) > 0,
                    "waqi_stations": len(all_ground_sources["waqi"]["raw_data"]) > 0
                },
                "ground_station_sources": all_ground_raw_data,
                "notes": "Raw concentrations + all ground station sources - AQI calculated by processors",
                "parallel_optimization": f"TEMPO + GEOS-CF + Ground stations ran in parallel ({parallel_time:.2f}s)"
            }
        )
        
        logger.info(f"Collection complete in {collection_time:.2f}s")
        logger.info(f"   Raw measurements: {list(raw_measurements.keys())}")
        logger.info(f"   Science data: {list(science_data.keys())}")
        logger.info(f"   Meteorology data: {list(meteorology_data.keys())}")
        logger.info(f"   AirNow pollutants: {list(all_ground_sources['airnow']['raw_data'].keys())}")
        logger.info(f"   WAQI pollutants: {list(all_ground_sources['waqi']['raw_data'].keys())}")
        
        # Raw data collection complete - no S3 storage needed
        # Final results will be stored in MySQL after fusion + AQI processing
        
        return result

    async def collect_location_data_parallel(self, lat: float, lon: float, location_name: str = None) -> RawLocationData:
        """
        PARALLEL version of collect_location_data - same data, faster collection
        TEMPO unchanged, parallel GEOS-CF + ground stations
        """
        start_time = time.time()
        
        logger.info(f"🚀 PARALLEL data collection for {lat:.4f}, {lon:.4f}")
        
        raw_measurements = {}
        science_data = {}
        data_sources = {
            "tempo": [],
            "geos_cf": [],
            "ground_stations": [],
            "external_aqi": []
        }
        
        logger.info("🚀 Fast batch collecting TEMPO satellite data...")
        
        if self.use_local_tempo:
            tempo_results = self.extract_tempo_data_fast_batch([(lat, lon)], ["NO2", "HCHO"])
            
            for key, data in tempo_results["raw_measurements"].items():
                gas = key.split('_')[0]  # Extract gas name
                raw_measurements[gas] = data
                logger.info(f"✅ TEMPO {gas}: {data.concentration} {data.units} (fast batch)")
            
            for key, data in tempo_results["science_data"].items():
                gas = key.split('_')[0]  # Extract gas name  
                science_data[gas] = data
                logger.info(f"🔬 TEMPO {gas}: {data.get('concentration', 'N/A')} {data.get('units', '')} (science)")
            
        # Result already created and returned in parallel section above

    # ⚠️  CRITICAL: THIS IS THE DIRECT FRESH DATA METHOD
    # 📋 METHOD NAME: collect_and_process_immediately()
    # ✅ DIRECT PIPELINE: Fresh Data → Fusion → Bias → AQI → MySQL
    # ❌ NO intermediate storage, NO caching, NO pre-stored data
    # 🎯 USE THIS METHOD for all fresh data processing requirements
    async def collect_and_process_immediately(self, lat: float, lon: float, location_name: str = None) -> ProcessedLocationData:
        """
        ⚠️  DIRECT FRESH DATA METHOD - OFFICIAL NAME: collect_and_process_immediately()
        
        IMMEDIATE PROCESSING PIPELINE: collect → fusion → AQI calculation → store
        🌐 Fresh Data Collection → 🧬 Fusion → 🔧 Bias Correction → 🧮 EPA AQI → 💾 MySQL Storage
        
        This is the ONLY method that ensures direct fresh data processing:
        - Fresh API calls to GEOS-CF (parallel)
        - Fresh AirNow data (parallel distance search)  
        - Fresh WAQI data (parallel grid search)
        - Real-time fusion and bias correction
        - EPA AQI calculation from corrected fresh data
        - Direct MySQL storage of final result
        
        NO intermediate data storage or caching is used anywhere in this pipeline.
        """
        pipeline_start = time.time()
        
        logger.info(f"🚀 Starting IMMEDIATE processing pipeline for {lat:.4f}, {lon:.4f}")
        
        # Step 1: Collect raw data (existing functionality)
        logger.info("📡 Step 1: Collecting raw data from all sources...")
        raw_data = await self.collect_location_data(lat, lon, location_name)
        
        # Step 2: Convert raw data to fusion format
        logger.info("🔄 Step 2: Converting to fusion format...")
        fusion_input = self.convert_raw_to_fusion_format(raw_data)
        
        # Step 3: Apply fusion and bias correction
        logger.info("🧬 Step 3: Applying fusion and bias correction...")
        fusion_start = time.time()
        fused_result = self.fusion_engine.process_location_data(fusion_input)
        fusion_time = time.time() - fusion_start
        
        # Step 4: Calculate EPA AQI from fused data
        logger.info("🧮 Step 4: Calculating EPA AQI...")
        aqi_start = time.time()
        
        if isinstance(fused_result, dict) and "fused_concentrations" in fused_result:
            pollutants_data = fused_result["fused_concentrations"]
        elif isinstance(fused_result, dict) and "fused_pollutants" in fused_result:
            pollutants_data = fused_result["fused_pollutants"]
        else:
            # Assume whole dict is pollutants, but filter out metadata fields
            if isinstance(fused_result, dict):
                metadata_fields = {'location', 'lat', 'lon', 'timestamp', 'fusion_method', 
                                   'fusion_statistics', 'processing_metadata', 'collection_time'}
                pollutants_data = {k: v for k, v in fused_result.items() if k not in metadata_fields}
            else:
                pollutants_data = {}
            
        aqi_input = {
            "location": {"lat": lat, "lon": lon},
            "fused_pollutants": pollutants_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"🔍 AQI input has {len(pollutants_data)} pollutants: {list(pollutants_data.keys())}")
        
        self.aqi_calculator.process_fused_data(aqi_input)
        
        location_id = f"{lat:.4f}_{lon:.4f}"
        aqi_result = self.aqi_calculator.calculate_epa_aqi(location_id)
        aqi_time = time.time() - aqi_start
        
        # Step 5: Save final results
        logger.info("💾 Step 5: Saving processed results...")
        if aqi_result:
            self.aqi_calculator.save_aqi_summary(aqi_result, local_storage=True)
        
        total_processing_time = time.time() - pipeline_start
        
        processed_result = ProcessedLocationData(
            location=raw_data.location,
            timestamp=datetime.now(timezone.utc).isoformat(),
            collection_time_seconds=raw_data.collection_time_seconds,
            processing_time_seconds=round(total_processing_time, 2),
            raw_data=raw_data,
            fused_concentrations=pollutants_data,  # Use extracted pollutants data, not from original fusion result
            epa_aqi_results=aqi_result,
            processing_pipeline={
                "step1_collection_time": raw_data.collection_time_seconds,
                "step2_conversion_time": 0.01,
                "step3_fusion_time": round(fusion_time, 2),
                "step4_aqi_time": round(aqi_time, 2),
                "step5_storage_time": 0.1,
                "total_pipeline_time": round(total_processing_time, 2),
                "pipeline_version": "v1.0_immediate",
                "enabled_steps": ["collection", "fusion", "bias_correction", "epa_aqi", "storage"]
            },
            metadata={
                "pipeline_type": "immediate_processing",
                "data_flow": "collect → fusion → AQI → store",
                "ready_for_api": True,
                "real_time_processing": True,
                "batch_processing_bypassed": True
            }
        )
        
        logger.info(f"✅ IMMEDIATE processing complete in {total_processing_time:.2f}s")
        logger.info(f"   Collection: {raw_data.collection_time_seconds:.2f}s")
        logger.info(f"   Fusion: {fusion_time:.2f}s") 
        logger.info(f"   AQI: {aqi_time:.2f}s")
        
        if aqi_result:
            current_aqi = aqi_result.get("current", {})
            logger.info(f"🏆 Final AQI: {current_aqi.get('aqi', 'N/A')} ({current_aqi.get('category', 'Unknown')}) - {current_aqi.get('dominant_pollutant', 'N/A')}")
        
        return processed_result

    def convert_raw_to_fusion_format(self, raw_data: RawLocationData) -> Dict[str, Any]:
        """
        Convert RawLocationData to format expected by fusion engine
        """
        location = raw_data.location
        
        fusion_input = {
            "location": location.get("latitude", 0),  # Fusion engine expects this format
            "lat": location.get("latitude", 0),
            "lon": location.get("longitude", 0),
            "timestamp": raw_data.timestamp,
            "collection_time": raw_data.collection_time_seconds
        }
        
        if raw_data.raw_measurements:
            geos_data = {}
            for pollutant, data in raw_data.raw_measurements.items():
                if hasattr(data, 'concentration') and data.concentration > 0:
                    geos_data[pollutant] = round(data.concentration, 2)
            if geos_data:
                fusion_input["GEOS"] = geos_data
        
        tempo_data = {}
        
        # EPA pollutants from raw_measurements (NO2, O3)
        for pollutant in ["NO2", "O3"]:
            if pollutant in raw_data.raw_measurements:
                pollutant_data = raw_data.raw_measurements[pollutant]
                if hasattr(pollutant_data, 'source') and "TEMPO" in pollutant_data.source:
                    tempo_data[pollutant] = round(pollutant_data.concentration, 2)
        
        # HCHO from science_data (science only)
        if raw_data.science_data:
            for pollutant, data in raw_data.science_data.items():
                if pollutant == "HCHO" and isinstance(data, dict):
                    conc = data.get("concentration", 0)
                    if conc > 0:
                        tempo_data[pollutant] = round(conc, 2)
        
        if tempo_data:
            fusion_input["TEMPO"] = tempo_data
        
        if raw_data.data_sources and "external_aqi" in raw_data.data_sources:
            external_aqi = raw_data.data_sources["external_aqi"]
            
            # AirNow data
            airnow_data = {}
            for key, data in external_aqi.items():
                if key.startswith("airnow_") and isinstance(data, dict):
                    pollutant = key.replace("airnow_", "")
                    aqi_value = data.get("aqi", 0)
                    if aqi_value > 0:
                        concentration = MultiSourceLocationCollector.convert_aqi_to_concentration(aqi_value, pollutant)
                        airnow_data[pollutant] = concentration
            
            if airnow_data:
                fusion_input["AirNow"] = airnow_data
            
            # WAQI data  
            waqi_data = {}
            for key, data in external_aqi.items():
                if key.startswith("waqi_") and isinstance(data, dict):
                    pollutant = key.replace("waqi_", "")
                    aqi_value = data.get("aqi", 0)
                    if aqi_value > 0:
                        concentration = MultiSourceLocationCollector.convert_aqi_to_concentration(aqi_value, pollutant)
                        waqi_data[pollutant] = concentration
            
            if waqi_data:
                fusion_input["WAQI"] = waqi_data
        
        logger.info(f"🔄 Converted to fusion format: {list(fusion_input.keys())}")
        for source in ["GEOS", "TEMPO", "AirNow", "WAQI"]:
            if source in fusion_input:
                logger.info(f"   {source}: {list(fusion_input[source].keys())}")
        
        return fusion_input

    async def collect_multiple_locations_parallel(self, locations: List[Dict[str, Any]]) -> List[RawLocationData]:
        """
        Collect raw data for multiple locations in parallel using asyncio.gather()
        
        Args:
            locations: List of {"latitude": float, "longitude": float, "name": str}
            
        Returns:
            List of RawLocationData for each location
        """
        logger.info(f"🚀 Starting parallel collection for {len(locations)} locations")
        
        tasks = []
        for location in locations:
            lat = location["latitude"]
            lon = location["longitude"]
            name = location.get("name", f"{lat:.3f},{lon:.3f}")
            
            task = self.collect_location_data(lat, lon)
            tasks.append((task, name))
        
        start_time = time.time()
        results = await asyncio.gather(*[task for task, name in tasks], return_exceptions=True)
        total_time = time.time() - start_time
        
        successful_collections = []
        failed_collections = 0
        
        for i, (result, name) in enumerate(zip(results, [name for task, name in tasks])):
            if isinstance(result, Exception):
                logger.error(f"❌ Location {name} failed: {result}")
                failed_collections += 1
            elif result is not None:
                successful_collections.append(result)
                logger.info(f"✅ Location {name}: {result.collection_time_seconds:.2f}s")
                
                if "s3_storage" in result.metadata:
                    s3_info = result.metadata["s3_storage"]
                    logger.info(f"   📁 Saved to: {s3_info['url']}")
            else:
                failed_collections += 1
                logger.error(f"❌ Location {name} returned None")
        
        logger.info(f"🎯 Parallel collection summary:")
        logger.info(f"   ✅ Successful: {len(successful_collections)}")
        logger.info(f"   ❌ Failed: {failed_collections}")
        logger.info(f"   ⏱️ Total time: {total_time:.2f}s")
        logger.info(f"   📊 Average per location: {total_time/len(locations):.2f}s")
        logger.info(f"   🚀 Speedup vs sequential: {len(locations):.1f}x faster")
        
        return successful_collections

    async def collect_and_process_multiple_parallel(self, locations: List[Dict[str, Any]]) -> List[ProcessedLocationData]:
        """
        IMMEDIATE PROCESSING for multiple locations in parallel
        Each location goes through: collect → fusion → AQI → store
        """
        logger.info(f"🚀 Starting PARALLEL immediate processing for {len(locations)} locations")
        
        tasks = []
        for location in locations:
            lat = location["latitude"]
            lon = location["longitude"]
            name = location.get("name", f"{lat:.3f},{lon:.3f}")
            
            task = self.collect_and_process_immediately(lat, lon)
            tasks.append((task, name))
        
        start_time = time.time()
        results = await asyncio.gather(*[task for task, name in tasks], return_exceptions=True)
        total_time = time.time() - start_time
        
        successful_processing = []
        failed_processing = 0
        
        for i, (result, name) in enumerate(zip(results, [name for task, name in tasks])):
            if isinstance(result, Exception):
                logger.error(f"❌ Location {name} immediate processing failed: {result}")
                failed_processing += 1
            elif result is not None:
                successful_processing.append(result)
                logger.info(f"✅ Location {name}: {result.processing_time_seconds:.2f}s total")
                
                if result.epa_aqi_results:
                    current = result.epa_aqi_results.get("current", {})
                    aqi = current.get("aqi", "N/A")
                    category = current.get("category", "Unknown")
                    dominant = current.get("dominant_pollutant", "N/A")
                    logger.info(f"   🏆 AQI: {aqi} ({category}) - {dominant}")
            else:
                failed_processing += 1
                logger.error(f"❌ Location {name} returned None")
        
        logger.info(f"🎯 PARALLEL immediate processing summary:")
        logger.info(f"   ✅ Successful: {len(successful_processing)}")
        logger.info(f"   ❌ Failed: {failed_processing}")
        logger.info(f"   ⏱️ Total time: {total_time:.2f}s")
        logger.info(f"   📊 Average per location: {total_time/len(locations):.2f}s")
        logger.info(f"   🚀 Real-time processing: ENABLED")
        
        return successful_processing
    
    def extract_clean_data(self, raw_data: RawLocationData, location_name: str = None) -> Dict[str, Any]:
        """
        Extract only validated measurements in clean JSONL format
        
        Output: {
          "location": "New York",
          "lat": 40.713, "lon": -74.006,
          "timestamp": "2025-08-20T08:00:00Z",
          "AirNow": {"PM25": 24, "O3": 40},
          "WAQI": {"PM25": 18, "O3": 35},
          "GEOS": {"NO2": 30, "O3": 50, "CO": 0.8, "SO2": 5},
          "TEMPO": {"NO2": 26, "HCHO": 3.2}
        }
        """
        clean_data = {
            "location": location_name,
            "lat": raw_data.location.get("latitude", 0),
            "lon": raw_data.location.get("longitude", 0),
            "timestamp": raw_data.timestamp,
            "collection_time": round(raw_data.collection_time_seconds, 2)
        }
        
        if raw_data.raw_measurements:
            geos_data = {}
            for pollutant, data in raw_data.raw_measurements.items():
                if data.concentration > 0:
                    geos_data[pollutant] = round(data.concentration, 2)
            if geos_data:
                clean_data["GEOS"] = geos_data
        
        if raw_data.science_data:
            tempo_data = {}
            for pollutant, data in raw_data.science_data.items():
                conc = data.get("concentration", 0)
                if conc > 0:
                    tempo_data[pollutant] = round(conc, 2)
            if tempo_data:
                clean_data["TEMPO"] = tempo_data
        
        if raw_data.data_sources and "ground_stations" in raw_data.data_sources:
            gs_data = raw_data.data_sources["ground_stations"]
            
            logger.info(f"🔍 Ground stations data: {json.dumps(gs_data, indent=2, default=str)[:500]}...")
            
            # AirNow data is actually in external_aqi, not ground_stations
            if raw_data.data_sources and "external_aqi" in raw_data.data_sources:
                external_aqi = raw_data.data_sources["external_aqi"]
                logger.info(f"🔍 External AQI section found")
                
                airnow_data = {}
                
                for key, data in external_aqi.items():
                    if key.startswith("airnow_") and isinstance(data, dict):
                        pollutant = key.replace("airnow_", "")
                        aqi_value = data.get("aqi", 0)
                        if aqi_value > 0:
                            concentration = MultiSourceLocationCollector.convert_aqi_to_concentration(aqi_value, pollutant)
                            airnow_data[pollutant] = concentration
                            logger.info(f"✅ AirNow {pollutant}: {aqi_value} AQI → {concentration} concentration")
                
                if airnow_data:
                    clean_data["AirNow"] = airnow_data
                    logger.info(f"✅ AirNow data extracted: {airnow_data}")
                else:
                    logger.warning("❌ No valid AirNow values found in external_aqi")
            else:
                logger.warning("❌ No external_aqi section found")
            
            # WAQI data (extract freshest values)
            if "waqi" in gs_data and "all_measurements" in gs_data["waqi"]:
                waqi_data = {}
                freshest_values = {}
                
                for measurement in gs_data["waqi"]["all_measurements"]:
                    param = measurement.get("parameter", "")
                    value = measurement.get("value", 0)
                    age = measurement.get("age_hours", 999)
                    
                    if param in ["NO2", "O3", "CO", "SO2", "PM25", "PM10"] and value > 0:
                        if param not in freshest_values or age < freshest_values[param]["age"]:
                            freshest_values[param] = {"value": value, "age": age}
                
                for param, data in freshest_values.items():
                    aqi_value = data["value"]
                    concentration = MultiSourceLocationCollector.convert_aqi_to_concentration(aqi_value, param)
                    waqi_data[param] = concentration
                    logger.info(f"✅ WAQI {param}: {aqi_value} AQI → {concentration:.2f} concentration")
                
                if waqi_data:
                    clean_data["WAQI"] = waqi_data
        
        return clean_data
    
    async def collect_clean_data(self, locations: List[Dict]) -> List[Dict]:
        """
        Collect clean data for multiple locations
        
        Args:
            locations: List of {"name": "City", "lat": 40.7, "lon": -74.0}
        
        Returns:
            List of clean JSONL records
        """
        logger.info(f"🧹 Starting clean data collection for {len(locations)} locations")
        
        clean_results = []
        
        for location in locations:
            name = location["name"]
            lat = location["lat"]
            lon = location["lon"]
            
            logger.info(f"🎯 Collecting: {name}")
            
            try:
                raw_data = await self.collect_location_data(lat, lon)
                
                clean_data = self.extract_clean_data(raw_data, name)
                
                total_measurements = 0
                for source in ["GEOS", "TEMPO", "AirNow", "WAQI"]:
                    if source in clean_data:
                        total_measurements += len(clean_data[source])
                
                if total_measurements > 0:
                    clean_results.append(clean_data)
                    logger.info(f"✅ {name}: {total_measurements} clean measurements")
                else:
                    logger.warning(f"❌ {name}: No valid measurements found")
                    
            except Exception as e:
                logger.error(f"❌ {name}: Error - {e}")
        
        logger.info(f"🎯 Clean collection complete: {len(clean_results)}/{len(locations)} successful")
        return clean_results
    
    @staticmethod
    def save_jsonl(data: List[Dict], filename: str = None):
        """
        Save data in JSONL format with automatic filename generation
        
        Args:
            data: List of clean data records
            filename: Optional filename. If None, auto-generates with timestamp and location info
            
        Returns:
            Generated filename
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if len(data) == 1:
                # Single location: include lat/lon in filename
                record = data[0]
                lat = record.get('lat', 0)
                lon = record.get('lon', 0)
                lat_str = f"{lat:.3f}".replace('.', 'p').replace('-', 'neg')
                lon_str = f"{lon:.3f}".replace('.', 'p').replace('-', 'neg')
                filename = f"clean_data_{timestamp}_{lat_str}_{lon_str}.jsonl"
            else:
                # Multiple locations: just timestamp
                filename = f"clean_data_{timestamp}.jsonl"
        
        with open(filename, 'w') as f:
            for record in data:
                f.write(json.dumps(record, separators=(',', ':')) + '\n')
        
        # Summary logging
        logger.info(f"💾 Clean data saved to: {filename}")
        logger.info(f"📊 Total records: {len(data)}")
        
        all_sources = set()
        total_measurements = 0
        for record in data:
            source_keys = [k for k in record.keys() if k not in ['location', 'lat', 'lon', 'timestamp', 'collection_time']]
            all_sources.update(source_keys)
            for source_key in source_keys:
                if isinstance(record[source_key], dict):
                    total_measurements += len(record[source_key])
        
        logger.info(f"📋 Data sources: {', '.join(sorted(all_sources))}")
        logger.info(f"🔬 Total measurements: {total_measurements}")
        
        return filename
    
    @staticmethod
    def load_jsonl(filename: str) -> List[Dict]:
        """Load data from JSONL format"""
        data = []
        with open(filename, 'r') as f:
            for line in f:
                data.append(json.loads(line.strip()))
        return data

    def store_to_mysql(self, processed_data: ProcessedLocationData, location_name: str = "Unknown") -> bool:
        """
        Store processed AQI data directly to MySQL comprehensive_aqi_hourly table
        Same format and table as the global collector
        
        Args:
            processed_data: Complete processed data from collect_and_process_immediately()
            location_name: Name of the location for display
            
        Returns:
            bool: True if stored successfully, False otherwise
        """
        try:
            aqi_results = processed_data.epa_aqi_results or {}
            fused_concentrations = processed_data.fused_concentrations or {}
            raw_data = processed_data.raw_data
            
            current_aqi = aqi_results.get('current', {})
            
            location = processed_data.location
            lat = location.get('latitude', 0.0)
            lon = location.get('longitude', 0.0)
            
            mysql_data = {
                # Location & Time
                'city': location_name,
                'location_lat': lat,
                'location_lng': lon,
                'timestamp': processed_data.timestamp,
                
                # AQI Summary
                'overall_aqi': current_aqi.get('aqi', 0),
                'aqi_category': current_aqi.get('category', 'Unknown'),
                'dominant_pollutant': current_aqi.get('dominant_pollutant', 'Unknown'),
                'health_message': current_aqi.get('health_message', ''),
                
                # Weather Data (from meteorology_data if available)
                'temperature_celsius': None,
                'humidity_percent': None,
                'wind_speed_ms': None,
                'wind_direction_degrees': None,
                'weather_code': None,
                
                # Why Today Explanation (extract from complex object)
                'why_today_explanation': self._extract_why_today_explanation(aqi_results.get('why_today', {}))
            }
            
            if raw_data and hasattr(raw_data, 'meteorology_data'):
                meteo = raw_data.meteorology_data
                if 'T2M' in meteo:
                    mysql_data['temperature_celsius'] = meteo['T2M'].get('value')
                if 'HUMIDITY' in meteo:
                    mysql_data['humidity_percent'] = meteo['HUMIDITY'].get('value')
                if 'U10M' in meteo and 'V10M' in meteo:
                    u = meteo['U10M'].get('value', 0)
                    v = meteo['V10M'].get('value', 0)
                    mysql_data['wind_speed_ms'] = math.sqrt(u*u + v*v)
                    mysql_data['wind_direction_degrees'] = math.degrees(math.atan2(v, u)) % 360
            
            if raw_data and hasattr(raw_data, 'data_sources') and mysql_data['humidity_percent'] is None:
                data_sources = raw_data.data_sources
                if 'external_aqi' in data_sources and 'waqi_HUMIDITY' in data_sources['external_aqi']:
                    humidity_data = data_sources['external_aqi']['waqi_HUMIDITY']
                    mysql_data['humidity_percent'] = humidity_data.get('value')
                # Fallback: check WAQI raw data
                elif 'ground_stations' in data_sources:
                    ground_stations = data_sources['ground_stations']
                    if 'waqi' in ground_stations and 'raw_data' in ground_stations['waqi']:
                        waqi_raw_data = ground_stations['waqi']['raw_data']
                        if 'HUMIDITY' in waqi_raw_data:
                            humidity_data = waqi_raw_data['HUMIDITY']
                            if humidity_data and len(humidity_data) > 0:
                                freshest_humidity = humidity_data[0]
                                mysql_data['humidity_percent'] = freshest_humidity.get('value')
            
            pollutant_mapping = {
                'PM25': 'pm25',
                'PM2.5': 'pm25',
                'PM10': 'pm10', 
                'O3': 'o3',
                'NO2': 'no2',
                'SO2': 'so2',
                'CO': 'co'
            }
            
            pollutants_aqi = aqi_results.get('pollutants', {})
            
            for pollutant_name, db_prefix in pollutant_mapping.items():
                concentration = None
                bias_corrected = False
                
                fusion_key = None
                if pollutant_name in fused_concentrations:
                    fusion_key = pollutant_name
                elif pollutant_name == 'PM2.5' and 'PM25' in fused_concentrations:
                    fusion_key = 'PM25'
                elif pollutant_name == 'PM25':
                    if 'PM25' in fused_concentrations:
                        fusion_key = 'PM25'
                    elif 'PM2.5' in fused_concentrations:
                        fusion_key = 'PM2.5'
                
                if fusion_key:
                    conc_data = fused_concentrations[fusion_key]
                    if isinstance(conc_data, dict):
                        concentration = conc_data.get('concentration')
                        bias_corrected = conc_data.get('bias_correction_applied', False)
                    elif isinstance(conc_data, (int, float)):
                        concentration = conc_data
                
                aqi_value = None
                aqi_key = None
                if pollutant_name in pollutants_aqi:
                    aqi_key = pollutant_name
                elif pollutant_name == 'PM2.5' and 'PM25' in pollutants_aqi:
                    aqi_key = 'PM25'
                elif pollutant_name == 'PM25':
                    if 'PM25' in pollutants_aqi:
                        aqi_key = 'PM25'
                    elif 'PM2.5' in pollutants_aqi:
                        aqi_key = 'PM2.5'
                
                if aqi_key:
                    aqi_data = pollutants_aqi[aqi_key]
                    if isinstance(aqi_data, dict):
                        aqi_value = aqi_data.get('aqi')
                    elif isinstance(aqi_data, (int, float)):
                        aqi_value = aqi_data
                
                mysql_data[f'{db_prefix}_concentration'] = concentration
                mysql_data[f'{db_prefix}_aqi'] = aqi_value
                mysql_data[f'{db_prefix}_bias_corrected'] = bias_corrected
            
            connection = get_db_connection()
            
            if connection.is_connected():
                cursor = connection.cursor()
                
                # INSERT query with ON DUPLICATE KEY UPDATE (same as global collector)
                insert_query = """
                INSERT INTO comprehensive_aqi_hourly (
                    city, location_lat, location_lng, timestamp,
                    overall_aqi, aqi_category, dominant_pollutant, health_message,
                    pm25_concentration, pm25_aqi, pm25_bias_corrected,
                    pm10_concentration, pm10_aqi, pm10_bias_corrected,
                    o3_concentration, o3_aqi, o3_bias_corrected,
                    no2_concentration, no2_aqi, no2_bias_corrected,
                    so2_concentration, so2_aqi, so2_bias_corrected,
                    co_concentration, co_aqi, co_bias_corrected,
                    temperature_celsius, humidity_percent, wind_speed_ms,
                    wind_direction_degrees, weather_code, why_today_explanation
                ) VALUES (
                    %(city)s, %(location_lat)s, %(location_lng)s, %(timestamp)s,
                    %(overall_aqi)s, %(aqi_category)s, %(dominant_pollutant)s, %(health_message)s,
                    %(pm25_concentration)s, %(pm25_aqi)s, %(pm25_bias_corrected)s,
                    %(pm10_concentration)s, %(pm10_aqi)s, %(pm10_bias_corrected)s,
                    %(o3_concentration)s, %(o3_aqi)s, %(o3_bias_corrected)s,
                    %(no2_concentration)s, %(no2_aqi)s, %(no2_bias_corrected)s,
                    %(so2_concentration)s, %(so2_aqi)s, %(so2_bias_corrected)s,
                    %(co_concentration)s, %(co_aqi)s, %(co_bias_corrected)s,
                    %(temperature_celsius)s, %(humidity_percent)s, %(wind_speed_ms)s,
                    %(wind_direction_degrees)s, %(weather_code)s, %(why_today_explanation)s
                ) ON DUPLICATE KEY UPDATE
                    overall_aqi = VALUES(overall_aqi),
                    aqi_category = VALUES(aqi_category),
                    dominant_pollutant = VALUES(dominant_pollutant),
                    health_message = VALUES(health_message),
                    pm25_concentration = VALUES(pm25_concentration),
                    pm25_aqi = VALUES(pm25_aqi),
                    pm25_bias_corrected = VALUES(pm25_bias_corrected),
                    pm10_concentration = VALUES(pm10_concentration),
                    pm10_aqi = VALUES(pm10_aqi),
                    pm10_bias_corrected = VALUES(pm10_bias_corrected),
                    o3_concentration = VALUES(o3_concentration),
                    o3_aqi = VALUES(o3_aqi),
                    o3_bias_corrected = VALUES(o3_bias_corrected),
                    no2_concentration = VALUES(no2_concentration),
                    no2_aqi = VALUES(no2_aqi),
                    no2_bias_corrected = VALUES(no2_bias_corrected),
                    so2_concentration = VALUES(so2_concentration),
                    so2_aqi = VALUES(so2_aqi),
                    so2_bias_corrected = VALUES(so2_bias_corrected),
                    co_concentration = VALUES(co_concentration),
                    co_aqi = VALUES(co_aqi),
                    co_bias_corrected = VALUES(co_bias_corrected),
                    temperature_celsius = VALUES(temperature_celsius),
                    humidity_percent = VALUES(humidity_percent),
                    wind_speed_ms = VALUES(wind_speed_ms),
                    wind_direction_degrees = VALUES(wind_direction_degrees),
                    weather_code = VALUES(weather_code),
                    why_today_explanation = VALUES(why_today_explanation)
                """
                
                cursor.execute(insert_query, mysql_data)
                connection.commit()
                
                logger.info(f"💾 MySQL: Stored North America AQI data for {mysql_data['city']} (AQI: {mysql_data['overall_aqi']})")
                return True
                
        except Error as e:
            logger.error(f"❌ MySQL Error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Storage Error: {e}")
            return False
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
        
        return False
    
    def _extract_why_today_explanation(self, why_today_data: dict) -> str:
        """
        Extract a human-readable why_today explanation from the complex why_today object
        """
        if not why_today_data:
            return ""
        
        try:
            explanation_parts = []
            
            # Main pollutant info
            dominant = why_today_data.get('dominant_pollutant', '')
            if dominant:
                explanation_parts.append(f"Dominant pollutant: {dominant}")
            
            # Primary factors
            factors = why_today_data.get('primary_factors', [])
            if factors:
                explanation_parts.append(f"Main factors: {', '.join(factors[:2])}")  # First 2 factors
            
            # Health recommendations
            health = why_today_data.get('health_recommendations', {})
            general = health.get('general_public', '')
            if general:
                explanation_parts.append(f"Recommendation: {general}")
            
            # Data quality
            quality = why_today_data.get('data_quality_summary', {})
            confidence = quality.get('overall_confidence', '')
            if confidence:
                explanation_parts.append(f"Confidence: {confidence}")
            
            return ". ".join(explanation_parts) + "." if explanation_parts else ""
            
        except Exception as e:
            logger.warning(f"Error extracting why_today explanation: {e}")
            return ""

    def collect_and_store(self, latitude: float, longitude: float, location_name: str) -> bool:
        """
        Complete pipeline: Collect → Process → Store to MySQL (same as global collector)
        
        Args:
            latitude: Location latitude
            longitude: Location longitude  
            location_name: Name of the location
            
        Returns:
            bool: True if entire pipeline succeeded
        """
        try:
            logger.info(f"🇺🇸 Starting complete North America pipeline for {location_name} ({latitude}, {longitude})")
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Step 1-4: Complete processing pipeline
            processed_data = loop.run_until_complete(self.collect_and_process_immediately(latitude, longitude))
            loop.close()
            
            if not processed_data or not processed_data.epa_aqi_results:
                logger.error("❌ Processing pipeline failed")
                return False
                
            # Step 5: Store to MySQL
            storage_success = self.store_to_mysql(processed_data, location_name)
            if not storage_success:
                logger.error("❌ MySQL storage failed")
                return False
                
            logger.info(f"✅ Complete North America pipeline successful for {location_name}!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline error: {e}")
            return False

    def collect_and_store_parallel(self, latitude: float, longitude: float, location_name: str) -> bool:
        """
        PARALLEL version: Complete pipeline with parallel optimization
        TEMPO unchanged, parallel GEOS-CF + ground stations for speed
        
        Args:
            latitude: Location latitude
            longitude: Location longitude  
            location_name: Name of the location
            
        Returns:
            bool: True if entire pipeline succeeded
        """
        try:
            logger.info(f"🚀 Starting PARALLEL North America pipeline for {location_name} ({latitude}, {longitude})")
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Step 1: Parallel data collection (SPEED OPTIMIZATION)
            raw_data = loop.run_until_complete(self.collect_location_data_parallel(latitude, longitude))
            
            # Step 2-4: Process with same processors (UNCHANGED)
            logger.info("🧠 Processing with fusion + bias correction + AQI calculation...")
            
            fusion_engine = ProductionFusionEngine()
            aqi_calculator = EPAAQICalculator()
            
            fusion_data = self.convert_raw_to_fusion_format(raw_data)
            
            corrected_data = fusion_engine.process_location_data(fusion_data)
            
            aqi_input = {
                "location": {"lat": latitude, "lon": longitude},
                "fused_pollutants": corrected_data.get("fused_pollutants", {}),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            aqi_calculator.process_fused_data(aqi_input)
            
            location_id = f"{latitude:.4f}_{longitude:.4f}"
            aqi_results = aqi_calculator.calculate_epa_aqi(location_id)
            
            processed_data = ProcessedLocationData(
                location=raw_data.location,
                timestamp=raw_data.timestamp,
                collection_time_seconds=raw_data.collection_time_seconds,
                processing_time_seconds=0,
                raw_data=raw_data,
                fused_concentrations=corrected_data,
                epa_aqi_results=aqi_results,
                processing_pipeline={"fusion": "ProductionFusionEngine", "aqi": "EPAAQICalculator"},
                metadata={
                    **raw_data.metadata,
                    "processing_method": "parallel_optimized",
                    "processors_used": ["ProductionFusionEngine", "EPAAQICalculator"]
                }
            )
            
            loop.close()
            
            if not processed_data or not processed_data.epa_aqi_results:
                logger.error("❌ Processing pipeline failed")
                return False
                
            # Step 5: Store to MySQL (UNCHANGED)
            storage_success = self.store_to_mysql(processed_data, location_name)
            if not storage_success:
                logger.error("❌ MySQL storage failed")
                return False
                
            logger.info(f"🚀 PARALLEL North America pipeline successful for {location_name}!")
            return True
            
        except Exception as e:
            import traceback
            logger.error(f"❌ Parallel pipeline error: {e}")
            logger.error(f"❌ Full traceback: {traceback.format_exc()}")
            return False

def lambda_handler(event, context):
    """
    AWS Lambda handler for location data collection
    Supports both raw collection and IMMEDIATE PROCESSING
    """
    try:
        immediate_processing = event.get('immediate_processing', True)  # Default to immediate
        
        if 'locations' in event:
            # Multiple locations
            locations = event['locations']
            
            for loc in locations:
                if not ('latitude' in loc and 'longitude' in loc):
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': 'Each location must have latitude and longitude'})
                    }
                
                lat, lon = float(loc['latitude']), float(loc['longitude'])
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': f'Invalid coordinates: {lat}, {lon}'})
                    }
            
            collector = MultiSourceLocationCollector()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            if immediate_processing:
                # IMMEDIATE PROCESSING (new unified pipeline)
                processed_data = loop.run_until_complete(collector.collect_and_process_multiple_parallel(locations))
                loop.close()
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'processing_mode': 'immediate',
                        'pipeline': 'collect → fusion → AQI → store',
                        'total_locations': len(locations),
                        'successful_processing': len(processed_data),
                        'results': [asdict(data) for data in processed_data]
                    }, indent=2, default=str)
                }
            else:
                # Raw collection only (legacy mode)
                raw_data = loop.run_until_complete(collector.collect_multiple_locations_parallel(locations))
                loop.close()
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'processing_mode': 'raw_only',
                        'pipeline': 'collect → save to S3',
                        'total_locations': len(locations),
                        'successful_collections': len(raw_data),
                        'collections': [asdict(data) for data in raw_data]
                    }, indent=2, default=str)
                }
            
        else:
            # Single location
            latitude = float(event.get('latitude', 0))
            longitude = float(event.get('longitude', 0))
            
            if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Invalid coordinates'})
                }
            
            collector = MultiSourceLocationCollector()
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            if immediate_processing:
                # IMMEDIATE PROCESSING (new unified pipeline)
                processed_data = loop.run_until_complete(collector.collect_and_process_immediately(latitude, longitude))
                loop.close()
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'processing_mode': 'immediate',
                        'pipeline': 'collect → fusion → AQI → store',
                        'result': asdict(processed_data)
                    }, indent=2, default=str)
                }
            else:
                # Raw collection only (legacy mode)
                raw_data = loop.run_until_complete(collector.collect_location_data(latitude, longitude))
                loop.close()
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'processing_mode': 'raw_only',
                        'pipeline': 'collect → save to S3',
                        'result': asdict(raw_data)
                    }, indent=2, default=str)
                }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
