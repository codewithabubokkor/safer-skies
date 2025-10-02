#!/usr/bin/env python3
"""
🔥 DAILY FIRE DATA COLLECTOR
============================
Separate fire detection system for daily wildfire smoke monito         # DynamoDB and database connections configured   # Fire collector readyIRE DATA SOURCES:
- NASA FIRMS: Global fire detection from MODIS/VIIRS satellites
- Daily collection frequency (not hourly like air quality)
- Location-based database storage with 24-hour TTL
- No fusion processing - direct fire data collection only

FEATURES:
- Daily fire data collection per location
- Production DynamoDB storage with TTL
- NASA-recommended fire filtering (confidence + brightness + FRP)
- Smoke risk assessment without fusion bias
- Distance-based fire impact analysis
- Automatic daily scheduling with location caching

SEPARATION FROM MAIN COLLECTOR:
- Fire data: Daily collection, direct storage, no fusion
- Air quality: Hourly collection, fusion processing, realtime AQI
"""

import json
import time
import requests
import os
import math
import sys
from decimal import Decimal
import boto3
import mysql.connector
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from utils.database_connection import get_db_connection

logging.basicConfig(
    level=logging.INFO, 
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def safe_numeric_convert(value, target_type=float, default=0):
    """Safely convert any numeric value including Decimal objects"""
    try:
        if value is None:
            return target_type(default)
        if isinstance(value, Decimal):
            return target_type(float(value))
        return target_type(value)
    except (ValueError, TypeError, AttributeError):
        return target_type(default)

@dataclass
class FireDetection:
    """Individual fire detection from NASA FIRMS"""
    latitude: float
    longitude: float
    confidence: int
    brightness: float
    frp: float  # Fire Radiative Power
    scan_date: str
    scan_time: str
    satellite: str
    instrument: str
    version: str
    distance_km: float
    smoke_risk_level: str

@dataclass
@dataclass
class LocationFireData:
    """Complete fire data for a specific location"""
    location: Dict[str, float]
    collection_date: str
    total_fires: int
    nearby_fires: List[FireDetection]
    fire_summary: Dict[str, Any]
    smoke_risk_assessment: Dict[str, Any]
    collection_timestamp: str
    success: bool = True

class DailyFireCollector:
    """
    🔥 Daily Fire Data Collector - Separate from Air Quality System
    
    Collects fire detection data once per day per location
    Stores in database with location-based caching
    No fusion processing - direct fire data only
    """
    
    def __init__(self):
        """Initialize fire collector with NASA FIRMS API, MySQL Database and DynamoDB"""
        
        # 🔥 NASA FIRMS Fire Detection API
        self.firms_api_key = os.getenv('FIRMS_API_KEY', "b1f04672ce2f68cddfb836bcc14d75cc")
        self.firms_base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
        
        # 🗄️ MySQL Database Configuration (Primary Storage - Same as AQI data)
        self.mysql_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USERNAME', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': 'safer_skies'  # Same database as AQI data
        }
        
        # Test MySQL connection
        try:
            conn = get_db_connection()
            conn.close()
            self.mysql_available = True
            # Database configured
        except Exception as e:
            logger.warning(f"⚠️ MySQL Database: ❌ UNAVAILABLE - {e}")
            self.mysql_available = False
        
        # 📦 Production DynamoDB for Daily Fire Cache (Backup Storage)
        try:
            self.dynamodb = boto3.client('dynamodb', region_name='us-east-1')
            self.table_name = 'naq_daily_fire_cache'
            self.dynamodb_available = True
        except Exception as e:
            logger.warning(f"⚠️ DynamoDB not available: {e}")
            self.dynamodb_available = False
        
        # Fire detection parameters (NASA recommended)
        self.fire_search_radius_km = 100  # 100km radius for fire impact
        self.min_confidence = 75  # NASA recommended minimum confidence
        self.min_brightness = 300  # Kelvin - significant fire threshold
        self.min_frp = 10  # MW - minimum Fire Radiative Power
        
        # Daily collection tracking
        self.collection_date = datetime.now(timezone.utc).date()
        
        # Fire collector ready - minimal logging for speed

    def generate_cache_key(self, lat: float, lon: float) -> str:
        """Generate unique cache key for location and date"""
        date_str = self.collection_date.strftime('%Y-%m-%d')
        location_key = f"{lat:.3f},{lon:.3f}"
        return f"fire_daily_{location_key}_{date_str}"

    def is_fire_data_cached_today(self, lat: float, lon: float) -> bool:
        """Check if fire data for this location was already collected today"""
        if not self.dynamodb_available:
            return False
            
        cache_key = self.generate_cache_key(lat, lon)
        
        try:
            response = self.dynamodb.get_item(
                TableName=self.table_name,
                Key={'cache_key': {'S': cache_key}}
            )
            
            if 'Item' in response:
                logger.info(f"🔥 Fire data already cached today for {lat:.3f}, {lon:.3f}")
                return True
            else:
                logger.info(f"🔥 No fire cache found - collecting fresh data for {lat:.3f}, {lon:.3f}")
                return False
                
        except Exception as e:
            logger.warning(f"⚠️ Cache check failed: {e}")
            return False

    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using haversine formula"""
        R = 6371  # Earth radius in kilometers
        
        lat1 = safe_numeric_convert(lat1, float)
        lon1 = safe_numeric_convert(lon1, float)
        lat2 = safe_numeric_convert(lat2, float)
        lon2 = safe_numeric_convert(lon2, float)
        
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c

    def collect_fire_data_for_location(self, lat: float, lon: float, location_name: str = None) -> LocationFireData:
        """
        🔥 Collect fire detection data for specific location (DAILY FREQUENCY)
        
        Args:
            lat: Target latitude
            lon: Target longitude
            location_name: Optional location name for logging
            
        Returns:
            Complete fire data for the location
        """
        location_name = location_name or f"{lat:.3f}, {lon:.3f}"
        
        logger.info("="*60)
        logger.info(f"🔥 DAILY FIRE DATA COLLECTION")
        logger.info(f"📍 Location: {location_name}")
        logger.info(f"📅 Date: {self.collection_date}")
        logger.info(f"🔍 Search Radius: {self.fire_search_radius_km}km")
        logger.info("="*60)
        
        if self.is_fire_data_in_mysql_today(lat, lon) or self.is_fire_data_cached_today(lat, lon):
            cached_data = self.get_cached_fire_data(lat, lon)
            if cached_data:
                logger.info(f"✅ Using cached fire data for today")
                return cached_data
        
        start_time = time.time()
        
        lat = safe_numeric_convert(lat, float)
        lon = safe_numeric_convert(lon, float)
        
        lat_offset = self.fire_search_radius_km / 111.32  # 1 degree ≈ 111.32 km
        lon_offset = self.fire_search_radius_km / (111.32 * math.cos(math.radians(lat)))
        
        north = lat + lat_offset
        south = lat - lat_offset
        east = lon + lon_offset
        west = lon - lon_offset
        
        # NASA FIRMS API call for 24-hour fire data
        firms_url = f"{self.firms_base_url}/{self.firms_api_key}/MODIS_NRT/{west},{south},{east},{north}/1"
        
        logger.info(f"🛰️ Calling NASA FIRMS API...")
        logger.info(f"   📦 Bounding Box: {west:.3f}, {south:.3f}, {east:.3f}, {north:.3f}")
        
        try:
            response = requests.get(firms_url, timeout=30)
            response.raise_for_status()
            
            csv_lines = response.text.strip().split('\n')
            
            if len(csv_lines) <= 1:  # Only header or empty
                logger.info(f"🔥 No fires detected within {self.fire_search_radius_km}km of {location_name}")
                fire_detections = []
            else:
                logger.info(f"🔥 Processing {len(csv_lines)-1} fire detections...")
                
                header = csv_lines[0].split(',')
                fire_detections = []
                
                for i, line in enumerate(csv_lines[1:], 1):
                    try:
                        fields = line.split(',')
                        
                        fire_lat = float(fields[0])
                        fire_lon = float(fields[1])
                        brightness = float(fields[2]) if fields[2] else 0
                        scan_date = fields[5]
                        scan_time = fields[6]
                        satellite = fields[7]
                        confidence = int(fields[8]) if fields[8] else 0
                        version = fields[9] if len(fields) > 9 else "unknown"
                        frp = float(fields[4]) if fields[4] else 0  # Fire Radiative Power
                        instrument = fields[10] if len(fields) > 10 else "MODIS"
                        
                        distance = self.haversine_distance(lat, lon, fire_lat, fire_lon)
                        
                        if (confidence >= self.min_confidence and 
                            brightness >= self.min_brightness and 
                            frp >= self.min_frp and 
                            distance <= self.fire_search_radius_km):
                            
                            # Determine smoke risk level based on distance and intensity
                            if distance <= 25 and frp >= 50:
                                smoke_risk = "HIGH"
                            elif distance <= 50 and frp >= 25:
                                smoke_risk = "MODERATE"
                            elif distance <= 75:
                                smoke_risk = "LOW"
                            else:
                                smoke_risk = "MINIMAL"
                            
                            fire_detection = FireDetection(
                                latitude=fire_lat,
                                longitude=fire_lon,
                                confidence=confidence,
                                brightness=brightness,
                                frp=frp,
                                scan_date=scan_date,
                                scan_time=scan_time,
                                satellite=satellite,
                                instrument=instrument,
                                version=version,
                                distance_km=round(distance, 2),
                                smoke_risk_level=smoke_risk
                            )
                            
                            fire_detections.append(fire_detection)
                            
                            logger.info(f"   🔥 Fire #{len(fire_detections)}: {distance:.1f}km, {confidence}% conf, {frp:.1f}MW FRP, {smoke_risk} risk")
                        
                    except Exception as parse_error:
                        logger.warning(f"⚠️ Error parsing fire detection #{i}: {parse_error}")
                        continue
            
            collection_time = time.time() - start_time
            
            fire_summary = self.generate_fire_summary(fire_detections)
            smoke_risk_assessment = self.assess_smoke_risk(fire_detections, lat, lon)
            
            location_fire_data = LocationFireData(
                location={'lat': lat, 'lon': lon, 'name': location_name},
                collection_date=self.collection_date.isoformat(),
                total_fires=len(fire_detections),
                nearby_fires=fire_detections,
                fire_summary=fire_summary,
                smoke_risk_assessment=smoke_risk_assessment,
                collection_timestamp=datetime.now(timezone.utc).isoformat()
            )
            
            mysql_success = self.save_fire_data_to_mysql(location_fire_data)
            
            # Cache in DynamoDB (backup storage)
            self.cache_fire_data(lat, lon, location_fire_data)
            
            logger.info(f"✅ Fire collection complete for {location_name}")
            logger.info(f"🔥 Total fires found: {len(fire_detections)}")
            logger.info(f"🗄️ MySQL Storage: {'✅ SAVED' if mysql_success else '❌ FAILED'}")
            logger.info(f"⏱️ Collection time: {collection_time:.2f}s")
            logger.info(f"📦 Cached for 24 hours")
            logger.info("="*60)
            
            return location_fire_data
            
        except Exception as e:
            logger.error(f"❌ Fire data collection failed: {e}")
            
            return LocationFireData(
                location={'lat': lat, 'lon': lon, 'name': location_name},
                collection_date=self.collection_date.isoformat(),
                total_fires=0,
                nearby_fires=[],
                fire_summary={'error': str(e)},
                smoke_risk_assessment={'overall_risk': 'UNKNOWN', 'error': str(e)},
                collection_timestamp=datetime.now(timezone.utc).isoformat()
            )

    def generate_fire_summary(self, fire_detections: List[FireDetection]) -> Dict[str, Any]:
        """Generate statistical summary of fire detections"""
        if not fire_detections:
            return {
                'total_fires': 0,
                'risk_levels': {'HIGH': 0, 'MODERATE': 0, 'LOW': 0, 'MINIMAL': 0},
                'distance_stats': {},
                'intensity_stats': {}
            }
        
        risk_counts = {'HIGH': 0, 'MODERATE': 0, 'LOW': 0, 'MINIMAL': 0}
        distances = []
        frp_values = []
        confidence_values = []
        
        for fire in fire_detections:
            risk_counts[fire.smoke_risk_level] += 1
            distances.append(safe_numeric_convert(fire.distance_km, float))
            frp_values.append(safe_numeric_convert(fire.frp, float))
            confidence_values.append(safe_numeric_convert(fire.confidence, float))
        
        return {
            'total_fires': len(fire_detections),
            'risk_levels': risk_counts,
            'distance_stats': {
                'closest_km': min(distances),
                'furthest_km': max(distances),
                'average_km': round(sum(distances) / len(distances), 2)
            },
            'intensity_stats': {
                'max_frp_mw': max(frp_values),
                'avg_frp_mw': round(sum(frp_values) / len(frp_values), 2),
                'avg_confidence': round(sum(confidence_values) / len(confidence_values), 1)
            }
        }

    def assess_smoke_risk(self, fire_detections: List[FireDetection], lat: float, lon: float) -> Dict[str, Any]:
        """Assess overall smoke risk for the location based on fire detections"""
        if not fire_detections:
            return {
                'overall_risk': 'NONE',
                'risk_factors': [],
                'recommendations': ['No active fires detected nearby'],
                'air_quality_impact': 'MINIMAL'
            }
        
        high_risk_fires = [f for f in fire_detections if f.smoke_risk_level == 'HIGH']
        moderate_risk_fires = [f for f in fire_detections if f.smoke_risk_level == 'MODERATE']
        
        risk_factors = []
        recommendations = []
        
        if high_risk_fires:
            overall_risk = 'HIGH'
            risk_factors.append(f"{len(high_risk_fires)} high-intensity fires within 25km")
            recommendations.append("Monitor air quality closely")
            recommendations.append("Consider limiting outdoor activities")
            air_quality_impact = 'SIGNIFICANT'
        elif moderate_risk_fires:
            overall_risk = 'MODERATE'
            risk_factors.append(f"{len(moderate_risk_fires)} moderate fires within 50km")
            recommendations.append("Check air quality before outdoor activities")
            air_quality_impact = 'MODERATE'
        else:
            overall_risk = 'LOW'
            risk_factors.append(f"{len(fire_detections)} distant fires detected")
            recommendations.append("Normal precautions sufficient")
            air_quality_impact = 'MINIMAL'
        
        return {
            'overall_risk': overall_risk,
            'risk_factors': risk_factors,
            'recommendations': recommendations,
            'air_quality_impact': air_quality_impact,
            'total_fires_nearby': len(fire_detections)
        }

    def cache_fire_data(self, lat: float, lon: float, fire_data: LocationFireData):
        """Cache fire data in DynamoDB with 24-hour TTL"""
        if not self.dynamodb_available:
            logger.info("📦 DynamoDB not available - skipping cache")
            return
        
        cache_key = self.generate_cache_key(lat, lon)
        ttl_timestamp = int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())
        
        try:
            item = {
                'cache_key': {'S': cache_key},
                'location_lat': {'N': str(lat)},
                'location_lon': {'N': str(lon)},
                'collection_date': {'S': fire_data.collection_date},
                'total_fires': {'N': str(fire_data.total_fires)},
                'fire_data': {'S': json.dumps(asdict(fire_data), default=str)},
                'ttl': {'N': str(ttl_timestamp)}
            }
            
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item=item
            )
            
            logger.info(f"✅ Fire data cached with key: {cache_key}")
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to cache fire data: {e}")

    def save_fire_data_to_mysql(self, fire_data: LocationFireData) -> bool:
        """Save fire data to MySQL database (Primary Storage)"""
        if not self.mysql_available:
            logger.warning("🗄️ MySQL not available - skipping database storage")
            return False
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            location_query = """
                INSERT INTO fire_locations (
                    location_lat, location_lng, location_city, collection_date,
                    total_fires, high_risk_fires, max_distance_km, avg_confidence, 
                    max_frp, smoke_risk_level, air_quality_impact, collection_timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    total_fires = VALUES(total_fires),
                    high_risk_fires = VALUES(high_risk_fires),
                    max_distance_km = VALUES(max_distance_km),
                    avg_confidence = VALUES(avg_confidence),
                    max_frp = VALUES(max_frp),
                    smoke_risk_level = VALUES(smoke_risk_level),
                    air_quality_impact = VALUES(air_quality_impact),
                    collection_timestamp = VALUES(collection_timestamp)
            """
            
            def safe_float(value):
                """Safely convert Decimal objects to float"""
                return safe_numeric_convert(value, float, 0.0)
            
            high_risk_count = len([f for f in fire_data.nearby_fires if safe_float(f.confidence) >= 85 and safe_float(f.distance_km) <= 25])
            max_distance = max([safe_float(f.distance_km) for f in fire_data.nearby_fires]) if fire_data.nearby_fires else 0
            avg_confidence = sum([safe_float(f.confidence) for f in fire_data.nearby_fires]) / len(fire_data.nearby_fires) if fire_data.nearby_fires else 0
            max_frp = max([safe_float(f.frp) for f in fire_data.nearby_fires]) if fire_data.nearby_fires else 0
            
            location_values = (
                fire_data.location['lat'],
                fire_data.location['lon'], 
                fire_data.location.get('name', ''),
                fire_data.collection_date,
                fire_data.total_fires,
                high_risk_count,
                max_distance,
                avg_confidence,
                max_frp,
                fire_data.smoke_risk_assessment.get('overall_risk', 'low'),
                fire_data.smoke_risk_assessment.get('air_quality_impact', 'minimal'),
                fire_data.collection_timestamp
            )
            
            cursor.execute(location_query, location_values)
            fire_location_id = cursor.lastrowid
            
            if fire_data.nearby_fires:
                detection_query = """
                    INSERT INTO fire_detections (
                        fire_location_id, fire_lat, fire_lng, confidence, brightness, frp,
                        scan_date, scan_time, satellite, instrument, version, 
                        distance_km, smoke_risk_level
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                detection_values = []
                for fire in fire_data.nearby_fires:
                    detection_values.append((
                        fire_location_id,
                        fire.latitude,
                        fire.longitude,
                        fire.confidence,
                        fire.brightness,
                        fire.frp,
                        fire.scan_date,
                        fire.scan_time,
                        fire.satellite,
                        fire.instrument,
                        fire.version,
                        fire.distance_km,
                        fire.smoke_risk_level
                    ))
                
                cursor.executemany(detection_query, detection_values)
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Fire data saved to MySQL: {fire_data.total_fires} fires for {fire_data.location.get('name', 'location')}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save fire data to MySQL: {e}")
            return False

    def get_fire_data_from_mysql(self, lat: float, lon: float, date: str = None) -> Optional[LocationFireData]:
        """Retrieve fire data from MySQL database"""
        if not self.mysql_available:
            return None
        
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            location_query = """
                SELECT * FROM fire_locations 
                WHERE ABS(location_lat - %s) < 0.01 
                AND ABS(location_lng - %s) < 0.01 
                AND collection_date = %s
                ORDER BY collection_timestamp DESC
                LIMIT 1
            """
            
            cursor.execute(location_query, (lat, lon, date))
            location_data = cursor.fetchone()
            
            if not location_data:
                conn.close()
                return None
            
            detection_query = """
                SELECT * FROM fire_detections
                WHERE fire_location_id = %s
                ORDER BY distance_km ASC
            """
            
            cursor.execute(detection_query, (location_data['id'],))
            detections = cursor.fetchall()
            
            conn.close()
            
            # Reconstruct fire detection objects
            fire_detections = []
            for det in detections:
                fire_detections.append(FireDetection(
                    latitude=det['fire_lat'],
                    longitude=det['fire_lng'],
                    confidence=det['confidence'],
                    brightness=det['brightness'],
                    frp=det['frp'],
                    scan_date=str(det['scan_date']),
                    scan_time=str(det['scan_time']),
                    satellite=det['satellite'] or '',
                    instrument=det['instrument'] or '',
                    version=det['version'] or '',
                    distance_km=det['distance_km'],
                    smoke_risk_level=det['smoke_risk_level']
                ))
            
            # Reconstruct LocationFireData
            return LocationFireData(
                location={'lat': location_data['location_lat'], 'lon': location_data['location_lng'], 'name': location_data['location_city']},
                collection_date=str(location_data['collection_date']),
                total_fires=location_data['total_fires'],
                nearby_fires=fire_detections,
                fire_summary={
                    'high_risk_fires': location_data['high_risk_fires'],
                    'max_distance_km': location_data['max_distance_km'],
                    'avg_confidence': location_data['avg_confidence'],
                    'max_frp': location_data['max_frp']
                },
                smoke_risk_assessment={
                    'overall_risk': location_data['smoke_risk_level'],
                    'air_quality_impact': location_data['air_quality_impact']
                },
                collection_timestamp=str(location_data['collection_timestamp'])
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to retrieve fire data from MySQL: {e}")
            return None

    def is_fire_data_in_mysql_today(self, lat: float, lon: float) -> bool:
        """Check if fire data for this location exists in MySQL for today"""
        if not self.mysql_available:
            return False
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT COUNT(*) FROM fire_locations 
                WHERE ABS(location_lat - %s) < 0.01 
                AND ABS(location_lng - %s) < 0.01 
                AND collection_date = %s
            """
            
            cursor.execute(query, (lat, lon, today))
            count = cursor.fetchone()[0]
            
            conn.close()
            return count > 0
            
        except Exception as e:
            logger.error(f"❌ Error checking MySQL fire data: {e}")
            return False

    def get_cached_fire_data(self, lat: float, lon: float) -> Optional[LocationFireData]:
        """Retrieve cached fire data from MySQL first, then DynamoDB"""
        mysql_data = self.get_fire_data_from_mysql(lat, lon)
        if mysql_data:
            logger.info("✅ Using fire data from MySQL database")
            return mysql_data
        
        # Fallback to DynamoDB
        if not self.dynamodb_available:
            return None
        
        cache_key = self.generate_cache_key(lat, lon)
        
        try:
            response = self.dynamodb.get_item(
                TableName=self.table_name,
                Key={'cache_key': {'S': cache_key}}
            )
            
            if 'Item' in response:
                fire_data_json = response['Item']['fire_data']['S']
                fire_data_dict = json.loads(fire_data_json)
                
                # Reconstruct LocationFireData object
                return LocationFireData(
                    location=fire_data_dict['location'],
                    collection_date=fire_data_dict['collection_date'],
                    total_fires=fire_data_dict['total_fires'],
                    nearby_fires=[FireDetection(**fire) for fire in fire_data_dict['nearby_fires']],
                    fire_summary=fire_data_dict['fire_summary'],
                    smoke_risk_assessment=fire_data_dict['smoke_risk_assessment'],
                    collection_timestamp=fire_data_dict['collection_timestamp']
                )
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to retrieve cached fire data: {e}")
        
        return None

    def save_fire_data_to_file(self, fire_data: LocationFireData, output_dir: str = "fire_results") -> str:
        """Save fire data to JSON file for local storage"""
        os.makedirs(output_dir, exist_ok=True)
        
        location_name = fire_data.location.get('name', f"{fire_data.location['lat']},{fire_data.location['lon']}")
        safe_name = location_name.replace(' ', '_').replace(',', '_')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"fire_data_{safe_name}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(asdict(fire_data), f, indent=2, default=str)
        
        logger.info(f"💾 Fire data saved to: {filepath}")
        return filepath

    def collect_daily_fire_for_cities(self, cities: List[Dict[str, Any]]) -> List[LocationFireData]:
        """Collect daily fire data for multiple cities"""
        results = []
        
        logger.info(f"🔥 Starting daily fire collection for {len(cities)} locations")
        
        for city in cities:
            try:
                fire_data = self.collect_fire_data_for_location(
                    lat=city['lat'],
                    lon=city['lon'],
                    location_name=city.get('name', f"{city['lat']}, {city['lon']}")
                )
                results.append(fire_data)
                
                self.save_fire_data_to_file(fire_data)
                
            except Exception as e:
                logger.error(f"❌ Fire collection failed for {city}: {e}")
        
        logger.info(f"🔥 Daily fire collection complete: {len(results)} locations processed")
        return results

# Example usage for daily fire collection
# Daily fire collector - import and use in other modules