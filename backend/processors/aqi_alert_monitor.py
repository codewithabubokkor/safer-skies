#!/usr/bin/env python3
"""
Automated AQI Alert Monitor
Monitors database AQI data and creates alerts when thresholds are exceeded
"""

import mysql.connector
import logging
import time
import json
import requests
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import os
import sys

# Add backend to path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from processors.alert_engine import AirQualityAlertEngine

logger = logging.getLogger(__name__)

class AQIAlertMonitor:
    """
    Monitors MySQL database for AQI changes and creates alerts automatically
    """
    
    def __init__(self):
        # Database configuration - Same database as AQI data
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USERNAME', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': 'safer_skies'  # Same database as AQI data
        }
        
        # Alert API endpoint
        self.alert_api_base = os.getenv('ALERT_API_BASE', 'http://localhost:5003')
        
        # Alert engine for threshold checking
        self.alert_engine = AirQualityAlertEngine(local_mode=True)
        
        # Fire alert configuration
        self.fire_search_radius_km = 50  # km radius for fire alerts
        self.nasa_firms_api_key = os.getenv('NASA_FIRMS_API_KEY', 'MAP_KEY_PLACEHOLDER')
        
        # Track last check time to avoid duplicate alerts
        self.last_check_time = datetime.now() - timedelta(hours=1)
        
    def get_user_alert_preferences(self) -> List[Dict]:
        """Get all users with their alert preferences and locations"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            import sqlite3
            user_conn = sqlite3.connect('data/subscriptions.db')
            user_conn.row_factory = sqlite3.Row
            
            users = user_conn.execute('''
                SELECT u.user_id, u.name, u.email,
                       np.email as email_enabled, np.push as push_enabled,
                       ap.location_lat, ap.location_lng, ap.location_city,
                       ap.aqi_threshold, ap.pollutants,
                       GROUP_CONCAT(hc.condition) as health_conditions
                FROM users u
                LEFT JOIN notification_preferences np ON u.user_id = np.user_id
                LEFT JOIN alert_preferences ap ON u.user_id = ap.user_id
                LEFT JOIN user_health_conditions hc ON u.user_id = hc.user_id
                WHERE ap.enabled = 1
                GROUP BY u.user_id
            ''').fetchall()
            
            user_list = []
            for user in users:
                user_dict = dict(user)
                user_dict['health_conditions'] = user_dict.get('health_conditions', '').split(',') if user_dict.get('health_conditions') else []
                user_dict['pollutants'] = json.loads(user_dict.get('pollutants', '[]'))
                user_list.append(user_dict)
            
            user_conn.close()
            conn.close()
            
            return user_list
            
        except Exception as e:
            logger.error(f"Error getting user preferences: {e}")
            return []
    
    def get_recent_aqi_data(self, lat: float, lon: float, hours_back: int = 1) -> Optional[Dict]:
        """Get recent AQI data for a location from database"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT * FROM comprehensive_aqi_hourly 
            WHERE location_lat BETWEEN %s - 0.1 AND %s + 0.1
            AND location_lng BETWEEN %s - 0.1 AND %s + 0.1
            AND collection_timestamp >= NOW() - INTERVAL %s HOUR
            ORDER BY collection_timestamp DESC
            LIMIT 1
            """
            
            cursor.execute(query, [lat, lat, lon, lon, hours_back])
            result = cursor.fetchone()
            
            conn.close()
            return result
            
        except Exception as e:
            logger.error(f"Error getting AQI data: {e}")
            return None
    
    def should_create_alert(self, user: Dict, aqi_data: Dict) -> Optional[Dict]:
        """Check if alert should be created based on user preferences and AQI data"""
        try:
            user_threshold = user.get('aqi_threshold', 100)
            current_aqi = aqi_data.get('epa_aqi_value', 0)
            dominant_pollutant = aqi_data.get('dominant_pollutant', 'PM25')
            
            if current_aqi <= user_threshold:
                return None
            
            user_pollutants = user.get('pollutants', [])
            if user_pollutants and dominant_pollutant not in user_pollutants:
                return None
            
            is_sensitive = self.alert_engine.check_user_sensitivity(
                user.get('health_conditions', []), 
                dominant_pollutant
            )
            
            # Determine alert level
            alert_level = self.get_alert_level(current_aqi, is_sensitive)
            
            epa_message = self.alert_engine.get_epa_message(
                dominant_pollutant, 
                current_aqi,
                user.get('health_conditions', [])
            )
            
            alert_data = {
                'user_id': user['user_id'],
                'location': {
                    'city': user.get('location_city', 'Unknown'),
                    'lat': user.get('location_lat', 0),
                    'lng': user.get('location_lng', 0)
                },
                'pollutant': dominant_pollutant,
                'aqi_value': current_aqi,
                'alert_level': alert_level,
                'epa_message': epa_message,
                'is_sensitive_user': is_sensitive,
                'data_timestamp': aqi_data.get('collection_timestamp')
            }
            
            return alert_data
            
        except Exception as e:
            logger.error(f"Error checking alert conditions: {e}")
            return None
    
    def get_alert_level(self, aqi_value: int, is_sensitive: bool) -> str:
        """Get EPA alert level based on AQI value and user sensitivity"""
        if is_sensitive:
            if aqi_value >= 201:
                return 'very_unhealthy'
            elif aqi_value >= 151:
                return 'unhealthy'
            elif aqi_value >= 101:
                return 'unhealthy_sensitive'
            elif aqi_value >= 51:
                return 'moderate'
            else:
                return 'good'
        else:
            if aqi_value >= 301:
                return 'hazardous'
            elif aqi_value >= 201:
                return 'very_unhealthy'
            elif aqi_value >= 151:
                return 'unhealthy'
            elif aqi_value >= 101:
                return 'unhealthy_sensitive'
            elif aqi_value >= 51:
                return 'moderate'
            else:
                return 'good'
    
    def create_alert_via_api(self, alert_data: Dict) -> bool:
        """Create alert via alert API"""
        try:
            response = requests.post(
                f"{self.alert_api_base}/api/alerts/create",
                json=alert_data,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    logger.info(f"Alert created successfully: {result.get('alert_id')}")
                    return True
                else:
                    logger.error(f"Alert creation failed: {result.get('error')}")
                    return False
            else:
                logger.error(f"Alert API error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating alert via API: {e}")
            return False
    
    def haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using haversine formula"""
        R = 6371  # Earth radius in kilometers
        
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = (math.sin(dlat/2)**2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c

    def get_nearby_fires(self, lat: float, lon: float) -> List[Dict]:
        """Get active fires within radius of location from MySQL database"""
        try:
            fires_from_db = self.get_fires_from_database(lat, lon)
            if fires_from_db:
                logger.info(f"Found {len(fires_from_db)} fires from database")
                return fires_from_db
            
            # Fallback to NASA FIRMS API if no database data
            logger.info("No database fire data found, using NASA FIRMS API as fallback")
            return self.get_fires_from_api(lat, lon)
            
        except Exception as e:
            logger.error(f"Error fetching fire data: {e}")
            return []

    def get_fires_from_database(self, lat: float, lon: float) -> List[Dict]:
        """Get fire data from MySQL database"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            query = """
                SELECT fd.*, fl.location_city
                FROM fire_detections fd
                JOIN fire_locations fl ON fd.fire_location_id = fl.id
                WHERE fl.collection_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                AND (
                    (6371 * acos(cos(radians(%s)) * cos(radians(fd.fire_lat)) * 
                     cos(radians(fd.fire_lng) - radians(%s)) + 
                     sin(radians(%s)) * sin(radians(fd.fire_lat)))) <= %s
                )
                AND fd.confidence >= 70 
                AND fd.brightness >= 300 
                AND fd.frp >= 10
                ORDER BY fd.distance_km ASC
            """
            
            cursor.execute(query, (lat, lon, lat, self.fire_search_radius_km))
            db_fires = cursor.fetchall()
            
            conn.close()
            
            fires = []
            for fire in db_fires:
                fires.append({
                    'latitude': fire['fire_lat'],
                    'longitude': fire['fire_lng'], 
                    'confidence': fire['confidence'],
                    'brightness': fire['brightness'],
                    'frp': fire['frp'],
                    'distance_km': round(fire['distance_km'], 1),
                    'acq_date': str(fire['scan_date']),
                    'acq_time': str(fire['scan_time']),
                    'satellite': fire.get('satellite', ''),
                    'smoke_risk_level': fire.get('smoke_risk_level', 'low')
                })
            
            return fires
            
        except Exception as e:
            logger.error(f"Error getting fires from database: {e}")
            return []

    def get_fires_from_api(self, lat: float, lon: float) -> List[Dict]:
        """Get fire data from NASA FIRMS API (fallback method)"""
        try:
            # NASA FIRMS API endpoint for recent fires (last 24 hours)
            url = f"https://firms.modaps.eosdis.nasa.gov/api/active_fire/viirs-snpp/csv/{self.nasa_firms_api_key}/1"
            
            response = requests.get(url, timeout=30)
            if response.status_code != 200:
                logger.warning(f"NASA FIRMS API error: {response.status_code}")
                return []
            
            lines = response.text.strip().split('\n')
            if len(lines) < 2:
                return []
            
            headers = lines[0].split(',')
            fires = []
            
            for line in lines[1:]:
                try:
                    values = line.split(',')
                    fire_data = dict(zip(headers, values))
                    
                    fire_lat = float(fire_data.get('latitude', '0'))
                    fire_lon = float(fire_data.get('longitude', '0'))
                    confidence = int(fire_data.get('confidence', '0'))
                    brightness = float(fire_data.get('brightness', '0'))
                    frp = float(fire_data.get('frp', '0'))
                    
                    distance = self.haversine_distance(lat, lon, fire_lat, fire_lon)
                    
                    if (distance <= self.fire_search_radius_km and 
                        confidence >= 70 and 
                        brightness >= 300 and 
                        frp >= 10):
                        
                        fires.append({
                            'latitude': fire_lat,
                            'longitude': fire_lon,
                            'confidence': confidence,
                            'brightness': brightness,
                            'frp': frp,
                            'distance_km': round(distance, 1),
                            'acq_date': fire_data.get('acq_date', ''),
                            'acq_time': fire_data.get('acq_time', '')
                        })
                        
                except (ValueError, IndexError) as e:
                    continue
            
            return fires
            
        except Exception as e:
            logger.error(f"Error fetching fire data from API: {e}")
            return []

    def should_create_fire_alert(self, fires: List[Dict], user_pref: Dict) -> tuple[bool, str]:
        """Determine if fire alert should be created"""
        if not fires:
            return False, "No nearby fires detected"
        
        high_risk_fires = [f for f in fires if f['distance_km'] <= 25 and f['confidence'] >= 80]
        moderate_risk_fires = [f for f in fires if f['distance_km'] <= 50 and f['confidence'] >= 70]
        
        if high_risk_fires:
            closest_fire = min(high_risk_fires, key=lambda f: f['distance_km'])
            return True, f"HIGH RISK: Active wildfire detected {closest_fire['distance_km']}km from your location. Confidence: {closest_fire['confidence']}%, Intensity: {closest_fire['frp']}MW. Air quality may be severely impacted by smoke."
        
        elif len(moderate_risk_fires) >= 3:
            avg_distance = sum(f['distance_km'] for f in moderate_risk_fires[:3]) / 3
            return True, f"MODERATE RISK: Multiple active fires ({len(moderate_risk_fires)}) detected within 50km (avg distance: {avg_distance:.1f}km). Monitor air quality closely."
        
        elif moderate_risk_fires:
            closest_fire = min(moderate_risk_fires, key=lambda f: f['distance_km'])
            return True, f"CAUTION: Wildfire detected {closest_fire['distance_km']}km away. Monitor air quality for potential smoke impact."
        
        return False, "No significant fire risk detected"

    def monitor_fire_alerts(self):
        """Monitor for fire-related alerts"""
        logger.info("🔥 Starting fire alert monitoring...")
        
        try:
            preferences = self.get_user_alert_preferences()
            if not preferences:
                logger.info("No user preferences found for fire alerts")
                return
            
            fire_alerts_created = 0
            
            for pref in preferences:
                if not pref.get('enabled', True):
                    continue
                
                user_id = pref['user_id']
                lat = pref.get('location_lat', 0)
                lon = pref.get('location_lng', 0)
                
                if lat == 0 and lon == 0:
                    logger.warning(f"No location set for user {user_id}")
                    continue
                
                fires = self.get_nearby_fires(lat, lon)
                logger.info(f"Found {len(fires)} nearby fires for user {user_id} at {lat:.3f}, {lon:.3f}")
                
                should_alert, alert_message = self.should_create_fire_alert(fires, pref)
                
                if should_alert:
                    alert_data = {
                        'user_id': user_id,
                        'alert_type': 'fire',
                        'severity': 'high' if 'HIGH RISK' in alert_message else 'moderate',
                        'location': {
                            'lat': lat,
                            'lng': lon,
                            'city': pref.get('location_city', 'Unknown Location')
                        },
                        'message': alert_message,
                        'fire_count': len(fires),
                        'closest_fire_distance': min(f['distance_km'] for f in fires) if fires else None,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    if self.create_alert_via_api(alert_data):
                        fire_alerts_created += 1
                        logger.info(f"🔥 Fire alert created for user {user_id}")
                    else:
                        logger.error(f"Failed to create fire alert for user {user_id}")
            
            logger.info(f"🔥 Fire monitoring completed. Created {fire_alerts_created} fire alerts")
            
        except Exception as e:
            logger.error(f"Error in fire alert monitoring: {e}")

    def monitor_and_create_alerts(self):
        """Main monitoring function - now includes both AQI and fire alerts"""
        logger.info("🚨 Starting comprehensive alert monitoring (AQI + Fire)...")
        
        # Monitor AQI alerts
        self.monitor_aqi_alerts()
        
        # Monitor fire alerts
        self.monitor_fire_alerts()
        
        logger.info("✅ Comprehensive alert monitoring completed")

    def monitor_aqi_alerts(self):
        """Monitor AQI-specific alerts (original functionality)"""
        logger.info("🌬️ Starting AQI alert monitoring...")
        
        try:
            preferences = self.get_user_alert_preferences()
            if not preferences:
                logger.info("No user preferences found for AQI monitoring")
                return
            
            aqi_alerts_created = 0
            
            for pref in preferences:
                if not pref.get('enabled', True):
                    continue
                
                user_id = pref['user_id']
                lat = pref.get('location_lat', 0)
                lon = pref.get('location_lng', 0)
                
                if lat == 0 and lon == 0:
                    logger.warning(f"No location set for user {user_id}")
                    continue
                
                aqi_data = self.get_recent_aqi_data(lat, lon)
                if not aqi_data:
                    logger.warning(f"No AQI data found for user {user_id} location")
                    continue
                
                should_alert, alert_message = self.should_create_alert(aqi_data, pref)
                
                if should_alert:
                    alert_data = {
                        'user_id': user_id,
                        'alert_type': 'aqi',
                        'severity': self.get_severity_level(aqi_data.get('overall_aqi', 0)),
                        'location': {
                            'lat': lat,
                            'lng': lon,
                            'city': pref.get('location_city', aqi_data.get('location_city', 'Unknown'))
                        },
                        'aqi_data': aqi_data,
                        'message': alert_message,
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    if self.create_alert_via_api(alert_data):
                        aqi_alerts_created += 1
                        logger.info(f"🌬️ AQI alert created for user {user_id}")
                    else:
                        logger.error(f"Failed to create AQI alert for user {user_id}")
            
            logger.info(f"🌬️ AQI monitoring completed. Created {aqi_alerts_created} AQI alerts")
            
        except Exception as e:
            logger.error(f"Error in AQI alert monitoring: {e}")

    def get_severity_level(self, aqi: int) -> str:
        """Get severity level based on AQI value"""
        if aqi >= 200:
            return 'very_high'
        elif aqi >= 150:
            return 'high'
        elif aqi >= 100:
            return 'moderate'
        else:
            return 'low'
    def run_continuous_monitoring(self, check_interval_minutes: int = 30):
        """Run continuous monitoring for both AQI and fire alerts"""
        logger.info(f"🚀 Starting continuous monitoring (AQI + Fire) every {check_interval_minutes} minutes")
        
        while True:
            try:
                self.monitor_and_create_alerts()
                logger.info(f"⏰ Next check in {check_interval_minutes} minutes...")
                time.sleep(check_interval_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("🛑 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in monitoring loop: {e}")
                time.sleep(60)  # Wait 1 minute before retrying

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    monitor = AQIAlertMonitor()
    
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
        monitor.run_continuous_monitoring()
    else:
        monitor.monitor_and_create_alerts()