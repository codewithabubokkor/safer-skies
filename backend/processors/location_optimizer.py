#!/usr/bin/env python3
"""
Smart Location Collection Optimizer for Safer Skies
Team AURA - NASA Space Apps Challenge 2025

Optimizes data collection by prioritizing frequently searched locations
and user alert setups to prevent collecting entire continents.
Achieves 90%+ resource savings by collecting ~100 priority locations vs thousands.
"""

import json
import time
import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import logging
from dotenv import load_dotenv

load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
import sys
sys.path.append(backend_dir)

from utils.database_connection import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LocationPriority:
    """Data class for location priority scoring"""
    location_key: str
    city: str
    latitude: float
    longitude: float
    priority_score: float
    last_collected: Optional[datetime] = None
    collection_frequency: int = 3600
    user_count: int = 0
    search_frequency: int = 0
    alert_count: int = 0

class SmartLocationOptimizer:
    """Intelligent location prioritization for data collection optimization"""
    
    def __init__(self):
        """Initialize location optimizer with MySQL database"""
        self.locations = {}
        self.init_database()
        
    def init_database(self):
        """Initialize MySQL database tables (using existing connection)"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.error("Failed to get database connection from shared utility")
                raise Exception("Database connection failed")
                
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS search_frequency (
                    location_key VARCHAR(50) PRIMARY KEY,
                    city VARCHAR(255),
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    search_count INT DEFAULT 1,
                    last_searched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    priority_score DECIMAL(5, 2) DEFAULT 1.0
                )
            ''')
            
            # Alert locations table (enhanced for frontend integration)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alert_locations (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    user_email VARCHAR(255),
                    location_key VARCHAR(50),
                    city VARCHAR(255),
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    display_name VARCHAR(255),
                    threshold_type VARCHAR(50) DEFAULT 'category',
                    threshold_value INT,
                    threshold_category VARCHAR(50),
                    pollutants TEXT,
                    notification_channels TEXT,
                    alert_types TEXT,
                    health_conditions TEXT,
                    quiet_hours_start TIME,
                    quiet_hours_end TIME,
                    user_timezone VARCHAR(50),
                    active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    priority_score DECIMAL(5, 2) DEFAULT 2.0
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_email VARCHAR(255) PRIMARY KEY,
                    notification_channels TEXT,
                    alert_types TEXT,
                    health_conditions TEXT,
                    quiet_hours_start TIME DEFAULT '22:00:00',
                    quiet_hours_end TIME DEFAULT '07:00:00',
                    timezone VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_cache (
                    location_key VARCHAR(50) PRIMARY KEY,
                    last_collected TIMESTAMP,
                    collection_frequency_hours INT DEFAULT 1,
                    data_quality DECIMAL(3, 2) DEFAULT 1.0,
                    user_demand_score DECIMAL(5, 2) DEFAULT 1.0
                )
            ''')
            
            conn.commit()
            conn.close()
            
            # Database connected
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def generate_location_key(self, latitude: float, longitude: float) -> str:
        """Generate unique location key from coordinates"""
        return f"{latitude:.4f},{longitude:.4f}"
    
    def find_nearest_location(self, latitude: float, longitude: float, max_distance_km: float = 10.0) -> Optional[Dict]:
        """Find nearest location within distance radius for coordinate-based matching"""
        try:
            conn = get_db_connection()
            if not conn:
                return None
            cursor = conn.cursor(dictionary=True)
            
            lat_range = max_distance_km / 111.0  # Rough conversion: 1 degree ≈ 111 km
            lng_range = max_distance_km / (111.0 * abs(latitude / 90.0)) if latitude != 0 else max_distance_km / 111.0
            
            cursor.execute('''
                SELECT location_key, city, latitude, longitude,
                       COUNT(user_email) as user_count,
                       MAX(updated_at) as last_activity
                FROM alert_locations
                WHERE latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
                AND active = 1
                GROUP BY location_key
                UNION
                SELECT location_key, city, latitude, longitude,
                       0 as user_count, last_searched as last_activity
                FROM search_frequency
                WHERE latitude BETWEEN %s AND %s
                AND longitude BETWEEN %s AND %s
            ''', (
                latitude - lat_range, latitude + lat_range,
                longitude - lng_range, longitude + lng_range,
                latitude - lat_range, latitude + lat_range,
                longitude - lng_range, longitude + lng_range
            ))
            
            candidates = cursor.fetchall()
            conn.close()
            
            if not candidates:
                return None
            
            closest_location = None
            min_distance = float('inf')
            
            for candidate in candidates:
                city = candidate['city']
                lat = float(candidate['latitude'])
                lng = float(candidate['longitude'])
                user_count = candidate['user_count']
                last_activity = candidate['last_activity']
                
                distance = self.calculate_distance(latitude, longitude, lat, lng)
                
                if distance <= max_distance_km and distance < min_distance:
                    min_distance = distance
                    closest_location = {
                        'location_key': self.generate_location_key(lat, lng),
                        'city': city,
                        'latitude': lat,
                        'longitude': lng,
                        'distance_km': round(distance, 2),
                        'user_count': user_count,
                        'last_activity': last_activity
                    }
            
            return closest_location
            
        except Exception as e:
            logger.error(f"❌ Error finding nearest location: {e}")
            return None
    
    def handle_location_disambiguation(self, latitude: float, longitude: float, city: str) -> Dict:
        """Handle cases where multiple cities have the same name globally"""
        try:
            conn = get_db_connection()
            if not conn:
                return {'error': 'Database connection failed', 'disambiguation_needed': False}
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT location_key, city, latitude, longitude,
                       COUNT(user_email) as user_count
                FROM alert_locations
                WHERE LOWER(city) LIKE LOWER(%s)
                AND active = 1
                GROUP BY location_key
                UNION
                SELECT location_key, city, latitude, longitude,
                       0 as user_count
                FROM search_frequency
                WHERE LOWER(city) LIKE LOWER(%s)
            ''', (f"%{city}%", f"%{city}%"))
            
            similar_locations = cursor.fetchall()
            conn.close()
            
            if not similar_locations:
                return {
                    'requested_location': {'city': city, 'latitude': latitude, 'longitude': longitude},
                    'similar_locations': [],
                    'disambiguation_needed': False
                }
            
            location_options = []
            for loc in similar_locations:
                _, loc_city, lat, lng, user_count = loc
                distance = self.calculate_distance(latitude, longitude, lat, lng)
                
                location_options.append({
                    'city': loc_city,
                    'latitude': lat,
                    'longitude': lng,
                    'distance_km': round(distance, 2),
                    'user_count': user_count,
                    'location_key': self.generate_location_key(lat, lng)
                })
            
            location_options.sort(key=lambda x: x['distance_km'])
            
            return {
                'requested_location': {'city': city, 'latitude': latitude, 'longitude': longitude},
                'similar_locations': location_options[:5],  # Top 5 closest
                'closest_match': location_options[0] if location_options else None,
                'disambiguation_needed': len([loc for loc in location_options if loc['distance_km'] < 50]) > 1
            }
            
        except Exception as e:
            logger.error(f"❌ Error in location disambiguation: {e}")
            return {
                'error': str(e),
                'disambiguation_needed': False
            }
    
    @staticmethod
    def calculate_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two coordinates using Haversine formula"""
        import math
        
        lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth's radius in kilometers
        return c * 6371

    def register_user_alert(self, alert_data: Dict) -> bool:
        """Register user alert setup from frontend form data"""
        try:
            conn = get_db_connection()
            if not conn:
                return False
            cursor = conn.cursor()
            
            user_email = alert_data.get('userDetails', {}).get('email')
            locations = alert_data.get('locations', [])
            threshold = alert_data.get('threshold', {})
            threshold_category = alert_data.get('thresholdCategory', 'moderate')
            pollutants = json.dumps(alert_data.get('pollutants', ['all']))
            notifications = json.dumps(alert_data.get('notifications', {}))
            alert_types = json.dumps(alert_data.get('alertTypes', {}))
            health_conditions = json.dumps(alert_data.get('healthConditions', []))
            quiet_hours = alert_data.get('quietHours', {})
            user_timezone = alert_data.get('userDetails', {}).get('timezone', 'UTC')
            
            if not user_email or not locations:
                logger.error("❌ Invalid alert data: missing email or locations")
                return False
                
            cursor.execute('''
                INSERT INTO user_preferences
                (user_email, notification_channels, alert_types, health_conditions,
                 quiet_hours_start, quiet_hours_end, timezone, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                notification_channels = VALUES(notification_channels),
                alert_types = VALUES(alert_types),
                health_conditions = VALUES(health_conditions),
                quiet_hours_start = VALUES(quiet_hours_start),
                quiet_hours_end = VALUES(quiet_hours_end),
                timezone = VALUES(timezone),
                updated_at = NOW()
            ''', (
                user_email,
                notifications,
                alert_types,
                health_conditions,
                quiet_hours.get('start', '22:00'),
                quiet_hours.get('end', '07:00'),
                user_timezone
            ))
            
            for location in locations:
                if not location.get('lat') or not location.get('lng'):
                    continue
                    
                location_key = self.generate_location_key(location['lat'], location['lng'])
                
                cursor.execute('''
                    INSERT INTO alert_locations
                    (user_email, location_key, city, latitude, longitude, display_name,
                     threshold_type, threshold_value, threshold_category, pollutants,
                     notification_channels, alert_types, health_conditions,
                     quiet_hours_start, quiet_hours_end, user_timezone, updated_at, priority_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                    ON DUPLICATE KEY UPDATE
                    city = VALUES(city),
                    display_name = VALUES(display_name),
                    threshold_type = VALUES(threshold_type),
                    threshold_value = VALUES(threshold_value),
                    threshold_category = VALUES(threshold_category),
                    pollutants = VALUES(pollutants),
                    updated_at = NOW()
                ''', (
                    user_email,
                    location_key,
                    location.get('city', ''),
                    location['lat'],
                    location['lng'],
                    location.get('display_name', location.get('city', '')),
                    threshold.get('type', 'category'),
                    threshold.get('value', 100),
                    threshold_category,
                    pollutants,
                    notifications,
                    alert_types,
                    health_conditions,
                    quiet_hours.get('start', '22:00'),
                    quiet_hours.get('end', '07:00'),
                    user_timezone,
                    2.5  # High priority score for alert locations
                ))
                
                self.update_collection_cache(location_key, priority_boost=2.0)
                
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Registered alert for {user_email} with {len(locations)} locations")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error registering user alert: {e}")
            return False

    def register_search(self, city: str, latitude: float, longitude: float) -> bool:
        """Register user search activity (called from frontend GeolocationSearch)"""
        try:
            location_key = self.generate_location_key(latitude, longitude)
            
            conn = get_db_connection()
            if not conn:
                logger.error("Failed to get database connection for search registration")
                return False
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO search_frequency (location_key, city, latitude, longitude, search_count, last_searched)
                VALUES (%s, %s, %s, %s, 1, NOW())
                ON DUPLICATE KEY UPDATE
                search_count = search_count + 1,
                last_searched = NOW(),
                priority_score = CASE
                    WHEN search_count < 5 THEN 1.0
                    WHEN search_count < 10 THEN 1.5
                    WHEN search_count < 20 THEN 2.0
                    ELSE 2.5
                END
            ''', (location_key, city, latitude, longitude))
            
            self.update_collection_cache(location_key, priority_boost=1.2)
            
            conn.commit()
            conn.close()
            
            logger.info(f"📍 Registered search for {city} ({location_key})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error registering search: {e}")
            return False

    def update_collection_cache(self, location_key: str, priority_boost: float = 1.0):
        """Update collection cache with priority boost"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.error("Failed to get database connection for collection cache update")
                return
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO collection_cache (location_key, last_collected, user_demand_score)
                VALUES (%s, NULL, %s)
                ON DUPLICATE KEY UPDATE
                user_demand_score = GREATEST(user_demand_score, %s)
            ''', (location_key, priority_boost, priority_boost))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Error updating collection cache: {e}")

    def get_priority_locations(self, limit: int = 100) -> List[LocationPriority]:
        """Get prioritized list of locations for data collection"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.error("Failed to get database connection for priority locations")
                return []
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT
                    COALESCE(al.location_key, sf.location_key) as location_key,
                    COALESCE(al.city, sf.city) as city,
                    COALESCE(al.latitude, sf.latitude) as latitude,
                    COALESCE(al.longitude, sf.longitude) as longitude,
                    COUNT(DISTINCT al.user_email) as alert_count,
                    COALESCE(sf.search_count, 0) as search_count,
                    sf.last_searched,
                    cc.last_collected,
                    cc.collection_frequency_hours,
                    cc.data_quality,
                    cc.user_demand_score
                FROM
                    (SELECT DISTINCT location_key, city, latitude, longitude, user_email
                     FROM alert_locations WHERE active = 1) al
                LEFT JOIN search_frequency sf ON al.location_key = sf.location_key
                LEFT JOIN collection_cache cc ON al.location_key = cc.location_key
                GROUP BY COALESCE(al.location_key, sf.location_key)
                
                UNION
                
                SELECT 
                    sf.location_key,
                    sf.city,
                    sf.latitude,
                    sf.longitude,
                    0 as alert_count,
                    sf.search_count,
                    sf.last_searched,
                    cc.last_collected,
                    cc.collection_frequency_hours,
                    cc.data_quality,
                    cc.user_demand_score
                FROM search_frequency sf
                LEFT JOIN collection_cache cc ON sf.location_key = cc.location_key
                WHERE sf.location_key NOT IN (SELECT DISTINCT location_key FROM alert_locations WHERE active = 1)
                
                ORDER BY 
                    (alert_count * 3 + search_count * 0.1 + COALESCE(user_demand_score, 1.0)) DESC
                LIMIT %s
            ''', (limit,))
            
            rows = cursor.fetchall()
            conn.close()
            
            priority_locations = []
            for row in rows:
                if row[0] and row[2] is not None and row[3] is not None:  # Valid location data
                    alert_count = float(row[4] or 0)
                    search_count = float(row[5] or 0)
                    user_demand_score = float(row[10] or 1.0)
                    
                    priority_score = (alert_count * 3.0) + (search_count * 0.1) + user_demand_score
                    
                    last_collected = None
                    if row[7]:
                        try:
                            if isinstance(row[7], str):
                                last_collected = datetime.fromisoformat(row[7])
                            elif isinstance(row[7], datetime):
                                last_collected = row[7]
                        except (ValueError, TypeError):
                            last_collected = None
                    
                    priority_locations.append(LocationPriority(
                        location_key=row[0],
                        city=row[1] or 'Unknown',
                        latitude=float(row[2]),
                        longitude=float(row[3]),
                        priority_score=priority_score,
                        last_collected=last_collected,
                        collection_frequency=int(row[8] or 24) * 3600,  # Convert hours to seconds
                        user_count=int(alert_count),
                        search_frequency=int(search_count),
                        alert_count=int(alert_count)
                    ))
            
            logger.info(f"🎯 Generated priority list: {len(priority_locations)} locations")
            return priority_locations
            
        except Exception as e:
            logger.error(f"❌ Error getting priority locations: {e}")
            return []

    def should_collect_location(self, location_key: str) -> bool:
        """Determine if location should be collected based on priority and freshness"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.error("Failed to get database connection for collection check")
                return False
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT cc.last_collected, cc.collection_frequency_hours, cc.user_demand_score,
                       COUNT(DISTINCT al.user_email) as alert_users,
                       sf.search_count, sf.last_searched
                FROM collection_cache cc
                LEFT JOIN alert_locations al ON cc.location_key = al.location_key AND al.active = 1
                LEFT JOIN search_frequency sf ON cc.location_key = sf.location_key
                WHERE cc.location_key = %s
                GROUP BY cc.location_key
            ''', (location_key,))
            
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                return True  # Collect if no cache entry
                
            last_collected, frequency, demand_score, alert_users, search_count, last_searched = result
            
            # Always collect locations with active alerts
            if alert_users and alert_users > 0:
                if not last_collected:
                    return True
                if isinstance(last_collected, str):
                    last_collection_time = datetime.fromisoformat(last_collected)
                else:
                    last_collection_time = last_collected
                alert_frequency = max(1800, (frequency or 1) * 3600 // (1 + alert_users))  # More frequent for more users
                return (datetime.now() - last_collection_time).total_seconds() >= alert_frequency
            
            if search_count and search_count > 0:
                if not last_collected:
                    return search_count >= 3  # Only collect after 3+ searches
                if isinstance(last_collected, str):
                    last_collection_time = datetime.fromisoformat(last_collected)
                else:
                    last_collection_time = last_collected
                search_frequency = (frequency or 1) * 3600 * max(1, 5 - search_count)  # More searches = more frequent
                return (datetime.now() - last_collection_time).total_seconds() >= search_frequency
                
            return False  # Don't collect locations with no user interest
            
        except Exception as e:
            logger.error(f"❌ Error checking collection status: {e}")
            return False

    def mark_collected(self, location_key: str, data_quality: float = 1.0):
        """Mark location as collected with quality score"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.error("Failed to get database connection for marking collection")
                return
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO collection_cache (location_key, last_collected, data_quality)
                VALUES (%s, NOW(), %s)
                ON DUPLICATE KEY UPDATE
                last_collected = NOW(),
                data_quality = %s
            ''', (location_key, data_quality, data_quality))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Error marking location collected: {e}")

    def update_collection_stats(self, successful_collections: int, failed_collections: int):
        """Update collection statistics (method expected by smart hourly collector)"""
        try:
            total = successful_collections + failed_collections
            success_rate = (successful_collections / total * 100) if total > 0 else 0
            logger.info(f"📊 Collection stats updated: {successful_collections}/{total} successful ({success_rate:.1f}%)")
            
        except Exception as e:
            logger.error(f"❌ Error updating collection stats: {e}")

    def get_collection_statistics(self) -> Dict:
        """Get optimization statistics"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.error("Failed to get database connection for statistics")
                return {}
            cursor = conn.cursor()
            
            stats = {}
            
            # Alert locations count
            cursor.execute('SELECT COUNT(DISTINCT location_key) FROM alert_locations WHERE active = 1')
            result = cursor.fetchone()
            stats['alert_locations'] = result[0] if result else 0
            
            cursor.execute('SELECT COUNT(*) FROM search_frequency')
            result = cursor.fetchone()
            stats['search_locations'] = result[0] if result else 0
            
            cursor.execute('''
                SELECT COUNT(DISTINCT location_key) FROM (
                    SELECT location_key FROM alert_locations WHERE active = 1
                    UNION
                    SELECT location_key FROM search_frequency
                ) AS combined_locations
            ''')
            result = cursor.fetchone()
            stats['total_unique_locations'] = result[0] if result else 0
            
            # Active users
            cursor.execute('SELECT COUNT(DISTINCT user_email) FROM alert_locations WHERE active = 1')
            result = cursor.fetchone()
            stats['active_users'] = result[0] if result else 0
            
            cursor.execute('''
                SELECT COUNT(*) FROM collection_cache
                WHERE last_collected > DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ''')
            result = cursor.fetchone()
            stats['collections_24h'] = result[0] if result else 0
            
            conn.close()
            
            total_locations = max(stats['total_unique_locations'], 1)
            stats['optimization_ratio'] = f"{(1 - total_locations / 10000) * 100:.1f}%"
            stats['last_updated'] = datetime.now().isoformat()
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting statistics: {e}")
            return {}

# Location optimizer - import and use in other modules