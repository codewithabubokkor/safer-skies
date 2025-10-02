#!/usr/bin/env python3
"""
🤖 SMART DATA MANAGER
Integrates APIs with Smart Collectors for instant data availability
"""

import os
import sys
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Add backend to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.database_connection import get_db_connection
from processors.location_optimizer import SmartLocationOptimizer

logger = logging.getLogger(__name__)

class SmartDataManager:
    """
    Manages data availability and triggers collectors when needed
    - Checks if data exists for location/time
    - Triggers smart collectors for missing data
    - Handles instant collection for new locations
    """
    
    def __init__(self):
        self.location_optimizer = SmartLocationOptimizer()
        # Lazy load collectors to avoid initialization overhead
        self._hourly_collector = None
        self._forecast_collector = None
    
    @property
    def hourly_collector(self):
        """Lazy load smart hourly collector"""
        if self._hourly_collector is None:
            from collectors.smart_hourly_collector import SmartHourlyCollector
            self._hourly_collector = SmartHourlyCollector()
        return self._hourly_collector
    
    @property 
    def forecast_collector(self):
        """Lazy load smart 5-day collector"""
        if self._forecast_collector is None:
            from collectors.smart_5day_collector import Smart5DayCollector
            self._forecast_collector = Smart5DayCollector()
        return self._forecast_collector
    
    def _get_smart_location_name(self, lat: float, lon: float, city_name: str = None) -> str:
        """
        Get intelligent location name using multiple strategies:
        1. Use provided city_name if available
        2. Try database lookup for existing names
        3. Fallback to coordinate-based format
        """
        if city_name:
            return city_name
        
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor(dictionary=True)
                
                # Look for nearby locations with proper names (within 20km)
                query = """
                SELECT DISTINCT location_name
                FROM forecast_5day_data 
                WHERE location_lat BETWEEN %s - 0.2 AND %s + 0.2
                AND location_lng BETWEEN %s - 0.2 AND %s + 0.2
                AND location_name NOT LIKE '%°N%'
                AND location_name NOT LIKE '%°S%'
                AND location_name != ''
                ORDER BY
                    ABS(location_lat - %s) + ABS(location_lng - %s) ASC
                LIMIT 1
                """
                
                cursor.execute(query, [lat, lat, lon, lon, lat, lon])
                result = cursor.fetchone()
                
                if result and result['location_name']:
                    cursor.close()
                    conn.close()
                    return result['location_name']
                
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.debug(f"Database location lookup failed: {e}")
        
        # Fallback: coordinate-based format
        lat_dir = "N" if lat >= 0 else "S"
        lon_dir = "E" if lon >= 0 else "W"
        return f"{abs(lat):.3f}°{lat_dir}, {abs(lon):.3f}°{lon_dir}"
    
    def check_hourly_data_exists(self, lat: float, lon: float, city_name: str = None) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        Check if recent hourly data exists for location
        Returns: (data_exists, data_frame)
        """
        try:
            conn = get_db_connection()
            if not conn:
                return False, None
            
            query = """
            SELECT * FROM comprehensive_aqi_hourly
            WHERE location_lat BETWEEN %s - 0.01 AND %s + 0.01
            AND location_lng BETWEEN %s - 0.01 AND %s + 0.01
            AND timestamp >= NOW() - INTERVAL 2 HOUR
            ORDER BY timestamp DESC
            LIMIT 1
            """
            
            df = pd.read_sql(query, conn, params=[lat, lat, lon, lon])
            conn.close()
            
            has_data = not df.empty
            if has_data:
                logger.info(f"✅ Recent hourly data exists for {city_name or f'{lat:.3f},{lon:.3f}'}")
            else:
                logger.info(f"❌ No recent hourly data for {city_name or f'{lat:.3f},{lon:.3f}'}")
            
            return has_data, df if has_data else None
            
        except Exception as e:
            logger.error(f"Error in hourly data check: {e}")
            return False, None
    
    async def get_hourly_with_auto_collect(self, lat: float, lng: float, city_name: str = None) -> Tuple[bool, Optional[pd.DataFrame]]:
        """Get hourly AQI data with automatic collection if not available or stale"""
        try:
            has_data, df = self.check_hourly_data_exists(lat, lng, city_name)
            
            if has_data and df is not None:
                from datetime import datetime, timedelta
                latest_timestamp = df['timestamp'].max()
                latest_dt = pd.to_datetime(latest_timestamp)
                now = datetime.now()
                
                if (now - latest_dt.replace(tzinfo=None)).total_seconds() > 2 * 3600:
                    logger.info(f"⏰ Hourly data is stale, auto-collecting fresh data for ({lat}, {lng})")
                    collection_success = await self.trigger_hourly_collection(lat, lng, city_name)
                    
                    if collection_success:
                        has_data, df = self.check_hourly_data_exists(lat, lng, city_name)
                        return has_data, df
                
                return has_data, df
            else:
                # No data exists, trigger collection
                logger.info(f"📊 No hourly data exists, auto-collecting for ({lat}, {lng})")
                collection_success = await self.trigger_hourly_collection(lat, lng, city_name)
                
                if collection_success:
                    has_data, df = self.check_hourly_data_exists(lat, lng, city_name)
                    return has_data, df
                else:
                    return False, None
                    
        except Exception as e:
            logger.error(f"Error in auto-collect hourly: {e}")
            return False, None
    
    async def trigger_forecast_collection(self, lat: float, lng: float, city_name: str = None):
        """Auto-collect forecast data when not available"""
        try:
            logger.info(f"🔄 Auto-triggering forecast collection for ({lat}, {lng})")
            
            location_name = city_name or self._get_smart_location_name(lat, lng)
            
            success = await self.forecast_collector.collect_instant_forecast(lat, lng, location_name)
            
            if success:
                logger.info(f"✅ Auto-forecast collection completed for ({lat}, {lng})")
                return True
            else:
                logger.warning(f"⚠️ Auto-forecast collection failed for ({lat}, {lng})")
                return False
                
        except Exception as e:
            logger.error(f"Error in auto-forecast collection: {e}")
            return False
    
    async def trigger_hourly_collection(self, lat: float, lng: float, city_name: str = None):
        """Trigger smart hourly collector for current AQI data using working pattern"""
        try:
            logger.info(f"🔄 Triggering hourly collection for ({lat}, {lng})")
            
            if city_name:
                location_name = city_name
            else:
                location_name = self._get_smart_location_name(lat, lng)
            
            # Determine if this is North America or Global location
            is_north_america = (20 <= lat <= 85 and -170 <= lng <= -50)
            
            if is_north_america:
                # North America pattern: collect_and_process_immediately + store_to_mysql
                from collectors.northamerica_collector import MultiSourceLocationCollector
                collector = MultiSourceLocationCollector()
                
                processed_result = await collector.collect_and_process_immediately(lat, lng)
                
                if processed_result:
                    mysql_success = collector.store_to_mysql(processed_result, location_name)
                    success = mysql_success
                else:
                    success = False
            else:
                # Global pattern: collect_and_store (does everything in one call)
                from collectors.global_realtime_collector import GlobalRealtimeCollector
                collector = GlobalRealtimeCollector()
                success = collector.collect_and_store(lat, lng, location_name)
            
            if success:
                logger.info(f"✅ Hourly collection completed for ({lat}, {lng})")
                return True
            else:
                logger.warning(f"⚠️ Hourly collection failed for ({lat}, {lng})")
                return False
                
        except Exception as e:
            logger.error(f"Error triggering hourly collection: {e}")
            return False
    
    async def trigger_fire_collection(self, lat: float, lng: float, city_name: str = None):
        """Auto-collect fire data when not available"""
        try:
            logger.info(f"🔄 Auto-triggering fire collection for ({lat}, {lng})")
            
            location_name = city_name or self._get_smart_location_name(lat, lng)
            
            from collectors.fire_collector import FireCollector
            fire_collector = FireCollector()
            
            success = fire_collector.collect_fire_data_for_location(
                lat=lat, lon=lng, location_name=location_name
            )
            
            if success:
                logger.info(f"✅ Auto-fire collection completed for ({lat}, {lng})")
                return True
            else:
                logger.warning(f"⚠️ Auto-fire collection failed for ({lat}, {lng})")
                return False
                
        except Exception as e:
            logger.error(f"Error in auto-fire collection: {e}")
            return False
    
    async def trigger_why_today_generation(self, lat: float, lng: float):
        """Trigger why today explanation generation"""
        try:
            logger.info(f"🔄 Triggering why today generation for ({lat}, {lng})")
            
            # Import why today processor
            from processors.why_today_explainer import WhyTodayExplainer
            explainer = WhyTodayExplainer()
            
            aqi_data = self.check_current_aqi_data_exists(lat, lng)
            if aqi_data:
                explanation = explainer.generate_explanation(
                    aqi_data, 
                    weather_data={},  # Mock weather for now
                    location_data={'lat': lat, 'lng': lng}
                )
                
                self._save_why_today_explanation(lat, lng, explanation)
                
                logger.info(f"✅ Why today generation completed for ({lat}, {lng})")
                return True
            else:
                logger.warning(f"⚠️ No AQI data available for why today generation")
                return False
                
        except Exception as e:
            logger.error(f"Error triggering why today generation: {e}")
            return False
    
    def check_current_aqi_data_exists(self, lat: float, lng: float) -> Optional[Dict]:
        """Check if current AQI data exists for location"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT * FROM comprehensive_aqi_hourly 
            WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
            AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
            AND timestamp >= NOW() - INTERVAL 2 HOUR
            ORDER BY ABS(location_lat - %s) + ABS(location_lng - %s) ASC,
                     timestamp DESC
            LIMIT 1
            """
            
            cursor.execute(query, [lat, lat, lng, lng, lat, lng])
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'aqi': result['overall_aqi'],
                    'aqi_category': result['aqi_category'],
                    'primary_pollutant': result['dominant_pollutant'],
                    'location_name': result['city'],
                    'lat': result['location_lat'],
                    'lng': result['location_lng'],
                    'timestamp': result['timestamp'].isoformat()
                }
            return None
            
        except Exception as e:
            logger.error(f"Error checking current AQI data: {e}")
            return None
    
    def _save_why_today_explanation(self, lat: float, lng: float, explanation: Dict):
        """Save why today explanation to database"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            update_query = """
            UPDATE comprehensive_aqi_hourly 
            SET why_today_explanation = %s
            WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
            AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
            AND timestamp >= NOW() - INTERVAL 2 HOUR
            ORDER BY timestamp DESC
            LIMIT 1
            """
            
            cursor.execute(update_query, [
                json.dumps(explanation),
                lat, lat, lng, lng
            ])
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Why today explanation saved for ({lat}, {lng})")
            
        except Exception as e:
            logger.error(f"Error saving why today explanation: {e}")
    
    def ensure_current_aqi_data(self, lat: float, lng: float, city_name: str = None) -> Optional[Dict]:
        """Ensure current AQI data exists, trigger collection if missing"""
        existing_data = self.check_current_aqi_data_exists(lat, lng)
        
        if existing_data:
            logger.info(f"📋 Current AQI data found for ({lat}, {lng})")
            return {
                'aqi': existing_data['aqi'],
                'category': existing_data['aqi_category'],
                'dominant_pollutant': existing_data['primary_pollutant'],
                'timestamp': existing_data['timestamp'],
                'data_source': 'database'
            }
        else:
            logger.info(f"⚡ No current AQI data, triggering collection for ({lat}, {lng})")
            import asyncio
            try:
                asyncio.create_task(self.trigger_hourly_collection(lat, lng, city_name))
            except:
                pass
            return None
    
    def check_forecast_data_exists(self, lat: float, lon: float, city_name: str = None) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        Check if valid forecast data exists for location using unified search
        Searches by: city name OR coordinates (exact OR broad radius) in single query
        Returns: (data_exists, data_frame)
        """
        try:
            conn = get_db_connection()
            if not conn:
                return False, None
            
            # Unified query: Search by city name OR coordinates (multiple radius levels)
            if city_name:
                query = """
                SELECT * FROM forecast_5day_data 
                WHERE (
                    -- Option 1: City name match (highest priority)
                    location_name LIKE %s OR
                    location_name LIKE %s OR
                    location_name LIKE %s OR
                    -- Option 2: Exact coordinate match (±0.01° ≈ 1km)
                    (location_lat BETWEEN %s - 0.01 AND %s + 0.01
                     AND location_lng BETWEEN %s - 0.01 AND %s + 0.01) OR
                    -- Option 3: Broad coordinate match (±0.1° ≈ 10km)
                    (location_lat BETWEEN %s - 0.1 AND %s + 0.1
                     AND location_lng BETWEEN %s - 0.1 AND %s + 0.1)
                )
                AND DATE_ADD(forecast_timestamp, INTERVAL forecast_hour HOUR) >= NOW()
                ORDER BY
                    -- Priority: exact city match > exact coords > broad coords
                    CASE
                        WHEN location_name LIKE %s THEN 1
                        WHEN location_name LIKE %s THEN 2
                        WHEN location_name LIKE %s THEN 3
                        WHEN (location_lat BETWEEN %s - 0.01 AND %s + 0.01
                              AND location_lng BETWEEN %s - 0.01 AND %s + 0.01) THEN 4
                        ELSE 5 + ABS(location_lat - %s) + ABS(location_lng - %s)
                    END,
                    forecast_timestamp ASC, forecast_hour ASC
                """
                
                city_patterns = [
                    f"%{city_name}%",  # Full match
                    f"{city_name}%",   # Starts with
                    f"%{city_name.split(',')[0].strip()}%" if ',' in city_name else f"%{city_name.split()[0]}%"  # First part
                ]
                
                params = (
                    # City name patterns (3 times for WHERE clause)
                    city_patterns[0], city_patterns[1], city_patterns[2],
                    # Exact coordinate bounds (4 params)
                    lat, lat, lon, lon,
                    # Broad coordinate bounds (4 params) 
                    lat, lat, lon, lon,
                    # City name patterns for ORDER BY (3 times)
                    city_patterns[0], city_patterns[1], city_patterns[2],
                    # Exact coordinate bounds for ORDER BY (4 params)
                    lat, lat, lon, lon,
                    # Distance calculation (2 params)
                    lat, lon
                )
            else:
                # Coordinate-only search with fallback radius
                query = """
                SELECT * FROM forecast_5day_data
                WHERE (
                    -- Exact coordinate match (±0.01° ≈ 1km)
                    (location_lat BETWEEN %s - 0.01 AND %s + 0.01
                     AND location_lng BETWEEN %s - 0.01 AND %s + 0.01) OR
                    -- Broad coordinate match (±0.1° ≈ 10km)
                    (location_lat BETWEEN %s - 0.1 AND %s + 0.1
                     AND location_lng BETWEEN %s - 0.1 AND %s + 0.1)
                )
                AND DATE_ADD(forecast_timestamp, INTERVAL forecast_hour HOUR) >= NOW()
                ORDER BY
                    -- Priority: exact match first, then by distance
                    CASE
                        WHEN (location_lat BETWEEN %s - 0.01 AND %s + 0.01
                              AND location_lng BETWEEN %s - 0.01 AND %s + 0.01) THEN 1
                        ELSE 2 + ABS(location_lat - %s) + ABS(location_lng - %s)
                    END,
                    forecast_timestamp ASC, forecast_hour ASC
                """
                
                params = (
                    # Exact coordinate bounds
                    lat, lat, lon, lon,
                    # Broad coordinate bounds
                    lat, lat, lon, lon,
                    # Order by exact bounds
                    lat, lat, lon, lon,
                    # Distance calculation
                    lat, lon
                )
            
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            
            if not df.empty:
                match_type = "unknown"
                found_city = df.iloc[0]['location_name']
                found_lat = float(df.iloc[0]['location_lat'])
                found_lng = float(df.iloc[0]['location_lng'])
                distance = abs(found_lat - lat) + abs(found_lng - lon)
                
                # Determine match type
                if city_name and city_name.lower() in found_city.lower():
                    match_type = "city name"
                elif distance < 0.02:  # Within ~2km
                    match_type = "exact coordinates"
                else:
                    match_type = f"nearby (~{distance*111:.1f}km)"
                
                logger.info(f"✅ Found via {match_type}: '{found_city}' at ({found_lat}, {found_lng}) - {len(df)} records")
                return True, df
            else:
                search_desc = f"city '{city_name}' or coords ({lat:.3f},{lon:.3f})" if city_name else f"coords ({lat:.3f},{lon:.3f})"
                logger.info(f"❌ No forecast data found for {search_desc}")
                return False, None
            
        except Exception as e:
            logger.error(f"Error in unified forecast search: {e}")
            return False, None
    
    async def get_forecast_with_auto_collect(self, lat: float, lng: float, city_name: str = None) -> Tuple[bool, Optional[pd.DataFrame]]:
        """Get forecast data - check if exists first, only collect if missing"""
        try:
            has_data, df = self.check_forecast_data_exists(lat, lng, city_name)
            
            if has_data and df is not None:
                from datetime import datetime, timedelta
                latest_forecast = df['forecast_timestamp'].max()
                latest_dt = pd.to_datetime(latest_forecast)
                now = datetime.now()
                
                if (now - latest_dt.replace(tzinfo=None)).total_seconds() <= 6 * 3600:
                    logger.info(f"✅ Using existing fresh forecast data for ({lat}, {lng})")
                    return has_data, df
                else:
                    logger.info(f"📅 Forecast data is stale, collecting fresh data for ({lat}, {lng})")
                    collection_success = await self.trigger_forecast_collection(lat, lng, city_name)
                    
                    if collection_success:
                        has_data, df = self.check_forecast_data_exists(lat, lng, city_name)
                        return has_data, df
                    else:
                        return has_data, df
            else:
                # No data exists, collect new data
                logger.info(f"📊 No forecast data exists, collecting for ({lat}, {lng})")
                collection_success = await self.trigger_forecast_collection(lat, lng, city_name)
                
                if collection_success:
                    has_data, df = self.check_forecast_data_exists(lat, lng, city_name)
                    return has_data, df
                else:
                    return False, None
                    
        except Exception as e:
            logger.error(f"Error in forecast check/collect: {e}")
            return False, None
    


    def get_full_forecast_data(self, lat: float, lon: float, city_name: str = None) -> Optional[pd.DataFrame]:
        """
        Get essential 5-day forecast AQI data for location (optimized for frontend)
        Returns: data_frame with only AQI fields needed for display
        """
        try:
            conn = get_db_connection()
            if not conn:
                return None
            
            query = """
            SELECT 
                forecast_timestamp,
                forecast_hour,
                overall_aqi,
                aqi_category,
                dominant_pollutant,
                pm25_ugm3,
                o3_ppb,
                no2_ppb,
                temperature_celsius
            FROM forecast_5day_data
            WHERE location_lat BETWEEN %s - 0.01 AND %s + 0.01
            AND location_lng BETWEEN %s - 0.01 AND %s + 0.01
            AND forecast_timestamp >= CURDATE()
            ORDER BY forecast_timestamp ASC, forecast_hour ASC
            """
            
            df = pd.read_sql(query, conn, params=[lat, lat, lon, lon])
            conn.close()
            
            if not df.empty:
                df['aqi'] = df.apply(lambda row: {
                    'overall': row['overall_aqi'],
                    'category': row['aqi_category'],
                    'dominant': row['dominant_pollutant']
                }, axis=1)
                
                df['hour'] = df['forecast_timestamp'].dt.hour
                df['time'] = df['forecast_timestamp'].dt.strftime('%H:%M')
                
                logger.info(f"✅ Retrieved {len(df)} optimized forecast records for {city_name or f'{lat:.3f},{lon:.3f}'}")
                return df
            else:
                logger.info(f"❌ No forecast data found for {city_name or f'{lat:.3f},{lon:.3f}'}")
                return None
            
        except Exception as e:
            logger.error(f"Error getting optimized forecast data: {e}")
            return None
    
    async def ensure_hourly_data(self, lat: float, lon: float, city_name: str = None) -> Optional[pd.DataFrame]:
        """
        Ensure hourly data exists - collect if missing
        Returns: data_frame or None
        """
        has_data, existing_data = self.check_hourly_data_exists(lat, lon, city_name)
        
        if has_data:
            return existing_data
        
        # No data exists - trigger instant collection
        logger.info(f"🚀 Triggering instant hourly collection for {city_name or f'{lat:.3f},{lon:.3f}'}")
        
        try:
            self.location_optimizer.register_search(city_name or "Unknown", lat, lon)
            
            # Trigger instant collection (this will be implemented in the collector)
            success = await self._trigger_instant_hourly_collection(lat, lon, city_name)
            
            if success:
                # Re-check for data after collection
                has_data, new_data = self.check_hourly_data_exists(lat, lon, city_name)
                return new_data if has_data else None
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error ensuring hourly data: {e}")
            return None
    
    async def ensure_forecast_data(self, lat: float, lon: float, city_name: str = None) -> Optional[pd.DataFrame]:
        """
        Ensure forecast data exists - collect if missing
        Returns: data_frame or None
        """
        has_data, existing_data = self.check_forecast_data_exists(lat, lon, city_name)
        
        if has_data:
            return existing_data
        
        # No data exists - trigger instant collection
        logger.info(f"🚀 Triggering instant forecast collection for {city_name or f'{lat:.3f},{lon:.3f}'}")
        
        try:
            self.location_optimizer.register_search(city_name or "Unknown", lat, lon)
            
            # Trigger instant collection
            success = await self.forecast_collector.collect_instant_forecast(lat, lon, city_name)
            
            if success:
                # Re-check for data after collection
                has_data, new_data = self.check_forecast_data_exists(lat, lon, city_name)
                return new_data if has_data else None
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error ensuring forecast data: {e}")
            return None
    
    async def _trigger_instant_hourly_collection(self, lat: float, lon: float, city_name: str = None) -> bool:
        """
        Trigger instant hourly collection for a specific location
        This will be implemented based on the collector's capabilities
        """
        try:
            # This could be enhanced to trigger immediate collection
            location_key = self.location_optimizer.generate_location_key(lat, lon)
            self.location_optimizer.mark_collected(location_key, 0.1)  # Low quality to trigger collection
            
            logger.info(f"📍 Marked {city_name or f'{lat:.3f},{lon:.3f}'} for priority hourly collection")
            return True
            
        except Exception as e:
            logger.error(f"Error triggering instant hourly collection: {e}")
            return False
    
    def get_data_status(self, lat: float, lon: float, city_name: str = None) -> Dict:
        """
        Get comprehensive data availability status for a location
        """
        hourly_exists, hourly_data = self.check_hourly_data_exists(lat, lon, city_name)
        forecast_exists, forecast_data = self.check_forecast_data_exists(lat, lon, city_name)
        
        status = {
            'location': {
                'lat': lat,
                'lon': lon,
                'city': city_name or f"{lat:.3f},{lon:.3f}"
            },
            'hourly_data': {
                'exists': hourly_exists,
                'count': len(hourly_data) if hourly_data is not None else 0,
                'last_updated': hourly_data.iloc[0]['timestamp'] if hourly_exists else None
            },
            'forecast_data': {
                'exists': forecast_exists,
                'count': len(forecast_data) if forecast_data is not None else 0,
                'coverage_days': 0  # Will be calculated if data exists
            },
            'needs_collection': {
                'hourly': not hourly_exists,
                'forecast': not forecast_exists
            },
            'timestamp': datetime.now().isoformat()
        }
        
        return status


# Global instance for use by APIs
smart_data_manager = SmartDataManager()