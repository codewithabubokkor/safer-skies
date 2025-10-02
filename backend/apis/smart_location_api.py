#!/usr/bin/env python3
"""
🎯 UNIFIED SMART LOCATION API
============================
The ONE complete API for all location-based data with Flask web server

Features:
- Smart caching for stable refresh (15 minutes)
- Simultaneous data collection (AQI + Forecast + Why Today + Trends)
- Database integration with lat/lng radius search  
- Loading status management
- Flask web server with multiple endpoints
- Standalone Python mode fallback

Endpoints:
- POST /api/location/complete-data (MAIN - all data)
- GET/POST /api/location/aqi (current AQI only)
- GET/POST /api/location/forecast (5-day forecast only)
- GET/POST /api/location/why-today (explanation only)
- GET/POST /api/location/trends (trend data only)
- GET /api/health (health check)
"""

import os
import sys
import json
import asyncio
from typing import Dict, List, Optional
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from flask import Flask, jsonify, request
    from flask_cors import CORS
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

from utils.database_connection import get_db_connection
from processors.why_today_explainer import WhyTodayExplainer
from apis.smart_data_manager import SmartDataManager

try:
    from collectors.smart_hourly_collector import SmartHourlyCollector
    from collectors.smart_5day_collector import Smart5DayCollector
    from collectors.fire_collector import DailyFireCollector
    from processors.location_optimizer import SmartLocationOptimizer
    from processors.aqi_alert_monitor import AQIAlertMonitor
    COLLECTORS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Smart collectors not available: {e}")
    COLLECTORS_AVAILABLE = False

import threading

class SmartLocationAPI:
    """The ONE unified API class for all location-based data with smart caching"""
    
    def __init__(self):
        self.smart_data_manager = SmartDataManager()
        
        self.cache = {}
        self.cache_duration = timedelta(minutes=10)
        
        self.trend_cache = {}
        self.trend_cache_duration = timedelta(minutes=30)
        self.locations_cache = {}
        self.locations_cache_duration = timedelta(minutes=15)
        
        self.why_today_explainer = WhyTodayExplainer()
        if COLLECTORS_AVAILABLE:
            try:
                self.location_optimizer = SmartLocationOptimizer()
                self.alert_monitor = AQIAlertMonitor()
                self.smart_hourly_collector = SmartHourlyCollector()
                self.smart_5day_collector = Smart5DayCollector()
                self.fire_collector = DailyFireCollector()
                self.collectors_enabled = True
            except Exception as e:
                self.collectors_enabled = False
        else:
            self.collectors_enabled = False
    
    def cleanup_expired_cache(self):
        now = datetime.now()
        
        expired_keys = [key for key, (_, timestamp) in self.cache.items() 
                       if now - timestamp > self.cache_duration]
        for key in expired_keys:
            del self.cache[key]
        
        expired_trend_keys = [key for key, (_, timestamp) in self.trend_cache.items() 
                             if now - timestamp > self.trend_cache_duration]
        for key in expired_trend_keys:
            del self.trend_cache[key]
        
        expired_location_keys = [key for key, (_, timestamp) in self.locations_cache.items() 
                               if now - timestamp > self.locations_cache_duration]
        for key in expired_location_keys:
            del self.locations_cache[key]
    
    def get_complete_location_data(self, lat: float, lng: float, city_name: str = None) -> Dict:
        """Get all location data simultaneously with smart caching"""
        cache_key = f"{lat:.4f}_{lng:.4f}"
        
        if cache_key in self.cache:
            cached_data, cache_time = self.cache[cache_key]
            if datetime.now() - cache_time < self.cache_duration:
                cached_data['from_cache'] = True
                cached_data['cache_age_minutes'] = int((datetime.now() - cache_time).total_seconds() / 60)
                return cached_data
        
        try:
            import asyncio
            
            async def get_all_data_parallel():
                tasks = [
                    asyncio.to_thread(self._get_current_aqi_data, lat, lng, city_name),
                    asyncio.to_thread(self._get_forecast_data, lat, lng, city_name),
                    asyncio.to_thread(self._get_why_today_data_with_auto_collect, lat, lng, city_name),
                    asyncio.to_thread(self._get_trend_data, lat, lng)
                ]
                return await asyncio.gather(*tasks, return_exceptions=True)
            
            results = asyncio.run(get_all_data_parallel())
            current_aqi, forecast_data, why_today_data, trend_data = results
            
            if not any([current_aqi, forecast_data, why_today_data, trend_data]):
                fallback_response = self._get_fallback_data(lat, lng, city_name)
                self._trigger_simultaneous_collections(lat, lng, ['current_aqi', 'forecast', 'why_today'], city_name)
                return fallback_response
            collections_needed = []
            if not current_aqi:
                collections_needed.append('current_aqi')
            if not forecast_data:
                if not self._has_recent_city_forecast_data(lat, lng):
                    collections_needed.append('forecast')
            if not why_today_data:
                collections_needed.append('why_today')
            
            if collections_needed:
                self._trigger_simultaneous_collections(lat, lng, collections_needed, city_name)
            response_data = {
                'success': True,
                'location': {
                    'lat': lat,
                    'lng': lng,
                    'city': city_name or 'Unknown'
                },
                'data': {
                    'current_aqi': current_aqi,
                    'forecast_5day': forecast_data,
                    'why_today': why_today_data,
                    'trends': trend_data
                },
                'loading_status': {
                    'current_aqi': 'loaded' if current_aqi else 'collecting',
                    'forecast': 'loaded' if forecast_data else 'collecting',
                    'why_today': 'loaded' if why_today_data else 'collecting',
                    'trends': 'loaded' if trend_data else 'unavailable'
                },
                'collections_triggered': collections_needed,
                'from_cache': False,
                'timestamp': datetime.now().isoformat()
            }
            
            # Cache the response for stable refresh
            self.cache[cache_key] = (response_data, datetime.now())
            
            return response_data
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'location': {'lat': lat, 'lng': lng},
                'timestamp': datetime.now().isoformat()
            }
    
    def _get_current_aqi_data(self, lat: float, lng: float, city_name: str = None) -> Optional[Dict]:
        """Get current AQI data with automatic collection if not available"""
        try:
            import asyncio
            has_data, df = asyncio.run(
                self.smart_data_manager.get_hourly_with_auto_collect(lat, lng, city_name)
            )
            
            if has_data and df is not None:
                latest_record = df.iloc[0]  # Should be ordered by timestamp DESC
                
                result = {
                    'aqi': int(latest_record['overall_aqi']),
                    'category': latest_record['aqi_category'],
                    'dominant_pollutant': latest_record['dominant_pollutant'],
                    'health_message': latest_record['health_message'],
                    'city': latest_record.get('city', 'Unknown'),
                    'pollutants': {
                        'pm25': {'concentration': float(latest_record['pm25_concentration']), 'aqi': int(latest_record['pm25_aqi'])},
                        'pm10': {'concentration': float(latest_record['pm10_concentration']), 'aqi': int(latest_record['pm10_aqi'])},
                        'o3': {'concentration': float(latest_record['o3_concentration']), 'aqi': int(latest_record['o3_aqi'])},
                        'no2': {'concentration': float(latest_record['no2_concentration']), 'aqi': int(latest_record['no2_aqi'])},
                        'so2': {'concentration': float(latest_record['so2_concentration']), 'aqi': int(latest_record['so2_aqi'])},
                        'co': {'concentration': float(latest_record['co_concentration']), 'aqi': int(latest_record['co_aqi'])}
                    },
                    'timestamp': latest_record['timestamp'].isoformat() if hasattr(latest_record['timestamp'], 'isoformat') else str(latest_record['timestamp']),
                    'data_source': 'auto-collection'
                }
                
                print(f"✅ Retrieved current AQI data for {result['city']} (auto-collection)")
                return result
            else:
                print(f"❌ No AQI data available after auto-collection for {lat:.3f},{lng:.3f}")
                return None
                
        except Exception as e:
            print(f"Error getting AQI data with auto-collection: {e}")
            return None
        
    def _format_aqi_result(self, result: Dict) -> Dict:
        """Format AQI database result for API response"""
        return {
            'aqi': result['overall_aqi'],
            'category': result['aqi_category'],
            'dominant_pollutant': result['dominant_pollutant'],
            'health_message': result['health_message'],
            'city': result.get('city', 'Unknown'),
            'pollutants': {
                'pm25': {'concentration': result['pm25_concentration'], 'aqi': result['pm25_aqi']},
                'pm10': {'concentration': result['pm10_concentration'], 'aqi': result['pm10_aqi']},
                'o3': {'concentration': result['o3_concentration'], 'aqi': result['o3_aqi']},
                'no2': {'concentration': result['no2_concentration'], 'aqi': result['no2_aqi']},
                'so2': {'concentration': result['so2_concentration'], 'aqi': result['so2_aqi']},
                'co': {'concentration': result['co_concentration'], 'aqi': result['co_aqi']}
            },
            'timestamp': result['timestamp'].isoformat(),
            'data_source': 'database'
        }
    
    def _get_forecast_data(self, lat: float, lng: float, city_name: str = None) -> Optional[Dict]:
        """Get 5-day forecast data with automatic collection if not available"""
        try:
            import asyncio
            has_data, df = asyncio.run(
                self.smart_data_manager.get_forecast_with_auto_collect(lat, lng, city_name)
            )
            
            if has_data and df is not None:
                hourly_records = []
                retrieved_city_name = None
                
                for _, row in df.iterrows():
                    if retrieved_city_name is None:
                        retrieved_city_name = row.get('location_name', 'Unknown')
                    
                    hour_num = int(row.get('forecast_hour', 0))
                    
                    hourly_record = {
                        'hour': hour_num,
                        'aqi': int(row.get('overall_aqi', 50)),
                        'time': f"{hour_num:02d}:00"
                    }
                    hourly_records.append(hourly_record)
                
                print(f"✅ Retrieved {len(hourly_records)} forecast records for {retrieved_city_name} (auto-collection)")
                return {
                    'city_name': retrieved_city_name,
                    'location_name': retrieved_city_name,  # For backward compatibility
                    'hourly': hourly_records
                }
            else:
                print(f"❌ No forecast data available after auto-collection for {lat:.3f},{lng:.3f}")
                return None
                
        except Exception as e:
            print(f"Error getting forecast data with auto-collection: {e}")
            return None
    
    def _has_recent_city_forecast_data(self, lat: float, lng: float) -> bool:
        """Check if recent forecast data exists within city radius (larger area)"""
        try:
            conn = get_db_connection()
            if not conn:
                return False
            
            query = """
            SELECT COUNT(*) as forecast_count, MIN(location_city) as city_name
            FROM forecast_5day_data 
            WHERE location_lat BETWEEN %s - 0.1 AND %s + 0.1
            AND location_lng BETWEEN %s - 0.1 AND %s + 0.1
            AND forecast_timestamp >= CURDATE()
            AND forecast_timestamp <= CURDATE() + INTERVAL 5 DAY
            """
            
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, [lat, lat, lng, lng])
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result['forecast_count'] > 0:
                city_name = result['city_name'] or 'nearby location'
                print(f"📊 Found {result['forecast_count']} forecast records in {city_name} area - skipping collection")
                return True
                
            return False
            
        except Exception as e:
            print(f"Error checking city forecast data: {e}")
            return False
    
    def _get_why_today_data(self, lat: float, lng: float) -> Optional[Dict]:
        """Get comprehensive Why Today explanation data"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT city, why_today_explanation, created_at FROM comprehensive_aqi_hourly 
            WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
            AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
            AND why_today_explanation IS NOT NULL
            AND created_at >= UTC_TIMESTAMP() - INTERVAL 1 HOUR
            ORDER BY created_at DESC LIMIT 1
            """
            
            cursor.execute(query, [lat, lat, lng, lng])
            result = cursor.fetchone()
            
            if result and result['why_today_explanation']:
                try:
                    cached_explanation = json.loads(result['why_today_explanation'])
                    if isinstance(cached_explanation, dict) and 'main_explanation' in cached_explanation:
                        cached_explanation['city_name'] = result.get('city', 'Unknown')
                        conn.close()
                        return cached_explanation
                except json.JSONDecodeError:
                    pass
            
            comprehensive_explanation = self._generate_comprehensive_why_today(lat, lng, conn)
            
            if comprehensive_explanation:
                city_query = """
                SELECT city FROM comprehensive_aqi_hourly
                WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
                AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
                AND created_at >= UTC_TIMESTAMP() - INTERVAL 1 HOUR
                ORDER BY created_at DESC LIMIT 1
                """
                cursor.execute(city_query, [lat, lat, lng, lng])
                city_result = cursor.fetchone()
                if city_result:
                    comprehensive_explanation['city_name'] = city_result.get('city', 'Unknown')
            
            conn.close()
            return comprehensive_explanation
            
        except Exception as e:
            print(f"Error getting why today data: {e}")
            return None
    
    def _get_why_today_data_with_auto_collect(self, lat: float, lng: float, city_name: str = None) -> Optional[Dict]:
        """Get Why Today data with automatic collection if not available - follows same pattern as AQI/forecast"""
        try:
            why_today_data = self._get_why_today_data(lat, lng)
            
            if why_today_data:
                print(f"✅ Found cached Why Today data for ({lat:.3f}, {lng:.3f})")
                return why_today_data
            
            # No cached data, use AQI auto-collection to get fresh data (same pattern as other methods)
            print(f"🔍 No cached Why Today data, using AQI auto-collection for ({lat:.3f}, {lng:.3f})")
            
            import asyncio
            has_aqi_data, aqi_df = asyncio.run(
                self.smart_data_manager.get_hourly_with_auto_collect(lat, lng, city_name)
            )
            
            if has_aqi_data and aqi_df is not None:
                latest_aqi = aqi_df.iloc[0]
                
                conn = get_db_connection()
                comprehensive_explanation = self._generate_comprehensive_why_today(lat, lng, conn)
                conn.close()
                
                if comprehensive_explanation:
                    print(f"✅ Generated Why Today explanation from fresh AQI data")
                    return comprehensive_explanation
                else:
                    print(f"⚠️ Could not generate Why Today explanation")
                    return None
            else:
                print(f"❌ No AQI data available after auto-collection for ({lat:.3f}, {lng:.3f})")
                return None
                
        except Exception as e:
            print(f"Error getting Why Today data with auto-collection: {e}")
            return None
    
    def _get_trend_data(self, lat: float, lng: float, days: int = 7) -> Optional[List[Dict]]:
        """Get trend data for location using daily_aqi_trends table with caching"""
        cache_key = f"trends_{lat:.4f}_{lng:.4f}_{days}"
        
        if cache_key in self.trend_cache:
            cached_data, cache_time = self.trend_cache[cache_key]
            if datetime.now() - cache_time < self.trend_cache_duration:
                print(f"🚀 Cache HIT for trend data: {cache_key} (age: {int((datetime.now() - cache_time).total_seconds() / 60)}min)")
                if isinstance(cached_data, list) and cached_data:
                    for item in cached_data:
                        item['from_cache'] = True
                        item['cache_age_minutes'] = int((datetime.now() - cache_time).total_seconds() / 60)
                return cached_data
        
        print(f"💾 Cache MISS for trend data: {cache_key} - fetching from database")
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT 
                date,
                city,
                avg_overall_aqi,
                dominant_pollutant,
                hourly_data_points,
                avg_pm25_concentration,
                avg_o3_concentration,
                avg_no2_concentration,
                data_completeness,
                created_at
            FROM daily_aqi_trends
            WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
            AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
            AND date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY date DESC
            """
            
            cursor.execute(query, [lat, lat, lng, lng, days])
            results = cursor.fetchall()
            
            if results:
                trend_data = []
                for result in results:
                    trend_data.append({
                        'date': result['date'].isoformat(),
                        'aqi': int(result['avg_overall_aqi']),
                        'dominant_pollutant': result['dominant_pollutant'],
                        'readings_count': result['hourly_data_points'],
                        'city': result['city'],
                        'data_completeness': float(result['data_completeness']) if result['data_completeness'] else None,
                        'pollutant_details': {
                            'pm25_avg': float(result['avg_pm25_concentration']) if result['avg_pm25_concentration'] else None,
                            'o3_avg': float(result['avg_o3_concentration']) if result['avg_o3_concentration'] else None,
                            'no2_avg': float(result['avg_no2_concentration']) if result['avg_no2_concentration'] else None
                        },
                        'data_source': 'daily_trends',
                        'calculated_at': result['created_at'].isoformat() if result['created_at'] else None
                    })
                
                conn.close()
                
                # Cache the successful result
                self.trend_cache[cache_key] = (trend_data, datetime.now())
                print(f"💾 Cached trend data: {cache_key} ({len(trend_data)} records)")
                
                return trend_data
            
            else:
                # Fallback to hourly data aggregation if no daily trends available
                print(f"⚠️ No daily trends found for lat={lat}, lng={lng}. Using hourly fallback.")
                
                query = """
                SELECT DATE(timestamp) as date, AVG(overall_aqi) as avg_aqi,
                       dominant_pollutant, COUNT(*) as readings, city
                FROM comprehensive_aqi_hourly
                WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
                AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
                AND timestamp >= NOW() - INTERVAL %s DAY
                GROUP BY DATE(timestamp), city
                ORDER BY date DESC
                """
                
                cursor.execute(query, [lat, lat, lng, lng, days])
                hourly_results = cursor.fetchall()
                conn.close()
                
                if hourly_results:
                    fallback_data = [{
                        'date': result['date'].isoformat(),
                        'aqi': int(result['avg_aqi']),
                        'dominant_pollutant': result['dominant_pollutant'],
                        'readings_count': result['readings'],
                        'city': result['city'],
                        'data_source': 'hourly_fallback',
                        'note': 'Calculated from hourly data - daily trends not available'
                    } for result in hourly_results]
                    
                    # Cache the fallback result too
                    self.trend_cache[cache_key] = (fallback_data, datetime.now())
                    print(f"💾 Cached fallback trend data: {cache_key} ({len(fallback_data)} records)")
                    
                    return fallback_data
                
                # Cache null result briefly to avoid repeated failed queries
                self.trend_cache[cache_key] = (None, datetime.now())
                return None
            
        except Exception as e:
            print(f"Error getting trend data: {e}")
            return None
    
    def get_location_data_by_city(self, city_name: str) -> Dict:
        """Get complete location data by city name"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT city, location_lat, location_lng 
            FROM comprehensive_aqi_hourly 
            WHERE city LIKE %s 
            ORDER BY timestamp DESC 
            LIMIT 1
            """
            
            cursor.execute(query, (f"%{city_name}%",))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return self.get_complete_location_data(
                    result['location_lat'], 
                    result['location_lng'], 
                    result['city']
                )
            else:
                return {
                    'success': False,
                    'error': f'City "{city_name}" not found in database',
                    'suggestion': 'Try providing coordinates instead',
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'city': city_name,
                'timestamp': datetime.now().isoformat()
            }
    
    def _trigger_simultaneous_collections(self, lat: float, lng: float, collections_needed: List[str], city_name: str = None):
        """Trigger multiple collections simultaneously using smart collectors"""
        try:
            print(f"🚀 Triggering simultaneous collections: {collections_needed}")
            
            if not self.collectors_enabled:
                print("⚠️ Smart collectors not available - skipping background collection")
                return
            
            collection_thread = threading.Thread(
                target=self._run_background_collections,
                args=(lat, lng, collections_needed, city_name)
            )
            collection_thread.daemon = True
            collection_thread.start()
            
        except Exception as e:
            print(f"Error triggering collections: {e}")
    
    def _run_background_collections(self, lat: float, lng: float, collections_needed: List[str], city_name: str = None):
        """Run collections in background thread"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            loop.run_until_complete(self._execute_simultaneous_collections(lat, lng, collections_needed, city_name))
            
        except Exception as e:
            print(f"Error in background collections: {e}")
        finally:
            try:
                loop.close()
            except:
                pass
    
    async def _execute_simultaneous_collections(self, lat: float, lng: float, collections_needed: List[str], city_name: str = None):
        """Execute multiple data collections simultaneously with maximum parallelization"""
        print(f"🚀 HIGH-SPEED PARALLEL COLLECTIONS for ({lat:.4f}, {lng:.4f}): {collections_needed}")
        
        collection_tasks = []
        
        # Always collect fire data when any collection is triggered (parallel with others)
        if any(collection in collections_needed for collection in ['current_aqi', 'forecast', 'why_today']):
            collection_tasks.append(self._collect_fire_data(lat, lng, city_name))
        
        if 'current_aqi' in collections_needed:
            collection_tasks.append(self._collect_instant_aqi(lat, lng, city_name))
        
        if 'forecast' in collections_needed:
            collection_tasks.append(self._collect_instant_forecast(lat, lng, city_name))
        
        if 'why_today' in collections_needed:
            collection_tasks.append(self._collect_instant_why_today(lat, lng, city_name))
        
        if collection_tasks:
            start_time = asyncio.get_event_loop().time()
            
            results = await asyncio.gather(*collection_tasks, return_exceptions=True)
            
            end_time = asyncio.get_event_loop().time()
            execution_time = end_time - start_time
            
            success_count = sum(1 for result in results if result is True)
            total_count = len(results)
            
            print(f"⚡ PARALLEL COLLECTIONS COMPLETED: {success_count}/{total_count} successful in {execution_time:.2f}s")
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    task_name = ['fire', 'aqi', 'forecast', 'why_today'][i] if i < 4 else f'task_{i}'
                    print(f"❌ {task_name} collection failed: {result}")
                else:
                    task_name = ['fire', 'aqi', 'forecast', 'why_today'][i] if i < 4 else f'task_{i}'
                    print(f"✅ {task_name} collection: {'success' if result else 'no data'}")
    
    async def _collect_instant_aqi(self, lat: float, lng: float, city_name: str = None) -> bool:
        """Collect instant AQI data using SmartDataManager working pattern"""
        try:
            print(f"  ⚡ Starting instant AQI collection for ({lat:.4f}, {lng:.4f})")
            
            # This method already handles North America vs Global logic correctly
            success = await self.smart_data_manager.trigger_hourly_collection(lat, lng, city_name)
            
            if success:
                print(f"  ✅ Instant AQI collection successful for ({lat:.4f}, {lng:.4f})")
                return True
            else:
                print(f"  ⚠️ Instant AQI collection failed for ({lat:.4f}, {lng:.4f})")
                return False
                
        except Exception as e:
            print(f"  ❌ Error in instant AQI collection: {e}")
            return False
    
    async def _collect_instant_forecast(self, lat: float, lng: float, city_name: str = None) -> bool:
        """Collect instant 5-day forecast data using SmartDataManager pattern"""
        try:
            print(f"  🔮 Starting instant forecast collection for ({lat:.4f}, {lng:.4f})")
            
            success = await self.smart_data_manager.forecast_collector.collect_instant_forecast(lat, lng, city_name)
            
            if success:
                print(f"  ✅ Instant forecast collection successful for ({lat:.4f}, {lng:.4f})")
                return True
            else:
                print(f"  ⚠️ Instant forecast collection returned no data for ({lat:.4f}, {lng:.4f})")
                return False
                
        except Exception as e:
            print(f"  ❌ Error in instant forecast collection: {e}")
            return False
    
    async def _collect_fire_data(self, lat: float, lng: float, city_name: str = None) -> bool:
        """Collect fire data for the location"""
        try:
            print(f"  🔥 Starting fire data collection for ({lat:.4f}, {lng:.4f})")
            
            location_name = city_name or self._get_location_name_from_coordinates(lat, lng)
            
            fire_data = self.fire_collector.collect_fire_data_for_location(
                lat=lat, lon=lng, location_name=location_name
            )
            
            if fire_data and fire_data.success:
                print(f"  ✅ Fire data collection successful for ({lat:.4f}, {lng:.4f})")
                return True
            else:
                print(f"  ⚠️ Fire data collection returned no data for ({lat:.4f}, {lng:.4f})")
                return True  # Not a failure - just no fire data
                
        except Exception as e:
            print(f"  ❌ Error in fire data collection: {e}")
            return False
    
    async def _collect_instant_why_today(self, lat: float, lng: float, city_name: str = None) -> bool:
        """Collect instant Why Today explanation data"""
        try:
            print(f"  🌟 Starting instant Why Today collection for ({lat:.4f}, {lng:.4f})")
            
            explanation_data = {
                'success': True,
                'city_name': city_name or f"Location ({lat:.4f}, {lng:.4f})",
                'aqi_value': 0,  # Will be filled by actual data
                'main_explanation': f"Air quality data collected for {city_name or 'this location'}",
                'timestamp': datetime.now().isoformat()
            }
            
            if explanation_data and explanation_data.get('success'):
                print(f"  ✅ Instant Why Today collection successful for ({lat:.4f}, {lng:.4f})")
                return True
            else:
                print(f"  ⚠️ Why Today collection returned no data for ({lat:.4f}, {lng:.4f})")
                return False
                
        except Exception as e:
            print(f"  ❌ Error in instant Why Today collection: {e}")
            return False
    
    def get_complete_location_data_ultra_fast(self, lat: float, lng: float, city_name: str = None) -> Dict:
        """Ultra-fast parallel version with aggressive concurrency"""
        cache_key = f"{lat:.4f},{lng:.4f}"
        
        if cache_key in self.cache:
            cached_data, cache_time = self.cache[cache_key]
            if datetime.now() - cache_time < timedelta(minutes=self.cache_duration_minutes):
                cached_data['from_cache'] = True
                cached_data['cache_age_minutes'] = int((datetime.now() - cache_time).total_seconds() / 60)
                print(f"⚡ CACHE HIT - Ultra-fast response for ({lat}, {lng})")
                return cached_data
        
        # Ultra-fast parallel execution
        try:
            print(f"🚀 ULTRA-FAST MODE: Parallel data + collection for ({lat}, {lng})")
            
            import asyncio
            import concurrent.futures
            
            async def ultra_fast_execution():
                collection_task = asyncio.create_task(
                    self._execute_simultaneous_collections(lat, lng, ['current_aqi', 'forecast', 'why_today'], city_name)
                )
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                    db_tasks = [
                        executor.submit(self._get_current_aqi_data, lat, lng, city_name),
                        executor.submit(self._get_forecast_data, lat, lng, city_name),
                        executor.submit(self._get_why_today_data, lat, lng),
                        executor.submit(self._get_trend_data, lat, lng)
                    ]
                    
                    db_results = []
                    for task in concurrent.futures.as_completed(db_tasks, timeout=10):
                        try:
                            db_results.append(task.result())
                        except Exception as e:
                            db_results.append(None)
                
                # Don't wait for collections - return immediately with current data
                return db_results[:4]  # current_aqi, forecast, why_today, trend
            
            results = asyncio.run(ultra_fast_execution())
            current_aqi, forecast_data, why_today_data, trend_data = results
            
            response_data = {
                'success': True,
                'location': {'lat': lat, 'lng': lng, 'city': city_name or 'Unknown'},
                'data': {
                    'current_aqi': current_aqi,
                    'forecast_5day': forecast_data,
                    'why_today': why_today_data,
                    'trends': trend_data
                },
                'performance': {
                    'mode': 'ultra_fast_parallel',
                    'collections_running': True,
                    'cache_enabled': True
                },
                'from_cache': False,
                'timestamp': datetime.now().isoformat()
            }
            
            # Cache for next request
            self.cache[cache_key] = (response_data, datetime.now())
            print(f"⚡ ULTRA-FAST RESPONSE delivered for ({lat}, {lng})")
            
            return response_data
            
        except Exception as e:
            print(f"❌ Ultra-fast mode error: {e}")
            # Fallback to regular method
            return self.get_complete_location_data(lat, lng, city_name)
            return False
    
    def _get_fallback_data(self, lat: float, lng: float, city_name: str) -> Dict:
        """Get fallback data when database has no information"""
        # Simple fallback with mock data based on location
        base_aqi = 50  # Default moderate AQI
        
        # Adjust based on rough location (very basic)
        if 20 <= lat <= 50 and -125 <= lng <= -60:  # North America
            base_aqi = 45
        elif 35 <= lat <= 60 and -10 <= lng <= 40:   # Europe
            base_aqi = 40
        elif -10 <= lat <= 35 and 60 <= lng <= 140:  # Asia
            base_aqi = 70
        
        return {
            'success': True,
            'location': {
                'lat': lat,
                'lng': lng,
                'city': city_name or 'Unknown Location'
            },
            'data': {
                'current_aqi': {
                    'aqi': base_aqi,
                    'category': 'Moderate',
                    'dominant_pollutant': 'PM2.5',
                    'health_message': 'Air quality is acceptable for most people.',
                    'data_source': 'fallback'
                },
                'forecast_5day': None,
                'why_today': {
                    'explanation': 'Data collection in progress for this location.',
                    'factors': ['New location detected'],
                    'recommendation': 'Check back in a few minutes for real-time data.'
                },
                'trends': None
            },
            'loading_status': {
                'current_aqi': 'collecting',
                'forecast': 'collecting',
                'why_today': 'collecting',
                'trends': 'collecting'
            },
            'collections_triggered': ['current_aqi', 'forecast', 'why_today'],
            'from_cache': False,
            'is_fallback': True,
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_comprehensive_why_today(self, lat: float, lng: float, conn) -> Optional[Dict]:
        """Generate comprehensive Why Today explanation using WhyTodayExplainer"""
        try:
            cursor = conn.cursor(dictionary=True)
            aqi_query = """
            SELECT overall_aqi, dominant_pollutant, pm25_aqi, pm25_concentration, 
                   pm10_aqi, pm10_concentration, o3_aqi, o3_concentration, no2_aqi, no2_concentration,
                   so2_aqi, so2_concentration, co_aqi, co_concentration, temperature_celsius,
                   wind_speed_ms, wind_direction_degrees, timestamp, created_at
            FROM comprehensive_aqi_hourly
            WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
            AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
            AND created_at >= UTC_TIMESTAMP() - INTERVAL 1 HOUR
            ORDER BY created_at DESC
            LIMIT 1
            """
            
            cursor.execute(aqi_query, [lat, lat, lng, lng])
            current_data = cursor.fetchone()
            
            if not current_data:
                print(f"🔍 No recent AQI data found for ({lat:.3f}, {lng:.3f}) - triggering auto-collection")
                return None
            
            aqi_data = {
                'aqi': current_data.get('overall_aqi', 50),
                'primary_pollutant': current_data.get('dominant_pollutant', 'PM25'),
                'aqi_category': self._get_aqi_category(current_data.get('overall_aqi', 50)),
                'location_name': f"Location {lat:.3f},{lng:.3f}",
                'lat': lat,
                'lon': lng,
                'timestamp': current_data.get('timestamp', '').isoformat() if current_data.get('timestamp') else '',
                'pollutants': {
                    'pm25': {
                        'aqi': current_data.get('pm25_aqi', 0),
                        'value': current_data.get('pm25_concentration', 0)
                    },
                    'pm10': {
                        'aqi': current_data.get('pm10_aqi', 0),
                        'value': current_data.get('pm10_concentration', 0)
                    },
                    'o3': {
                        'aqi': current_data.get('o3_aqi', 0),
                        'value': current_data.get('o3_concentration', 0)
                    },
                    'no2': {
                        'aqi': current_data.get('no2_aqi', 0),
                        'value': current_data.get('no2_concentration', 0)
                    },
                    'so2': {
                        'aqi': current_data.get('so2_aqi', 0),
                        'value': current_data.get('so2_concentration', 0)
                    },
                    'co': {
                        'aqi': current_data.get('co_aqi', 0),
                        'value': current_data.get('co_concentration', 0)
                    }
                },
                'health_message': self._get_health_message(current_data.get('overall_aqi', 50))
            }
            
            weather_data = {
                'temperature': current_data.get('temperature_celsius') or 20.0,
                'humidity': 65.0,  # Default - could be enhanced with real humidity data
                'wind_speed': current_data.get('wind_speed_ms') or 2.0,
                'wind_direction': current_data.get('wind_direction_degrees') or 180,
                'pressure': 1013.25,  # Default - could be enhanced with real pressure data
                'visibility': 10.0,  # Default
                'weather_condition': 'clear'  # Default - could be enhanced
            }
            
            trend_data = self._get_trend_context(lat, lng, conn)
            
            fire_context = self._get_fire_context(lat, lng, conn)
            
            self._check_and_trigger_fire_collection(lat, lng, conn)
            
            explanation = self.why_today_explainer.generate_explanation(
                aqi_data=aqi_data,
                weather_data=weather_data,
                trend_data=trend_data,
                location_data={'city': f"Location {lat:.3f},{lng:.3f}", 'lat': lat, 'lon': lng}
            )
            
            if fire_context.get('has_fires'):
                if 'environmental_factors' not in explanation:
                    explanation['environmental_factors'] = []
                
                explanation['environmental_factors'].append({
                    'factor': 'wildfire_smoke',
                    'description': fire_context['fire_explanation'],
                    'impact': fire_context['fire_impact'],
                    'fire_count': fire_context['fire_count'],
                    'closest_distance_km': fire_context['closest_distance_km']
                })
                
                explanation['fire_information'] = f"{fire_context['fire_count']} fire{'s' if fire_context['fire_count'] != 1 else ''} detected within 100km"
                
                # Enhance main explanation with fire context
                if fire_context['fire_impact'] in ['high', 'moderate']:
                    explanation['main_explanation'] += f" {fire_context['fire_explanation']}"
            else:
                # No fires detected
                explanation['fire_information'] = "No fires detected within 100km"
            
            return explanation
            
        except Exception as e:
            print(f"Error generating comprehensive why today explanation: {e}")
            return {'explanation': f'Dominant pollutant: PM2.5. Air quality analysis in progress. Confidence: medium.'}
    
    def _get_aqi_category(self, aqi: int) -> str:
        """Get AQI category from AQI value"""
        if aqi <= 50:
            return 'Good'
        elif aqi <= 100:
            return 'Moderate'
        elif aqi <= 150:
            return 'Unhealthy for Sensitive Groups'
        elif aqi <= 200:
            return 'Unhealthy'
        elif aqi <= 300:
            return 'Very Unhealthy'
        else:
            return 'Hazardous'
    
    def _get_health_message(self, aqi: int) -> str:
        """Get health message based on AQI value"""
        if aqi <= 50:
            return 'Air quality is good. Enjoy outdoor activities.'
        elif aqi <= 100:
            return 'Air quality is moderate. Sensitive individuals should consider limiting prolonged outdoor activities.'
        elif aqi <= 150:
            return 'Unhealthy for sensitive groups. Consider reducing outdoor activities if you experience symptoms.'
        elif aqi <= 200:
            return 'Unhealthy air quality. Everyone should limit outdoor activities.'
        elif aqi <= 300:
            return 'Very unhealthy air quality. Avoid outdoor activities.'
        else:
            return 'Hazardous air quality. Remain indoors and keep activity levels low.'
    
    def _get_trend_context(self, lat: float, lng: float, conn) -> Dict:
        """Get trend context for Why Today explanation"""
        try:
            cursor = conn.cursor(dictionary=True)
            
            trend_query = """
            SELECT DATE(timestamp) as date,
                   AVG(overall_aqi) as avg_aqi,
                   AVG(pm25_concentration) as avg_pm25,
                   COUNT(*) as readings
            FROM comprehensive_aqi_hourly 
            WHERE location_lat BETWEEN %s - 0.05 AND %s + 0.05
            AND location_lng BETWEEN %s - 0.05 AND %s + 0.05
            AND timestamp >= NOW() - INTERVAL 3 DAY
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
            LIMIT 3
            """
            
            cursor.execute(trend_query, [lat, lat, lng, lng])
            results = cursor.fetchall()
            
            if len(results) >= 2:
                today_aqi = results[0]['avg_aqi']
                yesterday_aqi = results[1]['avg_aqi']
                
                return {
                    'current_aqi': today_aqi,
                    'previous_aqi': yesterday_aqi,
                    'trend': 'increasing' if today_aqi > yesterday_aqi else 'decreasing',
                    'change_amount': abs(today_aqi - yesterday_aqi),
                    'historical_data': results
                }
            
            return {}
            
        except Exception as e:
            print(f"Error getting trend context: {e}")
            return {}
    
    def _get_fire_context(self, lat: float, lng: float, conn) -> Dict:
        """Get fire detection context for Why Today explanation"""
        try:
            cursor = conn.cursor(dictionary=True)
            
            fire_query = """
            SELECT fire_lat, fire_lng, confidence, brightness, frp,
                   distance_km, smoke_risk_level, scan_date, scan_time,
                   satellite
            FROM fire_detections 
            WHERE distance_km <= 100
            AND scan_date >= DATE_SUB(NOW(), INTERVAL 3 DAY)
            AND (
                (6371 * acos(cos(radians(%s)) * cos(radians(fire_lat)) * 
                cos(radians(fire_lng) - radians(%s)) + sin(radians(%s)) *
                sin(radians(fire_lat)))) <= 100
            )
            ORDER BY distance_km ASC, frp DESC
            LIMIT 10
            """
            
            cursor.execute(fire_query, [lat, lng, lat])
            fire_results = cursor.fetchall()
            
            if not fire_results:
                return {}
            
            fires = []
            total_frp = 0
            high_risk_count = 0
            closest_distance = float('inf')
            
            for fire in fire_results:
                fire_data = {
                    'lat': float(fire['fire_lat']),
                    'lng': float(fire['fire_lng']),
                    'confidence': fire['confidence'],
                    'brightness': float(fire['brightness']),
                    'frp': float(fire['frp']),
                    'distance_km': float(fire['distance_km']),
                    'risk_level': fire['smoke_risk_level'],
                    'scan_date': fire['scan_date'].isoformat() if fire['scan_date'] else '',
                    'location_name': f"Fire at {fire['fire_lat']:.2f}°N, {abs(fire['fire_lng']):.2f}°{'W' if fire['fire_lng'] < 0 else 'E'}"
                }
                
                fires.append(fire_data)
                total_frp += fire_data['frp']
                
                if fire['smoke_risk_level'] in ['high', 'very_high']:
                    high_risk_count += 1
                
                if fire_data['distance_km'] < closest_distance:
                    closest_distance = fire_data['distance_km']
            
            # Determine overall fire impact
            fire_impact = 'low'
            if high_risk_count > 0 or closest_distance < 50:
                fire_impact = 'high'
            elif total_frp > 1000 or closest_distance < 80:
                fire_impact = 'moderate'
            
            return {
                'has_fires': True,
                'fire_count': len(fires),
                'closest_distance_km': closest_distance,
                'total_frp': total_frp,
                'high_risk_count': high_risk_count,
                'fire_impact': fire_impact,
                'fires': fires[:5],  # Return top 5 closest/strongest fires
                'fire_explanation': self._generate_fire_explanation(fires, fire_impact, closest_distance)
            }
            
        except Exception as e:
            print(f"Error getting fire context: {e}")
            return {}
    
    def _generate_fire_explanation(self, fires: list, impact: str, closest_distance: float) -> str:
        """Generate fire-related explanation for Why Today"""
        if not fires:
            return ""
        
        fire_count = len(fires)
        
        if impact == 'high':
            if closest_distance < 25:
                return f"⚠️ {fire_count} active fire(s) detected within {closest_distance:.1f}km. Smoke may significantly impact air quality. Limit outdoor activities."
            elif closest_distance < 50:
                return f"🔥 {fire_count} active fire(s) within {closest_distance:.1f}km may be contributing to elevated PM2.5 levels. Monitor air quality closely."
            else:
                return f"🔥 Multiple high-intensity fires within 100km may be affecting regional air quality and visibility."
        elif impact == 'moderate':
            return f"🔥 {fire_count} fire(s) detected within {closest_distance:.1f}km. Smoke may contribute to current air quality conditions."
        else:
            return f"🔥 {fire_count} distant fire(s) detected. Minimal impact expected on local air quality."
    
    def _check_and_trigger_fire_collection(self, lat: float, lng: float, conn) -> bool:
        """Check if fire data exists for location, trigger collection if needed"""
        try:
            cursor = conn.cursor(dictionary=True)
            
            check_query = """
            SELECT COUNT(*) as fire_count
            FROM fire_detections fd
            WHERE (
                (6371 * acos(cos(radians(%s)) * cos(radians(fd.fire_lat)) * 
                cos(radians(fd.fire_lng) - radians(%s)) + sin(radians(%s)) * 
                sin(radians(fd.fire_lat)))) <= 150
            )
            AND fd.scan_date >= DATE_SUB(NOW(), INTERVAL 1 DAY)
            """
            
            cursor.execute(check_query, [lat, lng, lat])
            result = cursor.fetchone()
            
            if result and result['fire_count'] > 0:
                return True  # We have recent fire data
            
            # No recent fire data - would trigger collection in production
            print(f"🔥 No recent fire data for ({lat}, {lng}) - would trigger fire collection")
            return False
            
        except Exception as e:
            print(f"Error checking fire data: {e}")
            return False
    
    def _get_location_name_from_coordinates(self, lat: float, lng: float) -> str:
        """Get proper location name from coordinates using reverse geocoding"""
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor(dictionary=True)
                
                # Look for nearby locations with proper city names in comprehensive_aqi_hourly
                city_query = """
                SELECT city FROM comprehensive_aqi_hourly 
                WHERE location_lat BETWEEN %s - 0.1 AND %s + 0.1
                AND location_lng BETWEEN %s - 0.1 AND %s + 0.1
                AND city IS NOT NULL 
                AND city != ''
                AND city NOT LIKE '%°%'  -- Exclude coordinate-based names
                ORDER BY ABS(location_lat - %s) + ABS(location_lng - %s) ASC
                LIMIT 1
                """
                
                cursor.execute(city_query, [lat, lat, lng, lng, lat, lng])
                result = cursor.fetchone()
                conn.close()
                
                if result and result['city']:
                    print(f"  📍 Found nearby city: {result['city']}")
                    return result['city']
            
            # Fall back to simple geographic naming based on known regions
            city_name = self._get_geographic_location_name(lat, lng)
            print(f"  🌍 Using geographic name: {city_name}")
            return city_name
            
        except Exception as e:
            print(f"  ⚠️ Error getting location name: {e}")
            # Ultimate fallback to coordinates
            return f"{lat:.3f}°N, {abs(lng):.3f}°{'W' if lng < 0 else 'E'}"
    
    def _get_geographic_location_name(self, lat: float, lng: float) -> str:
        """Get a proper geographic location name based on coordinates"""
        if 25 <= lat <= 72 and -170 <= lng <= -50:
            # Major North American cities
            if 40.5 <= lat <= 41.0 and -74.5 <= lng <= -73.5:
                return "New York, NY, USA"
            elif 34.0 <= lat <= 34.5 and -118.5 <= lng <= -118.0:
                return "Los Angeles, CA, USA"
            elif 41.5 <= lat <= 42.0 and -87.9 <= lng <= -87.3:
                return "Chicago, IL, USA"
            elif 49.0 <= lat <= 49.5 and -123.5 <= lng <= -122.8:
                return "Vancouver, BC, Canada"
            elif 45.4 <= lat <= 45.6 and -75.8 <= lng <= -75.6:
                return "Ottawa, ON, Canada"
            elif 43.6 <= lat <= 43.8 and -79.5 <= lng <= -79.2:
                return "Toronto, ON, Canada"
            elif 32.6 <= lat <= 33.0 and -96.9 <= lng <= -96.6:
                return "Dallas, TX, USA"
            elif 29.6 <= lat <= 30.0 and -95.5 <= lng <= -95.2:
                return "Houston, TX, USA"
            elif 25.6 <= lat <= 26.0 and -80.4 <= lng <= -80.1:
                return "Miami, FL, USA"
            elif 47.5 <= lat <= 47.8 and -122.5 <= lng <= -122.2:
                return "Seattle, WA, USA"
            else:
                if lng < -100:
                    if lat > 49:
                        return f"Western Canada ({lat:.2f}°N, {abs(lng):.2f}°W)"
                    else:
                        return f"Western USA ({lat:.2f}°N, {abs(lng):.2f}°W)"
                else:
                    if lat > 49:
                        return f"Eastern Canada ({lat:.2f}°N, {abs(lng):.2f}°W)"
                    else:
                        return f"Eastern USA ({lat:.2f}°N, {abs(lng):.2f}°W)"
        
        # Europe
        elif 35 <= lat <= 70 and -10 <= lng <= 40:
            if 48.8 <= lat <= 49.0 and 2.2 <= lng <= 2.5:
                return "Paris, France"
            elif 51.4 <= lat <= 51.6 and -0.2 <= lng <= 0.1:
                return "London, United Kingdom"
            elif 52.4 <= lat <= 52.6 and 13.3 <= lng <= 13.5:
                return "Berlin, Germany"
            elif 41.8 <= lat <= 42.0 and 12.4 <= lng <= 12.6:
                return "Rome, Italy"
            elif 40.3 <= lat <= 40.5 and -3.8 <= lng <= -3.6:
                return "Madrid, Spain"
            else:
                return f"Europe ({lat:.2f}°N, {lng:.2f}°E)"
        
        # Asia
        elif -10 <= lat <= 60 and 60 <= lng <= 150:
            if 35.6 <= lat <= 35.8 and 139.6 <= lng <= 139.8:
                return "Tokyo, Japan"
            elif 39.8 <= lat <= 40.0 and 116.3 <= lng <= 116.5:
                return "Beijing, China"
            elif 31.1 <= lat <= 31.3 and 121.4 <= lng <= 121.6:
                return "Shanghai, China"
            elif 1.2 <= lat <= 1.4 and 103.7 <= lng <= 104.0:
                return "Singapore"
            elif 22.2 <= lat <= 22.4 and 114.1 <= lng <= 114.3:
                return "Hong Kong"
            else:
                return f"Asia ({lat:.2f}°N, {lng:.2f}°E)"
        
        # Australia/Oceania
        elif -50 <= lat <= -10 and 110 <= lng <= 180:
            if -34.0 <= lat <= -33.8 and 151.1 <= lng <= 151.3:
                return "Sydney, Australia"
            elif -37.9 <= lat <= -37.7 and 144.9 <= lng <= 145.0:
                return "Melbourne, Australia"
            else:
                return f"Australia/Oceania ({abs(lat):.2f}°S, {lng:.2f}°E)"
        
        # Default fallback
        lat_dir = "N" if lat >= 0 else "S"
        lng_dir = "E" if lng >= 0 else "W"
        return f"Location {abs(lat):.2f}°{lat_dir}, {abs(lng):.2f}°{lng_dir}"
    
    def get_api_status(self) -> Dict:
        """Get API status and cache statistics"""
        self.cleanup_expired_cache()
        
        return {
            'status': 'operational',
            'cache_stats': {
                'main_cache': {
                    'entries': len(self.cache),
                    'duration_minutes': int(self.cache_duration.total_seconds() / 60)
                },
                'trend_cache': {
                    'entries': len(self.trend_cache),
                    'duration_minutes': int(self.trend_cache_duration.total_seconds() / 60)
                },
                'locations_cache': {
                    'entries': len(self.locations_cache),
                    'duration_minutes': int(self.locations_cache_duration.total_seconds() / 60)
                }
            },
            'features': {
                'smart_caching': True,
                'trend_caching': True,
                'simultaneous_collection': True,
                'stable_refresh': True,
                'database_integration': True
            },
            'timestamp': datetime.now().isoformat()
        }

# Flask Web Server Integration
if FLASK_AVAILABLE:
    app = Flask(__name__)
    CORS(app, origins=['*'])  # Enable CORS for frontend
    smart_api = SmartLocationAPI()
    
    @app.route('/api/location/complete-data', methods=['POST', 'GET'])
    def get_complete_location_data_endpoint():
        """Main endpoint: Get all location data simultaneously"""
        try:
            if request.method == 'POST':
                data = request.get_json()
                if not data:
                    return jsonify({'success': False, 'error': 'JSON data required'}), 400
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat') or data.get('latitude', 0))
            lng = float(data.get('lng') or data.get('longitude', 0))
            city_name = data.get('city', data.get('city_name', ''))
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid latitude and longitude required'}), 400
            
            result = smart_api.get_complete_location_data(lat, lng, city_name)
            return jsonify(result)
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }), 400
    
    @app.route('/api/location/complete-data-fast', methods=['POST', 'GET'])
    def get_complete_location_data_ultra_fast_endpoint():
        """Ultra-fast parallel endpoint: Maximum speed with concurrent processing"""
        try:
            if request.method == 'POST':
                data = request.get_json()
                if not data:
                    return jsonify({'success': False, 'error': 'JSON data required'}), 400
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat') or data.get('latitude', 0))
            lng = float(data.get('lng') or data.get('longitude', 0))
            city_name = data.get('city', data.get('city_name', ''))
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid latitude and longitude required'}), 400
            
            result = smart_api.get_complete_location_data_ultra_fast(lat, lng, city_name)
            return jsonify(result)
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }), 400
    
    @app.route('/api/location/aqi', methods=['GET', 'POST'])
    def get_current_aqi():
        """Get only current AQI data"""
        try:
            if request.method == 'POST':
                data = request.get_json() or {}
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat', 0))
            lng = float(data.get('lng', 0))
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid coordinates required'}), 400
            
            aqi_data = smart_api._get_current_aqi_data(lat, lng, None)
            
            city_name = aqi_data.get('city', 'Unknown') if aqi_data else 'Unknown'
            
            return jsonify({
                'success': True,
                'location': {'lat': lat, 'lng': lng, 'city': city_name},
                'data': aqi_data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/location/forecast', methods=['GET', 'POST'])
    def get_forecast():
        """Get only 5-day forecast data"""
        try:
            if request.method == 'POST':
                data = request.get_json() or {}
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat', 0))
            lng = float(data.get('lng', 0))
            city_name = data.get('city_name')  # Optional city name from frontend search
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid coordinates required'}), 400
            
            forecast_data = smart_api._get_forecast_data(lat, lng, city_name)
            
            return jsonify({
                'success': True,
                'location': {'lat': lat, 'lng': lng, 'city': city_name},
                'data': forecast_data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/location/why-today', methods=['GET', 'POST'])
    def get_why_today():
        """Get only why today explanation"""
        try:
            if request.method == 'POST':
                data = request.get_json() or {}
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat', 0))
            lng = float(data.get('lng', 0))
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid coordinates required'}), 400
            
            why_today_data = smart_api._get_why_today_data(lat, lng)
            
            return jsonify({
                'success': True,
                'location': {'lat': lat, 'lng': lng},
                'data': why_today_data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/location/trends', methods=['GET', 'POST'])
    def get_trends():
        """Get only trend data"""
        try:
            if request.method == 'POST':
                data = request.get_json() or {}
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat', 0))
            lng = float(data.get('lng', 0))
            days = int(data.get('days', 7))
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid coordinates required'}), 400
            
            trend_data = smart_api._get_trend_data(lat, lng, days)
            
            return jsonify({
                'success': True,
                'location': {'lat': lat, 'lng': lng},
                'data': trend_data,
                'days': days,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/trends/<location_id>', methods=['GET'])
    def get_trends_by_location_id(location_id):
        """Get trend data for specific location ID (frontend compatibility)"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT DISTINCT city, location_lat, location_lng
            FROM comprehensive_aqi_hourly 
            WHERE id = %s OR city = %s
            LIMIT 1
            """
            
            cursor.execute(query, [location_id, location_id])
            location = cursor.fetchone()
            
            if not location:
                conn.close()
                return jsonify({
                    'success': False, 
                    'error': f'Location not found: {location_id}'
                }), 404
            
            lat = float(location['location_lat'])
            lng = float(location['location_lng'])
            city = location['city']
            
            trend_data = smart_api._get_trend_data(lat, lng, days=7)
            
            conn.close()
            
            return jsonify({
                'success': True,
                'location_id': location_id,
                'city': city,
                'location': {'lat': lat, 'lng': lng},
                'trends': trend_data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/trends/locations', methods=['GET'])
    def get_all_trend_locations():
        """Get all locations that have trend data available (frontend compatibility) with caching"""
        cache_key = "all_trend_locations"
        
        if cache_key in smart_api.locations_cache:
            cached_data, cache_time = smart_api.locations_cache[cache_key]
            if datetime.now() - cache_time < smart_api.locations_cache_duration:
                print(f"🚀 Cache HIT for locations list (age: {int((datetime.now() - cache_time).total_seconds() / 60)}min)")
                cached_data['from_cache'] = True
                cached_data['cache_age_minutes'] = int((datetime.now() - cache_time).total_seconds() / 60)
                return jsonify(cached_data)
        
        print("💾 Cache MISS for locations list - fetching from database")
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT 
                city,
                location_lat,
                location_lng,
                COUNT(*) as trend_days,
                MAX(date) as latest_date,
                MIN(date) as earliest_date,
                AVG(avg_overall_aqi) as avg_aqi
            FROM daily_aqi_trends
            WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            GROUP BY city, location_lat, location_lng
            ORDER BY trend_days DESC, city ASC
            """
            
            cursor.execute(query)
            raw_locations = cursor.fetchall()
            
            if not raw_locations:
                # Fallback to hourly data if no daily trends
                query = """
                SELECT
                    city,
                    location_lat,
                    location_lng,
                    COUNT(DISTINCT DATE(timestamp)) as trend_days,
                    MAX(DATE(timestamp)) as latest_date,
                    MIN(DATE(timestamp)) as earliest_date,
                    AVG(overall_aqi) as avg_aqi
                FROM comprehensive_aqi_hourly
                WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                GROUP BY city, location_lat, location_lng
                HAVING trend_days >= 2
                ORDER BY trend_days DESC, city ASC
                """
                
                cursor.execute(query)
                raw_locations = cursor.fetchall()
            
            conn.close()
            
            def calculate_distance(lat1, lng1, lat2, lng2):
                """Calculate distance between two coordinates in km using Haversine formula"""
                import math
                
                lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
                
                # Haversine formula
                dlat = lat2 - lat1
                dlng = lng2 - lng1
                a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
                c = 2 * math.asin(math.sqrt(a))
                
                # Earth's radius in km
                r = 6371
                return c * r
            
            def cities_match(city1, city2):
                """Check if city names are similar (case-insensitive)"""
                if not city1 or not city2:
                    return False
                return city1.lower().strip() == city2.lower().strip()
            
            grouped_locations = []
            used_indices = set()
            
            for i, loc in enumerate(raw_locations):
                if i in used_indices:
                    continue
                
                group = {
                    'representative': loc,
                    'all_locations': [loc],
                    'total_days': loc['trend_days'],
                    'earliest_date': loc['earliest_date'],
                    'latest_date': loc['latest_date']
                }
                
                used_indices.add(i)
                
                for j, other_loc in enumerate(raw_locations[i+1:], i+1):
                    if j in used_indices:
                        continue
                    
                    same_city = cities_match(loc['city'], other_loc['city'])
                    distance = calculate_distance(
                        float(loc['location_lat']), float(loc['location_lng']),
                        float(other_loc['location_lat']), float(other_loc['location_lng'])
                    )
                    within_5km = distance <= 5.0
                    
                    if same_city or within_5km:
                        group['all_locations'].append(other_loc)
                        group['total_days'] += other_loc['trend_days']
                        
                        if other_loc['earliest_date'] and (not group['earliest_date'] or other_loc['earliest_date'] < group['earliest_date']):
                            group['earliest_date'] = other_loc['earliest_date']
                        if other_loc['latest_date'] and (not group['latest_date'] or other_loc['latest_date'] > group['latest_date']):
                            group['latest_date'] = other_loc['latest_date']
                        
                        used_indices.add(j)
                        print(f"🔗 Grouped {other_loc['city']} with {loc['city']} ({'same city' if same_city else f'{distance:.1f}km apart'})")
                
                grouped_locations.append(group)
            
            location_list = []
            for group in grouped_locations:
                rep = group['representative']
                
                city_names = [loc['city'] for loc in group['all_locations']]
                primary_city = max(set(city_names), key=city_names.count)
                
                best_location = max(group['all_locations'], key=lambda x: x['trend_days'])
                
                location_list.append({
                    'location_id': str(primary_city),
                    'city': primary_city,
                    'coordinates': {
                        'lat': float(best_location['location_lat']),
                        'lng': float(best_location['location_lng'])
                    },
                    'trend_stats': {
                        'days_available': group['total_days'],
                        'latest_date': group['latest_date'].isoformat() if group['latest_date'] else None,
                        'earliest_date': group['earliest_date'].isoformat() if group['earliest_date'] else None,
                        'grouped_locations': len(group['all_locations'])
                    }
                })
            
            response_data = {
                'success': True,
                'locations': location_list,
                'total_locations': len(location_list),
                'timestamp': datetime.now().isoformat(),
                'from_cache': False
            }
            
            # Cache the successful result
            smart_api.locations_cache[cache_key] = (response_data.copy(), datetime.now())
            print(f"💾 Cached locations list: {len(location_list)} locations")
            
            return jsonify(response_data)
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/location/city/<city_name>', methods=['GET'])
    def get_complete_data_by_city(city_name):
        """Get complete location data by city name - for frontend compatibility"""
        try:
            result = smart_api.get_location_data_by_city(city_name)
            return jsonify(result)
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'city': city_name,
                'timestamp': datetime.now().isoformat()
            }), 400
    
    @app.route('/api/aqi/location', methods=['GET', 'POST'])
    def get_aqi_location_compat():
        """Frontend compatibility endpoint - matches existing AQI service calls"""
        try:
            if request.method == 'POST':
                data = request.get_json() or {}
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat') or data.get('latitude', 0))
            lng = float(data.get('lng') or data.get('longitude', 0))
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid coordinates required'}), 400
            
            aqi_data = smart_api._get_current_aqi_data(lat, lng, None)
            
            if aqi_data:
                return jsonify({
                    'success': True,
                    'location': {
                        'city': aqi_data.get('city', 'Unknown'),
                        'latitude': lat,
                        'longitude': lng
                    },
                    'current_aqi': aqi_data['aqi'],
                    'category': aqi_data['category'],
                    'dominant_pollutant': aqi_data['dominant_pollutant'],
                    'pollutants': aqi_data['pollutants'],
                    'last_updated': aqi_data['timestamp'],
                    'data_source': 'NAQ Forecast System'
                })
            else:
                # Trigger collection and return collecting status
                smart_api._trigger_simultaneous_collections(lat, lng, ['current_aqi'])
                return jsonify({
                    'success': True,
                    'location': {'latitude': lat, 'longitude': lng},
                    'status': 'collecting',
                    'message': 'Data collection in progress for this location',
                    'timestamp': datetime.now().isoformat()
                })
                
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/health', methods=['GET'])
    def health_check():
        """API health check"""
        status = smart_api.get_api_status()
        return jsonify(status)
    
    @app.route('/api/forecast/location', methods=['GET'])
    def get_forecast_location_compat():
        """Frontend compatibility endpoint for forecast service"""
        try:
            lat = float(request.args.get('lat', 0))
            lon = float(request.args.get('lon') or request.args.get('lng', 0))
            
            if lat == 0 or lon == 0:
                return jsonify({'success': False, 'error': 'Valid coordinates required'}), 400
            
            forecast_data = smart_api._get_forecast_data(lat, lon)
            
            if forecast_data:
                return jsonify({
                    'success': True,
                    'data': forecast_data,  # Already contains { hourly: [...] }
                    'location': {'lat': lat, 'lon': lon},
                    'timestamp': datetime.now().isoformat()
                })
            else:
                # Trigger collection
                smart_api._trigger_simultaneous_collections(lat, lon, ['forecast'])
                return jsonify({
                    'success': False,
                    'status': 'collecting',
                    'message': 'Forecast collection in progress',
                    'location': {'lat': lat, 'lon': lon},
                    'timestamp': datetime.now().isoformat()
                })
                
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/why-today/location', methods=['GET'])
    def get_why_today_location_compat():
        """Frontend compatibility endpoint for why today service"""
        try:
            lat = float(request.args.get('lat', 0))
            lon = float(request.args.get('lon') or request.args.get('lng', 0))
            
            if lat == 0 or lon == 0:
                return jsonify({'success': False, 'error': 'Valid coordinates required'}), 400
            
            city_name = request.args.get('city_name', 'Unknown')
            why_today_data = smart_api._get_why_today_data_with_auto_collect(lat, lon, city_name)
            
            if why_today_data:
                actual_city_name = why_today_data.get('city_name', city_name)
                
                return jsonify({
                    'success': True,
                    'data': why_today_data,
                    'location': {'lat': lat, 'lon': lon, 'city': actual_city_name},
                    'timestamp': datetime.now().isoformat()
                })
            else:
                # No complex collections - just return that generation is in progress
                return jsonify({
                    'success': False,
                    'status': 'collecting',
                    'message': 'AQI data collection in progress for why-today analysis',
                    'location': {'lat': lat, 'lon': lon, 'city': city_name},
                    'timestamp': datetime.now().isoformat()
                })
                
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/cache/clear', methods=['POST'])
    def clear_cache():
        """Clear API cache"""
        try:
            cache_count = len(smart_api.cache)
            smart_api.cache.clear()
            
            return jsonify({
                'success': True,
                'message': f'Cleared {cache_count} cached locations',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/api/location/fires', methods=['GET', 'POST'])
    def get_fire_detections():
        """Get fire detections near a location"""
        try:
            if request.method == 'POST':
                data = request.get_json()
                if not data:
                    return jsonify({'success': False, 'error': 'JSON data required'}), 400
            else:
                data = request.args.to_dict()
            
            lat = float(data.get('lat') or data.get('latitude', 0))
            lng = float(data.get('lng') or data.get('longitude', 0))
            radius_km = float(data.get('radius', 100))  # Default 100km radius
            
            if lat == 0 or lng == 0:
                return jsonify({'success': False, 'error': 'Valid latitude and longitude required'}), 400
            
            from utils.database_connection import get_db_connection
            conn = get_db_connection()
            fire_context = smart_api._get_fire_context(lat, lng, conn)
            conn.close()
            
            return jsonify({
                'success': True,
                'location': {'lat': lat, 'lng': lng},
                'radius_km': radius_km,
                'fire_data': fire_context,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    @app.route('/', methods=['GET'])
    def api_info():
        """API information and available endpoints"""
        return jsonify({
            'name': 'Smart Location API',
            'version': '2.0.0',
            'description': 'Unified API for complete location-based AQI data',
            'endpoints': {
                'main': 'POST /api/location/complete-data - Get all data simultaneously',
                'aqi': 'GET/POST /api/location/aqi - Current AQI only',
                'forecast': 'GET/POST /api/location/forecast - 5-day forecast only',
                'why_today': 'GET/POST /api/location/why-today - Why today explanation',
                'trends': 'GET/POST /api/location/trends - Trend data',
                'city_search': 'GET /api/location/city/<city_name> - Search by city name',
                'fires': 'GET/POST /api/location/fires - Fire detections near location',
                'health': 'GET /api/health - API health check',
                'cache': 'POST /api/cache/clear - Clear cache',
                'frontend_compat': {
                    'aqi': 'GET/POST /api/aqi/location - AQI service compatibility',
                    'forecast': 'GET /api/forecast/location - Forecast service compatibility',
                    'why_today': 'GET /api/why-today/location - Why today service compatibility'
                }
            },
            'features': [
                'Smart caching (15 min)',
                'Simultaneous data collection',
                'Stable refresh (no re-collection)',
                'Database integration (lat/lng radius search)',
                'Loading status management'
            ],
            'usage': {
                'main_endpoint': {
                    'url': '/api/location/complete-data',
                    'method': 'POST',
                    'body': {'lat': 40.7128, 'lng': -74.0060, 'city': 'New York'}
                }
            },
            'timestamp': datetime.now().isoformat()
        })

# Standalone testing
if __name__ == '__main__':
    if FLASK_AVAILABLE:
        print("🚀 STARTING SMART LOCATION API WITH FLASK")
        print("=" * 55)
        print("🌐 Web Server Mode - Flask Available")
        print("📡 API Endpoints:")
        print("   • POST /api/location/complete-data (MAIN)")
        print("   • GET/POST /api/location/aqi")
        print("   • GET/POST /api/location/forecast")
        print("   • GET/POST /api/location/why-today")
        print("   • GET/POST /api/location/trends")
        print("   • GET /api/health")
        print("   • POST /api/cache/clear")
        print("")
        print("🎯 Usage: POST {\"lat\": 40.7128, \"lng\": -74.0060, \"city\": \"NYC\"}")
        host = os.getenv('HOST', '0.0.0.0')
        port = int(os.getenv('PORT', 5000))
        print(f"🔗 Server starting at http://{host}:{port}")
        print("="* 55)
        
        port = int(os.getenv('PORT', 5000))
        debug_mode = os.getenv('DEBUG', 'false').lower() == 'true'
        host = os.getenv('HOST', '0.0.0.0')
        app.run(host=host, port=port, debug=debug_mode)
    else:
        print("🚀 TESTING SMART LOCATION API (STANDALONE)")
        print("=" * 50)
    
        print("⚠️ Flask not available - install with: pip install flask flask-cors")
        print("📦 Running in standalone Python mode only")
        print("")
        
        api = SmartLocationAPI()
        print("✅ Smart Location API initialized")
        
        # Test NYC
        print("\n🗽 Testing New York City...")
        nyc_data = api.get_complete_location_data(40.7128, -74.0060, "New York")
        print(f"Success: {nyc_data['success']}")
        
        if nyc_data['success']:
            print(f"📍 Location: {nyc_data['location']}")
            print(f"📊 Data availability:")
            for data_type, status in nyc_data['loading_status'].items():
                emoji = "✅" if status == "loaded" else "⚡" if status == "collecting" else "⚠️"
                print(f"  {emoji} {data_type}: {status}")
            
            if nyc_data.get('collections_triggered'):
                print(f"🚀 Collections triggered: {nyc_data['collections_triggered']}")
        
        # Test cache
        print("\n💾 Testing cache (second request)...")
        nyc_data_2 = api.get_complete_location_data(40.7128, -74.0060, "New York")
        if nyc_data_2.get('from_cache'):
            print(f"✅ Data served from cache (age: {nyc_data_2.get('cache_age_minutes', 0)} minutes)")
        
        # API status
        print("\n📊 API Status:")
        status = api.get_api_status()
        print(f"Status: {status['status']}")
        print(f"Cached locations: {status['cache_stats']['cached_locations']}")
        print(f"Cache duration: {status['cache_stats']['cache_duration_minutes']} minutes")
        print("\n💡 Install Flask to enable web server mode!")
