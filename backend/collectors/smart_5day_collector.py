#!/usr/bin/env python3
"""
Smart 5-Day Forecast Collector with Location Optimization
=========================================================
Collects 5-day forecast data for priority locations:
1. Every 5 days for existing locations (refresh forecasts)
2. Instantly for new locations (first-time collection)
3. Uses SmartLocationOptimizer to manage collection priorities

ENHANCED FEATURES:
- Checks database before collecting (avoids duplicates)
- Stores 5-day forecast data in MySQL forecast tables
- Optimized collection schedule (every 5 days vs daily)
- Instant collection for new high-priority locations

This prevents excessive forecast collection while ensuring fresh data.
"""

import asyncio
import logging
import os
import sys
import json
import mysql.connector
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any

current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from processors.location_optimizer import SmartLocationOptimizer
from collectors.forecast_5day_collector import Forecast5DayCollector
from utils.database_connection import get_db_connection

logger = logging.getLogger(__name__)

class Smart5DayCollector:
    """
    Smart collector for 5-day forecasts with location optimization
    - Collects every 5 days for existing locations
    - Instant collection for new priority locations
    - Database-aware to prevent duplicate collections
    """
    
    def __init__(self):
        # Lazy loading - only initialize when needed
        self._forecast_collector = None
        self._optimizer = None
        
        # Quick database connection test
        self._test_database_connection()
    
    @property
    def forecast_collector(self):
        """Lazy load forecast collector only when needed"""
        if self._forecast_collector is None:
            self._forecast_collector = Forecast5DayCollector()
        return self._forecast_collector
    
    @property
    def optimizer(self):
        """Lazy load optimizer only when needed"""
        if self._optimizer is None:
            self._optimizer = SmartLocationOptimizer()
        return self._optimizer
        
        self.stats = {
            'locations_checked': 0,
            'forecasts_collected': 0,
            'instant_collections': 0,
            'skipped_recent': 0,
            'mysql_stored': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
    
    async def run_5day_collection(self):
        """Run smart 5-day forecast collection"""
        self.stats['start_time'] = datetime.now()
        
        logger.info("🔮 Starting smart 5-day forecast collection...")
        
        priority_locations = self.optimizer.get_priority_locations(50)  # Limit to top 50 for forecasts
        
        logger.info(f"📍 Found {len(priority_locations)} priority locations for forecast collection")
        
        if not priority_locations:
            logger.info("✅ No priority locations found - system optimized!")
            return
        
        for location in priority_locations:
            await self._process_location_forecast(location)
        
        self.stats['end_time'] = datetime.now()
        self._log_collection_summary()
    
    def _test_database_connection(self):
        """Quick database connection test"""
        try:
            conn = get_db_connection()
            if conn:
                conn.close()
                        # 5-day collector ready
                return True
            else:
                logger.warning("⚠️ Smart 5-day collector: No database connection")
                return False
        except Exception as e:
            logger.warning(f"⚠️ Smart 5-day collector: Database check failed - {e}")
            return False
    
    async def _process_location_forecast(self, location: Dict):
        """Process 5-day forecast for a single location"""
        try:
            lat, lon = location.latitude, location.longitude
            city_name = location.city
            
            self.stats['locations_checked'] += 1
            
            logger.info(f"🔄 Checking forecast for: {city_name} ({lat:.4f}, {lon:.4f})")
            
            needs_collection = self._needs_forecast_collection(lat, lon, city_name)
            
            if needs_collection:
                logger.info(f"📊 Collecting 5-day forecast for {city_name}...")
                
                forecast_result = self.forecast_collector.collect_and_process_immediately(
                    lat=lat, 
                    lon=lon, 
                    location_name=city_name
                )
                
                if forecast_result and hasattr(forecast_result, 'forecast_stored') and forecast_result.forecast_stored:
                    self.stats['forecasts_collected'] += 1
                    
                    # Mark as collected in optimizer
                    location_key = self.optimizer.generate_location_key(lat, lon)
                    self.optimizer.mark_collected(location_key, 1.0)  # High quality score
                    
                    logger.info(f"✅ 5-day forecast collected for {city_name}")
                    
                    if self._is_new_location(lat, lon):
                        self.stats['instant_collections'] += 1
                        logger.info(f"🚀 Instant collection completed for new location: {city_name}")
                
                else:
                    logger.warning(f"⚠️ Failed to collect forecast for {city_name}")
                    self.stats['errors'] += 1
            
            else:
                self.stats['skipped_recent'] += 1
                logger.info(f"⏭️ Skipped {city_name} (recent forecast exists)")
                
        except Exception as e:
            logger.error(f"❌ Error processing forecast for {city_name}: {e}")
            self.stats['errors'] += 1
    
    def _needs_forecast_collection(self, lat: float, lon: float, city_name: str) -> bool:
        """Check if location needs 5-day forecast collection"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.warning("⚠️ Database connection failed - collecting all forecasts")
                return True
            cursor = conn.cursor()
            
            query = """
            SELECT MAX(forecast_timestamp) as last_forecast, 
                   MAX(DATE_ADD(forecast_timestamp, INTERVAL forecast_hour HOUR)) as latest_forecast_date,
                   COUNT(*) as total_forecasts
            FROM forecast_5day_data 
            WHERE location_name = %s 
            AND location_lat BETWEEN %s - 0.01 AND %s + 0.01
            AND location_lng BETWEEN %s - 0.01 AND %s + 0.01
            AND forecast_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """
            
            cursor.execute(query, (city_name, lat, lat, lon, lon))
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:  # Has recent forecast data
                last_forecast = result[0]
                latest_forecast_date = result[1]
                hours_since = (datetime.now() - last_forecast).total_seconds() / 3600
                
                # Smart refresh logic: refresh when we have <3 days of future forecasts
                if latest_forecast_date:
                    if isinstance(latest_forecast_date, datetime):
                        latest_date = latest_forecast_date.date()
                    else:
                        latest_date = latest_forecast_date
                    
                    days_remaining = (latest_date - datetime.now().date()).days
                    
                    if days_remaining <= 2:  # Less than 3 days of forecast remaining
                        logger.info(f"📅 {city_name}: Only {days_remaining} days of forecast left - refreshing")
                        return True
                    elif hours_since >= 72:  # Also refresh if last collection was 3+ days ago
                        logger.info(f"🕒 {city_name}: Last collection {hours_since:.1f}h ago - refreshing")
                        return True
                    else:
                        logger.info(f"✅ {city_name}: {days_remaining} days forecast remaining ({hours_since:.1f}h ago)")
                        return False
                else:
                    # Fallback to time-based check if forecast_date not available
                    if hours_since >= 72:  # 3 days
                        logger.info(f"🕒 {city_name}: {hours_since:.1f}h ago - needs refresh")
                        return True
                    else:
                        logger.info(f"✅ Recent forecast exists for {city_name}: {hours_since:.1f}h ago")
                        return False
            else:
                # No recent forecast data - collect instantly
                logger.info(f"🚀 No forecast data found for {city_name} - instant collection needed")
                return True
                
        except ImportError:
            logger.warning("⚠️ MySQL not available - collecting all forecasts")
            return True
        except Exception as e:
            logger.error(f"❌ Database check failed for {city_name}: {e}")
            return True  # Collect on error to be safe
    
    def _is_new_location(self, lat: float, lon: float) -> bool:
        """Check if this is a new location (no historical forecast data)"""
        try:
            conn = get_db_connection()
            if not conn:
                return False
            cursor = conn.cursor()
            
            query = """
            SELECT COUNT(*) as total_records
            FROM forecast_5day_data 
            WHERE location_lat BETWEEN %s - 0.01 AND %s + 0.01
            AND location_lng BETWEEN %s - 0.01 AND %s + 0.01
            """
            
            cursor.execute(query, (lat, lat, lon, lon))
            result = cursor.fetchone()
            conn.close()
            
            return result[0] == 0  # True if no historical data
            
        except Exception as e:
            logger.error(f"❌ Failed to check location history: {e}")
            return False  # Assume not new on error
    
    def _log_collection_summary(self):
        """Log summary of 5-day forecast collection"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info(f"""
🔮 SMART 5-DAY FORECAST COLLECTION SUMMARY:
   ⏱️  Duration: {duration:.1f} seconds
   📍 Locations checked: {self.stats['locations_checked']}
   📊 Forecasts collected: {self.stats['forecasts_collected']}
   🚀 Instant collections (new locations): {self.stats['instant_collections']}
   ⏭️ Skipped (recent data): {self.stats['skipped_recent']}
   ❌ Errors: {self.stats['errors']}
   ✅ Success rate: {((self.stats['forecasts_collected'])/(max(1, self.stats['forecasts_collected'] + self.stats['errors']))*100):.1f}%
        """)
        
        opt_stats = self.optimizer.get_collection_statistics()
        logger.info(f"🎯 Location optimization stats: {opt_stats}")
        
        if self.stats['instant_collections'] > 0:
            logger.info(f"🚀 New locations served: {self.stats['instant_collections']} instant forecast collections")
    
    async def collect_instant_forecast(self, lat: float, lon: float, city_name: str = None) -> bool:
        """Collect instant 5-day forecast for a specific location (public method)"""
        logger.info(f"🚀 Instant 5-day forecast collection for ({lat:.4f}, {lon:.4f})")
        
        try:
            if not city_name:
                city_name = f"{lat:.3f}°N, {abs(lon):.3f}°{'W' if lon < 0 else 'E'}"
            
            forecast_result = self.forecast_collector.collect_and_process_immediately(
                lat=lat, 
                lon=lon, 
                location_name=city_name
            )
            
            if forecast_result and forecast_result.storage_status.get('database_saved', False):
                # Mark location as collected in optimizer
                location_key = self.optimizer.generate_location_key(lat, lon)
                self.optimizer.mark_collected(location_key, 1.0)
                
                logger.info(f"✅ Instant 5-day forecast completed for {city_name}")
                return True
            else:
                logger.error(f"❌ Instant forecast failed for {city_name}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in instant forecast collection: {e}")
            return False

async def main():
    """Main entry point for 5-day forecast collection"""
    collector = Smart5DayCollector()
    await collector.run_5day_collection()

