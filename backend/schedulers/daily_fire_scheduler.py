#!/usr/bin/env python3
"""
Daily Fire Data Collection Scheduler
Collects fire data once daily for optimized locations from location_optimizer.py
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict

# Add backend to path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from processors.location_optimizer import SmartLocationOptimizer
from collectors.fire_collector import DailyFireCollector
from utils.database_connection import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DailyFireScheduler:
    """Daily fire data collection scheduler using optimized locations"""
    
    def __init__(self):
        """Initialize scheduler with location optimizer and fire collector"""
        self.location_optimizer = SmartLocationOptimizer()
        self.fire_collector = DailyFireCollector()
        
        logger.info("🔥 Daily Fire Scheduler initialized")
        logger.info(f"📍 Location Optimizer: {'✅ Ready' if self.location_optimizer else '❌ Failed'}")
        logger.info(f"🛰️ Fire Collector: {'✅ Ready' if self.fire_collector else '❌ Failed'}")

    def get_locations_for_fire_collection(self) -> List[Dict]:
        """Get priority locations for fire data collection"""
        try:
            priority_locations = self.location_optimizer.get_priority_locations(limit=50)
            
            locations = []
            for loc in priority_locations:
                locations.append({
                    'lat': loc.latitude,
                    'lon': loc.longitude,
                    'name': loc.city,
                    'priority_score': loc.priority_score,
                    'user_count': loc.user_count,
                    'alert_count': loc.alert_count
                })
            
            logger.info(f"📍 Found {len(locations)} priority locations for fire collection")
            return locations
            
        except Exception as e:
            logger.error(f"❌ Error getting priority locations: {e}")
            # Fallback to default locations
            return self.get_default_locations()

    def get_default_locations(self) -> List[Dict]:
        """Fallback default locations for fire collection"""
        default_locations = [
            {'lat': 37.7749, 'lon': -122.4194, 'name': 'San Francisco, CA'},
            {'lat': 34.0522, 'lon': -118.2437, 'name': 'Los Angeles, CA'},
            {'lat': 45.5152, 'lon': -122.6784, 'name': 'Portland, OR'},
            {'lat': 47.6062, 'lon': -122.3321, 'name': 'Seattle, WA'},
            {'lat': 39.7392, 'lon': -104.9903, 'name': 'Denver, CO'},
            {'lat': 33.4484, 'lon': -112.0740, 'name': 'Phoenix, AZ'},
            {'lat': 40.7128, 'lon': -74.0060, 'name': 'New York, NY'},
            {'lat': 25.7617, 'lon': -80.1918, 'name': 'Miami, FL'},
            {'lat': 29.7604, 'lon': -95.3698, 'name': 'Houston, TX'},
            {'lat': 41.8781, 'lon': -87.6298, 'name': 'Chicago, IL'}
        ]
        
        logger.info(f"📍 Using {len(default_locations)} default locations")
        return default_locations

    def add_new_location_for_fire_collection(self, lat: float, lon: float, city: str = None):
        """Add a new location for fire data collection"""
        try:
            location_key = f"{lat:.3f},{lon:.3f}"
            
            if city:
                self.location_optimizer.add_search_frequency(location_key, city, lat, lon)
                logger.info(f"📍 New location added for fire collection: {city} ({lat:.3f}, {lon:.3f})")
            else:
                logger.info(f"📍 New location added for fire collection: {lat:.3f}, {lon:.3f}")
            
            fire_data = self.fire_collector.collect_fire_data_for_location(
                lat=lat, lon=lon, location_name=city or f"{lat:.3f}, {lon:.3f}"
            )
            
            logger.info(f"🔥 Fire collection completed for new location: {fire_data.total_fires} fires found")
            return fire_data
            
        except Exception as e:
            logger.error(f"❌ Error adding new location for fire collection: {e}")
            return None

    def run_daily_fire_collection(self):
        """Run daily fire data collection for all priority locations"""
        start_time = time.time()
        
        logger.info("🔥 Starting daily fire data collection")
        logger.info("=" * 60)
        
        try:
            locations = self.get_locations_for_fire_collection()
            
            if not locations:
                logger.warning("⚠️ No locations found for fire collection")
                return
            
            total_fires = 0
            successful_collections = 0
            failed_collections = 0
            
            logger.info(f"🔍 Collecting fire data for {len(locations)} locations...")
            
            for i, location in enumerate(locations, 1):
                try:
                    lat = location['lat']
                    lon = location['lon']
                    name = location['name']
                    
                    logger.info(f"[{i}/{len(locations)}] 🔥 Collecting: {name}")
                    
                    fire_data = self.fire_collector.collect_fire_data_for_location(
                        lat=lat, lon=lon, location_name=name
                    )
                    
                    if fire_data:
                        total_fires += fire_data.total_fires
                        successful_collections += 1
                        
                        if fire_data.total_fires > 0:
                            logger.info(f"   🔥 Found {fire_data.total_fires} fires (Risk: {fire_data.smoke_risk_assessment.get('overall_risk', 'unknown')})")
                        else:
                            logger.info(f"   ✅ No fires detected")
                    else:
                        failed_collections += 1
                        logger.warning(f"   ❌ Collection failed")
                    
                    # Small delay between collections to avoid API rate limits
                    time.sleep(2)
                    
                except Exception as e:
                    failed_collections += 1
                    logger.error(f"   ❌ Error collecting fire data for {location.get('name', 'unknown')}: {e}")
            
            # Summary
            collection_time = time.time() - start_time
            
            logger.info("=" * 60)
            logger.info("🎯 Daily Fire Collection Summary:")
            logger.info(f"   📍 Locations processed: {len(locations)}")
            logger.info(f"   ✅ Successful collections: {successful_collections}")
            logger.info(f"   ❌ Failed collections: {failed_collections}")
            logger.info(f"   🔥 Total fires detected: {total_fires}")
            logger.info(f"   ⏱️ Total time: {collection_time:.1f}s")
            logger.info(f"   📅 Next collection: Tomorrow at same time")
            logger.info("=" * 60)
            
            return {
                'locations_processed': len(locations),
                'successful_collections': successful_collections,
                'failed_collections': failed_collections,
                'total_fires': total_fires,
                'collection_time': collection_time
            }
            
        except Exception as e:
            logger.error(f"❌ Daily fire collection failed: {e}")
            return None

    def is_collection_needed_today(self) -> bool:
        """Check if fire collection has already been done today"""
        try:
            conn = get_db_connection()
            if not conn:
                logger.warning("⚠️ Could not connect to database, proceeding with collection")
                return True
                
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COUNT(*) FROM fire_locations 
                WHERE collection_date = CURDATE()
            """)
            
            today_collections = cursor.fetchone()[0]
            conn.close()
            
            if today_collections > 0:
                logger.info(f"✅ Fire data already collected today ({today_collections} locations)")
                return False
            else:
                logger.info("🔄 Fire data collection needed for today")
                return True
                
        except Exception as e:
            logger.warning(f"⚠️ Could not check today's collections, proceeding with collection: {e}")
            return True

def main():
    """Main function for daily fire collection"""
    print("🔥 Safer Skies Daily Fire Data Collection")
    print("=" * 50)
    
    try:
        scheduler = DailyFireScheduler()
        
        if not scheduler.is_collection_needed_today():
            print("✅ Fire data already collected today. Exiting.")
            return
        
        result = scheduler.run_daily_fire_collection()
        
        if result:
            print("🎉 Daily fire collection completed successfully!")
        else:
            print("❌ Daily fire collection failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error in daily fire collection: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()