#!/usr/bin/env python3
"""
AWS EventBridge Daily Fire Data Collector
Integrates with existing AWS infrastructure for daily fire data collection
Uses location_optimizer.py for smart location prioritization
"""

import os
import sys
import json
import boto3
import logging
from datetime import datetime, timedelta
from typing import List, Dict

# Add backend to path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from processors.location_optimizer import SmartLocationOptimizer
from collectors.fire_collector import DailyFireCollector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AWSFireDataScheduler:
    """AWS-integrated daily fire data collection scheduler"""
    
    def __init__(self):
        """Initialize AWS fire scheduler"""
        self.location_optimizer = SmartLocationOptimizer()
        self.fire_collector = DailyFireCollector()
        
        # AWS configuration
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        
        try:
            self.eventbridge = boto3.client('events', region_name=self.region)
            self.lambda_client = boto3.client('lambda', region_name=self.region)
            self.aws_available = True
        except Exception as e:
            logger.warning(f"⚠️ AWS services not available: {e}")
            self.aws_available = False
        
        logger.info("🔥 AWS Fire Data Scheduler initialized")
        logger.info(f"🌐 AWS Integration: {'✅ Available' if self.aws_available else '❌ Local only'}")

    def get_optimized_fire_locations(self, limit: int = 50) -> List[Dict]:
        """Get priority locations from location optimizer for fire collection"""
        try:
            priority_locations = self.location_optimizer.get_priority_locations(limit=limit)
            
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
            
            logger.info(f"📍 Retrieved {len(locations)} optimized locations for fire collection")
            return locations
            
        except Exception as e:
            logger.error(f"❌ Error getting optimized locations: {e}")
            return self.get_default_high_risk_locations()

    def get_default_high_risk_locations(self) -> List[Dict]:
        """High fire-risk locations in case optimizer is unavailable"""
        high_risk_locations = [
            # California - High fire risk
            {'lat': 37.7749, 'lon': -122.4194, 'name': 'San Francisco Bay Area, CA'},
            {'lat': 34.0522, 'lon': -118.2437, 'name': 'Los Angeles, CA'},
            {'lat': 32.7157, 'lon': -117.1611, 'name': 'San Diego, CA'},
            {'lat': 38.5816, 'lon': -121.4944, 'name': 'Sacramento, CA'},
            
            # Pacific Northwest - Wildfire prone
            {'lat': 45.5152, 'lon': -122.6784, 'name': 'Portland, OR'},
            {'lat': 47.6062, 'lon': -122.3321, 'name': 'Seattle, WA'},
            
            {'lat': 33.4484, 'lon': -112.0740, 'name': 'Phoenix, AZ'},
            {'lat': 39.7392, 'lon': -104.9903, 'name': 'Denver, CO'},
            {'lat': 35.6870, 'lon': -105.9378, 'name': 'Santa Fe, NM'},
            
            # Texas - Large wildfire area
            {'lat': 29.7604, 'lon': -95.3698, 'name': 'Houston, TX'},
            
            # Major population centers
            {'lat': 40.7128, 'lon': -74.0060, 'name': 'New York, NY'},
            {'lat': 41.8781, 'lon': -87.6298, 'name': 'Chicago, IL'},
            {'lat': 25.7617, 'lon': -80.1918, 'name': 'Miami, FL'},
        ]
        
        logger.info(f"📍 Using {len(high_risk_locations)} default high-risk locations")
        return high_risk_locations

    def collect_fire_data_for_locations(self, locations: List[Dict]) -> Dict:
        """Collect fire data for all specified locations"""
        start_time = datetime.now()
        results = {
            'total_locations': len(locations),
            'successful_collections': 0,
            'failed_collections': 0,
            'total_fires_detected': 0,
            'high_risk_locations': [],
            'collection_time': 0
        }
        
        logger.info(f"🔥 Starting fire data collection for {len(locations)} locations")
        
        for i, location in enumerate(locations, 1):
            try:
                lat = location['lat']
                lon = location['lon']
                name = location['name']
                
                logger.info(f"[{i}/{len(locations)}] 🔍 {name}")
                
                fire_data = self.fire_collector.collect_fire_data_for_location(
                    lat=lat, lon=lon, location_name=name
                )
                
                if fire_data:
                    results['successful_collections'] += 1
                    results['total_fires_detected'] += fire_data.total_fires
                    
                    # Track high-risk locations
                    if fire_data.smoke_risk_assessment.get('overall_risk') in ['high', 'very_high']:
                        results['high_risk_locations'].append({
                            'name': name,
                            'fires': fire_data.total_fires,
                            'risk': fire_data.smoke_risk_assessment.get('overall_risk')
                        })
                    
                    logger.info(f"   ✅ {fire_data.total_fires} fires, Risk: {fire_data.smoke_risk_assessment.get('overall_risk', 'unknown')}")
                else:
                    results['failed_collections'] += 1
                    logger.warning(f"   ❌ Collection failed")
                
            except Exception as e:
                results['failed_collections'] += 1
                logger.error(f"   ❌ Error: {e}")
        
        results['collection_time'] = (datetime.now() - start_time).total_seconds()
        return results

    def create_eventbridge_rule(self):
        """Create AWS EventBridge rule for daily fire collection"""
        if not self.aws_available:
            logger.warning("⚠️ AWS not available - cannot create EventBridge rule")
            return False
        
        try:
            rule_name = 'safer-skies-daily-fire-collection'
            
            self.eventbridge.put_rule(
                Name=rule_name,
                ScheduleExpression='cron(0 6 * * ? *)',  # Daily at 6 AM UTC
                Description='Daily fire data collection for Safer Skies',
                State='ENABLED'
            )
            
            logger.info(f"✅ EventBridge rule '{rule_name}' created for daily fire collection")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create EventBridge rule: {e}")
            return False

    def lambda_handler(self, event, context):
        """AWS Lambda handler for fire data collection"""
        try:
            logger.info("🚀 AWS Lambda fire collection triggered")
            
            locations = self.get_optimized_fire_locations()
            
            if not locations:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'No locations available for collection'})
                }
            
            results = self.collect_fire_data_for_locations(locations)
            
            logger.info("🎯 Fire Collection Summary:")
            logger.info(f"   📍 Locations: {results['successful_collections']}/{results['total_locations']}")
            logger.info(f"   🔥 Total fires: {results['total_fires_detected']}")
            logger.info(f"   ⚠️ High-risk areas: {len(results['high_risk_locations'])}")
            logger.info(f"   ⏱️ Time: {results['collection_time']:.1f}s")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Fire data collection completed',
                    'results': results
                })
            }
            
        except Exception as e:
            logger.error(f"❌ Lambda execution failed: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }

    def run_local_daily_collection(self):
        """Run daily fire collection locally (non-AWS)"""
        logger.info("🔥 Running local daily fire data collection")
        
        try:
            if self.is_already_collected_today():
                logger.info("✅ Fire data already collected today")
                return True
            
            locations = self.get_optimized_fire_locations()
            results = self.collect_fire_data_for_locations(locations)
            
            print("\n🎯 Daily Fire Collection Results:")
            print(f"   📍 Successful collections: {results['successful_collections']}/{results['total_locations']}")
            print(f"   🔥 Total fires detected: {results['total_fires_detected']}")
            print(f"   ⚠️ High-risk locations: {len(results['high_risk_locations'])}")
            print(f"   ⏱️ Collection time: {results['collection_time']:.1f}s")
            
            if results['high_risk_locations']:
                print("\n🚨 High-Risk Fire Areas:")
                for area in results['high_risk_locations']:
                    print(f"   🔥 {area['name']}: {area['fires']} fires ({area['risk']} risk)")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Local fire collection failed: {e}")
            return False

    def is_already_collected_today(self) -> bool:
        """Check if fire data was already collected today"""
        try:
            import mysql.connector
            
            db_config = {
                'host': 'localhost',
                'user': 'root',
                'password': '',
                'database': 'safer_skies'
            }
            
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COUNT(*) FROM fire_locations 
                WHERE collection_date = CURDATE()
            """)
            
            count = cursor.fetchone()[0]
            conn.close()
            
            return count > 0
            
        except Exception as e:
            logger.warning(f"⚠️ Could not check today's collections: {e}")
            return False

def main():
    """Main function for AWS fire data collection"""
    print("🔥 Safer Skies AWS Fire Data Collection")
    print("=" * 50)
    
    scheduler = AWSFireDataScheduler()
    
    success = scheduler.run_local_daily_collection()
    
    if success:
        print("🎉 Daily fire collection completed successfully!")
    else:
        print("❌ Daily fire collection failed!")
        sys.exit(1)

# AWS Lambda entry point
def lambda_handler(event, context):
    """AWS Lambda entry point"""
    scheduler = AWSFireDataScheduler()
    return scheduler.lambda_handler(event, context)

if __name__ == "__main__":
    main()