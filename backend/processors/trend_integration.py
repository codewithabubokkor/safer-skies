#!/usr/bin/env python3
"""
🔄 TREND INTEGRATION SCRIPT
==========================
Connects trend processor with existing data collection pipeline

This script:
- Integrates trend_processor.py into existing collection workflow
- Adds trend storage to your current data collection  
- Maintains EPA compliance while adding 30-day trend capability
- Minimal changes to existing proven systems

INTEGRATION STRATEGY:
1. Hook into existing collectors (enhanced_epa_collector.py, etc.)
2. Store both EPA compliance data AND trend data
3. Daily processing at midnight UTC
4. Frontend API endpoints for dashboard
"""

import json
import logging
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import asyncio
from typing import Dict, List, Optional, Any

sys.path.append(os.path.join(os.getenv('PROJECT_ROOT', '/app'), 'backend'))

# Import existing processors
try:
    from .trend_processor import TrendProcessor
    from .aqi_calculator import EPAAQICalculator
    from collectors.northamerica_collector import NorthAmericaCollector  # EPA collector
except ImportError as e:
    try:
        from processors.trend_processor import TrendProcessor
        from processors.aqi_calculator import EPAAQICalculator
    except ImportError:
        print(f"⚠️ Import warning: {e}")
        print("Make sure you're running from the correct directory")
        class TrendProcessor:
            def __init__(self):
                print("⚠️ TrendProcessor not available")
        class EPAAQICalculator:
            def __init__(self):
                print("⚠️ EPAAQICalculator not available")

logger = logging.getLogger(__name__)

class TrendIntegrationManager:
    """
    Manages integration between existing data collection and new trend system
    Designed to work with your existing proven collection pipeline
    """
    
    def __init__(self):
        self.trend_processor = TrendProcessor()
        self.epa_calculator = EPAAQICalculator()
        
        # Your existing collection locations (from your data)
        self.monitoring_locations = [
            {"id": "40.7128_-74.006", "name": "New York City", "lat": 40.7128, "lon": -74.006},
            {"id": "34.052_-118.244", "name": "Los Angeles", "lat": 34.052, "lon": -118.244},
            {"id": "33.448_-112.074", "name": "Phoenix", "lat": 33.448, "lon": -112.074},
            {"id": "32.716_-117.161", "name": "San Diego", "lat": 32.716, "lon": -117.161},
            {"id": "24.363_88.624", "name": "Rajshahi", "lat": 24.363, "lon": 88.624}
        ]
        
        logger.info("✅ Trend Integration Manager initialized")
    
    def integrate_with_hourly_collection(self, location_data: Dict[str, Any]) -> bool:
        """
        Integration point #1: Hook into your existing hourly data collection
        
        Call this function from your existing collectors after successful data collection
        
        Args:
            location_data: Your existing location data structure
            
        Returns:
            True if trend integration successful
        """
        try:
            location_id = self._extract_location_id(location_data)
            pollutant_data = self._extract_pollutant_data(location_data)
            
            if not location_id or not pollutant_data:
                logger.warning(f"⚠️ Could not extract data for trend storage")
                return False
            
            success = self.trend_processor.store_hourly_epa_data(location_id, pollutant_data)
            
            if success:
                logger.debug(f"📊 Integrated trend storage for {location_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Trend integration failed: {e}")
            return False
    
    def run_daily_processing(self, target_date: Optional[str] = None) -> Dict[str, bool]:
        """
        Integration point #2: Daily trend processing at midnight UTC
        
        Call this once per day to process 24-hour EPA data into trend summaries
        Add to your existing cron job or scheduled task
        
        Args:
            target_date: Date to process (defaults to yesterday)
            
        Returns:
            Dict of processing results per location
        """
        results = {}
        
        try:
            logger.info("🌙 Starting daily trend processing...")
            
            for location in self.monitoring_locations:
                location_id = location["id"]
                
                try:
                    success = self.trend_processor.process_daily_trends(location_id, target_date)
                    results[location_id] = success
                    
                    if success:
                        logger.info(f"✅ Daily trends processed: {location['name']}")
                    else:
                        logger.warning(f"⚠️ Daily processing failed: {location['name']}")
                        
                except Exception as e:
                    logger.error(f"❌ Daily processing error for {location['name']}: {e}")
                    results[location_id] = False
            
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            
            logger.info(f"🌅 Daily processing complete: {successful}/{total} locations successful")
            return results
            
        except Exception as e:
            logger.error(f"❌ Daily processing failed: {e}")
            return results
    
    def get_trend_data_for_api(self, location_id: str, days: int = 30) -> Optional[Dict[str, Any]]:
        """
        Integration point #3: API endpoint for frontend dashboard
        
        Use this in your existing API endpoints to serve trend data to frontend
        
        Args:
            location_id: Location identifier
            days: Number of days (7, 14, or 30)
            
        Returns:
            Frontend-ready trend data
        """
        try:
            trend_data = self.trend_processor.create_trend_summary_for_frontend(location_id, days)
            
            if trend_data:
                logger.debug(f"📊 Retrieved {days}-day trends for {location_id}")
                return trend_data
            else:
                logger.warning(f"⚠️ No trend data available for {location_id}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to get trend data for {location_id}: {e}")
            return None
    
    def batch_get_all_locations_trends(self, days: int = 7) -> Dict[str, Any]:
        """
        Get trend data for all monitoring locations
        Useful for dashboard overview pages
        
        Args:
            days: Number of days to retrieve
            
        Returns:
            Dict with trend data for all locations
        """
        all_trends = {}
        
        for location in self.monitoring_locations:
            location_id = location["id"]
            location_name = location["name"]
            
            trend_data = self.get_trend_data_for_api(location_id, days)
            
            if trend_data:
                all_trends[location_id] = {
                    "name": location_name,
                    "data": trend_data
                }
        
        logger.info(f"📊 Retrieved trends for {len(all_trends)} locations")
        return all_trends
    
    def _extract_location_id(self, location_data: Dict[str, Any]) -> Optional[str]:
        """Extract location ID from your existing data format"""
        # Adapt this to match your existing data structure
        
        if "location_id" in location_data:
            return location_data["location_id"]
        
        if "lat" in location_data and "lon" in location_data:
            lat = location_data["lat"] 
            lon = location_data["lon"]
            return f"{lat}_{lon}"
        
        if "coordinates" in location_data:
            coords = location_data["coordinates"]
            if isinstance(coords, list) and len(coords) >= 2:
                return f"{coords[0]}_{coords[1]}"
        
        if "location" in location_data:
            return self._extract_location_id(location_data["location"])
        
        logger.warning("⚠️ Could not extract location_id from data")
        return None
    
    def _extract_pollutant_data(self, location_data: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """Extract pollutant measurements from your existing data format"""
        pollutants = {}
        
        # Common pollutant field mappings from your existing system
        field_mappings = {
            # Direct matches
            "PM25": ["PM25", "pm25", "PM2.5", "pm2.5"],  
            "PM10": ["PM10", "pm10"],
            "O3": ["O3", "o3", "ozone"],
            "NO2": ["NO2", "no2"], 
            "SO2": ["SO2", "so2"],
            "CO": ["CO", "co"]
        }
        
        for standard_name, possible_fields in field_mappings.items():
            for field in possible_fields:
                if field in location_data and location_data[field] is not None:
                    try:
                        pollutants[standard_name] = float(location_data[field])
                        break
                    except (ValueError, TypeError):
                        continue
        
        if "pollutants" in location_data:
            pollutant_data = location_data["pollutants"]
            for standard_name, possible_fields in field_mappings.items():
                for field in possible_fields:
                    if field in pollutant_data and pollutant_data[field] is not None:
                        try:
                            pollutants[standard_name] = float(pollutant_data[field])
                            break
                        except (ValueError, TypeError):
                            continue
        
        for data_key in ["data", "measurements", "current", "latest"]:
            if data_key in location_data:
                nested_data = location_data[data_key]
                for standard_name, possible_fields in field_mappings.items():
                    for field in possible_fields:
                        if field in nested_data and nested_data[field] is not None:
                            try:
                                pollutants[standard_name] = float(nested_data[field])
                                break
                            except (ValueError, TypeError):
                                continue
        
        return pollutants if pollutants else None

# Example integration hooks for your existing collectors

def integrate_with_enhanced_epa_collector():
    """
    Example: How to integrate with your existing enhanced_epa_collector.py
    Add these calls to your existing collection workflow
    """
    integration_manager = TrendIntegrationManager()
    
    """
    # Your existing collection code...
    collected_data = collector.collect_location_data(lat, lon)
    
    # ADD THIS: Integration with trend system
    integration_manager.integrate_with_hourly_collection(collected_data)
    """
    
    print("💡 Integration example ready!")
    print("Add trend integration calls to your existing collectors")

def create_daily_cron_job():
    """
    Example cron job setup for daily processing
    Run this at midnight UTC every day
    """
    integration_manager = TrendIntegrationManager()
    
    results = integration_manager.run_daily_processing()
    
    print(f"🌙 Daily processing completed: {results}")

def create_api_endpoint_example():
    """
    Example API endpoint for frontend dashboard
    Add to your existing API routes
    """
    integration_manager = TrendIntegrationManager()
    
    # Example Flask/FastAPI endpoint:
    """
    @app.route('/api/trends/<location_id>')
    def get_trends(location_id):
        days = request.args.get('days', 30, type=int)
        trend_data = integration_manager.get_trend_data_for_api(location_id, days)
        
        if trend_data:
            return jsonify(trend_data)
        else:
            return jsonify({"error": "No trend data available"}), 404
    """
    
    print("🔗 API endpoint example ready!")

# Trend integration - import and use in other modules
if False:  # Disabled main
    # Test the integration system
    logging.basicConfig(level=logging.INFO)
    
    print("🔄 TESTING TREND INTEGRATION")
    print("=" * 50)
    
    integration_manager = TrendIntegrationManager()
    
    # Test data extraction
    test_data = {
        "location_id": "40.7128_-74.006",
        "lat": 40.7128,
        "lon": -74.006,
        "pollutants": {
            "PM25": 15.2,
            "O3": 45.8,
            "NO2": 28.5
        }
    }
    
    print("📊 Testing data integration...")
    success = integration_manager.integrate_with_hourly_collection(test_data)
    print(f"✅ Integration test: {'SUCCESS' if success else 'FAILED'}")
    
    print("\n🔗 Integration examples:")
    integrate_with_enhanced_epa_collector()
    create_api_endpoint_example()
    
    print("\n✅ Trend Integration ready for production!")
    print("\nNext steps:")
    print("1. Add integration_manager.integrate_with_hourly_collection() to your collectors")
    print("2. Set up daily cron job: integration_manager.run_daily_processing()")  
    print("3. Add API endpoints using integration_manager.get_trend_data_for_api()")
    print("4. Frontend can now call /api/trends/{location_id}?days=30 for dashboard")