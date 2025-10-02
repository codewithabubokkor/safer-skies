#!/usr/bin/env python3
"""
📊 DAILY TREND CALCULATOR
========================
Calculates daily AQI trends from hourly comprehensive_aqi_hourly data
Stores results in daily_aqi_trends table for the trend API

Process:
1. Read hourly data from comprehensive_aqi_hourly 
2. Group by city/location and date
3. Calculate daily averages for each pollutant
4. Store in daily_aqi_trends table
5. Can run daily via cron job

Usage:
- Run daily: python3 daily_trend_calculator.py
- Run for specific date: python3 daily_trend_calculator.py --date 2025-09-28
- Backfill trends: python3 daily_trend_calculator.py --backfill 30
"""

import sys
import os
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.database_connection import get_db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DailyTrendCalculator:
    """Calculates daily AQI trends from hourly data"""
    
    def __init__(self):
        self.db = get_db_connection()
        if not self.db:
            raise Exception("❌ Failed to connect to database")
        
        logger.info("✅ Daily Trend Calculator initialized")
    
    def calculate_daily_trends(self, target_date: date = None) -> int:
        """
        Calculate daily trends for a specific date
        
        Args:
            target_date: Date to calculate trends for (default: yesterday)
            
        Returns:
            Number of location-days processed
        """
        if target_date is None:
            target_date = date.today() - timedelta(days=1)
        
        logger.info(f"📊 Calculating daily trends for {target_date}")
        
        hourly_data = self._get_hourly_data(target_date)
        
        if not hourly_data:
            logger.warning(f"⚠️ No hourly data found for {target_date}")
            return 0
        
        location_groups = self._group_by_location(hourly_data)
        
        processed_count = 0
        
        for location_key, records in location_groups.items():
            try:
                daily_trend = self._calculate_location_daily_average(records, target_date)
                
                if self._store_daily_trend(daily_trend):
                    processed_count += 1
                    logger.info(f"✅ Stored trend for {daily_trend['city']} on {target_date}")
                else:
                    logger.error(f"❌ Failed to store trend for {location_key}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing {location_key}: {e}")
        
        logger.info(f"📊 Processed {processed_count} locations for {target_date}")
        return processed_count
    
    def _get_hourly_data(self, target_date: date) -> List[Dict]:
        """Get all hourly data for a specific date"""
        
        query = """
        SELECT 
            city,
            location_lat,
            location_lng,
            timestamp,
            overall_aqi,
            aqi_category,
            dominant_pollutant,
            pm25_concentration, pm25_aqi,
            pm10_concentration, pm10_aqi,
            o3_concentration, o3_aqi,
            no2_concentration, no2_aqi,
            so2_concentration, so2_aqi,
            co_concentration, co_aqi,
            temperature_celsius,
            humidity_percent,
            wind_speed_ms
        FROM comprehensive_aqi_hourly
        WHERE DATE(timestamp) = %s
        ORDER BY city, timestamp
        """
        
        cursor = self.db.cursor(dictionary=True)
        cursor.execute(query, (target_date,))
        results = cursor.fetchall()
        cursor.close()
        
        logger.info(f"📥 Found {len(results)} hourly records for {target_date}")
        return results
    
    def _group_by_location(self, hourly_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Group hourly data by location"""
        
        groups = {}
        
        for record in hourly_data:
            lat = float(record['location_lat'])
            lng = float(record['location_lng'])
            location_key = f"{lat:.4f}_{lng:.4f}"
            
            if location_key not in groups:
                groups[location_key] = []
            
            groups[location_key].append(record)
        
        logger.info(f"📍 Grouped data into {len(groups)} locations")
        return groups
    
    def _calculate_location_daily_average(self, records: List[Dict], target_date: date) -> Dict:
        """Calculate daily averages for a single location"""
        
        if not records:
            raise ValueError("No records to process")
        
        sample_record = records[0]
        
        aqi_sum = 0
        aqi_count = 0
        
        pollutant_sums = {
            'pm25_concentration': 0, 'pm25_aqi': 0, 'pm25_count': 0,
            'pm10_concentration': 0, 'pm10_aqi': 0, 'pm10_count': 0,
            'o3_concentration': 0, 'o3_aqi': 0, 'o3_count': 0,
            'no2_concentration': 0, 'no2_aqi': 0, 'no2_count': 0,
            'so2_concentration': 0, 'so2_aqi': 0, 'so2_count': 0,
            'co_concentration': 0, 'co_aqi': 0, 'co_count': 0
        }
        
        weather_sums = {
            'temperature': 0, 'temp_count': 0,
            'humidity': 0, 'humidity_count': 0,
            'wind_speed': 0, 'wind_count': 0
        }
        
        dominant_pollutants = []
        
        # Sum all values
        for record in records:
            if record['overall_aqi']:
                aqi_sum += record['overall_aqi']
                aqi_count += 1
            
            # Pollutants
            for pollutant in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
                conc_key = f"{pollutant}_concentration"
                aqi_key = f"{pollutant}_aqi"
                count_key = f"{pollutant}_count"
                
                if record[conc_key] is not None:
                    pollutant_sums[conc_key] += record[conc_key]
                    pollutant_sums[count_key] += 1
                
                if record[aqi_key] is not None:
                    pollutant_sums[aqi_key] += record[aqi_key]
            
            # Weather
            if record['temperature_celsius'] is not None:
                weather_sums['temperature'] += record['temperature_celsius']
                weather_sums['temp_count'] += 1
            
            if record['humidity_percent'] is not None:
                weather_sums['humidity'] += record['humidity_percent']
                weather_sums['humidity_count'] += 1
            
            if record['wind_speed_ms'] is not None:
                weather_sums['wind_speed'] += record['wind_speed_ms']
                weather_sums['wind_count'] += 1
            
            # Dominant pollutants
            if record['dominant_pollutant']:
                dominant_pollutants.append(record['dominant_pollutant'])
        
        avg_aqi = aqi_sum / aqi_count if aqi_count > 0 else 0
        
        # Determine AQI category
        if avg_aqi <= 50:
            aqi_category = "Good"
        elif avg_aqi <= 100:
            aqi_category = "Moderate"
        elif avg_aqi <= 150:
            aqi_category = "Unhealthy for Sensitive Groups"
        elif avg_aqi <= 200:
            aqi_category = "Unhealthy"
        elif avg_aqi <= 300:
            aqi_category = "Very Unhealthy"
        else:
            aqi_category = "Hazardous"
        
        # Most common dominant pollutant
        dominant_pollutant = max(set(dominant_pollutants), key=dominant_pollutants.count) if dominant_pollutants else 'PM25'
        
        daily_trend = {
            'city': sample_record['city'],
            'location_lat': float(sample_record['location_lat']),
            'location_lng': float(sample_record['location_lng']),
            'date': target_date,
            'avg_overall_aqi': round(avg_aqi, 2),
            'avg_aqi_category': aqi_category,
            'dominant_pollutant': dominant_pollutant,
            'hourly_data_points': aqi_count,
            'data_completeness': round((aqi_count / 24) * 100, 2)
        }
        
        for pollutant in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
            conc_key = f"avg_{pollutant}_concentration"
            aqi_key = f"avg_{pollutant}_aqi"
            count_key = f"{pollutant}_count"
            
            if pollutant_sums[count_key] > 0:
                daily_trend[conc_key] = round(pollutant_sums[f"{pollutant}_concentration"] / pollutant_sums[count_key], 3)
                daily_trend[aqi_key] = round(pollutant_sums[f"{pollutant}_aqi"] / pollutant_sums[count_key], 2)
            else:
                daily_trend[conc_key] = None
                daily_trend[aqi_key] = None
        
        if weather_sums['temp_count'] > 0:
            daily_trend['avg_temperature_celsius'] = round(weather_sums['temperature'] / weather_sums['temp_count'], 2)
        else:
            daily_trend['avg_temperature_celsius'] = None
        
        if weather_sums['humidity_count'] > 0:
            daily_trend['avg_humidity_percent'] = round(weather_sums['humidity'] / weather_sums['humidity_count'], 2)
        else:
            daily_trend['avg_humidity_percent'] = None
        
        if weather_sums['wind_count'] > 0:
            daily_trend['avg_wind_speed_ms'] = round(weather_sums['wind_speed'] / weather_sums['wind_count'], 2)
        else:
            daily_trend['avg_wind_speed_ms'] = None
        
        return daily_trend
    
    def _store_daily_trend(self, daily_trend: Dict) -> bool:
        """Store daily trend in database"""
        
        try:
            query = """
            INSERT INTO daily_aqi_trends (
                city, location_lat, location_lng, date,
                avg_overall_aqi, avg_aqi_category, dominant_pollutant,
                avg_pm25_concentration, avg_pm25_aqi,
                avg_pm10_concentration, avg_pm10_aqi,
                avg_o3_concentration, avg_o3_aqi,
                avg_no2_concentration, avg_no2_aqi,
                avg_so2_concentration, avg_so2_aqi,
                avg_co_concentration, avg_co_aqi,
                avg_temperature_celsius, avg_humidity_percent, avg_wind_speed_ms,
                hourly_data_points, data_completeness
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                avg_overall_aqi = VALUES(avg_overall_aqi),
                avg_aqi_category = VALUES(avg_aqi_category),
                dominant_pollutant = VALUES(dominant_pollutant),
                avg_pm25_concentration = VALUES(avg_pm25_concentration),
                avg_pm25_aqi = VALUES(avg_pm25_aqi),
                avg_pm10_concentration = VALUES(avg_pm10_concentration),
                avg_pm10_aqi = VALUES(avg_pm10_aqi),
                avg_o3_concentration = VALUES(avg_o3_concentration),
                avg_o3_aqi = VALUES(avg_o3_aqi),
                avg_no2_concentration = VALUES(avg_no2_concentration),
                avg_no2_aqi = VALUES(avg_no2_aqi),
                avg_so2_concentration = VALUES(avg_so2_concentration),
                avg_so2_aqi = VALUES(avg_so2_aqi),
                avg_co_concentration = VALUES(avg_co_concentration),
                avg_co_aqi = VALUES(avg_co_aqi),
                avg_temperature_celsius = VALUES(avg_temperature_celsius),
                avg_humidity_percent = VALUES(avg_humidity_percent),
                avg_wind_speed_ms = VALUES(avg_wind_speed_ms),
                hourly_data_points = VALUES(hourly_data_points),
                data_completeness = VALUES(data_completeness)
            """
            
            cursor = self.db.cursor()
            cursor.execute(query, (
                daily_trend['city'],
                daily_trend['location_lat'],
                daily_trend['location_lng'],
                daily_trend['date'],
                daily_trend['avg_overall_aqi'],
                daily_trend['avg_aqi_category'],
                daily_trend['dominant_pollutant'],
                daily_trend.get('avg_pm25_concentration'),
                daily_trend.get('avg_pm25_aqi'),
                daily_trend.get('avg_pm10_concentration'),
                daily_trend.get('avg_pm10_aqi'),
                daily_trend.get('avg_o3_concentration'),
                daily_trend.get('avg_o3_aqi'),
                daily_trend.get('avg_no2_concentration'),
                daily_trend.get('avg_no2_aqi'),
                daily_trend.get('avg_so2_concentration'),
                daily_trend.get('avg_so2_aqi'),
                daily_trend.get('avg_co_concentration'),
                daily_trend.get('avg_co_aqi'),
                daily_trend.get('avg_temperature_celsius'),
                daily_trend.get('avg_humidity_percent'),
                daily_trend.get('avg_wind_speed_ms'),
                daily_trend['hourly_data_points'],
                daily_trend['data_completeness']
            ))
            
            self.db.commit()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error storing daily trend: {e}")
            return False
    
    def backfill_trends(self, days: int = 30) -> int:
        """Backfill daily trends for the last N days"""
        
        logger.info(f"🔄 Backfilling trends for last {days} days")
        
        total_processed = 0
        current_date = date.today() - timedelta(days=1)  # Start with yesterday
        
        for i in range(days):
            target_date = current_date - timedelta(days=i)
            processed = self.calculate_daily_trends(target_date)
            total_processed += processed
            
            if processed == 0:
                logger.warning(f"⚠️ No data to process for {target_date}")
        
        logger.info(f"🔄 Backfill complete: {total_processed} location-days processed")
        return total_processed
    
    def cleanup_old_trends(self, keep_days: int = 60):
        """Remove trends older than specified days"""
        
        cutoff_date = date.today() - timedelta(days=keep_days)
        
        try:
            cursor = self.db.cursor()
            cursor.execute("DELETE FROM daily_aqi_trends WHERE date < %s", (cutoff_date,))
            deleted_count = cursor.rowcount
            self.db.commit()
            cursor.close()
            
            logger.info(f"🗑️ Cleaned up {deleted_count} old trend records before {cutoff_date}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Error cleaning up old trends: {e}")
            return 0
    
    def close(self):
        """Close database connection"""
        if self.db:
            self.db.close()

def main():
    """Main function with command line arguments"""
    
    parser = argparse.ArgumentParser(description='Calculate daily AQI trends')
    parser.add_argument('--date', type=str, help='Specific date to process (YYYY-MM-DD)')
    parser.add_argument('--backfill', type=int, help='Backfill trends for N days')
    parser.add_argument('--cleanup', type=int, help='Clean up trends older than N days', default=60)
    
    args = parser.parse_args()
    
    calculator = DailyTrendCalculator()
    
    try:
        if args.backfill:
            total = calculator.backfill_trends(args.backfill)
            print(f"✅ Backfilled {total} location-days")
            
        elif args.date:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            count = calculator.calculate_daily_trends(target_date)
            print(f"✅ Processed {count} locations for {target_date}")
            
        else:
            # Default: process yesterday
            count = calculator.calculate_daily_trends()
            print(f"✅ Processed {count} locations for yesterday")
        
        if args.cleanup:
            deleted = calculator.cleanup_old_trends(args.cleanup)
            print(f"🗑️ Cleaned up {deleted} old records")
    
    finally:
        calculator.close()

if __name__ == "__main__":
    main()