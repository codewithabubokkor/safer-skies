#!/usr/bin/env python3
"""
📊 TREND DATA PROCESSOR
======================
30-Day Trend Storage & EPA Compliance System

This module provides:
- Dual storage: EPA 24-hour data + 30-day trends  
- EPA-compliant time averaging (O3: 8hr, PM: 24hr, NO2/SO2: 1hr, CO: 8hr)
- Fast trend visualization data for frontend dashboard
- S3 lifecycle management for automatic cleanup
- Production-ready performance (<100ms response)

STORAGE STRATEGY:
Level 1: EPA 24-hour data (s3://bucket/epa-data/{location}/hourly/)
Level 2: 30-day trends (s3://bucket/trends/{location}/daily_averages.json)
"""

import json
import logging
import os
import sys
import boto3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import numpy as np
from dataclasses import dataclass, asdict
import math
from collections import defaultdict

# Import existing EPA calculator for consistency
try:
    from .aqi_calculator import EPAAQICalculator
    from .forecast_aqi_calculator import ForecastAQICalculator
except ImportError:
    import sys
    sys.path.append(os.path.join(os.getenv('PROJECT_ROOT', '/app'), 'backend'))
    from processors.aqi_calculator import EPAAQICalculator
    from processors.forecast_aqi_calculator import ForecastAQICalculator

logger = logging.getLogger(__name__)

@dataclass 
class DailyPollutantData:
    """EPA-compliant daily pollutant statistics"""
    epa_value: float          # EPA-compliant daily value (8hr/24hr/1hr)
    method: str               # EPA averaging method used
    min_value: float          # Daily minimum
    max_value: float          # Daily maximum  
    data_points: int          # Number of valid hourly measurements
    completeness: float       # Data completeness percentage
    valid_for_epa: bool

@dataclass
class DailyAQIData:
    """Daily AQI summary"""
    value: int                # Overall AQI
    category: str             # AQI category (Good, Moderate, etc.)
    dominant_pollutant: str   # Pollutant driving the AQI
    color: str               # Color code for frontend

@dataclass
class DailyTrendData:
    """Complete daily trend data for a location"""
    date: str
    pollutants: Dict[str, DailyPollutantData]
    daily_aqi: DailyAQIData
    weather_context: Optional[Dict[str, Any]] = None

class TrendProcessor:
    """
    Production-ready trend processor for Safer Skies
    Follows existing patterns from aqi_calculator.py and forecast_aqi_calculator.py
    """
    
    def __init__(self, s3_bucket: str = "naq-forecast-data"):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        
        self.epa_calculator = EPAAQICalculator()
        self.forecast_calculator = ForecastAQICalculator()
        
        # EPA time averaging requirements (from existing calculators)
        self.epa_averaging_periods = {
            'O3': {'period': '8hr_rolling', 'min_hours': 6},      # 8-hour rolling, need 6+ hours
            'CO': {'period': '8hr_rolling', 'min_hours': 6},      # 8-hour rolling, need 6+ hours  
            'PM25': {'period': '24hr_average', 'min_hours': 18},  # 24-hour average, need 18+ hours
            'PM10': {'period': '24hr_average', 'min_hours': 18},  # 24-hour average, need 18+ hours
            'NO2': {'period': '1hr_maximum', 'min_hours': 18},    # 1-hour maximum, need 18+ hours
            'SO2': {'period': '1hr_maximum', 'min_hours': 18}     # 1-hour maximum, need 18+ hours
        }
        
        # Storage paths following existing patterns
        self.epa_data_prefix = "epa-data"        # 24-hour EPA compliance data
        self.trends_prefix = "trends"            # 30-day trend data
        self.max_trend_days = 30                 # Keep 30 days of trends
        
        # AQI colors (consistent with existing calculators)
        self.aqi_colors = {
            "Good": "#00E400",
            "Moderate": "#FFFF00", 
            "Unhealthy for Sensitive Groups": "#FF7E00",
            "Unhealthy": "#FF0000",
            "Very Unhealthy": "#8F3F97",
            "Hazardous": "#7E0023"
        }
        
        logger.info("✅ Trend Processor initialized with EPA compliance")
    
    def store_hourly_epa_data(self, location_id: str, hourly_data: Dict[str, Any]) -> bool:
        """
        Store hourly data for EPA 24-hour compliance
        Following the pattern from aqi_calculator.py
        
        Args:
            location_id: Location identifier (lat_lon format)
            hourly_data: Hourly measurement data
            
        Returns:
            True if stored successfully
        """
        try:
            current_time = datetime.now(timezone.utc)
            date_str = current_time.strftime('%Y-%m-%d')
            hour_str = current_time.strftime('%H')
            
            # S3 key: epa-data/{location_id}/{date}/{hour}.json
            s3_key = f"{self.epa_data_prefix}/{location_id}/{date_str}/{hour_str}.json"
            
            epa_hourly_data = {
                "location_id": location_id,
                "timestamp": current_time.isoformat(),
                "date": date_str,
                "hour": int(hour_str),
                "data": hourly_data,
                "data_quality": self._assess_data_quality(hourly_data),
                "stored_at": current_time.isoformat()
            }
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(epa_hourly_data, indent=2),
                ContentType='application/json',
                # Auto-delete after 25 hours (EPA compliance only)
                Expires=current_time + timedelta(hours=25)
            )
            
            logger.debug(f"📊 Stored EPA hourly data: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to store EPA hourly data for {location_id}: {e}")
            return False
    
    def process_daily_trends(self, location_id: str, target_date: Optional[str] = None) -> bool:
        """
        Process 24 hours of EPA data into daily trend averages
        Called once per day at midnight (UTC)
        
        Args:
            location_id: Location identifier
            target_date: Date to process (defaults to yesterday)
            
        Returns:
            True if processing successful
        """
        try:
            if target_date is None:
                yesterday = datetime.now(timezone.utc) - timedelta(days=1)
                target_date = yesterday.strftime('%Y-%m-%d')
            
            logger.info(f"📊 Processing daily trends for {location_id} on {target_date}")
            
            hourly_data = self._collect_24hour_epa_data(location_id, target_date)
            
            if not hourly_data:
                logger.warning(f"⚠️ No EPA data found for {location_id} on {target_date}")
                return False
            
            daily_pollutant_data = self._calculate_daily_epa_values(hourly_data)
            
            daily_aqi = self._calculate_daily_aqi(daily_pollutant_data)
            
            trend_data = DailyTrendData(
                date=target_date,
                pollutants=daily_pollutant_data,
                daily_aqi=daily_aqi
            )
            
            success = self._store_daily_trend(location_id, trend_data)
            
            if success:
                logger.info(f"✅ Daily trends processed successfully for {location_id}")
                self._cleanup_old_trends(location_id)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Failed to process daily trends for {location_id}: {e}")
            return False
    
    def get_location_trends(self, location_id: str, days: int = 30) -> Optional[Dict[str, Any]]:
        """
        Get trend data for frontend dashboard
        Fast <100ms response optimized for website display
        
        Args:
            location_id: Location identifier
            days: Number of days to retrieve (default 30)
            
        Returns:
            Trend data for frontend or None if not found
        """
        try:
            s3_key = f"{self.trends_prefix}/{location_id}/daily_averages.json"
            
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            trend_data = json.loads(response['Body'].read().decode('utf-8'))
            
            if 'daily_data' in trend_data:
                sorted_dates = sorted(trend_data['daily_data'].keys(), reverse=True)
                recent_dates = sorted_dates[:days]
                
                filtered_data = {
                    "location": trend_data.get("location", {}),
                    "last_updated": trend_data.get("last_updated"),
                    "daily_data": {date: trend_data['daily_data'][date] for date in recent_dates}
                }
                
                logger.debug(f"📊 Retrieved {len(recent_dates)} days of trend data for {location_id}")
                return filtered_data
            
            return trend_data
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"⚠️ No trend data found for location {location_id}")
            return None
        except Exception as e:
            logger.error(f"❌ Failed to get trends for {location_id}: {e}")
            return None
    
    def _collect_24hour_epa_data(self, location_id: str, date: str) -> List[Dict[str, Any]]:
        """Collect 24 hours of EPA data for a specific date"""
        hourly_data = []
        
        try:
            # List all hourly files for the date
            prefix = f"{self.epa_data_prefix}/{location_id}/{date}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return hourly_data
            
            for obj in response['Contents']:
                try:
                    file_response = self.s3_client.get_object(
                        Bucket=self.s3_bucket,
                        Key=obj['Key']
                    )
                    hourly_json = json.loads(file_response['Body'].read().decode('utf-8'))
                    hourly_data.append(hourly_json)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Failed to read hourly file {obj['Key']}: {e}")
                    continue
            
            hourly_data.sort(key=lambda x: x.get('hour', 0))
            
            logger.debug(f"📊 Collected {len(hourly_data)} hours of EPA data for {location_id} on {date}")
            return hourly_data
            
        except Exception as e:
            logger.error(f"❌ Failed to collect EPA data for {location_id} on {date}: {e}")
            return []
    
    def _calculate_daily_epa_values(self, hourly_data: List[Dict[str, Any]]) -> Dict[str, DailyPollutantData]:
        """
        Calculate EPA-compliant daily values using proper time averaging
        Following EPA standards from existing aqi_calculator.py
        """
        daily_values = {}
        
        pollutant_hours = defaultdict(list)
        
        for hour_data in hourly_data:
            if 'data' in hour_data:
                measurements = hour_data['data']
                for pollutant, value in measurements.items():
                    if value is not None and not math.isnan(value):
                        pollutant_hours[pollutant].append({
                            'hour': hour_data.get('hour', 0),
                            'value': float(value),
                            'timestamp': hour_data.get('timestamp')
                        })
        
        for pollutant, hour_values in pollutant_hours.items():
            if not hour_values:
                continue
            
            if pollutant not in self.epa_averaging_periods:
                logger.warning(f"⚠️ Unknown EPA averaging for pollutant: {pollutant}")
                continue
            
            averaging_info = self.epa_averaging_periods[pollutant]
            period = averaging_info['period']
            min_hours = averaging_info['min_hours']
            
            data_points = len(hour_values)
            completeness = (data_points / 24.0) * 100
            valid_for_epa = data_points >= min_hours
            
            values = [h['value'] for h in hour_values]
            
            if period == '24hr_average':
                epa_value = np.mean(values)
            elif period == '8hr_rolling':
                epa_value = self._calculate_8hr_rolling_max(hour_values)
            elif period == '1hr_maximum':
                epa_value = max(values)
            else:
                logger.warning(f"⚠️ Unknown averaging period: {period}")
                epa_value = np.mean(values)
            
            daily_values[pollutant] = DailyPollutantData(
                epa_value=round(epa_value, 2),
                method=period,
                min_value=round(min(values), 2),
                max_value=round(max(values), 2),
                data_points=data_points,
                completeness=round(completeness, 1),
                valid_for_epa=valid_for_epa
            )
        
        return daily_values
    
    def _calculate_8hr_rolling_max(self, hour_values: List[Dict[str, Any]]) -> float:
        """Calculate maximum 8-hour rolling average (EPA standard for O3/CO)"""
        if len(hour_values) < 6:  # Need at least 6 hours for valid 8-hour average
            return np.mean([h['value'] for h in hour_values])
        
        sorted_hours = sorted(hour_values, key=lambda x: x['hour'])
        values = [h['value'] for h in sorted_hours]
        
        rolling_averages = []
        
        for i in range(len(values) - 7):
            window = values[i:i+8]
            if len([v for v in window if v is not None]) >= 6:  # EPA requires 6+ valid hours
                rolling_avg = np.mean([v for v in window if v is not None])
                rolling_averages.append(rolling_avg)
        
        return max(rolling_averages) if rolling_averages else np.mean(values)
    
    def _calculate_daily_aqi(self, daily_pollutant_data: Dict[str, DailyPollutantData]) -> DailyAQIData:
        """Calculate overall daily AQI using EPA calculator"""
        pollutant_aqis = {}
        
        for pollutant, data in daily_pollutant_data.items():
            if not data.valid_for_epa:
                continue
            
            try:
                aqi_result = self._calculate_pollutant_aqi(pollutant, data.epa_value)
                if aqi_result:
                    pollutant_aqis[pollutant] = aqi_result
            except Exception as e:
                logger.warning(f"⚠️ Failed to calculate AQI for {pollutant}: {e}")
                continue
        
        if not pollutant_aqis:
            # Default if no valid AQI calculations
            return DailyAQIData(
                value=0,
                category="Good",
                dominant_pollutant="None",
                color="#00E400"
            )
        
        dominant_pollutant = max(pollutant_aqis.keys(), key=lambda p: pollutant_aqis[p]['aqi'])
        overall_aqi = pollutant_aqis[dominant_pollutant]['aqi']
        category = self._get_aqi_category(overall_aqi)
        
        return DailyAQIData(
            value=overall_aqi,
            category=category,
            dominant_pollutant=dominant_pollutant,
            color=self.aqi_colors[category]
        )
    
    def _calculate_pollutant_aqi(self, pollutant: str, concentration: float) -> Optional[Dict[str, Any]]:
        """Calculate AQI for a pollutant using existing calculator logic"""
        try:
            if pollutant == "PM25":
                result = self.forecast_calculator.calculate_pollutant_aqi("PM25", concentration, "μg/m³")
            elif pollutant == "O3":
                result = self.forecast_calculator.calculate_pollutant_aqi("O3", concentration, "ppm")
            elif pollutant == "NO2":
                result = self.forecast_calculator.calculate_pollutant_aqi("NO2", concentration, "ppb")
            elif pollutant == "SO2":
                result = self.forecast_calculator.calculate_pollutant_aqi("SO2", concentration, "ppb")
            elif pollutant == "CO":
                result = self.forecast_calculator.calculate_pollutant_aqi("CO", concentration, "ppm")
            else:
                return None
            
            if result:
                return {
                    'aqi': result.aqi,
                    'category': result.category,
                    'color': result.color
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ AQI calculation failed for {pollutant}: {e}")
            return None
    
    def _get_aqi_category(self, aqi_value: int) -> str:
        """Get AQI category from AQI value (consistent with forecast calculator)"""
        if aqi_value <= 50:
            return "Good"
        elif aqi_value <= 100:
            return "Moderate"  
        elif aqi_value <= 150:
            return "Unhealthy for Sensitive Groups"
        elif aqi_value <= 200:
            return "Unhealthy"
        elif aqi_value <= 300:
            return "Very Unhealthy"
        else:
            return "Hazardous"
    
    def _store_daily_trend(self, location_id: str, trend_data: DailyTrendData) -> bool:
        """Store daily trend data to S3"""
        try:
            # S3 key: trends/{location_id}/daily_averages.json
            s3_key = f"{self.trends_prefix}/{location_id}/daily_averages.json"
            
            try:
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                existing_data = json.loads(response['Body'].read().decode('utf-8'))
            except self.s3_client.exceptions.NoSuchKey:
                existing_data = {
                    "location": {
                        "id": location_id,
                        "lat": float(location_id.split('_')[0]) if '_' in location_id else 0,
                        "lon": float(location_id.split('_')[1]) if '_' in location_id else 0
                    },
                    "daily_data": {}
                }
            
            daily_json = {
                "pollutants": {k: asdict(v) for k, v in trend_data.pollutants.items()},
                "daily_aqi": asdict(trend_data.daily_aqi),
                "weather_context": trend_data.weather_context
            }
            
            existing_data["daily_data"][trend_data.date] = daily_json
            existing_data["last_updated"] = datetime.now(timezone.utc).isoformat()
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(existing_data, indent=2),
                ContentType='application/json'
            )
            
            logger.debug(f"📊 Stored daily trend for {location_id} on {trend_data.date}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to store daily trend for {location_id}: {e}")
            return False
    
    def _cleanup_old_trends(self, location_id: str) -> None:
        """Keep only the most recent 30 days of trend data"""
        try:
            s3_key = f"{self.trends_prefix}/{location_id}/daily_averages.json"
            
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            trend_data = json.loads(response['Body'].read().decode('utf-8'))
            
            if 'daily_data' in trend_data:
                # Keep only most recent 30 days
                sorted_dates = sorted(trend_data['daily_data'].keys(), reverse=True)
                recent_dates = sorted_dates[:self.max_trend_days]
                
                # Remove old data
                trend_data['daily_data'] = {
                    date: trend_data['daily_data'][date] 
                    for date in recent_dates
                }
                
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=json.dumps(trend_data, indent=2),
                    ContentType='application/json'
                )
                
                logger.debug(f"🧹 Cleaned old trends for {location_id}, kept {len(recent_dates)} days")
                
        except Exception as e:
            logger.warning(f"⚠️ Failed to cleanup old trends for {location_id}: {e}")
    
    def _assess_data_quality(self, hourly_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess data quality for EPA compliance"""
        total_pollutants = len(hourly_data)
        valid_pollutants = sum(1 for v in hourly_data.values() if v is not None and not math.isnan(v))
        
        return {
            "total_pollutants": total_pollutants,
            "valid_pollutants": valid_pollutants,
            "completeness": round((valid_pollutants / total_pollutants * 100) if total_pollutants > 0 else 0, 1),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def create_trend_summary_for_frontend(self, location_id: str, days: int = 7) -> Optional[Dict[str, Any]]:
        """
        Create optimized trend summary for frontend dashboard
        Fast response optimized for React components
        
        Args:
            location_id: Location identifier
            days: Number of days for trend (7, 14, or 30)
            
        Returns:
            Frontend-optimized trend data
        """
        try:
            trend_data = self.get_location_trends(location_id, days)
            
            if not trend_data or 'daily_data' not in trend_data:
                return None
            
            dates = []
            aqi_values = []  
            categories = []
            dominant_pollutants = []
            
            # Pollutant arrays for individual charts
            pollutant_trends = defaultdict(list)
            
            sorted_dates = sorted(trend_data['daily_data'].keys())
            
            for date in sorted_dates:
                day_data = trend_data['daily_data'][date]
                
                dates.append(date)
                
                # AQI data
                aqi_info = day_data.get('daily_aqi', {})
                aqi_values.append(aqi_info.get('value', 0))
                categories.append(aqi_info.get('category', 'Good'))
                dominant_pollutants.append(aqi_info.get('dominant_pollutant', 'None'))
                
                pollutants = day_data.get('pollutants', {})
                for pollutant, data in pollutants.items():
                    pollutant_trends[pollutant].append({
                        'date': date,
                        'value': data.get('epa_value', 0),
                        'min': data.get('min_value', 0),
                        'max': data.get('max_value', 0),
                        'method': data.get('method', ''),
                        'valid': data.get('valid_for_epa', False)
                    })
            
            frontend_summary = {
                "location": trend_data.get("location", {}),
                "period": f"{days}_days",
                "last_updated": trend_data.get("last_updated"),
                "summary": {
                    "total_days": len(dates),
                    "avg_aqi": round(np.mean(aqi_values)) if aqi_values else 0,
                    "max_aqi": max(aqi_values) if aqi_values else 0,
                    "min_aqi": min(aqi_values) if aqi_values else 0,
                    "trend_direction": self._calculate_trend_direction(aqi_values)
                },
                "time_series": {
                    "dates": dates,
                    "aqi_values": aqi_values,
                    "categories": categories,
                    "dominant_pollutants": dominant_pollutants
                },
                "pollutant_trends": dict(pollutant_trends),
                "chart_config": {
                    "colors": self.aqi_colors,
                    "responsive": True,
                    "animations": True
                }
            }
            
            logger.debug(f"📊 Created frontend trend summary for {location_id} ({days} days)")
            return frontend_summary
            
        except Exception as e:
            logger.error(f"❌ Failed to create frontend trend summary for {location_id}: {e}")
            return None
    
    def _calculate_trend_direction(self, values: List[int]) -> str:
        """Calculate overall trend direction for summary"""
        if len(values) < 2:
            return "stable"
        
        start_avg = np.mean(values[:3]) if len(values) >= 3 else values[0] 
        end_avg = np.mean(values[-3:]) if len(values) >= 3 else values[-1]
        
        change_percent = ((end_avg - start_avg) / start_avg * 100) if start_avg > 0 else 0
        
        if change_percent > 10:
            return "worsening"
        elif change_percent < -10:
            return "improving"
        else:
            return "stable"


# Trend processor - import and use in other modules
if False:  # Disabled main
    # Test the trend processor
    logging.basicConfig(level=logging.INFO)
    
    processor = TrendProcessor()
    
    print("🧪 TESTING TREND PROCESSOR")
    print("=" * 50)
    
    # Test location
    test_location = "40.7128_-74.006"  # NYC coordinates
    
    # Test storing hourly data
    test_hourly = {
        "PM25": 15.2,
        "O3": 45.8,
        "NO2": 28.5,
        "SO2": 12.1,
        "CO": 0.8
    }
    
    print(f"📊 Testing hourly data storage for {test_location}")
    success = processor.store_hourly_epa_data(test_location, test_hourly)
    print(f"✅ Hourly storage: {'SUCCESS' if success else 'FAILED'}")
    
    print("\n📊 Trend Processor ready for production!")
    print("Integration points:")
    print("- Call store_hourly_epa_data() every hour during data collection")
    print("- Call process_daily_trends() once per day at midnight UTC")
    print("- Call get_location_trends() or create_trend_summary_for_frontend() for API endpoints")