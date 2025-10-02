"""
Database-specific timezone-aware freshness checker for NAQ Forecast system
Works with specific table schemas and created_at timestamps
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
from timezonefinder import TimezoneFinder
import pytz
import pandas as pd

logger = logging.getLogger(__name__)

class DatabaseFreshnessChecker:
    """
    Timezone-aware freshness checker for database records
    Uses created_at timestamps and location coordinates for accurate freshness validation
    """
    
    def __init__(self):
        self.tf = TimezoneFinder()
        
        # Simple cache for timezone lookups to avoid repeated calculations
        self._timezone_cache = {}
        
        # Freshness rules based on data type
        self.FRESHNESS_RULES = {
            'aqi': 1,          # AQI/Why Today: 1 hour after created_at
            'forecast': 24,    # Forecast: 24 hours after created_at
            'fire': 24         # Fire data: 24 hours after created_at
        }
    
    def get_location_timezone(self, lat: float, lng: float) -> Optional[str]:
        """Get timezone string for given coordinates with caching"""
        cache_key = (round(lat, 2), round(lng, 2))
        
        if cache_key in self._timezone_cache:
            return self._timezone_cache[cache_key]
            
        try:
            timezone_str = self.tf.timezone_at(lat=lat, lng=lng)
            if timezone_str:
                # Cache the result
                self._timezone_cache[cache_key] = timezone_str
                logger.debug(f"🌍 Timezone: {timezone_str} for ({lat}, {lng})")
                return timezone_str
            else:
                logger.warning(f"⚠️ No timezone found for ({lat}, {lng})")
                return None
        except Exception as e:
            logger.error(f"Error detecting timezone: {e}")
            return None
    
    def get_location_time(self, lat: float, lng: float) -> datetime:
        """Get current time in location's timezone"""
        timezone_str = self.get_location_timezone(lat, lng)
        
        if timezone_str:
            try:
                local_tz = pytz.timezone(timezone_str)
                return datetime.now(local_tz)
            except Exception as e:
                logger.error(f"Error with timezone {timezone_str}: {e}")
                return datetime.now(pytz.UTC)
        else:
            # Fallback to UTC
            logger.info(f"🌐 Using UTC fallback for ({lat}, {lng})")
            return datetime.now(pytz.UTC)
    
    def check_data_freshness_by_type(self, df: pd.DataFrame, lat: float, lng: float, 
                                   data_type: str) -> Tuple[bool, float, str]:
        """
        Check if database data is fresh based on created_at timestamp and data type
        
        Args:
            df: DataFrame with database records containing created_at column
            lat, lng: Location coordinates for timezone detection
            data_type: Type of data ('aqi', 'forecast', 'fire')
            
        Returns:
            (is_fresh, age_in_hours, status_message)
        """
        try:
            if df is None or df.empty:
                return False, float('inf'), f"❌ No data available"
            
            max_age_hours = self.FRESHNESS_RULES.get(data_type, 24)
            
            created_at_column = None
            possible_columns = ['created_at', 'collection_timestamp', 'timestamp']
            
            for col in possible_columns:
                if col in df.columns:
                    created_at_column = col
                    break
            
            if created_at_column is None:
                return False, float('inf'), f"❌ No timestamp column found in {data_type} data"
            
            latest_created_at = df[created_at_column].max()
            
            if pd.isna(latest_created_at):
                return False, float('inf'), f"❌ Invalid timestamp in {data_type} data"
            
            current_time = self.get_location_time(lat, lng)
            timezone_str = self.get_location_timezone(lat, lng)
            
            if isinstance(latest_created_at, str):
                latest_dt = pd.to_datetime(latest_created_at)
            else:
                latest_dt = latest_created_at.to_pydatetime() if hasattr(latest_created_at, 'to_pydatetime') else latest_created_at
            
            if timezone_str:
                local_tz = pytz.timezone(timezone_str)
                current_local = current_time
                
                if latest_dt.tzinfo is None:
                    # Assume created_at is in UTC (MySQL default for TIMESTAMP)
                    latest_dt_utc = pytz.UTC.localize(latest_dt)
                else:
                    latest_dt_utc = latest_dt if latest_dt.tzinfo == pytz.UTC else latest_dt.astimezone(pytz.UTC)
                
                latest_dt_local = latest_dt_utc.astimezone(local_tz)
                
                age_seconds = (current_local.replace(tzinfo=None) - latest_dt_local.replace(tzinfo=None)).total_seconds()
            else:
                # Fallback: compare in UTC
                current_utc = datetime.now(pytz.UTC)
                if latest_dt.tzinfo is None:
                    # Assume created_at is in UTC
                    latest_dt_utc = pytz.UTC.localize(latest_dt)
                else:
                    latest_dt_utc = latest_dt
                
                age_seconds = (current_utc.replace(tzinfo=None) - latest_dt_utc.replace(tzinfo=None)).total_seconds()
            
            age_hours = age_seconds / 3600
            is_fresh = age_hours <= max_age_hours
            
            status_icon = "✅ FRESH" if is_fresh else "📅 STALE"
            tz_info = f" ({timezone_str})" if timezone_str else " (UTC)"
            data_type_label = data_type.upper()
            
            message = f"{status_icon} {data_type_label} - Age: {age_hours:.1f}h (max: {max_age_hours}h){tz_info}"
            
            logger.info(f"📊 {data_type_label} freshness check for ({lat}, {lng}): {message}")
            return is_fresh, age_hours, message
            
        except Exception as e:
            error_msg = f"❌ Error checking {data_type} freshness: {e}"
            logger.error(error_msg)
            return False, float('inf'), error_msg
    
    def check_aqi_freshness(self, df: pd.DataFrame, lat: float, lng: float) -> Tuple[bool, float, str]:
        """Check AQI data freshness (1 hour rule)"""
        return self.check_data_freshness_by_type(df, lat, lng, 'aqi')
    
    def check_forecast_freshness(self, df: pd.DataFrame, lat: float, lng: float) -> Tuple[bool, float, str]:
        """Check forecast data freshness (24 hour rule)"""
        return self.check_data_freshness_by_type(df, lat, lng, 'forecast')
    
    def check_fire_freshness(self, df: pd.DataFrame, lat: float, lng: float) -> Tuple[bool, float, str]:
        """Check fire data freshness (24 hour rule)"""
        return self.check_data_freshness_by_type(df, lat, lng, 'fire')
    
    def get_freshness_status(self, df: pd.DataFrame, lat: float, lng: float, data_type: str) -> dict:
        """
        Get comprehensive freshness status information
        
        Returns:
            Dictionary with freshness details
        """
        is_fresh, age_hours, message = self.check_data_freshness_by_type(df, lat, lng, data_type)
        
        timezone_str = self.get_location_timezone(lat, lng)
        current_time = self.get_location_time(lat, lng)
        max_age_hours = self.FRESHNESS_RULES.get(data_type, 24)
        
        return {
            'is_fresh': is_fresh,
            'age_hours': round(age_hours, 2),
            'max_age_hours': max_age_hours,
            'message': message,
            'timezone': timezone_str or 'UTC',
            'current_local_time': current_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'data_type': data_type,
            'location': f"({lat}, {lng})"
        }
    
    def log_freshness_summary(self, datasets: dict, lat: float, lng: float):
        """
        Log freshness summary for multiple datasets
        
        Args:
            datasets: Dict with data_type as key and DataFrame as value
            lat, lng: Location coordinates
        """
        timezone_str = self.get_location_timezone(lat, lng)
        current_time = self.get_location_time(lat, lng)
        
        logger.info(f"📋 Freshness Summary for ({lat}, {lng}) - {timezone_str or 'UTC'}")
        logger.info(f"   🕐 Current local time: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        fresh_count = 0
        stale_count = 0
        
        for data_type, df in datasets.items():
            is_fresh, age_hours, message = self.check_data_freshness_by_type(df, lat, lng, data_type)
            
            if is_fresh:
                fresh_count += 1
                logger.info(f"   ✅ {data_type.upper()}: Fresh ({age_hours:.1f}h old)")
            else:
                stale_count += 1
                logger.info(f"   📅 {data_type.upper()}: Stale ({age_hours:.1f}h old)")
        
        logger.info(f"   📊 Total: {fresh_count} fresh, {stale_count} stale")


# Global instance for easy access
db_freshness_checker = DatabaseFreshnessChecker()

# Convenience functions
def is_aqi_fresh(df: pd.DataFrame, lat: float, lng: float) -> bool:
    """Quick check if AQI data is fresh (1 hour rule)"""
    is_fresh, _, _ = db_freshness_checker.check_aqi_freshness(df, lat, lng)
    return is_fresh

def is_forecast_fresh(df: pd.DataFrame, lat: float, lng: float) -> bool:
    """Quick check if forecast data is fresh (24 hour rule)"""
    is_fresh, _, _ = db_freshness_checker.check_forecast_freshness(df, lat, lng)
    return is_fresh

def is_fire_fresh(df: pd.DataFrame, lat: float, lng: float) -> bool:
    """Quick check if fire data is fresh (24 hour rule)"""
    is_fresh, _, _ = db_freshness_checker.check_fire_freshness(df, lat, lng)
    return is_fresh

def get_data_age_hours(df: pd.DataFrame, lat: float, lng: float, data_type: str) -> float:
    """Get age of data in hours (location timezone)"""
    _, age_hours, _ = db_freshness_checker.check_data_freshness_by_type(df, lat, lng, data_type)
    return age_hours