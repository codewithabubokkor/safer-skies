"""
Timezone-aware file and data freshness checker
Handles freshness validation based on location coordinates and local timezone
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union
from timezonefinder import TimezoneFinder
import pytz
import pandas as pd

logger = logging.getLogger(__name__)

class TimezoneFreshnessChecker:
    """
    Timezone-aware freshness checker for files and data
    Uses location coordinates to determine local timezone for accurate freshness validation
    """
    
    def __init__(self):
        self.tf = TimezoneFinder()
    
    def get_location_timezone(self, lat: float, lng: float) -> Optional[str]:
        """Get timezone string for given coordinates"""
        try:
            timezone_str = self.tf.timezone_at(lat=lat, lng=lng)
            if timezone_str:
                logger.debug(f"🌍 Detected timezone: {timezone_str} for ({lat}, {lng})")
                return timezone_str
            else:
                logger.warning(f"⚠️ Could not detect timezone for ({lat}, {lng})")
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
                logger.error(f"Error creating timezone object: {e}")
                # Fallback to UTC
                return datetime.now(pytz.UTC)
        else:
            logger.warning(f"🌐 Fallback to UTC for ({lat}, {lng})")
            return datetime.now(pytz.UTC)
    
    def check_file_freshness(self, file_path: str, lat: float, lng: float, 
                           max_age_hours: float = 24) -> Tuple[bool, float, str]:
        """
        Check if a file is fresh based on location timezone
        
        Args:
            file_path: Path to file to check
            lat, lng: Location coordinates  
            max_age_hours: Maximum age in hours to consider fresh
            
        Returns:
            (is_fresh, age_in_hours, status_message)
        """
        try:
            if not os.path.exists(file_path):
                return False, float('inf'), f"❌ File does not exist: {file_path}"
            
            file_mtime = os.path.getmtime(file_path)
            file_datetime = datetime.fromtimestamp(file_mtime)
            
            current_time = self.get_location_time(lat, lng)
            timezone_str = self.get_location_timezone(lat, lng)
            
            if timezone_str:
                local_tz = pytz.timezone(timezone_str)
                # Assume file time is in UTC, convert to local
                file_datetime_utc = pytz.UTC.localize(file_datetime)
                file_datetime_local = file_datetime_utc.astimezone(local_tz)
                
                age_seconds = (current_time.replace(tzinfo=None) - file_datetime_local.replace(tzinfo=None)).total_seconds()
            else:
                # Fallback: both times in UTC
                current_utc = datetime.now(pytz.UTC)
                file_datetime_utc = pytz.UTC.localize(file_datetime)
                age_seconds = (current_utc.replace(tzinfo=None) - file_datetime_utc.replace(tzinfo=None)).total_seconds()
            
            age_hours = age_seconds / 3600
            is_fresh = age_hours <= max_age_hours
            
            status = "✅ FRESH" if is_fresh else "📅 STALE"
            tz_info = f" ({timezone_str})" if timezone_str else " (UTC fallback)"
            
            message = f"{status} - File age: {age_hours:.1f}h (max: {max_age_hours}h){tz_info}"
            
            logger.info(f"📁 File freshness check: {file_path} - {message}")
            return is_fresh, age_hours, message
            
        except Exception as e:
            error_msg = f"❌ Error checking file freshness: {e}"
            logger.error(error_msg)
            return False, float('inf'), error_msg
    
    def check_data_freshness(self, data_timestamp: Union[str, datetime, pd.Timestamp], 
                           lat: float, lng: float, max_age_hours: float = 24) -> Tuple[bool, float, str]:
        """
        Check if data is fresh based on its timestamp and location timezone
        
        Args:
            data_timestamp: Timestamp of the data (string, datetime, or pandas Timestamp)
            lat, lng: Location coordinates
            max_age_hours: Maximum age in hours to consider fresh
            
        Returns:
            (is_fresh, age_in_hours, status_message)
        """
        try:
            if isinstance(data_timestamp, str):
                data_dt = pd.to_datetime(data_timestamp)
            elif isinstance(data_timestamp, pd.Timestamp):
                data_dt = data_timestamp.to_pydatetime()
            else:
                data_dt = data_timestamp
            
            current_time = self.get_location_time(lat, lng)
            timezone_str = self.get_location_timezone(lat, lng)
            
            if timezone_str:
                local_tz = pytz.timezone(timezone_str)
                
                if data_dt.tzinfo is None:
                    # Assume UTC if no timezone info
                    data_dt_utc = pytz.UTC.localize(data_dt)
                elif data_dt.tzinfo != pytz.UTC:
                    data_dt_utc = data_dt.astimezone(pytz.UTC)
                else:
                    data_dt_utc = data_dt
                
                data_dt_local = data_dt_utc.astimezone(local_tz)
                
                age_seconds = (current_time.replace(tzinfo=None) - data_dt_local.replace(tzinfo=None)).total_seconds()
            else:
                # Fallback to UTC comparison
                current_utc = datetime.now(pytz.UTC)
                if data_dt.tzinfo is None:
                    data_dt = pytz.UTC.localize(data_dt)
                
                age_seconds = (current_utc.replace(tzinfo=None) - data_dt.replace(tzinfo=None)).total_seconds()
            
            age_hours = age_seconds / 3600
            is_fresh = age_hours <= max_age_hours
            
            status = "✅ FRESH" if is_fresh else "📅 STALE"
            tz_info = f" ({timezone_str})" if timezone_str else " (UTC fallback)"
            
            message = f"{status} - Data age: {age_hours:.1f}h (max: {max_age_hours}h){tz_info}"
            
            logger.info(f"📊 Data freshness check: {message}")
            return is_fresh, age_hours, message
            
        except Exception as e:
            error_msg = f"❌ Error checking data freshness: {e}"
            logger.error(error_msg)
            return False, float('inf'), error_msg
    
    def check_multiple_files_freshness(self, file_patterns: list, lat: float, lng: float,
                                     max_age_hours: float = 24) -> dict:
        """
        Check freshness of multiple files with glob patterns
        
        Args:
            file_patterns: List of file paths or glob patterns
            lat, lng: Location coordinates
            max_age_hours: Maximum age in hours
            
        Returns:
            Dictionary with file paths as keys and freshness info as values
        """
        import glob
        
        results = {}
        
        for pattern in file_patterns:
            if '*' in pattern or '?' in pattern:
                files = glob.glob(pattern)
            else:
                files = [pattern]
            
            for file_path in files:
                is_fresh, age_hours, message = self.check_file_freshness(
                    file_path, lat, lng, max_age_hours
                )
                results[file_path] = {
                    'is_fresh': is_fresh,
                    'age_hours': age_hours,
                    'message': message
                }
        
        return results
    
    def get_stale_files(self, file_patterns: list, lat: float, lng: float,
                       max_age_hours: float = 24) -> list:
        """Get list of stale files that need refresh"""
        results = self.check_multiple_files_freshness(file_patterns, lat, lng, max_age_hours)
        return [file_path for file_path, info in results.items() if not info['is_fresh']]
    
    def log_freshness_summary(self, file_patterns: list, lat: float, lng: float,
                            max_age_hours: float = 24) -> None:
        """Log a summary of file freshness status"""
        results = self.check_multiple_files_freshness(file_patterns, lat, lng, max_age_hours)
        timezone_str = self.get_location_timezone(lat, lng)
        
        fresh_count = sum(1 for info in results.values() if info['is_fresh'])
        stale_count = len(results) - fresh_count
        
        logger.info(f"📋 Freshness Summary for ({lat}, {lng}) {timezone_str or 'UTC'}:")
        logger.info(f"   ✅ Fresh files: {fresh_count}")
        logger.info(f"   📅 Stale files: {stale_count}")
        logger.info(f"   📁 Total files: {len(results)}")
        
        if stale_count > 0:
            stale_files = [f for f, info in results.items() if not info['is_fresh']]
            logger.info(f"   🔄 Files needing refresh: {stale_files[:3]}{'...' if len(stale_files) > 3 else ''}")


# Global instance for easy access
freshness_checker = TimezoneFreshnessChecker()

# Convenience functions
def check_file_fresh(file_path: str, lat: float, lng: float, max_age_hours: float = 24) -> bool:
    """Quick check if a file is fresh"""
    is_fresh, _, _ = freshness_checker.check_file_freshness(file_path, lat, lng, max_age_hours)
    return is_fresh

def check_data_fresh(data_timestamp, lat: float, lng: float, max_age_hours: float = 24) -> bool:
    """Quick check if data is fresh"""
    is_fresh, _, _ = freshness_checker.check_data_freshness(data_timestamp, lat, lng, max_age_hours)
    return is_fresh

def get_location_time_now(lat: float, lng: float) -> datetime:
    """Get current time in location's timezone"""
    return freshness_checker.get_location_time(lat, lng)