#!/usr/bin/env python3
"""
🌍 TIMEZONE HANDLER FOR NORTH AMERICA
====================================
Handles timezone conversions for TEMPO (UTC) and local time display
Covers all North American time zones for NASA Space Apps Challenge

Time Zones Supported:
- Eastern (EST/EDT): UTC-5/-4 (New York, Toronto, Miami)  
- Central (CST/CDT): UTC-6/-5 (Chicago, Dallas, Mexico City)
- Mountain (MST/MDT): UTC-7/-6 (Denver, Calgary, Phoenix)
- Pacific (PST/PDT): UTC-8/-7 (Los Angeles, Vancouver, Seattle)
- Alaska (AKST/AKDT): UTC-9/-8 (Anchorage)
- Hawaii (HST): UTC-10 (Honolulu - no DST)
- Atlantic (AST): UTC-4 (Puerto Rico, Nova Scotia)
- Newfoundland (NST): UTC-3:30 (Newfoundland)
"""

from datetime import datetime, timezone
import pytz
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class NorthAmericaTimezones:
    """
    Timezone handler for North American cities and coordinates
    """
    
    def __init__(self):
        self.setup_timezone_mappings()
    
    def setup_timezone_mappings(self):
        """Setup timezone mappings for North American cities and regions"""
        
        # City name to timezone mapping
        self.city_timezones = {
            # United States - Eastern
            "new york": "America/New_York",
            "new_york": "America/New_York", 
            "miami": "America/New_York",
            "boston": "America/New_York",
            "atlanta": "America/New_York",
            "philadelphia": "America/New_York",
            "washington": "America/New_York",
            "detroit": "America/New_York",
            
            # United States - Central  
            "chicago": "America/Chicago",
            "dallas": "America/Chicago",
            "houston": "America/Chicago",
            "san antonio": "America/Chicago",
            "san_antonio": "America/Chicago",
            "austin": "America/Chicago",
            "new orleans": "America/Chicago",
            "new_orleans": "America/Chicago",
            "minneapolis": "America/Chicago",
            "kansas city": "America/Chicago",
            "kansas_city": "America/Chicago",
            
            # United States - Mountain
            "denver": "America/Denver",
            "salt lake city": "America/Denver", 
            "salt_lake_city": "America/Denver",
            "albuquerque": "America/Denver",
            "phoenix": "America/Phoenix",  # No DST in most of Arizona
            "tucson": "America/Phoenix",
            "calgary": "America/Calgary",
            
            # United States - Pacific
            "los angeles": "America/Los_Angeles",
            "los_angeles": "America/Los_Angeles",
            "san francisco": "America/Los_Angeles",
            "san_francisco": "America/Los_Angeles", 
            "san diego": "America/Los_Angeles",
            "san_diego": "America/Los_Angeles",
            "san jose": "America/Los_Angeles",
            "san_jose": "America/Los_Angeles",
            "seattle": "America/Los_Angeles",
            "portland": "America/Los_Angeles",
            "las vegas": "America/Los_Angeles",
            "las_vegas": "America/Los_Angeles",
            
            # Alaska
            "anchorage": "America/Anchorage",
            "fairbanks": "America/Anchorage",
            
            # Hawaii
            "honolulu": "Pacific/Honolulu",
            
            # Canada - Eastern
            "toronto": "America/Toronto",
            "ottawa": "America/Toronto", 
            "montreal": "America/Montreal",
            "quebec": "America/Montreal",
            
            # Canada - Central
            "winnipeg": "America/Winnipeg",
            
            # Canada - Mountain
            "edmonton": "America/Edmonton",
            
            # Canada - Pacific
            "vancouver": "America/Vancouver",
            "victoria": "America/Vancouver",
            
            # Canada - Atlantic
            "halifax": "America/Halifax",
            
            # Canada - Newfoundland
            "st johns": "America/St_Johns",
            "st_johns": "America/St_Johns",
            
            # Mexico
            "mexico city": "America/Mexico_City",
            "mexico_city": "America/Mexico_City",
            "guadalajara": "America/Mexico_City",
            "monterrey": "America/Mexico_City",
            "tijuana": "America/Tijuana",
            "cancun": "America/Cancun",
        }
        
        # Coordinate-based timezone boundaries (approximate)
        self.coordinate_zones = [
            # Eastern Time Zone (approximate boundaries)
            (24.0, 50.0, -85.0, -67.0, "America/New_York"),
            
            # Central Time Zone  
            (25.0, 50.0, -105.0, -85.0, "America/Chicago"),
            
            # Mountain Time Zone
            (25.0, 50.0, -115.0, -105.0, "America/Denver"),
            
            # Pacific Time Zone
            (32.0, 50.0, -125.0, -115.0, "America/Los_Angeles"),
            
            # Alaska
            (55.0, 72.0, -180.0, -130.0, "America/Anchorage"),
            
            # Hawaii
            (18.0, 25.0, -162.0, -154.0, "Pacific/Honolulu"),
            
            # Mexico (most areas)
            (14.0, 32.0, -118.0, -86.0, "America/Mexico_City"),
        ]
    
    def get_timezone_for_city(self, city_name: str) -> Optional[str]:
        """
        Get timezone for a city name
        
        Args:
            city_name: City name (case insensitive)
            
        Returns:
            Timezone string or None if not found
        """
        city_key = city_name.lower().strip()
        return self.city_timezones.get(city_key)
    
    def get_timezone_for_coordinates(self, latitude: float, longitude: float) -> str:
        """
        Get timezone for coordinates using approximate boundaries
        
        Args:
            latitude: Latitude in decimal degrees
            longitude: Longitude in decimal degrees
            
        Returns:
            Timezone string (defaults to America/New_York if no match)
        """
        for lat_min, lat_max, lon_min, lon_max, tz in self.coordinate_zones:
            if lat_min <= latitude <= lat_max and lon_min <= longitude <= lon_max:
                return tz
        
        # Default fallback
        logger.warning(f"No timezone found for {latitude}, {longitude}, using America/New_York")
        return "America/New_York"
    
    def utc_to_local(self, utc_datetime: datetime, timezone_str: str) -> datetime:
        """
        Convert UTC datetime to local time
        
        Args:
            utc_datetime: UTC datetime (timezone-aware or naive)
            timezone_str: Target timezone string
            
        Returns:
            Localized datetime
        """
        try:
            if utc_datetime.tzinfo is None:
                utc_datetime = utc_datetime.replace(tzinfo=timezone.utc)
            elif utc_datetime.tzinfo != timezone.utc:
                utc_datetime = utc_datetime.astimezone(timezone.utc)
            
            target_tz = pytz.timezone(timezone_str)
            local_datetime = utc_datetime.astimezone(target_tz)
            
            return local_datetime
            
        except Exception as e:
            logger.error(f"Timezone conversion error: {e}")
            return utc_datetime
    
    def local_to_utc(self, local_datetime: datetime, timezone_str: str) -> datetime:
        """
        Convert local datetime to UTC
        
        Args:
            local_datetime: Local datetime (naive or timezone-aware)
            timezone_str: Source timezone string
            
        Returns:
            UTC datetime
        """
        try:
            source_tz = pytz.timezone(timezone_str)
            
            if local_datetime.tzinfo is None:
                local_datetime = source_tz.localize(local_datetime)
            
            utc_datetime = local_datetime.astimezone(timezone.utc)
            return utc_datetime
            
        except Exception as e:
            logger.error(f"Local to UTC conversion error: {e}")
            return local_datetime
    
    def format_local_time(self, utc_datetime: datetime, timezone_str: str, 
                         format_str: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
        """
        Format UTC datetime as local time string
        
        Args:
            utc_datetime: UTC datetime
            timezone_str: Target timezone
            format_str: Output format string
            
        Returns:
            Formatted local time string
        """
        local_dt = self.utc_to_local(utc_datetime, timezone_str)
        return local_dt.strftime(format_str)
    
    def get_timezone_info(self, city_name: str = None, 
                         latitude: float = None, longitude: float = None) -> Dict:
        """
        Get comprehensive timezone information for a location
        
        Args:
            city_name: City name (optional)
            latitude: Latitude (optional, used if city_name not found)
            longitude: Longitude (optional, used if city_name not found)
            
        Returns:
            Dictionary with timezone information
        """
        timezone_str = None
        
        if city_name:
            timezone_str = self.get_timezone_for_city(city_name)
        
        # Fall back to coordinates
        if not timezone_str and latitude is not None and longitude is not None:
            timezone_str = self.get_timezone_for_coordinates(latitude, longitude)
        
        if not timezone_str:
            timezone_str = "America/New_York"  # Default
        
        tz = pytz.timezone(timezone_str)
        
        # Current time info
        utc_now = datetime.now(timezone.utc)
        local_now = self.utc_to_local(utc_now, timezone_str)
        
        return {
            "timezone": timezone_str,
            "timezone_name": str(tz),
            "current_utc": utc_now.isoformat(),
            "current_local": local_now.isoformat(),
            "utc_offset": local_now.strftime("%z"),
            "timezone_abbreviation": local_now.strftime("%Z"),
            "is_dst": bool(local_now.dst()),
        }

# Global instance
na_timezones = NorthAmericaTimezones()

def get_local_time_for_location(city: str = None, lat: float = None, lon: float = None) -> Dict:
    """
    Convenience function to get timezone info for any North American location
    """
    return na_timezones.get_timezone_info(city, lat, lon)


