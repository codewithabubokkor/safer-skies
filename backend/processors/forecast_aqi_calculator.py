#!/usr/bin/env python3
"""
🔮 FORECAST AQI CALCULATOR
Dedicated AQI calculator for 5-day forecast data

This module provides:
- Fast AQI calculation for forecast data (120 hours)
- Direct EPA breakpoint implementation
- Optimized for batch processing
- Simplified data structures
- No complex averaging requirements
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ForecastAQIResult:
    """Simple AQI result for forecast data"""
    pollutant: str
    concentration: float
    units: str
    aqi: int
    category: str
    color: str

class ForecastAQICalculator:
    """
    Lightweight AQI calculator specifically designed for forecast data
    
    Features:
    - Fast batch processing for 120-hour forecasts
    - Direct EPA breakpoint lookup
    - No complex time averaging (uses hourly values directly)
    - Optimized for website display
    """
    
    def __init__(self):
        self.setup_epa_breakpoints()
        self.setup_colors()
        logger.info("✅ Forecast AQI Calculator initialized")
    
    def setup_epa_breakpoints(self):
        """Setup EPA AQI breakpoints for forecast pollutants - EXACT EPA COMPLIANCE"""
        self.breakpoints = {
            # O3 (8-hour) - ppm (CORRECTED EPA values - fixed continuity)
            "O3": [
                (0.000, 0.054, 0, 50, "Good"),
                (0.0541, 0.070, 51, 100, "Moderate"),
                (0.0700, 0.085, 101, 150, "Unhealthy for Sensitive Groups"),
                (0.0851, 0.105, 151, 200, "Unhealthy"),
                (0.1051, 0.200, 201, 300, "Very Unhealthy"),
                (0.2001, 0.604, 301, 500, "Hazardous")
            ],
            "NO2": [
                (0, 53, 0, 50, "Good"),
                (53.1, 100, 51, 100, "Moderate"),
                (101, 360, 101, 150, "Unhealthy for Sensitive Groups"),
                (361, 649, 151, 200, "Unhealthy"),
                (650, 1249, 201, 300, "Very Unhealthy"),
                (1250, 2049, 301, 500, "Hazardous")
            ],
            "SO2": [
                (0, 35, 0, 50, "Good"),
                (36, 75, 51, 100, "Moderate"),
                (76, 185, 101, 150, "Unhealthy for Sensitive Groups"),
                (186, 304, 151, 200, "Unhealthy"),
                (305, 604, 201, 300, "Very Unhealthy"),
                (605, 1004, 301, 500, "Hazardous")
            ],
            "CO": [
                (0.0, 4.4, 0, 50, "Good"),
                (4.5, 9.4, 51, 100, "Moderate"),
                (9.41, 12.4, 101, 150, "Unhealthy for Sensitive Groups"),
                (12.41, 15.4, 151, 200, "Unhealthy"),
                (15.41, 30.4, 201, 300, "Very Unhealthy"),
                (30.41, 50.4, 301, 500, "Hazardous")
            ],
            "PM25": [
                (0.0, 12.0, 0, 50, "Good"),
                (12.1, 35.4, 51, 100, "Moderate"),
                (35.41, 55.4, 101, 150, "Unhealthy for Sensitive Groups"),
                (55.5, 150.4, 151, 200, "Unhealthy"),
                (150.5, 250.4, 201, 300, "Very Unhealthy"),
                (250.5, 350.4, 301, 400, "Hazardous"),
                (350.5, 500.4, 401, 500, "Hazardous")
            ]
        }
    
    def setup_colors(self):
        """Setup AQI category colors for website display"""
        self.colors = {
            "Good": "#00E400",                    # Green
            "Moderate": "#FFFF00",                # Yellow
            "Unhealthy for Sensitive Groups": "#FF7E00",  # Orange
            "Unhealthy": "#FF0000",               # Red
            "Very Unhealthy": "#8F3F97",          # Purple
            "Hazardous": "#7E0023"                # Maroon
        }
    
    def calculate_pollutant_aqi(self, pollutant: str, concentration: float, 
                               input_units: str) -> Optional[ForecastAQIResult]:
        """
        Calculate AQI for a single pollutant concentration
        
        Args:
            pollutant: Pollutant name (O3, NO2, SO2, CO, PM25)
            concentration: Concentration value
            input_units: Units of the concentration (ppb, ppm, μg/m³)
            
        Returns:
            ForecastAQIResult or None if calculation fails
        """
        if concentration is None or concentration < 0:
            return None
        
        converted_concentration, target_units = self._convert_units(
            pollutant, concentration, input_units
        )
        
        if converted_concentration is None:
            logger.warning(f"⚠️ Unit conversion failed for {pollutant}: {concentration} {input_units}")
            return None
        
        if pollutant not in self.breakpoints:
            logger.warning(f"⚠️ No breakpoints for pollutant: {pollutant}")
            return None
        
        breakpoints = self.breakpoints[pollutant]
        
        for bp_lo, bp_hi, aqi_lo, aqi_hi, category in breakpoints:
            if bp_lo <= converted_concentration <= bp_hi:
                if bp_hi == bp_lo:
                    aqi = aqi_lo
                else:
                    aqi = ((aqi_hi - aqi_lo) / (bp_hi - bp_lo)) * (converted_concentration - bp_lo) + aqi_lo
                
                aqi_value = int(round(aqi))
                
                return ForecastAQIResult(
                    pollutant=pollutant,
                    concentration=concentration,  # Original concentration
                    units=input_units,            # Original units
                    aqi=aqi_value,
                    category=category,
                    color=self.colors[category]
                )
        
        logger.debug(f"📊 {pollutant} concentration {converted_concentration} {target_units} above scale")
        return ForecastAQIResult(
            pollutant=pollutant,
            concentration=concentration,
            units=input_units,
            aqi=500,
            category="Hazardous",
            color=self.colors["Hazardous"]
        )
    
    def _convert_units(self, pollutant: str, concentration: float, 
                      input_units: str) -> Tuple[Optional[float], str]:
        """
        Convert pollutant concentration to units expected by EPA breakpoints
        
        Returns:
            (converted_concentration, target_units) or (None, "") if conversion fails
        """
        # Unit conversion mapping
        conversions = {
            "O3": {
                "target_units": "ppm",
                "conversions": {
                    "ppb": lambda x: x / 1000.0,
                    "ppm": lambda x: x,
                    "μg/m³": lambda x: x * 0.000511
                }
            },
            "NO2": {
                "target_units": "ppb",
                "conversions": {
                    "ppb": lambda x: x,
                    "ppm": lambda x: x * 1000.0,
                    "μg/m³": lambda x: x * 0.532
                }
            },
            "SO2": {
                "target_units": "ppb",
                "conversions": {
                    "ppb": lambda x: x,
                    "ppm": lambda x: x * 1000.0,
                    "μg/m³": lambda x: x * 0.382
                }
            },
            "CO": {
                "target_units": "ppm",
                "conversions": {
                    "ppm": lambda x: x,
                    "ppb": lambda x: x / 1000.0,
                    "mg/m³": lambda x: x * 0.873
                }
            },
            "PM25": {
                "target_units": "μg/m³",
                "conversions": {
                    "μg/m³": lambda x: x,
                    "ugm3": lambda x: x,
                    "ug/m3": lambda x: x,
                    "mg/m³": lambda x: x * 1000.0
                }
            }
        }
        
        if pollutant not in conversions:
            return None, ""
        
        pollutant_conversions = conversions[pollutant]
        target_units = pollutant_conversions["target_units"]
        
        if input_units in pollutant_conversions["conversions"]:
            converter = pollutant_conversions["conversions"][input_units]
            converted = converter(concentration)
            return converted, target_units
        else:
            logger.warning(f"⚠️ Unknown units for {pollutant}: {input_units}")
            return None, ""
    
    def calculate_hourly_forecast_aqi(self, hourly_data: List[Dict]) -> List[Dict]:
        """
        Calculate AQI for all hours in forecast data
        
        Args:
            hourly_data: List of hourly forecast data dictionaries
            
        Returns:
            Updated hourly data with AQI calculations
        """
        logger.info(f"🔮 Calculating AQI for {len(hourly_data)} forecast hours")
        
        processed_hours = 0
        
        for hour_data in hourly_data:
            aqi_results = {}
            pollutant_aqis = {}
            
            pollutant_mapping = {
                "O3": ("O3_ppb", "ppb"),
                "NO2": ("NO2_ppb", "ppb"),
                "SO2": ("SO2_ppb", "ppb"),
                "CO": ("CO_ppm", "ppm"),
                "PM25": ("PM25_ugm3", "μg/m³")
            }
            
            for pollutant, (data_key, units) in pollutant_mapping.items():
                if data_key in hour_data and hour_data[data_key] is not None:
                    concentration = hour_data[data_key]
                    
                    aqi_result = self.calculate_pollutant_aqi(pollutant, concentration, units)
                    
                    if aqi_result:
                        aqi_results[pollutant] = aqi_result
                        pollutant_aqis[pollutant] = aqi_result.aqi
                        
                        hour_data[f"{pollutant}_aqi"] = aqi_result.aqi
                        hour_data[f"{pollutant}_category"] = aqi_result.category
            
            if pollutant_aqis:
                overall_aqi = max(pollutant_aqis.values())
                dominant_pollutant = max(pollutant_aqis, key=pollutant_aqis.get)
                
                # Determine overall category
                overall_category = self._get_aqi_category(overall_aqi)
                
                hour_data["overall_aqi"] = overall_aqi
                hour_data["dominant_pollutant"] = dominant_pollutant
                hour_data["aqi_category"] = overall_category
                hour_data["aqi_color"] = self.colors[overall_category]
                
                processed_hours += 1
        
        logger.info(f"✅ AQI calculations completed for {processed_hours} hours")
        return hourly_data
    
    def _get_aqi_category(self, aqi_value: int) -> str:
        """Get AQI category from AQI value"""
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
    
    def calculate_daily_aqi_summary(self, hourly_data: List[Dict]) -> List[Dict]:
        """
        Calculate daily AQI summaries from hourly data
        
        Args:
            hourly_data: List of hourly data with AQI calculations
            
        Returns:
            List of daily summary dictionaries
        """
        daily_summaries = []
        
        from collections import defaultdict
        from datetime import datetime
        
        daily_groups = defaultdict(list)
        
        for hour_data in hourly_data:
            if "timestamp" in hour_data and "overall_aqi" in hour_data:
                timestamp = hour_data["timestamp"]
                if isinstance(timestamp, str):
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                else:
                    dt = timestamp
                
                date_key = dt.date()
                daily_groups[date_key].append(hour_data)
        
        for date, hours in daily_groups.items():
            if not hours:
                continue
            
            aqi_values = [h["overall_aqi"] for h in hours if "overall_aqi" in h]
            
            if aqi_values:
                daily_summary = {
                    "date": date.isoformat(),
                    "max_aqi": max(aqi_values),
                    "min_aqi": min(aqi_values),
                    "avg_aqi": round(sum(aqi_values) / len(aqi_values)),
                    "max_aqi_hour": max(hours, key=lambda h: h.get("overall_aqi", 0))["timestamp"],
                    "dominant_pollutant": max(hours, key=lambda h: h.get("overall_aqi", 0))["dominant_pollutant"],
                    "category": self._get_aqi_category(max(aqi_values)),
                    "hours_count": len(hours)
                }
                
                daily_summaries.append(daily_summary)
        
        return sorted(daily_summaries, key=lambda x: x["date"])

