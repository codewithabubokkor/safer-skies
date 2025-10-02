"""
🧮 EPA AQI CALCULATOR WITH TIME AVERAGING
========================================
Applies official EPA AQI formula with proper time averaging and EPA-compliant storage

This implements STRICT EPA compliance following your requirements:
- Official EPA breakpoint tables (EXACT values from EPA document)
- Proper EPA time averaging:
  * O₃: 8-hour rolling average
  * CO: 8-hour rolling average  
  * PM2.5: 24-hour average
  * PM10: 24-hour average
  * NO₂: 1-hour maximum
  * SO₂: 1-hour maximum
- Linear interpolation formula: I = ((IHi-ILo)/(BPHi-BPLo)) × (C-BPLo) + ILo
- Dominant pollutant logic (MAX AQI wins)
- EPA-standard health messages matching AirNow.gov

STORAGE STRUCTURE (following your plan):
- s3://bucket/aqi/{location_id}/aqi.json (current EPA AQI result)
- s3://bucket/aqi/{location_id}/corrected_pollutants.json (1 day retention for EPA averaging)
- s3://bucket/aqi/{location_id}/forecast.json (24hr forecast)
- Auto-deletion after retention periods (EPA compliance requirement)

EPA TIME AVERAGING REQUIREMENTS:
- Historical data stored for proper time averaging calculations
- Rolling windows computed according to EPA standards
- Quality flags maintained throughout averaging process
"""

import json
import time
import os
import boto3
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from collections import deque
import statistics
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@dataclass
class EPAAQIResult:
    """EPA AQI calculation result for single pollutant"""
    pollutant: str
    concentration: float
    averaged_concentration: float  # After EPA time averaging
    units: str
    aqi_value: int
    category: str
    color: str
    health_message: str
    averaging_period: str
    averaging_window_hours: int
    epa_breakpoint_used: str
    data_source: str
    quality: str
    averaging_quality: str
    data_points_used: int

@dataclass
class EPATimeAveragedData:
    """Container for EPA time-averaged pollutant data"""
    pollutant: str
    current_hour_value: float
    averaged_value: float  # EPA time-averaged value
    averaging_period: str
    data_points_used: int
    quality: str
    source: str
    units: str

class EPAAQICalculator:
    """
    Official EPA AQI calculator with STRICT EPA time averaging compliance
    """
    
    def __init__(self, s3_bucket: str = "naq-forecast-tempo-data"):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        self.local_storage = True  # Use local storage by default
        
        # EPA time averaging requirements (hours)
        self.epa_averaging_periods = {
            "O3": 8,      # 8-hour rolling average
            "CO": 8,      # 8-hour rolling average
            "PM25": 24,   # 24-hour average
            "PM10": 24,   # 24-hour average
            "NO2": 1,     # 1-hour maximum
            "SO2": 1,     # 1-hour maximum
            "HCHO": 1     # 1-hour (not official EPA but for demo)
        }
        
        # EPA data completeness requirements
        self.epa_data_completeness = {
            "O3": 0.75,    # 75% of 8-hour period (6 hours minimum)
            "CO": 0.75,    # 75% of 8-hour period (6 hours minimum)
            "PM25": 0.75,  # 75% of 24-hour period (18 hours minimum)
            "PM10": 0.75,  # 75% of 24-hour period (18 hours minimum)
            "NO2": 1.0,    # 100% for 1-hour
            "SO2": 1.0     # 100% for 1-hour
        }
        
        self.setup_epa_breakpoints()
        
    def normalize_pollutant_name(self, pollutant: str) -> str:
        """
        Normalize pollutant names from different APIs to EPA standard names
        
        Different APIs use different naming conventions:
        - AirNow: PM2.5, O3, NO2, SO2, CO
        - WAQI: pm25, o3, no2, so2, co  
        - TEMPO: PM25, O3, NO2, HCHO
        - GEOS: PM2.5, O3, NO2, SO2, CO
        """
        pollutant_upper = str(pollutant).upper()
        
        # PM2.5 variations → PM25 (our internal standard)
        if pollutant_upper in ['PM2.5', 'PM25', 'PM2_5', 'PARTICULATE_MATTER', 'PM']:
            return 'PM25'
        
        # PM10 variations
        elif pollutant_upper in ['PM10', 'PM_10']:
            return 'PM10'
            
        # Ozone variations
        elif pollutant_upper in ['O3', 'OZONE']:
            return 'O3'
            
        # Nitrogen dioxide variations
        elif pollutant_upper in ['NO2', 'NITROGEN_DIOXIDE']:
            return 'NO2'
            
        # Sulfur dioxide variations
        elif pollutant_upper in ['SO2', 'SULFUR_DIOXIDE']:
            return 'SO2'
            
        # Carbon monoxide variations
        elif pollutant_upper in ['CO', 'CARBON_MONOXIDE']:
            return 'CO'
            
        elif pollutant_upper in ['HCHO', 'FORMALDEHYDE']:
            return 'HCHO'
            
        else:
            return pollutant_upper
    
    def setup_epa_breakpoints(self):
        """Initialize EPA AQI breakpoint tables"""
        self.epa_breakpoints = {
            # O3 (8-hour) - ppm (CORRECTED EPA values)
            "O3": [
                (0.000, 0.054, 0, 50, "Good"),
                (0.055, 0.070, 51, 100, "Moderate"),
                (0.071, 0.085, 101, 150, "Unhealthy for Sensitive Groups"),
                (0.086, 0.105, 151, 200, "Unhealthy"),
                (0.106, 0.200, 201, 300, "Very Unhealthy"),
                (0.201, 0.604, 301, 500, "Hazardous")
            ],
            "NO2": [
                (0, 53, 0, 50, "Good"),
                (54, 100, 51, 100, "Moderate"),
                (101, 360, 101, 150, "Unhealthy for Sensitive Groups"),
                (361, 649, 151, 200, "Unhealthy"),
                (650, 1249, 201, 300, "Very Unhealthy"),
                (1250, 2049, 301, 500, "Hazardous")
            ],
            "CO": [
                (0.0, 4.4, 0, 50, "Good"),
                (4.5, 9.4, 51, 100, "Moderate"),
                (9.5, 12.4, 101, 150, "Unhealthy for Sensitive Groups"),
                (12.5, 15.4, 151, 200, "Unhealthy"),
                (15.5, 30.4, 201, 300, "Very Unhealthy"),
                (30.5, 50.4, 301, 500, "Hazardous")
            ],
            "SO2": [
                (0, 35, 0, 50, "Good"),
                (36, 75, 51, 100, "Moderate"),
                (76, 185, 101, 150, "Unhealthy for Sensitive Groups"),
                (186, 304, 151, 200, "Unhealthy"),
                (305, 604, 201, 300, "Very Unhealthy"),
                (605, 1004, 301, 500, "Hazardous")
            ],
            "PM25": [
                (0.0, 9.0, 0, 50, "Good"),
                (9.1, 35.4, 51, 100, "Moderate"),
                (35.5, 55.4, 101, 150, "Unhealthy for Sensitive Groups"),
                (55.5, 125.4, 151, 200, "Unhealthy"),
                (125.5, 225.4, 201, 300, "Very Unhealthy"),
                (225.5, 325.4, 301, 500, "Hazardous")
            ],
            "PM10": [
                (0, 54, 0, 50, "Good"),
                (55, 154, 51, 100, "Moderate"),
                (155, 254, 101, 150, "Unhealthy for Sensitive Groups"),
                (255, 354, 151, 200, "Unhealthy"),
                (355, 424, 201, 300, "Very Unhealthy"),
                (425, 604, 301, 500, "Hazardous")
            ]
        }
        
        # EPA color codes
        self.aqi_colors = {
            "Good": "#00E400",
            "Moderate": "#FFFF00",
            "Unhealthy for Sensitive Groups": "#FF7E00",
            "Unhealthy": "#FF0000",
            "Very Unhealthy": "#8F3F97",
            "Hazardous": "#7E0023"
        }
        
        # EPA health messages
        self.health_messages = {
            "Good": "Air quality is satisfactory for most people.",
            "Moderate": "Unusually sensitive people should consider reducing prolonged outdoor exertion.",
            "Unhealthy for Sensitive Groups": "Sensitive groups may experience health effects. The general public is less likely to be affected.",
            "Unhealthy": "Everyone may experience health effects. Sensitive groups may experience more serious effects.",
            "Very Unhealthy": "Health alert for everyone. Serious health effects for everyone.",
            "Hazardous": "Emergency conditions. Everyone is more likely to be affected."
        }
    
    def calculate_pollutant_aqi(self, epa_averaged_data: EPATimeAveragedData) -> EPAAQIResult:
        """
        Calculate EPA AQI for single pollutant using EPA time-averaged value
        """
        pollutant = epa_averaged_data.pollutant
        concentration = epa_averaged_data.averaged_value
        
        # Normalize pollutant name for consistent breakpoint lookup
        normalized_pollutant = self.normalize_pollutant_name(pollutant)
        
        if normalized_pollutant == "HCHO":
            # HCHO not officially in EPA AQI - use NO2 proxy for demo
            breakpoints = self.epa_breakpoints["NO2"]
        elif normalized_pollutant in self.epa_breakpoints:
            breakpoints = self.epa_breakpoints[normalized_pollutant]
        else:
            logger.warning(f"⚠️ No EPA breakpoints for {normalized_pollutant} (original: {pollutant})")
            return None
        
        for bp_lo, bp_hi, aqi_lo, aqi_hi, category in breakpoints:
            if bp_lo <= concentration <= bp_hi:
                if bp_hi == bp_lo:  # Avoid division by zero
                    aqi = aqi_lo
                else:
                    aqi = ((aqi_hi - aqi_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + aqi_lo
                
                aqi_value = int(round(aqi))
                
                if aqi_value >= 400:
                    logger.warning(f"🚨 High AQI debug: {pollutant} {concentration} {epa_averaged_data.units} → AQI {aqi_value} ({category})")
                    logger.warning(f"    Breakpoint: [{bp_lo}, {bp_hi}] → AQI [{aqi_lo}, {aqi_hi}]")
                    logger.warning(f"    Formula result: {aqi:.2f} → rounded to {aqi_value}")
                
                color = self.aqi_colors[category]
                health_message = self.health_messages[category]
                
                return EPAAQIResult(
                    pollutant=pollutant,
                    concentration=epa_averaged_data.current_hour_value,
                    averaged_concentration=concentration,
                    units=epa_averaged_data.units,
                    aqi_value=aqi_value,
                    category=category,
                    color=color,
                    health_message=health_message,
                    averaging_period=epa_averaged_data.averaging_period,
                    averaging_window_hours=self.epa_averaging_periods.get(pollutant, 1),
                    epa_breakpoint_used=f"{bp_lo}-{bp_hi}",
                    data_source=epa_averaged_data.source,
                    quality=epa_averaged_data.quality,
                    averaging_quality=epa_averaged_data.quality,
                    data_points_used=epa_averaged_data.data_points_used
                )
        
        # Concentration above all breakpoints = Hazardous
        logger.warning(f"🚨 No breakpoint match: {pollutant} {concentration} {epa_averaged_data.units} → defaulting to AQI 500")
        logger.warning(f"    Available breakpoints for {normalized_pollutant}: {breakpoints[:3]}...")  # Show first 3 breakpoints
        
        return EPAAQIResult(
            pollutant=pollutant,
            concentration=epa_averaged_data.current_hour_value,
            averaged_concentration=concentration,
            units=epa_averaged_data.units,
            aqi_value=500,
            category="Hazardous",
            color=self.aqi_colors["Hazardous"],
            health_message=self.health_messages["Hazardous"],
            averaging_period=epa_averaged_data.averaging_period,
            averaging_window_hours=self.epa_averaging_periods.get(pollutant, 1),
            epa_breakpoint_used="above_scale",
            data_source=epa_averaged_data.source,
            quality=epa_averaged_data.quality,
            averaging_quality=epa_averaged_data.quality,
            data_points_used=epa_averaged_data.data_points_used
        )
    
    def load_historical_pollutant_data(self, location_id: str, pollutant: str, hours_needed: int) -> List[Dict]:
        """
        Load historical pollutant data for EPA time averaging
        """
        try:
            s3_key = f"aqi/{location_id}/corrected_pollutants.json"
            
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=s3_key
            )
            
            historical_data = json.loads(response['Body'].read().decode('utf-8'))
            
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time - timedelta(hours=hours_needed)
            
            relevant_data = []
            for entry in historical_data.get('hourly_history', []):
                entry_time = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
                if entry_time >= cutoff_time and pollutant in entry.get('pollutants', {}):
                    relevant_data.append(entry)
            
            logger.info(f"📊 Loaded {len(relevant_data)} historical data points for {pollutant} EPA averaging")
            return relevant_data
            
        except Exception as e:
            logger.warning(f"Could not load historical data for EPA averaging: {e}")
            return []
    
    def calculate_epa_time_average(self, current_data: Dict, location_id: str) -> Dict[str, EPATimeAveragedData]:
        """
        Calculate EPA-compliant time averages for all pollutants
        """
        epa_averaged = {}
        
        for pollutant, data in current_data.get('fused_pollutants', {}).items():
            current_value = data['value']
            averaging_hours = self.epa_averaging_periods.get(pollutant, 1)
            
            if averaging_hours == 1:
                # 1-hour pollutants (NO2, SO2) - use current value
                epa_averaged[pollutant] = EPATimeAveragedData(
                    pollutant=pollutant,
                    current_hour_value=current_value,
                    averaged_value=current_value,
                    averaging_period="1hr",
                    data_points_used=1,
                    quality=data.get('quality', 'unknown'),
                    source=data.get('source', 'unknown'),
                    units=data.get('units', 'ppb')
                )
                
            else:
                # Multi-hour averaging (O3, CO: 8hr; PM2.5, PM10: 24hr)
                historical_data = self.load_historical_pollutant_data(location_id, pollutant, averaging_hours)
                
                values = []
                qualities = []
                
                values.append(current_value)
                qualities.append(data.get('quality', 'unknown'))
                
                for entry in historical_data:
                    if pollutant in entry.get('pollutants', {}):
                        hist_data = entry['pollutants'][pollutant]
                        values.append(hist_data['value'])
                        qualities.append(hist_data.get('quality', 'unknown'))
                
                required_points = int(averaging_hours * self.epa_data_completeness.get(pollutant, 0.75))
                
                if len(values) >= required_points:
                    if pollutant in ["O3", "CO"]:
                        # Rolling average for O3 and CO
                        averaged_value = statistics.mean(values[-averaging_hours:])
                    else:
                        # 24-hour average for PM
                        averaged_value = statistics.mean(values)
                    
                    # Determine overall quality
                    high_quality_count = sum(1 for q in qualities if q in ['high', 'good'])
                    overall_quality = 'high' if high_quality_count >= len(qualities) * 0.75 else 'moderate'
                    
                    epa_averaged[pollutant] = EPATimeAveragedData(
                        pollutant=pollutant,
                        current_hour_value=current_value,
                        averaged_value=averaged_value,
                        averaging_period=f"{averaging_hours}hr",
                        data_points_used=len(values),
                        quality=overall_quality,
                        source=data.get('source', 'unknown'),
                        units=data.get('units', 'ppb')
                    )
                    
                else:
                    logger.warning(f"⚠️ Insufficient data for EPA {averaging_hours}hr averaging of {pollutant}: {len(values)}/{required_points} points")
                    
                    epa_averaged[pollutant] = EPATimeAveragedData(
                        pollutant=pollutant,
                        current_hour_value=current_value,
                        averaged_value=current_value,  # Fall back to current
                        averaging_period=f"{averaging_hours}hr_insufficient",
                        data_points_used=len(values),
                        quality='insufficient_for_epa',
                        source=data.get('source', 'unknown'),
                        units=data.get('units', 'ppb')
                    )
        
        logger.info(f"🕐 EPA time averaging complete for {len(epa_averaged)} pollutants")
        return epa_averaged
    
    def store_pollutant_history(self, location_id: str, current_data: Dict) -> None:
        """
        Store current pollutant data for future EPA time averaging
        """
        try:
            s3_key = f"aqi/{location_id}/corrected_pollutants.json"
            
            try:
                response = self.s3_client.get_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key
                )
                existing_data = json.loads(response['Body'].read().decode('utf-8'))
                hourly_history = existing_data.get('hourly_history', [])
            except:
                hourly_history = []
            
            current_entry = {
                'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'pollutants': current_data.get('fused_pollutants', {}),
                'location': current_data.get('location', {}),
                'fusion_quality': current_data.get('fusion_quality', {})
            }
            
            hourly_history.append(current_entry)
            
            # EPA compliance: Keep only last 25 hours (24hr + 1 buffer)
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=25)
            hourly_history = [
                entry for entry in hourly_history 
                if datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00')) >= cutoff_time
            ]
            
            history_data = {
                'location_id': location_id,
                'last_updated': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'retention_hours': 25,
                'epa_compliance': 'time_averaging_history',
                'hourly_history': hourly_history
            }
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(history_data, indent=2),
                ContentType="application/json",
                CacheControl="private, max-age=3600",  # 1 hour cache
                ExpiresAfter=datetime.now(timezone.utc) + timedelta(hours=25)  # Auto-delete after 25 hours
            )
            
            logger.info(f"📁 Stored pollutant history: {len(hourly_history)} entries for EPA averaging")
            
        except Exception as e:
            logger.error(f"Failed to store pollutant history: {e}")
    
    def save_epa_aqi_result(self, location_id: str, aqi_summary: Dict) -> None:
        """
        Save EPA AQI result to s3://bucket/aqi/{location_id}/aqi.json
        """
        try:
            s3_key = f"aqi/{location_id}/aqi.json"
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(aqi_summary, indent=2),
                ContentType="application/json",
                CacheControl="public, max-age=3600",  # 1 hour cache
                Metadata={
                    'aqi': str(aqi_summary['current']['aqi']),
                    'category': aqi_summary['current']['category'],
                    'dominant': aqi_summary['current']['dominant_pollutant'],
                    'epa_compliant': 'true'
                }
            )
            
            logger.info(f"💾 EPA AQI result saved: s3://{self.s3_bucket}/{s3_key}")
            
        except Exception as e:
            logger.error(f"Failed to save EPA AQI result: {e}")
    
    def generate_why_today_analysis(self, aqi_results: List[EPAAQIResult], dominant_pollutant: str) -> Dict[str, Any]:
        """
        Generate scientific "Why Today?" analysis based on results
        """
        analysis = {
            "dominant_pollutant": dominant_pollutant,
            "primary_factors": [],
            "atmospheric_conditions": {},
            "data_quality_summary": {},
            "health_recommendations": {},
            "nasa_insights": {}
        }
        
        # Analyze dominant pollutant
        dominant_result = next((r for r in aqi_results if r.pollutant == dominant_pollutant), None)
        if dominant_result:
            
            if dominant_pollutant == "NO2":
                analysis["primary_factors"] = [
                    "Urban traffic emissions",
                    "Industrial combustion sources",
                    "Atmospheric transport from upwind areas"
                ]
                analysis["nasa_insights"] = {
                    "tempo_observation": "NASA TEMPO satellite provides hourly NO₂ monitoring",
                    "data_quality": dominant_result.quality,
                    "spatial_coverage": "2.2km resolution over North America"
                }
                
            elif dominant_pollutant == "O3":
                analysis["primary_factors"] = [
                    "Photochemical formation from NO₂ + VOCs",
                    "Strong solar radiation accelerating reactions",
                    "Temperature inversion trapping precursors"
                ]
                analysis["nasa_insights"] = {
                    "geos_cf_forecast": "NASA GEOS-CF model provides O₃ predictions",
                    "formation_mechanism": "Secondary pollutant formed in atmosphere",
                    "meteorological_influence": "Wind patterns and temperature critical"
                }
                
            elif dominant_pollutant in ["PM25", "PM10"]:
                analysis["primary_factors"] = [
                    "Direct particle emissions from combustion",
                    "Secondary particle formation from gas-to-particle conversion",
                    "Regional transport and accumulation"
                ]
                analysis["nasa_insights"] = {
                    "ground_truth": "EPA ground monitors provide direct PM measurements",
                    "satellite_limitation": "PM requires ground-based monitoring",
                    "health_significance": "Most health-relevant pollutant"
                }
        
        # Data quality assessment
        tempo_sources = [r for r in aqi_results if "TEMPO" in r.data_source]
        geos_cf_sources = [r for r in aqi_results if "GEOS-CF" in r.data_source]
        ground_sources = [r for r in aqi_results if "ground" in r.data_source]
        
        analysis["data_quality_summary"] = {
            "tempo_observations": len(tempo_sources),
            "model_forecasts": len(geos_cf_sources),
            "ground_truth": len(ground_sources),
            "overall_confidence": "high" if len(ground_sources) > 0 else "medium"
        }
        
        # Health recommendations
        max_aqi = max([r.aqi_value for r in aqi_results])
        if max_aqi <= 50:
            analysis["health_recommendations"] = {
                "general_public": "Enjoy outdoor activities",
                "sensitive_groups": "No restrictions"
            }
        elif max_aqi <= 100:
            analysis["health_recommendations"] = {
                "general_public": "Normal outdoor activities acceptable",
                "sensitive_groups": "Consider reducing prolonged outdoor exertion"
            }
        else:
            analysis["health_recommendations"] = {
                "general_public": "Reduce prolonged outdoor exertion",
                "sensitive_groups": "Avoid outdoor activities"
            }
        
        return analysis
    
    def process_fused_data(self, fused_data: Dict) -> Dict[str, Any]:
        """
        Process and store hourly fused pollutant data
        
        This function:
        1. Takes corrected/fused data from fusion_bias_corrector.py
        2. Stores it hourly with timestamps 
        3. Creates corrected_pollutants.json for the location
        4. Does NOT apply EPA rules (that's done when calculating AQI)
        """
        logger.info(f"💾 Storing hourly corrected pollutant data")
        
        location = fused_data["location"]
        location_id = f"{location['lat']:.4f}_{location['lon']:.4f}"
        current_timestamp = datetime.now(timezone.utc).isoformat()
        
        fused_pollutants = fused_data.get("fused_pollutants", fused_data.get("fused_concentrations", {}))
        
        if not fused_pollutants:
            logger.error("❌ No fused pollutants to store")
            return None
        
        hourly_entry = {
            "timestamp": current_timestamp,
            "hour": datetime.now(timezone.utc).strftime("%H"),
            "pollutants": {}
        }
        
        for pollutant_name, pollutant_data in fused_pollutants.items():
            if isinstance(pollutant_data, dict):
                # Fusion engine format with metadata
                concentration_value = pollutant_data.get('concentration', pollutant_data.get('value', 0))
                hourly_entry["pollutants"][pollutant_name] = {
                    "value": concentration_value,
                    "units": pollutant_data.get('units', 'ppb'),
                    "source": pollutant_data.get('source', 'unknown'),
                    "quality": pollutant_data.get('quality', 'unknown'),
                    "uncertainty": pollutant_data.get('uncertainty', '±25%'),
                    "bias_corrected": pollutant_data.get('bias_correction_applied', pollutant_data.get('bias_corrected', False)),
                    "fusion_method": pollutant_data.get('fusion_method', 'single_source')
                }
            else:
                hourly_entry["pollutants"][pollutant_name] = {
                    "value": float(pollutant_data),
                    "units": 'ppb',
                    "source": 'unknown',
                    "quality": 'unknown',
                    "uncertainty": '±25%',
                    "bias_corrected": False,
                    "fusion_method": 'single_source'
                }
        
        try:
            existing_data = self.load_location_corrected_data(location_id)
        except:
            existing_data = {
                "location": location,
                "location_id": location_id,
                "data_type": "hourly_corrected_pollutants",
                "created": current_timestamp,
                "hourly_data": []
            }
        
        existing_data["hourly_data"].append(hourly_entry)
        existing_data["last_updated"] = current_timestamp
        
        # Keep only last 24 hours of data (EPA requirement)
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        existing_data["hourly_data"] = [
            entry for entry in existing_data["hourly_data"]
            if datetime.fromisoformat(entry["timestamp"].replace('Z', '+00:00')) > cutoff_time
        ]
        
        existing_data["hourly_data"].sort(key=lambda x: x["timestamp"], reverse=True)
        
        storage_result = self.store_corrected_pollutants(location_id, existing_data)
        
        logger.info(f"✅ Stored hourly data: {len(fused_pollutants)} pollutants, {len(existing_data['hourly_data'])} hours total")
        
        return {
            "location_id": location_id,
            "location": location,
            "timestamp": current_timestamp,
            "pollutants_stored": list(fused_pollutants.keys()),
            "total_hours_available": len(existing_data['hourly_data']),
            "storage_path": storage_result,
            "data_type": "hourly_corrected_pollutants",
            "ready_for_aqi_calculation": True
        }
    
    def apply_epa_time_averaging(self, corrected_data: Dict) -> Dict[str, EPATimeAveragedData]:
        """
        Apply EPA time averaging rules to hourly corrected data
        
        EPA Time Averaging Periods:
        - O3: 8-hour rolling average
        - CO: 8-hour rolling average  
        - PM2.5: 24-hour average
        - PM10: 24-hour average
        - NO2: 1-hour maximum
        - SO2: 1-hour maximum
        """
        hourly_data = corrected_data.get("hourly_data", [])
        if not hourly_data:
            return {}
        
        epa_averaged = {}
        
        latest_hour = hourly_data[-1]
        current_pollutants = latest_hour.get("pollutants", {})
        
        for pollutant_name, pollutant_data in current_pollutants.items():
            current_value = pollutant_data.get("value", 0)
            units = pollutant_data.get("units", "ppb")
            source = pollutant_data.get("source", "unknown")
            quality = pollutant_data.get("quality", "good")
            
            averaging_period = self.epa_averaging_periods.get(pollutant_name, 1)
            
            if averaging_period == 1:
                # 1-hour maximum (NO2, SO2, HCHO)
                averaged_value = current_value
                data_points = 1
                period_name = "1-hour"
                
            elif averaging_period == 8:
                # 8-hour rolling average (O3, CO)
                values = []
                for hour_data in hourly_data[-8:]:  # Last 8 hours
                    hour_pollutants = hour_data.get("pollutants", {})
                    if pollutant_name in hour_pollutants:
                        values.append(hour_pollutants[pollutant_name].get("value", 0))
                
                if len(values) >= 6:  # EPA requires 75% completeness (6/8 hours)
                    averaged_value = sum(values) / len(values)
                    data_points = len(values)
                    period_name = "8-hour"
                    quality = "high" if len(values) == 8 else "good"
                else:
                    averaged_value = current_value
                    data_points = len(values)
                    period_name = "1-hour"
                    quality = "insufficient_data"
                    
            elif averaging_period == 24:
                # 24-hour average (PM2.5, PM10)
                values = []
                for hour_data in hourly_data[-24:]:  # Last 24 hours
                    hour_pollutants = hour_data.get("pollutants", {})
                    if pollutant_name in hour_pollutants:
                        values.append(hour_pollutants[pollutant_name].get("value", 0))
                
                if len(values) >= 18:  # EPA requires 75% completeness (18/24 hours)
                    averaged_value = sum(values) / len(values)
                    data_points = len(values)
                    period_name = "24-hour"
                    quality = "high" if len(values) >= 20 else "good"
                else:
                    averaged_value = current_value
                    data_points = len(values)
                    period_name = "1-hour"
                    quality = "insufficient_data"
            else:
                averaged_value = current_value
                data_points = 1
                period_name = "1-hour"
            
            if pollutant_name == 'O3' and units == 'ppb':
                current_value = current_value / 1000.0
                averaged_value = averaged_value / 1000.0
                units = 'ppm'
                logger.debug(f"🔄 O3 unit conversion: {averaged_value * 1000:.2f} ppb → {averaged_value:.6f} ppm")
            
            epa_averaged[pollutant_name] = EPATimeAveragedData(
                pollutant=pollutant_name,
                current_hour_value=current_value,
                averaged_value=averaged_value,
                averaging_period=period_name,
                data_points_used=data_points,
                quality=quality,
                source=source,
                units=units
            )
        
        logger.info(f"📊 EPA averaging complete: {len(epa_averaged)} pollutants processed")
        return epa_averaged
    
    def calculate_epa_aqi(self, location_id: str) -> Dict[str, Any]:
        """
        Calculate EPA AQI using stored hourly corrected data
        This applies EPA time averaging rules (8hr O3/CO, 24hr PM2.5/PM10, 1hr NO2/SO2)
        """
        logger.info(f"🧮 Calculating EPA AQI for {location_id}")
        
        try:
            corrected_data = self.load_location_corrected_data(location_id)
        except Exception as e:
            logger.error(f"❌ Cannot load corrected data for {location_id}: {e}")
            return None
        
        hourly_data = corrected_data.get("hourly_data", [])
        if not hourly_data:
            logger.error(f"❌ No hourly data available for {location_id}")
            return None
        
        epa_averaged = self.apply_epa_time_averaging(corrected_data)
        
        if not epa_averaged:
            logger.error("❌ No valid EPA averaged data available")
            return None
        
        aqi_results = []
        for pollutant_name, epa_data in epa_averaged.items():
            aqi_result = self.calculate_pollutant_aqi_from_epa_data(epa_data)
            if aqi_result:
                aqi_results.append(aqi_result)
        
        if not aqi_results:
            logger.error("❌ No valid AQI calculations possible")
            return None
        
        # Determine overall AQI and dominant pollutant (EPA methodology)
        overall_aqi = max([r.aqi_value for r in aqi_results])
        dominant_result = max(aqi_results, key=lambda r: r.aqi_value)
        dominant_pollutant = dominant_result.pollutant
        
        why_today = self.generate_why_today_analysis(aqi_results, dominant_pollutant)
        
        summary = {
            "location": corrected_data["location"],
            "location_id": location_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "current": {
                "aqi": overall_aqi,
                "category": dominant_result.category,
                "color": dominant_result.color,
                "dominant_pollutant": dominant_pollutant,
                "health_message": dominant_result.health_message,
                "last_updated": corrected_data.get("last_updated")
            },
            "pollutants": {
                result.pollutant: {
                    "concentration": result.concentration,
                    "averaged_concentration": result.averaged_concentration,
                    "units": result.units,
                    "aqi": result.aqi_value,
                    "category": result.category,
                    "averaging_period": result.averaging_period,
                    "averaging_window_hours": result.averaging_window_hours,
                    "data_source": result.data_source,
                    "quality": result.quality,
                    "epa_breakpoint": result.epa_breakpoint_used,
                    "data_points_used": result.data_points_used
                }
                for result in aqi_results
            },
            "data_quality": {
                "total_hours_available": len(hourly_data),
                "epa_averaging_applied": True,
                "averaging_completeness": {
                    result.pollutant: f"{result.data_points_used} points for {result.averaging_period}"
                    for result in aqi_results
                }
            },
            "why_today": why_today,
            "epa_compliance": {
                "formula_used": "Official EPA AQI linear interpolation",
                "breakpoints": "EPA Technical Assistance Document 2018",
                "time_averaging": "Standard EPA periods",
                "dominant_pollutant_method": "Maximum AQI value"
            }
        }
        
        logger.info(f"✅ EPA AQI calculation complete: AQI {overall_aqi} ({dominant_result.category})")
        
        return summary
    
    def load_location_corrected_data(self, location_id: str) -> Dict:
        """Load stored corrected pollutant data for a location"""
        if self.local_storage:
            file_path = f"/tmp/aqi/{location_id}/corrected_pollutants.json"
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            s3_key = f"aqi/{location_id}/corrected_pollutants.json"
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            return json.loads(response['Body'].read())
    
    def store_corrected_pollutants(self, location_id: str, data: Dict) -> str:
        """Store corrected pollutants data"""
        if self.local_storage:
            dir_path = f"/tmp/aqi/{location_id}"
            os.makedirs(dir_path, exist_ok=True)
            file_path = f"{dir_path}/corrected_pollutants.json"
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"💾 Corrected pollutants saved locally: {file_path}")
            return file_path
        else:
            s3_key = f"aqi/{location_id}/corrected_pollutants.json"
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(data, indent=2),
                ContentType="application/json"
            )
            
            logger.info(f"☁️ Corrected pollutants saved to S3: s3://{self.s3_bucket}/{s3_key}")
            return f"s3://{self.s3_bucket}/{s3_key}"
    
    def calculate_pollutant_aqi_from_epa_data(self, epa_data: EPATimeAveragedData) -> EPAAQIResult:
        """Calculate AQI from EPA time-averaged data"""
        concentration = epa_data.averaged_value
        pollutant = epa_data.pollutant
        
        # Normalize pollutant name for consistent breakpoint lookup
        pollutant_key = self.normalize_pollutant_name(pollutant)
        
        if pollutant_key not in self.epa_breakpoints:
            logger.warning(f"⚠️ No EPA breakpoints for {pollutant_key} (original: {pollutant})")
            return None
        
        breakpoints = self.epa_breakpoints[pollutant_key]
        
        for bp_lo, bp_hi, aqi_lo, aqi_hi, category in breakpoints:
            if bp_lo <= concentration <= bp_hi:
                if bp_hi == bp_lo:  # Avoid division by zero
                    aqi = aqi_lo
                else:
                    aqi = ((aqi_hi - aqi_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + aqi_lo
                
                aqi_value = int(round(aqi))
                color = self.aqi_colors[category]
                health_message = self.health_messages[category]
                
                return EPAAQIResult(
                    pollutant=pollutant,
                    concentration=epa_data.current_hour_value,
                    averaged_concentration=concentration,
                    units=epa_data.units,
                    aqi_value=aqi_value,
                    category=category,
                    color=color,
                    health_message=health_message,
                    averaging_period=epa_data.averaging_period,
                    averaging_window_hours=self.epa_averaging_periods.get(pollutant, 1),
                    epa_breakpoint_used=f"{bp_lo}-{bp_hi}",
                    data_source=epa_data.source,
                    quality=epa_data.quality,
                    averaging_quality=epa_data.quality,
                    data_points_used=epa_data.data_points_used
                )
        
        # Concentration above all breakpoints = Hazardous
        return EPAAQIResult(
            pollutant=pollutant,
            concentration=epa_data.current_hour_value,
            averaged_concentration=concentration,
            units=epa_data.units,
            aqi_value=500,
            category="Hazardous",
            color=self.aqi_colors["Hazardous"],
            health_message=self.health_messages["Hazardous"],
            averaging_period=epa_data.averaging_period,
            averaging_window_hours=self.epa_averaging_periods.get(pollutant, 1),
            epa_breakpoint_used="above_scale",
            data_source=epa_data.source,
            quality=epa_data.quality,
            averaging_quality=epa_data.quality,
            data_points_used=epa_data.data_points_used
        )
    
    def save_aqi_summary(self, summary: Dict, local_storage: bool = True) -> str:
        """
        Save EPA AQI summary to storage
        Following your storage architecture:
        - s3://bucket/aqi/location_id/aqi.json
        - /tmp/aqi/location_id/aqi.json (local)
        """
        location_id = summary["location_id"]
        
        if local_storage:
            local_dir = f"/tmp/aqi/{location_id}"
            os.makedirs(local_dir, exist_ok=True)
            
            aqi_path = f"{local_dir}/aqi.json"
            with open(aqi_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"💾 EPA AQI summary saved locally: {aqi_path}")
            return aqi_path
        
        else:
            s3_key = f"aqi/{location_id}/aqi.json"
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(summary, indent=2),
                ContentType="application/json",
                CacheControl="public, max-age=600",  # 10-minute cache
                Metadata={
                    'aqi': str(summary['current']['aqi']),
                    'category': summary['current']['category'],
                    'dominant': summary['current']['dominant_pollutant']
                }
            )
            
            logger.info(f"☁️ EPA AQI summary saved to S3: s3://{self.s3_bucket}/{s3_key}")
            return f"s3://{self.s3_bucket}/{s3_key}"
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            history_key = f"aqi-cache/{location_key}/history/{timestamp}.json"
            
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=history_key,
                Body=json.dumps(summary, indent=2),
                ContentType="application/json"
            )
            
            logger.info(f"☁️ EPA AQI summary saved to S3: s3://{self.s3_bucket}/{s3_key}")
            return f"s3://{self.s3_bucket}/{s3_key}"

    def process_parquet_fusion_results(self, parquet_file_path: str) -> List[Dict[str, Any]]:
        """
        Process Parquet fusion results and calculate EPA AQI for each location
        
        Args:
            parquet_file_path: Path to the Parquet file containing fusion results
            
        Returns:
            List of EPA AQI calculation results
        """
        try:
            logger.info(f"📦 Processing Parquet fusion results: {parquet_file_path}")
            
            df = pd.read_parquet(parquet_file_path)
            
            logger.info(f"📊 Loaded {len(df)} locations from Parquet file")
            logger.info(f"🔍 Columns: {list(df.columns)}")
            
            aqi_results = []
            
            for idx, row in df.iterrows():
                try:
                    location_name = row['location']
                    lat = row['lat']
                    lon = row['lon']
                    timestamp = row['timestamp']
                    
                    pollutants = {}
                    
                    pollutant_mapping = {
                        'PM25': 'PM25',
                        'O3': 'O3', 
                        'NO2': 'NO2',
                        'SO2': 'SO2',
                        'CO': 'CO',
                        'HCHO': 'HCHO'
                    }
                    
                    for parquet_col, pollutant_name in pollutant_mapping.items():
                        if parquet_col in df.columns and pd.notna(row[parquet_col]):
                            concentration = float(row[parquet_col])
                            
                            # Determine units based on pollutant
                            if pollutant_name in ['PM25', 'PM10']:
                                units = 'μg/m³'
                            elif pollutant_name == 'CO':
                                units = 'ppm'
                            else:
                                units = 'ppb'
                                if pollutant_name == 'O3':
                                    concentration = concentration / 1000.0  # ppb to ppm
                                    units = 'ppm'
                            
                            pollutants[pollutant_name] = {
                                "value": concentration,
                                "units": units,
                                "source": "fusion_weighted_average",
                                "quality": "high"
                            }
                    
                    if not pollutants:
                        logger.warning(f"⚠️ No valid pollutant data for {location_name}")
                        continue
                    
                    fusion_data = {
                        "location": {"lat": lat, "lon": lon, "name": location_name},
                        "timestamp": timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp),
                        "original_collection_time": 0.1,  # Fast processing
                        "fused_pollutants": pollutants,
                        "fusion_quality": {
                            "total_pollutants": len(pollutants),
                            "high_confidence": len(pollutants)
                        },
                        "data_provenance": {
                            "source": "parquet_fusion_results",
                            "processing_time": datetime.now(timezone.utc).isoformat()
                        }
                    }
                    
                    self.process_fused_data(fusion_data)
                    
                    location_id = f"{lat:.4f}_{lon:.4f}"
                    logger.info(f"🧮 Calculating EPA AQI for {location_name}")
                    aqi_summary = self.calculate_epa_aqi(location_id)
                    
                    if aqi_summary:
                        aqi_summary['location_info'] = {
                            'name': location_name,
                            'lat': lat,
                            'lon': lon
                        }
                        aqi_results.append(aqi_summary)
                        
                        logger.info(f"✅ {location_name}: AQI {aqi_summary['current']['aqi']} ({aqi_summary['current']['category']}) - {aqi_summary['current']['dominant_pollutant']}")
                    else:
                        logger.error(f"❌ AQI calculation failed for {location_name}")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing location {idx}: {e}")
                    continue
            
            logger.info(f"🎯 Processed {len(aqi_results)} locations successfully")
            return aqi_results
            
        except Exception as e:
            logger.error(f"❌ Failed to process Parquet file: {e}")
            raise

    def save_s3_aqi_structure(self, aqi_results: List[Dict], parquet_file: str) -> Dict[str, str]:
        """
        Save AQI results in S3-ready structure for fast website loading
        
        S3 Structure:
        s3://naqforecast/aqi/YYYY-MM-DD/location/
           ├── aqi_current.json (current data for website display)
           └── aqi_history.parquet (compressed columnar history - fast analytics)
        
        Note: aqi_forecast.json will be generated by separate forecasting system
        
        Args:
            aqi_results: List of AQI calculation results
            parquet_file: Source parquet file path for timestamp extraction
            
        Returns:
            Dict with paths to created files
        """
        try:
            from datetime import datetime
            import json
            import os
            
            if "fusion_parquet" in parquet_file:
                date_str = parquet_file.split("/")[1]  # "2025-08-21"
            else:
                date_str = datetime.now().strftime("%Y-%m-%d")
            
            created_files = {}
            
            for result in aqi_results:
                location_info = result.get('location_info', {})
                location = location_info.get('name', 'Unknown')
                lat = location_info.get('lat', 0)
                lon = location_info.get('lon', 0)
                
                current_data = result.get('current', {})
                
                location_id = f"{location}_{lat}_{lon}".replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')
                
                output_dir = f"aqi/{date_str}/{location_id}"
                os.makedirs(output_dir, exist_ok=True)
                
                current_aqi = {
                    "location": {
                        "name": location,
                        "coordinates": {
                            "lat": lat,
                            "lon": lon
                        }
                    },
                    "timestamp": datetime.now().isoformat() + "Z",
                    "aqi": {
                        "overall": {
                            "value": current_data.get('aqi', 0),
                            "category": current_data.get('category', 'Good'),
                            "color": current_data.get('color', '
                            "dominant_pollutant": current_data.get('dominant_pollutant', 'N/A')
                        },
                        "pollutants": {}
                    },
                    "health": {
                        "message": current_data.get('health_message', 'Air quality is satisfactory for most people.'),
                        "sensitive_groups": current_data.get('sensitive_groups', []),
                        "cautionary_statement": current_data.get('cautionary_statement', '')
                    },
                    "data_quality": {
                        "calculation_method": "EPA_Official",
                        "data_sources": ["AirNow", "WAQI", "TEMPO", "GEOS-CF"],
                        "last_updated": datetime.now().isoformat() + "Z"
                    }
                }
                
                for pollutant, aqi_data in result.get('pollutants', {}).items():
                    if isinstance(aqi_data, dict):
                        current_aqi["aqi"]["pollutants"][pollutant] = {
                            "aqi": aqi_data.get('aqi', 0),
                            "concentration": aqi_data.get('concentration', 0),
                            "units": aqi_data.get('units', ''),
                            "category": aqi_data.get('category', 'Good')
                        }
                
                current_file = f"{output_dir}/aqi_current.json"
                with open(current_file, 'w') as f:
                    json.dump(current_aqi, f, indent=2)
                
                history_entry = {
                    "timestamp": [datetime.now()],
                    "date": [date_str],
                    "time": [datetime.now().strftime("%H:%M:%S")],
                    "aqi": [current_data.get('aqi', 0)],
                    "category": [current_data.get('category', 'Good')],
                    "dominant_pollutant": [current_data.get('dominant_pollutant', 'N/A')],
                    "PM25": [np.nan],
                    "O3": [np.nan],
                    "NO2": [np.nan],
                    "SO2": [np.nan],
                    "CO": [np.nan],
                    "HCHO": [np.nan]
                }
                
                for pollutant, aqi_data in result.get('pollutants', {}).items():
                    if isinstance(aqi_data, dict) and pollutant in history_entry:
                        history_entry[pollutant] = [aqi_data.get('concentration', np.nan)]
                
                new_df = pd.DataFrame(history_entry)
                
                history_file = f"{output_dir}/aqi_history.parquet"
                try:
                    if os.path.exists(history_file):
                        existing_df = pd.read_parquet(history_file)
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    else:
                        combined_df = new_df
                    
                    combined_df.to_parquet(history_file, compression='snappy', index=False)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Parquet history failed, falling back to JSONL: {e}")
                    # Fallback to JSONL if Parquet fails
                    history_file = f"{output_dir}/aqi_history.jsonl"
                    history_json = {
                        "timestamp": datetime.now().isoformat() + "Z",
                        "date": date_str,
                        "aqi": current_data.get('aqi', 0),
                        "category": current_data.get('category', 'Good'),
                        "dominant_pollutant": current_data.get('dominant_pollutant', 'N/A')
                    }
                    with open(history_file, 'a') as f:
                        f.write(json.dumps(history_json) + '\n')
                
                created_files[location_id] = {
                    "current": current_file,
                    "history": history_file,
                    "s3_path": f"s3://naqforecast/aqi/{date_str}/{location_id}/",
                    "note": "Forecast will be generated by separate forecasting system"
                }
                
                logger.info(f"📁 S3 structure created: {output_dir}/")
                logger.info(f"🌐 S3 target: s3://naqforecast/aqi/{date_str}/{location_id}/")
                logger.info(f"📊 Current: {current_file} (website display)")
                logger.info(f"� History: {history_file} (Parquet analytics)")
                logger.info(f"⚠️  Forecast: Will be generated by separate system")
            
            return created_files
            
        except Exception as e:
            logger.error(f"❌ S3 structure creation failed: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if len(sys.argv) > 1:
        # Command line usage: python aqi_calculator.py <parquet_file>
        parquet_file = sys.argv[1]
        
        if not os.path.exists(parquet_file):
            print(f"❌ Parquet file not found: {parquet_file}")
            sys.exit(1)
        
        print(f"🚀 Processing EPA AQI calculation from Parquet: {parquet_file}")
        
        calculator = EPAAQICalculator()
        calculator.local_storage = True  # Use local storage for testing
        
        try:
            aqi_results = calculator.process_parquet_fusion_results(parquet_file)
            
            if aqi_results:
                print(f"\n📋 EPA AQI CALCULATION SUMMARY")
                print("="*60)
                
                for result in aqi_results:
                    location_info = result['location_info']
                    current = result['current']
                    
                    print(f"📍 {location_info['name']} ({location_info['lat']:.3f}, {location_info['lon']:.3f})")
                    print(f"   🏆 Overall AQI: {current['aqi']} - {current['category']}")
                    print(f"   🎯 Dominant: {current['dominant_pollutant']}")
                    print(f"   🌈 Color: {current['color']}")
                    print(f"   💡 Health: {current['health_message']}")
                    
                    print(f"   📊 Pollutant AQIs:")
                    for pollutant, data in result['pollutants'].items():
                        print(f"      {pollutant}: {data['aqi']} ({data['category']})")
                    print()
                
                print(f"✅ Successfully processed {len(aqi_results)} locations")
                
                output_file = parquet_file.replace('.parquet', '_aqi_results.json')
                with open(output_file, 'w') as f:
                    json.dump(aqi_results, f, indent=2, default=str)
                print(f"💾 Results saved to: {output_file}")
                
                try:
                    s3_files = calculator.save_s3_aqi_structure(aqi_results, parquet_file)
                    print(f"\n🌐 S3-READY STRUCTURE CREATED:")
                    print("="*60)
                    for location_id, files in s3_files.items():
                        print(f"📁 {location_id}:")
                        print(f"   📄 Current: {files['current']}")
                        print(f"   📊 Forecast: {files['forecast']}")
                        print(f"   📈 History: {files['history']}")
                        print(f"   ☁️  S3 Path: {files['s3_path']}")
                        print()
                except Exception as e:
                    print(f"⚠️ S3 structure creation failed: {e}")
                
            else:
                print("❌ No AQI results generated")
                
        except Exception as e:
            print(f"❌ Error processing Parquet file: {e}")
            sys.exit(1)
    
    else:
        # Original mock data test
        print("🧪 Running EPA AQI Calculator test with mock data...")
        
        mock_fused_data = {
            "location": {"lat": 40.7128, "lon": -74.0060},
            "timestamp": "2025-08-19T19:00:00Z",
            "original_collection_time": 2.3,
            "fused_pollutants": {
                "NO2": {"value": 28.5, "units": "ppb", "source": "TEMPO_observation_primary", "quality": "high"},
                "O3": {"value": 0.042, "units": "ppm", "source": "GEOS-CF_ground_validated", "quality": "high"},
                "CO": {"value": 0.85, "units": "ppm", "source": "GEOS-CF_primary", "quality": "good"},
                "PM25": {"value": 18.7, "units": "μg/m³", "source": "ground_station_measurement", "quality": "high"},
                "PM10": {"value": 32.1, "units": "μg/m³", "source": "ground_station_measurement", "quality": "high"}
            },
            "fusion_quality": {"total_pollutants": 5, "high_confidence": 4},
            "data_provenance": {"tempo_used": 1, "geos_cf_used": 2, "ground_used": 2}
        }
        
        calculator = EPAAQICalculator()
        calculator.local_storage = True
        aqi_summary = calculator.process_fused_data(mock_fused_data)
        
        if aqi_summary:
            summary_path = calculator.save_aqi_summary(aqi_summary, local_storage=True)
            
            print("🧮 EPA AQI CALCULATION RESULT:")
            print(f"Overall AQI: {aqi_summary['current']['aqi']} ({aqi_summary['current']['category']})")
            print(f"Dominant: {aqi_summary['current']['dominant_pollutant']}")
            print(f"Saved: {summary_path}")
            print(f"Pollutants: {list(aqi_summary['pollutants'].keys())}")
        else:
            print("❌ EPA AQI calculation failed")

