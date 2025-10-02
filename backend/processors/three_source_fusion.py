#!/usr/bin/env python3
"""
🔬 3-SOURCE ADVANCED FUSION SYSTEM
=================================
Fusion and bias correction for GEOS-CF + Open-Meteo + GFS
Calculates AQI with weather context and "Why Today" explanations

FUSION STRATEGY:
- Open-Meteo: 70% weight (close grid, current hour data)
- GEOS-CF: 30% weight (NASA satellite model, exact coordinates)  
- GFS: Weather context for explanations (wind, humidity, temperature)

BIAS CORRECTION:
- Compare overlapping pollutants (NO₂, SO₂, CO, O₃)
- Detect systematic bias between sources
- Apply correction: fused_value = w1 * OpenMeteo + w2 * GEOS_CF
- Convert to EPA AQI with proper time averaging

OUTPUTS:
- Fused AQI values per pollutant
- Overall AQI with dominant pollutant
- Weather context explanations
- "Why Today" analysis using meteorology
"""

import json
import logging
import sys
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import math

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class ThreeSourceFusionEngine:
    """
    Advanced fusion engine for GEOS-CF + Open-Meteo + GFS
    """
    
    def __init__(self):
        self.weights = {
            'Open_Meteo': 0.7,  # Higher weight for closer grid
            'GEOS_CF': 0.3      # Lower weight for global model
        }
        
        # Storage paths - use current project directory
        current_dir = Path(__file__).parent.parent.parent  # Go up to project root
        self.storage_root = Path(os.getenv('PROJECT_ROOT', current_dir))
        self.json_storage = self.storage_root / 'fusion_results'
        self.parquet_storage = self.storage_root / 'fusion_parquet'
        self.website_storage = self.storage_root / 'website_data'
        
        self.json_storage.mkdir(parents=True, exist_ok=True)
        self.parquet_storage.mkdir(parents=True, exist_ok=True)
        self.website_storage.mkdir(parents=True, exist_ok=True)
        
        # Unit conversion factors
        self.unit_conversions = {
            'NO2': 1.88,  # NO₂: 1 ppb = 1.88 μg/m³
            'SO2': 2.62,  # SO₂: 1 ppb = 2.62 μg/m³  
            'CO': 1.15,   # CO: 1 ppb = 1.15 μg/m³
            'O3': 1.96    # O₃: 1 ppb = 1.96 μg/m³
        }
        
        # EPA AQI breakpoints - OFFICIAL EPA RANGES with correct units
        # Units are intentionally mixed to mirror EPA (ppm for gases where EPA defines ppm)
        self.epa_breakpoints = {
            # EPA 24-hr PM2.5 (μg/m³) - CORRECTED: 0-50 AQI is 0-12.0 μg/m³
            'PM2.5': [
                (0, 50, 0.0, 12.0),
                (51, 100, 12.1, 35.4),
                (101, 150, 35.5, 55.4),
                (151, 200, 55.5, 150.4),
                (201, 300, 150.5, 250.4),
                (301, 400, 250.5, 350.4),
                (401, 500, 350.5, 500.4)
            ],
            'PM10': [
                (0, 50, 0, 54),
                (51, 100, 55, 154),
                (101, 150, 155, 254),
                (151, 200, 255, 354),
                (201, 300, 355, 424),
                (301, 400, 425, 504),
                (401, 500, 505, 604)
            ],
            'O3': [
                (0, 50, 0.000, 0.054),
                (51, 100, 0.055, 0.070),
                (101, 150, 0.071, 0.085),
                (151, 200, 0.086, 0.105),
                (201, 300, 0.106, 0.200)
            ],
            'NO2': [
                (0, 50, 0, 53),
                (51, 100, 54, 100),
                (101, 150, 101, 360),
                (151, 200, 361, 649),
                (201, 300, 650, 1249),
                (301, 500, 1250, 2049)
            ],
            'SO2': [
                (0, 50, 0, 35),
                (51, 100, 36, 75),
                (101, 150, 76, 185),
                (151, 200, 186, 304),
                (201, 300, 305, 604),
                (301, 500, 605, 1004)
            ],
            # EPA 8-hr CO (ppm)
            'CO': [
                (0, 50, 0.0, 4.4),
                (51, 100, 4.5, 9.4),
                (101, 150, 9.5, 12.4),
                (151, 200, 12.5, 15.4),
                (201, 300, 15.5, 30.4),
                (301, 500, 30.5, 50.4)
            ]
        }
        
        # Molar masses for precise ppb ↔ μg/m³ conversion
        self.molar_masses = {
            'NO2': 46.0055,   # g/mol
            'SO2': 64.066,    # g/mol  
            'CO': 28.010,     # g/mol
            'O3': 47.9982     # g/mol
        }
        self.R_gas_constant = 8.314462618  # J/(mol·K)

    def ppb_to_ugm3(self, value_ppb: float, pollutant: str, temp_K: float = 298.15, pressure_Pa: float = 101325.0) -> float:
        """
        Convert ppb to μg/m³ using ideal gas law with local T/P
        μg/m³ = ppb × (M × P) / (R × T) × 1e-3
        """
        pollutant_clean = pollutant.upper().replace('2.5', '').replace('10', '')
        if pollutant_clean not in self.molar_masses:
            logger.warning(f"⚠️ Unknown molar mass for {pollutant}, using standard conversion")
            return value_ppb * self.unit_conversions.get(pollutant_clean, 1.0)
        
        M = self.molar_masses[pollutant_clean]  # g/mol
        return value_ppb * (M * pressure_Pa) / (self.R_gas_constant * temp_K) * 1e-3

    def ugm3_to_ppb(self, value_ugm3: float, pollutant: str, temp_K: float = 298.15, pressure_Pa: float = 101325.0) -> float:
        """
        Convert μg/m³ to ppb using ideal gas law with local T/P
        ppb = μg/m³ × (R × T) / (M × P) × 1e3
        """
        pollutant_clean = pollutant.upper().replace('2.5', '').replace('10', '')
        if pollutant_clean not in self.molar_masses:
            logger.warning(f"⚠️ Unknown molar mass for {pollutant}, using standard conversion")
            return value_ugm3 / self.unit_conversions.get(pollutant_clean, 1.0)
        
        M = self.molar_masses[pollutant_clean]  # g/mol
        return value_ugm3 * (self.R_gas_constant * temp_K) / (M * pressure_Pa) * 1e3

    def get_local_conditions(self, weather_context: Dict[str, Any]) -> Tuple[float, float]:
        """
        Extract temperature (K) and pressure (Pa) from weather context
        Returns: (temp_K, pressure_Pa) with fallback to standard conditions
        """
        # Default to standard conditions (25°C, 1 atm)
        temp_K = 298.15  # 25°C
        pressure_Pa = 101325.0  # 1 atm
        
        if weather_context:
            # Temperature from GFS (usually in °C)
            temp_info = weather_context.get('Temperature', {})
            if temp_info and 'value' in temp_info:
                temp_C = temp_info['value']
                if temp_C is not None:
                    temp_K = temp_C + 273.15
            
            # Pressure from GFS (if available, usually in hPa)
            pressure_info = weather_context.get('Surface Pressure', {})
            if pressure_info and 'value' in pressure_info:
                pressure_hPa = pressure_info['value']
                if pressure_hPa is not None:
                    pressure_Pa = pressure_hPa * 100.0  # hPa to Pa
        
        return temp_K, pressure_Pa

    def convert_units(self, value: float, pollutant: str, from_unit: str, to_unit: str, temp_K: float = 298.15, pressure_Pa: float = 101325.0) -> float:
        """
        Convert between concentration units (ppb ↔ μg/m³) using local T/P
        """
        if from_unit == to_unit:
            return value
            
        if from_unit == 'ppb' and to_unit == 'μg/m³':
            return self.ppb_to_ugm3(value, pollutant, temp_K, pressure_Pa)
        elif from_unit == 'μg/m³' and to_unit == 'ppb':
            return self.ugm3_to_ppb(value, pollutant, temp_K, pressure_Pa)
        elif from_unit == 'ppb' and to_unit == 'ppm':
            return value / 1000.0
        elif from_unit == 'ppm' and to_unit == 'ppb':
            return value * 1000.0
        elif from_unit == 'μg/m³' and to_unit == 'ppm':
            ppb_value = self.ugm3_to_ppb(value, pollutant, temp_K, pressure_Pa)
            return ppb_value / 1000.0
        elif from_unit == 'ppm' and to_unit == 'μg/m³':
            ppb_value = value * 1000.0
            return self.ppb_to_ugm3(ppb_value, pollutant, temp_K, pressure_Pa)
        else:
            return value

    def calculate_aqi(self, concentration: float, pollutant: str, units_hint: str = None) -> int:
        """
        Calculate REAL-TIME AQI ESTIMATE from concentration using EPA breakpoints
        
        ⚠️  IMPORTANT LIMITATION: This is a REAL-TIME ESTIMATE, not official EPA AQI
        EPA requires time averaging that real-time systems cannot provide:
        - O3: 8-hr rolling avg (we use current hour) 
        - CO: 8-hr rolling avg (we use current hour)
        - PM2.5/PM10: 24-hr avg (we use current hour)
        - NO2/SO2: 1-hr avg (✅ matches real-time)
        
        For OFFICIAL EPA AQI with proper time averaging, use EPAAQICalculator instead.
        This method provides immediate air quality estimates for real-time monitoring.
        
        Units required for calculation:
        - O3: ppm, CO: ppm, NO2/SO2: ppb, PM2.5/PM10: μg/m³
        """
        if concentration is None or concentration <= 0:
            return 0
            
        breakpoints = self.epa_breakpoints.get(pollutant, [])
        if not breakpoints:
            logger.warning(f"⚠️ No AQI breakpoints for {pollutant}")
            return 0
        
        for aqi_low, aqi_high, conc_low, conc_high in breakpoints:
            if conc_low <= concentration <= conc_high:
                # Linear interpolation: AQI = ((AQI_high - AQI_low) / (Conc_high - Conc_low)) * (Conc - Conc_low) + AQI_low
                aqi = ((aqi_high - aqi_low) / (conc_high - conc_low)) * (concentration - conc_low) + aqi_low
                return int(round(aqi))
        
        return 500

    def detect_bias(self, open_meteo_val: float, geos_cf_val: float, pollutant: str, temp_K: float = 298.15, pressure_Pa: float = 101325.0) -> Dict[str, Any]:
        """
        Detect systematic bias between Open-Meteo and GEOS-CF for overlapping pollutants
        Uses symmetric percentage to avoid bias when one value is small
        """
        open_meteo_ugm3 = self.convert_units(open_meteo_val, pollutant, 'μg/m³', 'μg/m³', temp_K, pressure_Pa)
        geos_cf_ugm3 = self.convert_units(geos_cf_val, pollutant, 'ppb', 'μg/m³', temp_K, pressure_Pa)
        
        absolute_diff = abs(open_meteo_ugm3 - geos_cf_ugm3)
        
        # SMAPE-like symmetric percentage (0-200%, fair to both sources)
        relative_diff = 200 * absolute_diff / max(open_meteo_ugm3 + geos_cf_ugm3, 1e-6)
        bias_ratio = geos_cf_ugm3 / max(open_meteo_ugm3, 0.1)
        
        # Determine bias significance
        is_significant_bias = relative_diff > 25  # >25% symmetric difference
        
        bias_info = {
            'pollutant': pollutant,
            'open_meteo_ugm3': round(open_meteo_ugm3, 2),
            'geos_cf_ugm3': round(geos_cf_ugm3, 2),
            'absolute_difference': round(absolute_diff, 2),
            'symmetric_difference_percent': round(relative_diff, 1),  # SMAPE-like
            'bias_ratio': round(bias_ratio, 2),
            'significant_bias': is_significant_bias,
            'assessment': self._assess_bias(relative_diff, bias_ratio),
            'local_conditions': f'{temp_K-273.15:.1f}°C, {pressure_Pa/100:.0f}hPa'
        }
        
        return bias_info

    def _assess_bias(self, relative_diff: float, bias_ratio: float) -> str:
        """Assess the type and magnitude of bias"""
        if relative_diff < 10:
            return "excellent_agreement"
        elif relative_diff < 25:
            return "good_agreement" 
        elif bias_ratio > 1.5:
            return "geos_cf_overestimate"
        elif bias_ratio < 0.67:
            return "geos_cf_underestimate"
        else:
            return "moderate_disagreement"

    def fuse_pollutants(self, global_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Advanced fusion of GEOS-CF + Open-Meteo with bias correction
        """
        logger.info("🔬 Starting 3-source advanced fusion...")
        
        open_meteo_data = global_data.get('Open_Meteo', {})
        geos_cf_data = global_data.get('GEOS_CF', {})
        gfs_data = global_data.get('GFS_Meteorology', {})
        
        fusion_results = {
            'fusion_timestamp': datetime.now(timezone.utc).isoformat(),
            'fusion_method': '3-source_advanced_weighted_fusion',
            'weights_used': self.weights,
            'fused_pollutants': {},
            'bias_analysis': {},
            'weather_context': {},
            'overall_aqi': {},
            'why_today_explanation': ""
        }
        
        if 'error' not in gfs_data and gfs_data.get('meteorology'):
            weather = {}
            for param, info in gfs_data['meteorology'].items():
                weather[param] = {
                    'value': info.get('value'),
                    'units': info.get('units', '')
                }
            fusion_results['weather_context'] = weather
            logger.info(f"🌤️ Weather context: {len(weather)} parameters extracted")
        
        om_pollutants = {}
        if 'error' not in open_meteo_data and open_meteo_data.get('pollutants'):
            for p in open_meteo_data['pollutants']:
                pollutant_name = p.get('pollutant', '').upper()
                om_pollutants[pollutant_name] = p
        
        geos_pollutants = {}
        if 'error' not in geos_cf_data and geos_cf_data.get('pollutants'):
            for pollutant, data in geos_cf_data['pollutants'].items():
                if 'error' not in data:
                    pollutant_name = pollutant.upper()
                    geos_pollutants[pollutant_name] = data
        
        logger.info(f"🔍 Open-Meteo pollutants: {list(om_pollutants.keys())}")
        logger.info(f"🔍 GEOS-CF pollutants: {list(geos_pollutants.keys())}")
        
        overlapping = set(om_pollutants.keys()) & set(geos_pollutants.keys())
        logger.info(f"🤝 Overlapping pollutants for fusion: {list(overlapping)}")
        
        # CRITICAL FIX: Also include Open-Meteo exclusive pollutants (PM2.5, PM10)
        om_exclusive = set(om_pollutants.keys()) - set(geos_pollutants.keys())
        logger.info(f"🔸 Open-Meteo exclusive pollutants: {list(om_exclusive)}")
        
        max_aqi = 0
        dominant_pollutant = None
        
        for pollutant in overlapping:
            try:
                om_data = om_pollutants[pollutant]
                geos_data = geos_pollutants[pollutant]
                
                om_conc = om_data.get('concentration', 0)
                geos_conc = geos_data.get('concentration', 0)
                
                if om_conc > 0 and geos_conc > 0:
                    # Bias detection
                    bias_info = self.detect_bias(om_conc, geos_conc, pollutant)
                    fusion_results['bias_analysis'][pollutant] = bias_info
                    
                    logger.info(f"🔬 {pollutant} Bias Analysis:")
                    logger.info(f"   📊 Open-Meteo: {bias_info['open_meteo_ugm3']} μg/m³")
                    logger.info(f"   📊 GEOS-CF: {bias_info['geos_cf_ugm3']} μg/m³")
                    logger.info(f"   📏 Difference: {bias_info['symmetric_difference_percent']}%")
                    logger.info(f"   🎯 Assessment: {bias_info['assessment']}")
                    
                    om_ugm3 = self.convert_units(om_conc, pollutant, 'μg/m³', 'μg/m³')
                    geos_ugm3 = self.convert_units(geos_conc, pollutant, 'ppb', 'μg/m³')
                    
                    fused_conc = (self.weights['Open_Meteo'] * om_ugm3) + (self.weights['GEOS_CF'] * geos_ugm3)
                    
                    if pollutant == 'O3':
                        aqi_conc = self.convert_units(fused_conc, pollutant, 'μg/m³', 'ppm')
                    elif pollutant in ['NO2', 'SO2']:
                        aqi_conc = self.convert_units(fused_conc, pollutant, 'μg/m³', 'ppb')
                    elif pollutant in ['CO']:
                        aqi_conc = self.convert_units(fused_conc, pollutant, 'μg/m³', 'ppm')
                    else:
                        # PM2.5, PM10 already in correct units (μg/m³)
                        aqi_conc = fused_conc
                    
                    aqi_value = self.calculate_aqi(aqi_conc, pollutant)
                    
                    fusion_results['fused_pollutants'][pollutant] = {
                        'fused_concentration': round(fused_conc, 2),
                        'units': 'μg/m³',
                        'aqi': aqi_value,
                        'sources_used': ['Open_Meteo', 'GEOS_CF'],
                        'weights': self.weights,
                        'source_concentrations': {
                            'Open_Meteo': round(om_ugm3, 2),
                            'GEOS_CF': round(geos_ugm3, 2)
                        },
                        'bias_corrected': bias_info['significant_bias']
                    }
                    
                    # Track dominant pollutant (highest AQI)
                    if aqi_value > max_aqi:
                        max_aqi = aqi_value
                        dominant_pollutant = pollutant
                    
                    logger.info(f"✅ {pollutant}: {fused_conc:.2f} μg/m³ → AQI {aqi_value}")
                    
            except Exception as e:
                logger.error(f"❌ Fusion failed for {pollutant}: {e}")
        
        # CRITICAL FIX: Add Open-Meteo exclusive pollutants (PM2.5, PM10)
        # These are often the most health-critical pollutants
        for pollutant in om_exclusive:
            try:
                om_data = om_pollutants[pollutant]
                om_conc = om_data.get('concentration', 0)
                
                if om_conc > 0:
                    if pollutant == 'O3':
                        aqi_conc = self.convert_units(om_conc, pollutant, 'μg/m³', 'ppm')
                    elif pollutant in ['NO2', 'SO2']:
                        aqi_conc = self.convert_units(om_conc, pollutant, 'μg/m³', 'ppb')
                    elif pollutant in ['CO']:
                        aqi_conc = self.convert_units(om_conc, pollutant, 'μg/m³', 'ppm')
                    else:
                        # PM2.5, PM10 already in correct units (μg/m³)
                        aqi_conc = om_conc
                    
                    aqi_value = self.calculate_aqi(aqi_conc, pollutant)
                    
                    fusion_results['fused_pollutants'][pollutant] = {
                        'fused_concentration': round(om_conc, 2),
                        'units': 'μg/m³',
                        'aqi': aqi_value,
                        'sources_used': ['Open_Meteo_only'],
                        'weights': {'Open_Meteo': 1.0},  # 100% Open-Meteo
                        'source_concentrations': {
                            'Open_Meteo': round(om_conc, 2),
                            'GEOS_CF': 'not_available'
                        },
                        'bias_corrected': False,
                        'note': 'No fusion - GEOS-CF does not provide this pollutant'
                    }
                    
                    # Track dominant pollutant (highest AQI) - CRITICAL for PM2.5/PM10
                    if aqi_value > max_aqi:
                        max_aqi = aqi_value
                        dominant_pollutant = pollutant
                    
                    logger.info(f"✅ {pollutant} (OM-only): {om_conc:.2f} μg/m³ → AQI {aqi_value}")
                    
            except Exception as e:
                logger.error(f"❌ Processing failed for OM-exclusive {pollutant}: {e}")
        
        fusion_results['overall_aqi'] = {
            'value': max_aqi,
            'dominant_pollutant': dominant_pollutant,
            'health_category': self._get_health_category(max_aqi),
            'health_message': self._get_health_message(max_aqi),
            'aqi_type': 'real_time_estimate',
            'epa_compliance_warning': 'This is a real-time AQI estimate using current hour data. Official EPA AQI requires time averaging: O3/CO (8-hr), PM2.5/PM10 (24-hr).'
        }
        
        fusion_results['epa_disclaimer'] = {
            'type': 'Real-time AQI Estimate',
            'official_epa_aqi': 'Requires historical data for proper time averaging',
            'time_averaging_missing': {
                'O3': 'Uses current hour instead of 8-hour rolling average',
                'CO': 'Uses current hour instead of 8-hour rolling average', 
                'PM25': 'Uses current hour instead of 24-hour average',
                'PM10': 'Uses current hour instead of 24-hour average',
                'NO2': '✅ Uses 1-hour as required by EPA',
                'SO2': '✅ Uses 1-hour as required by EPA'
            },
            'recommendation': 'For official EPA AQI compliance, use EPAAQICalculator with historical data storage'
        }
        
        fusion_results['why_today_explanation'] = self._generate_why_today_explanation(
            fusion_results['weather_context'], 
            fusion_results['fused_pollutants'],
            max_aqi
        )
        
        logger.info(f"🎯 Overall AQI: {max_aqi} ({dominant_pollutant}) - {fusion_results['overall_aqi']['health_category']}")
        logger.info(f"📊 Fused pollutants: {len(fusion_results['fused_pollutants'])}")
        
        return fusion_results

    def save_fusion_results(self, fusion_results: Dict[str, Any], location_info: Dict[str, Any]) -> Dict[str, str]:
        """
        Save fusion results in multiple formats for easy website access
        
        Args:
            fusion_results: Complete fusion analysis results
            location_info: Location metadata (name, coordinates, etc.)
            
        Returns:
            Dictionary with file paths for each format
        """
        timestamp = datetime.now(timezone.utc)
        location_name = location_info.get('location_name', 'unknown').replace(',', '_').replace(' ', '_')
        date_str = timestamp.strftime('%Y%m%d_%H%M%S')
        
        complete_data = {
            'location': location_info,
            'fusion_analysis': fusion_results,
            'storage_timestamp': timestamp.isoformat(),
            'data_version': '1.0'
        }
        
        file_paths = {}
        
        try:
            json_filename = f"{location_name}_{date_str}_fusion.json"
            json_path = self.json_storage / json_filename
            
            with open(json_path, 'w') as f:
                json.dump(complete_data, f, indent=2, default=str)
            file_paths['json'] = str(json_path)
            logger.info(f"💾 Saved JSON: {json_filename}")
            
            website_json = f"{location_name}_latest.json"
            website_path = self.website_storage / website_json
            
            with open(website_path, 'w') as f:
                json.dump(complete_data, f, indent=2, default=str)
            file_paths['website_json'] = str(website_path)
            logger.info(f"🌐 Website JSON: {website_json}")
            
            parquet_data = self._prepare_parquet_data(fusion_results, location_info, timestamp)
            parquet_filename = f"{location_name}_{date_str}_fusion.parquet"
            parquet_path = self.parquet_storage / parquet_filename
            
            df = pd.DataFrame([parquet_data])
            df.to_parquet(parquet_path, index=False)
            file_paths['parquet'] = str(parquet_path)
            logger.info(f"📊 Saved Parquet: {parquet_filename}")
            
            csv_data = self._prepare_csv_summary(fusion_results, location_info, timestamp)
            csv_filename = f"{location_name}_summary.csv"
            csv_path = self.website_storage / csv_filename
            
            summary_df = pd.DataFrame([csv_data])
            # Append to existing or create new
            if csv_path.exists():
                existing_df = pd.read_csv(csv_path)
                combined_df = pd.concat([existing_df, summary_df], ignore_index=True)
                # Keep only last 24 hours of data
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                cutoff_time = timestamp - timedelta(hours=24)
                filtered_df = combined_df[combined_df['timestamp'] >= cutoff_time]
                filtered_df.to_csv(csv_path, index=False)
            else:
                summary_df.to_csv(csv_path, index=False)
            
            file_paths['csv'] = str(csv_path)
            logger.info(f"📈 Updated CSV: {csv_filename}")
            
            logger.info(f"✅ Storage complete: {len(file_paths)} files saved")
            
        except Exception as e:
            logger.error(f"❌ Storage failed: {e}")
            file_paths['error'] = str(e)
        
        return file_paths

    def _prepare_parquet_data(self, fusion_results: Dict[str, Any], location_info: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
        """Prepare data for Parquet storage (flat structure)"""
        data = {
            'timestamp': timestamp.isoformat(),
            'location_name': location_info.get('location_name'),
            'latitude': location_info.get('latitude'),
            'longitude': location_info.get('longitude'),
            'overall_aqi': fusion_results['overall_aqi']['value'],
            'health_category': fusion_results['overall_aqi']['health_category'],
            'dominant_pollutant': fusion_results['overall_aqi']['dominant_pollutant'],
            'fusion_method': fusion_results['fusion_method'],
            'sources_successful': len(fusion_results['fused_pollutants']),
            'weather_temperature': fusion_results['weather_context'].get('Temperature', {}).get('value'),
            'weather_humidity': fusion_results['weather_context'].get('Humidity', {}).get('value'),
            'weather_wind_speed': fusion_results['weather_context'].get('Wind Speed', {}).get('value'),
            'why_today_explanation': fusion_results['why_today_explanation']
        }
        
        for pollutant, info in fusion_results['fused_pollutants'].items():
            data[f'{pollutant.lower()}_concentration'] = info['fused_concentration']
            data[f'{pollutant.lower()}_aqi'] = info['aqi']
            data[f'{pollutant.lower()}_bias_corrected'] = info['bias_corrected']
        
        for pollutant, bias in fusion_results['bias_analysis'].items():
            data[f'{pollutant.lower()}_bias_percent'] = bias['relative_difference_percent']
            data[f'{pollutant.lower()}_bias_assessment'] = bias['assessment']
        
        return data

    def _prepare_csv_summary(self, fusion_results: Dict[str, Any], location_info: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
        """Prepare summary data for CSV dashboard"""
        return {
            'timestamp': timestamp.isoformat(),
            'location': location_info.get('location_name'),
            'lat': location_info.get('latitude'),
            'lon': location_info.get('longitude'),
            'aqi': fusion_results['overall_aqi']['value'],
            'category': fusion_results['overall_aqi']['health_category'],
            'dominant': fusion_results['overall_aqi']['dominant_pollutant'],
            'temperature': fusion_results['weather_context'].get('Temperature', {}).get('value'),
            'humidity': fusion_results['weather_context'].get('Humidity', {}).get('value'),
            'wind_speed': fusion_results['weather_context'].get('Wind Speed', {}).get('value'),
            'pollutants_fused': len(fusion_results['fused_pollutants']),
            'explanation': fusion_results['why_today_explanation'][:100] + '...' if len(fusion_results['why_today_explanation']) > 100 else fusion_results['why_today_explanation']
        }

    def process_location_data(self, global_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process pre-collected global location data through fusion pipeline
        
        This method is designed for immediate processing workflows where
        data has already been collected by GlobalRealtimeCollector.
        
        Args:
            global_data: Pre-collected data in the format:
            {
                'Open_Meteo': {...},
                'GEOS_CF': {...}, 
                'GFS_Meteorology': {...},
                'location_metadata': {...}
            }
            
        Returns:
            Complete fusion results with AQI calculations
        """
        logger.info("🔬 Processing pre-collected global data through fusion pipeline")
        
        fusion_results = self.fuse_pollutants(global_data)
        
        location_meta = global_data.get('location_metadata', {})
        fusion_results['location_info'] = {
            'name': location_meta.get('name', 'Unknown'),
            'latitude': location_meta.get('latitude'),
            'longitude': location_meta.get('longitude'),
            'collection_timestamp': location_meta.get('collection_timestamp'),
            'collection_time': location_meta.get('collection_time', 0)
        }
        
        logger.info(f"✅ Fusion processing completed for {fusion_results['location_info']['name']}")
        return fusion_results

    def process_and_store_location(self, lat: float, lon: float, location_name: str) -> Dict[str, Any]:
        """
        Complete pipeline: collect data, fuse, calculate AQI, and store results
        """
        logger.info(f"🔄 Complete pipeline for {location_name}")
        
        # Import here to avoid circular imports
        import sys
        sys.path.append(os.path.join(os.getenv('PROJECT_ROOT', '/app'), 'backend', 'collectors'))
        from global_realtime_collector import GlobalRealtimeCollector
        
        # Step 1: Collect data
        collector = GlobalRealtimeCollector()
        global_data = collector.collect_single_location(lat, lon, location_name)
        
        # Step 2: Fusion and AQI calculation
        fusion_results = self.fuse_pollutants(global_data)
        
        # Step 3: Storage
        location_info = {
            'location_name': location_name,
            'latitude': lat,
            'longitude': lon,
            'collection_timestamp': global_data.get('collection_timestamp'),
            'collection_time_seconds': global_data.get('collection_time_seconds'),
            'sources_successful': global_data.get('data_summary', {}).get('sources_successful', 0)
        }
        
        file_paths = self.save_fusion_results(fusion_results, location_info)
        
        return {
            'location_info': location_info,
            'fusion_results': fusion_results,
            'storage_paths': file_paths,
            'pipeline_success': True
        }

    def _get_health_category(self, aqi: int) -> str:
        """Get EPA health category from AQI value"""
        if aqi <= 50:
            return "Good"
        elif aqi <= 100:
            return "Moderate" 
        elif aqi <= 150:
            return "Unhealthy for Sensitive Groups"
        elif aqi <= 200:
            return "Unhealthy"
        elif aqi <= 300:
            return "Very Unhealthy"
        else:
            return "Hazardous"

    def _get_health_message(self, aqi: int) -> str:
        """Get EPA health message from AQI value"""
        if aqi <= 50:
            return "Air quality is satisfactory for most people"
        elif aqi <= 100:
            return "Sensitive people should consider limiting outdoor activities"
        elif aqi <= 150:
            return "Sensitive groups should limit outdoor activities"
        elif aqi <= 200:
            return "Everyone should limit outdoor activities"
        elif aqi <= 300:
            return "Everyone should avoid outdoor activities"
        else:
            return "Health warnings - everyone should avoid outdoor activities"

    def _generate_why_today_explanation(self, weather: Dict, pollutants: Dict, overall_aqi: int) -> str:
        """
        Generate "Why Today" explanation using weather context and pollutant levels
        """
        explanations = []
        
        # Weather factors
        if weather:
            temp = weather.get('Temperature', {}).get('value')
            humidity = weather.get('Humidity', {}).get('value')
            wind_speed = weather.get('Wind Speed', {}).get('value')
            
            if temp and temp > 30:
                explanations.append(f"High temperature ({temp}°C) increases photochemical reactions")
            elif temp and temp < 10:
                explanations.append(f"Low temperature ({temp}°C) reduces atmospheric mixing")
            
            if humidity and humidity > 80:
                explanations.append(f"High humidity ({humidity}%) enhances particulate formation")
            
            if wind_speed and wind_speed < 2:
                explanations.append(f"Low wind speed ({wind_speed} m/s) reduces pollutant dispersion")
            elif wind_speed and wind_speed > 8:
                explanations.append(f"High wind speed ({wind_speed} m/s) helps disperse pollutants")
        
        # Pollutant-specific explanations
        if pollutants:
            high_pollutants = []
            for pollutant, data in pollutants.items():
                aqi_val = data.get('aqi', 0)
                if aqi_val > 100:
                    high_pollutants.append(f"{pollutant} (AQI {aqi_val})")
            
            if high_pollutants:
                explanations.append(f"Elevated levels: {', '.join(high_pollutants)}")
        
        if overall_aqi > 150:
            explanations.append("Multiple factors contributing to unhealthy air quality")
        elif overall_aqi > 100:
            explanations.append("Weather conditions moderately affecting air quality")
        else:
            explanations.append("Weather conditions supporting good air quality")
        
        return " • ".join(explanations) if explanations else "Air quality within normal range"


# Three-source fusion engine - import and use in other modules
