#!/usr/bin/env python3
"""
Production Data Fusion and Bias Correction System
===========================================================
This module implements ONLY fusion and bias correction for air quality data.

Fusion Hierarchy:
- AirNow/EPA (Ground): 0.5 weight (highest confidence)
- WAQI (Ground aggregated): 0.3 weight 
- TEMPO (Satellite): 0.15 weight
- GEOS-CF (Model): 0.05 weight (lowest confidence, boosted when others missing)

Bias Correction Formula:
Corrected = Model_value + (Obs_mean - Model_mean)

Note: AQI calculation handled separately by aqi_calculator.py
"""

import json
import logging
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class FusedPollutantData:
    """Data class for fused pollutant concentrations (no AQI calculation)"""
    pollutant: str
    concentration: float
    units: str = "ppb"
    source: str = "weighted_fusion"
    confidence: float = 0.8
    weights_used: Optional[Dict[str, float]] = None
    bias_correction_applied: bool = False
    missing_sources: Optional[List[str]] = None

class ProductionFusionEngine:
    """
    Production-grade data fusion engine implementing research best practices.
    Focuses ONLY on fusion and bias correction - AQI calculated separately.
    """
    
    def __init__(self):
        # Research-based weights (sum to exactly 1.0)
        self.weights = {
            "AirNow": 0.5,    # EPA ground stations - highest confidence
            "WAQI": 0.3,      # WAQI ground aggregated 
            "TEMPO": 0.15,    # NASA TEMPO satellite
            "GEOS": 0.05      # GEOS-CF model - lowest confidence (boosted when others missing)
        }
        
        self.source_mapping = {
            "AirNow": "airnow",
            "WAQI": "waqi", 
            "TEMPO": "tempo",
            "GEOS": "geos"
        }
        
        # Bias correction parameters from validation studies
        self.bias_corrections = {
            "NO2": {
                "tempo_vs_ground": {"slope": 0.92, "intercept": 2.1},
                "geos_vs_ground": {"slope": 0.85, "intercept": 3.8}
            },
            "O3": {
                "geos_vs_ground": {"slope": 0.95, "intercept": -1.2}
            },
            "PM2.5": {
                "model_vs_ground": {"slope": 0.78, "intercept": 5.2}
            },
            "PM25": {
                "model_vs_ground": {"slope": 0.78, "intercept": 5.2}
            },
            "HCHO": {
                "tempo_vs_ground": {"slope": 0.88, "intercept": 1.5}
            }
        }
    
    def should_apply_bias_correction(self, pollutant: str, available_sources: List[str]) -> bool:
        """
        Determine if bias correction should be applied based on available sources.
        
        Pollutant-specific logic:
        - PM2.5/PM25: Apply if any sources available (uses model_vs_ground correction)
        - NO2: Apply if TEMPO or GEOS available (specific corrections for each)  
        - O3: Apply if GEOS available (geos_vs_ground correction)
        - HCHO: Apply if TEMPO available (tempo_vs_ground correction)
        """
        if pollutant not in self.bias_corrections:
            return False
            
        if pollutant == "NO2":
            # NO2 has tempo_vs_ground and geos_vs_ground corrections
            return "TEMPO" in available_sources or "GEOS" in available_sources
            
        elif pollutant == "O3":
            # O3 has geos_vs_ground correction
            return "GEOS" in available_sources
            
        elif pollutant in ["PM2.5", "PM25"]:
            # PM2.5 has model_vs_ground correction - apply if any sources available
            return len(available_sources) > 0
            
        elif pollutant == "HCHO":
            # HCHO would have tempo_vs_ground correction if defined
            return "TEMPO" in available_sources
            
        # Default: require both model and ground data
        model_sources = {"GEOS", "TEMPO"}
        ground_sources = {"AirNow", "WAQI"}
        
        has_model = any(source in available_sources for source in model_sources)
        has_ground = any(source in available_sources for source in ground_sources)
        
        return has_model and has_ground
    
    def apply_bias_correction(self, value: float, pollutant: str, source: str) -> float:
        """Apply bias correction based on validation studies"""
        if pollutant not in self.bias_corrections:
            return value
        
        source_lower = self.source_mapping.get(source, source.lower())
        corrections = self.bias_corrections[pollutant]
        
        if source_lower == "tempo" and "tempo_vs_ground" in corrections:
            corr = corrections["tempo_vs_ground"]
            return value * corr["slope"] + corr["intercept"]
        elif source_lower == "geos" and "geos_vs_ground" in corrections:
            corr = corrections["geos_vs_ground"]
            return value * corr["slope"] + corr["intercept"]
        elif "model_vs_ground" in corrections:
            corr = corrections["model_vs_ground"]
            return value * corr["slope"] + corr["intercept"]
            
        return value
    
    def normalize_weights(self, available_sources: List[str]) -> Dict[str, float]:
        """Normalize weights for available sources to sum exactly to 1.0"""
        available_weights = {source: self.weights[source] for source in available_sources if source in self.weights}
        
        if not available_weights:
            return {}
            
        total_weight = sum(available_weights.values())
        if total_weight == 0:
            return {}
            
        # Ensure exact normalization (avoid floating point precision errors)
        normalized = {source: weight / total_weight for source, weight in available_weights.items()}
        
        # Final adjustment to ensure exact sum of 1.0
        total_normalized = sum(normalized.values())
        if total_normalized != 1.0 and normalized:
            # Adjust largest weight to make sum exactly 1.0
            largest_source = max(normalized.keys(), key=lambda k: normalized[k])
            normalized[largest_source] += (1.0 - total_normalized)
            
        return normalized
    
    def standardize_pollutant_name(self, pollutant: str) -> str:
        """Standardize pollutant naming for consistency"""
        name_mapping = {
            "PM25": "PM2.5",
            "PM2.5": "PM2.5",
            "PM10": "PM10",
            "NO2": "NO2",
            "O3": "O3",
            "CO": "CO", 
            "SO2": "SO2",
            "HCHO": "HCHO"
        }
        return name_mapping.get(pollutant, pollutant)
    
    def get_units_for_pollutant(self, pollutant: str) -> str:
        """Get appropriate units for each pollutant"""
        units_map = {
            "O3": "ppb",
            "NO2": "ppb", 
            "SO2": "ppb",
            "CO": "ppm",
            "PM2.5": "μg/m³",
            "PM25": "μg/m³",  # Alternative name
            "PM10": "μg/m³",
            "HCHO": "ppb"
        }
        return units_map.get(pollutant, "ppb")
    
    def fuse_pollutant_data(self, pollutant_data: Dict[str, Any], pollutant_name: str) -> Optional[FusedPollutantData]:
        """
        Fuse data from multiple sources with weighted averaging and bias correction.
        Returns fused concentrations only (no AQI calculation).
        """
        values = {}
        available_sources = []
        
        for source_key, source_value in pollutant_data.items():
            if source_value is not None:
                value = None
                
                if isinstance(source_value, dict):
                    # Complex structure with nested data
                    if 'aqi' in source_value and source_value['aqi'] is not None:
                        value = source_value['aqi']
                    elif 'concentration' in source_value and source_value['concentration'] is not None:
                        value = source_value['concentration']
                    elif 'value' in source_value and source_value['value'] is not None:
                        value = source_value['value']
                else:
                    value = source_value
                
                if value is not None and isinstance(value, (int, float)) and value > 0:
                    values[source_key] = value
                    available_sources.append(source_key)
                elif value is not None and value <= 0:
                    logger.debug(f"🔍 Ignoring zero/negative value for {pollutant_name} from {source_key}: {value}")
        
        if not values:
            logger.warning(f"❌ No valid values for {pollutant_name}")
            return None
        
        # Standardize pollutant name
        standardized_pollutant = self.standardize_pollutant_name(pollutant_name)
        
        # Determine if bias correction should be applied
        should_apply_bias = self.should_apply_bias_correction(standardized_pollutant, available_sources)
        
        corrected_values = {}
        for source, value in values.items():
            if should_apply_bias:
                corrected_value = self.apply_bias_correction(value, standardized_pollutant, source)
                corrected_values[source] = corrected_value
                logger.debug(f"🔧 {standardized_pollutant} bias correction: {source} {value} → {corrected_value:.2f}")
            else:
                corrected_values[source] = value
        
        # Normalize weights for available sources
        normalized_weights = self.normalize_weights(available_sources)
        
        if not normalized_weights:
            # Fallback to simple average
            fused_value = sum(corrected_values.values()) / len(corrected_values)
            confidence = 0.5
            logger.debug(f"⚠️ Using simple average for {standardized_pollutant}")
        else:
            # Weighted average
            fused_value = sum(corrected_values[source] * normalized_weights[source] for source in corrected_values.keys())
            # Confidence based on bias correction and source coverage
            bias_confidence_boost = 0.1 if should_apply_bias else 0.0
            coverage_confidence = len(available_sources) / len(self.weights)
            confidence = min(0.9, 0.6 + coverage_confidence * 0.2 + bias_confidence_boost)
        
        # Identify missing sources
        all_sources = set(self.weights.keys())
        missing_sources = list(all_sources - set(available_sources))
        
        return FusedPollutantData(
            pollutant=standardized_pollutant,
            concentration=round(fused_value, 2),
            units=self.get_units_for_pollutant(standardized_pollutant),
            source="weighted_fusion",
            confidence=round(confidence, 3),
            weights_used=normalized_weights,
            bias_correction_applied=should_apply_bias,
            missing_sources=missing_sources if missing_sources else None
        )
    
    def process_location_data(self, location_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process complete location data through fusion pipeline.
        Outputs fused concentrations only (no AQI calculation).
        """
        logger.info(f"🔬 Starting fusion for location: {location_data.get('location', 'Unknown')}")
        
        fused_concentrations = {}
        fusion_stats = {
            "total_pollutants_attempted": 0,
            "successful_fusions": 0,
            "bias_corrections_applied": 0,
            "high_confidence_results": 0
        }
        
        # Identify all unique pollutants across all sources
        all_pollutants = set()
        metadata_fields = {'location', 'timestamp', 'collection_time_seconds', 'lat', 'lon', 'collection_time'}
        
        for source_name, source_data in location_data.items():
            if source_name not in metadata_fields and isinstance(source_data, dict):
                all_pollutants.update(source_data.keys())
        
        for pollutant in all_pollutants:
            fusion_stats["total_pollutants_attempted"] += 1
            
            pollutant_data = {}
            for source_name, source_data in location_data.items():
                if source_name not in metadata_fields and isinstance(source_data, dict):
                    if pollutant in source_data:
                        pollutant_data[source_name] = source_data[pollutant]
            
            # Fuse the data
            fused_result = self.fuse_pollutant_data(pollutant_data, pollutant)
            
            if fused_result:
                fused_concentrations[pollutant] = asdict(fused_result)
                fusion_stats["successful_fusions"] += 1
                
                if fused_result.bias_correction_applied:
                    fusion_stats["bias_corrections_applied"] += 1
                    
                if fused_result.confidence > 0.7:
                    fusion_stats["high_confidence_results"] += 1
        
        result = {
            "location": location_data.get('location', 'Unknown'),
            "lat": location_data.get('lat'),
            "lon": location_data.get('lon'),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fusion_method": "weighted_average_with_bias_correction",
            "fused_concentrations": fused_concentrations,
            "fusion_statistics": fusion_stats,
            "processing_metadata": {
                "fusion_weights": self.weights,
                "bias_correction_available": list(self.bias_corrections.keys()),
                "original_collection_time": location_data.get('collection_time', location_data.get('collection_time_seconds', 0)),
                "next_step": "Use aqi_calculator.py to calculate AQI from these concentrations"
            }
        }
        
        logger.info(f"✅ Fusion complete: {fusion_stats['successful_fusions']}/{fusion_stats['total_pollutants_attempted']} pollutants, {fusion_stats['bias_corrections_applied']} bias corrections applied")
        
        return result

    def export_to_parquet(self, fused_results: List[Dict], output_path: str, s3_format: bool = False) -> str:
        """
        Export fused results to Parquet format for analytics
        
        Args:
            fused_results: List of fusion results from process_location_data()
            output_path: Local path or S3-style path for output
            s3_format: If True, creates S3-compatible folder structure
            
        Returns:
            Path to the created Parquet file
        """
        try:
            parquet_data = []
            
            for result in fused_results:
                location = result.get('location', 'Unknown')
                lat = result.get('lat')
                lon = result.get('lon')
                timestamp = result.get('timestamp', datetime.now(timezone.utc).isoformat())
                concentrations = result.get('fused_concentrations', {})
                
                row = {
                    'location': location,
                    'lat': lat,
                    'lon': lon,
                    'timestamp': timestamp,
                    'PM25': concentrations.get('PM2.5', {}).get('concentration') if isinstance(concentrations.get('PM2.5'), dict) else concentrations.get('PM25', {}).get('concentration') if isinstance(concentrations.get('PM25'), dict) else np.nan,
                    'O3': concentrations.get('O3', {}).get('concentration') if isinstance(concentrations.get('O3'), dict) else np.nan,
                    'NO2': concentrations.get('NO2', {}).get('concentration') if isinstance(concentrations.get('NO2'), dict) else np.nan,
                    'SO2': concentrations.get('SO2', {}).get('concentration') if isinstance(concentrations.get('SO2'), dict) else np.nan,
                    'CO': concentrations.get('CO', {}).get('concentration') if isinstance(concentrations.get('CO'), dict) else np.nan,
                    'HCHO': concentrations.get('HCHO', {}).get('concentration') if isinstance(concentrations.get('HCHO'), dict) else np.nan
                }
                parquet_data.append(row)
            
            df = pd.DataFrame(parquet_data)
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            if s3_format and len(df) > 0:
                date_str = df['timestamp'].dt.date.iloc[0].strftime('%Y-%m-%d')
                location = df['location'].iloc[0]
                lat = df['lat'].iloc[0]
                lon = df['lon'].iloc[0]
                location_id = f"{location}_{lat}_{lon}".replace(' ', '_').replace(',', '').replace('(', '').replace(')', '')
                s3_path = f"s3://naqforecast/processed/fusion/{date_str}/{location_id}.parquet"
                
                local_dir = f"fusion_parquet/{date_str}"
                os.makedirs(local_dir, exist_ok=True)
                output_path = f"{local_dir}/{location_id}.parquet"
                
                logger.info(f"📁 S3 target path: {s3_path}")
            
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_path, compression='snappy')
            
            logger.info(f"💾 Parquet exported: {output_path}")
            logger.info(f"📊 Data shape: {df.shape[0]} rows, {df.shape[1]} columns")
            
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Parquet export failed: {e}")
            raise

def process_clean_jsonl_file(input_file: str, output_file: Optional[str] = None, create_parquet: bool = True) -> str:
    """
    Process a clean JSONL file through the fusion pipeline
    
    Args:
        input_file: Path to clean JSONL file
        output_file: Optional output JSONL path
        create_parquet: If True, also creates Parquet output for analytics
    """
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = input_path.parent / f"fused_concentrations_{timestamp}.jsonl"
    else:
        output_file = Path(output_file)
    
    fusion_engine = ProductionFusionEngine()
    processed_count = 0
    fused_results = []  # Collect results for Parquet export
    
    logger.info(f"🔬 Starting fusion processing: {input_file} → {output_file}")
    
    with open(input_path, 'r') as infile, open(output_file, 'w') as outfile:
        for line_num, line in enumerate(infile, 1):
            try:
                location_data = json.loads(line.strip())
                
                fused_result = fusion_engine.process_location_data(location_data)
                
                if create_parquet:
                    fused_results.append(fused_result)
                
                outfile.write(json.dumps(fused_result) + '\n')
                processed_count += 1
                
                if processed_count % 10 == 0:
                    logger.info(f"📊 Processed {processed_count} locations...")
                    
            except Exception as e:
                logger.error(f"❌ Error processing line {line_num}: {e}")
                continue
    
    logger.info(f"🎯 Fusion complete! Processed {processed_count} locations → {output_file}")
    
    if create_parquet and fused_results:
        try:
            parquet_file = fusion_engine.export_to_parquet(fused_results, str(output_file).replace('.jsonl', '.parquet'), s3_format=True)
            logger.info(f"📦 Parquet analytics file: {parquet_file}")
        except Exception as e:
            logger.warning(f"⚠️ Parquet export failed (fusion still successful): {e}")
    
    return str(output_file)

# Fusion bias corrector - import and use in other modules
