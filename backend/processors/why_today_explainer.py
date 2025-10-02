#!/usr/bin/env python3
"""
Why Today Explainer
Generates scientific explanations for current air quality conditions
Based on AQI data, meteorology, and environmental factors
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math

logger = logging.getLogger(__name__)

class WhyTodayExplainer:
    """
    Generates scientific explanations for air quality conditions
    Analyzes meteorology, trends, and environmental factors
    """
    
    def __init__(self):
        # Meteorological thresholds for rule-based explanations
        self.wind_thresholds = {
            'calm': 3.0,           # < 3 m/s
            'light': 5.0,          # 3-5 m/s  
            'moderate': 10.0,      # 5-10 m/s
            'strong': 15.0         # > 15 m/s
        }
        
        self.temperature_thresholds = {
            'cold': 10.0,          # < 10°C
            'cool': 20.0,          # 10-20°C
            'warm': 30.0,          # 20-30°C
            'hot': 35.0            # > 35°C
        }
        
        # Pollutant-specific explanation rules
        self.pollutant_rules = {
            'PM25': self._get_pm25_rules(),
            'PM10': self._get_pm10_rules(),
            'O3': self._get_ozone_rules(),
            'NO2': self._get_no2_rules(),
            'SO2': self._get_so2_rules(),
            'CO': self._get_co_rules()
        }
        
        # Seasonal patterns
        self.seasonal_factors = {
            'winter': ['heating_emissions', 'temperature_inversions', 'reduced_mixing'],
            'spring': ['pollen_season', 'variable_weather', 'dust_events'],
            'summer': ['ozone_formation', 'wildfire_season', 'heat_dome'],
            'fall': ['burning_season', 'temperature_inversions', 'leaf_burning']
        }
    
    def generate_explanation(self, aqi_data: Dict, weather_data: Dict, 
                           trend_data: Dict = None, location_data: Dict = None) -> Dict:
        """
        Generate comprehensive explanation for current air quality
        
        Args:
            aqi_data: Current AQI and pollutant data
            weather_data: Current meteorological conditions
            trend_data: Historical trend information (optional)
            location_data: Geographic and environmental context (optional)
            
        Returns:
            Dictionary with explanation components
        """
        
        explanation = {
            'timestamp': datetime.now().isoformat(),
            'primary_pollutant': aqi_data.get('primary_pollutant', 'PM25'),
            'aqi_value': aqi_data.get('aqi', 0),
            'main_explanation': '',
            'meteorological_factors': [],
            'trend_explanation': '',
            'environmental_factors': [],
            'seasonal_context': '',
            'health_context': '',
            'forecast_insight': '',
            'confidence_score': 0.0
        }
        
        try:
            explanation['meteorological_factors'] = self._analyze_meteorology(
                aqi_data, weather_data
            )
            
            if trend_data:
                explanation['trend_explanation'] = self._analyze_trends(
                    aqi_data, trend_data
                )
            
            if location_data:
                explanation['environmental_factors'] = self._analyze_environment(
                    aqi_data, location_data, weather_data
                )
            
            explanation['seasonal_context'] = self._get_seasonal_context(
                aqi_data, weather_data
            )
            
            explanation['main_explanation'] = self._generate_main_explanation(
                explanation, aqi_data, weather_data
            )
            
            explanation['health_context'] = self._get_health_context(aqi_data)
            
            explanation['forecast_insight'] = self._get_forecast_insight(
                weather_data, aqi_data
            )
            
            explanation['confidence_score'] = self._calculate_confidence(
                aqi_data, weather_data, trend_data, location_data
            )
            
            logger.info(f"Generated explanation for {aqi_data.get('primary_pollutant')} "
                       f"AQI {aqi_data.get('aqi')} with confidence {explanation['confidence_score']:.2f}")
            
        except Exception as e:
            logger.error(f"Error generating explanation: {e}")
            explanation['main_explanation'] = "Unable to generate detailed explanation at this time."
        
        return explanation
    
    def _analyze_meteorology(self, aqi_data: Dict, weather_data: Dict) -> List[Dict]:
        """Analyze meteorological factors affecting air quality"""
        
        factors = []
        pollutant = aqi_data.get('primary_pollutant', 'PM25')
        
        wind_speed = weather_data.get('wind_speed', 0)
        if wind_speed is None:
            wind_speed = 0
            
        temperature = weather_data.get('temperature', 20)
        if temperature is None:
            temperature = 20
            
        humidity = weather_data.get('humidity', 50)
        if humidity is None:
            humidity = 50
            
        pressure = weather_data.get('pressure', 1013)
        if pressure is None:
            pressure = 1013
        
        # Wind analysis
        if wind_speed < self.wind_thresholds['calm']:
            factors.append({
                'factor': 'calm_winds',
                'description': f"Calm winds ({wind_speed:.1f} m/s) are allowing pollutants to accumulate",
                'impact': 'negative',
                'confidence': 0.9
            })
        elif wind_speed > self.wind_thresholds['strong']:
            factors.append({
                'factor': 'strong_winds',
                'description': f"Strong winds ({wind_speed:.1f} m/s) are dispersing pollutants",
                'impact': 'positive',
                'confidence': 0.8
            })
        
        # Temperature analysis
        if pollutant in ['O3'] and temperature > self.temperature_thresholds['hot']:
            factors.append({
                'factor': 'high_temperature',
                'description': f"High temperatures ({temperature:.1f}°C) are accelerating ozone formation",
                'impact': 'negative',
                'confidence': 0.9
            })
        elif temperature < self.temperature_thresholds['cold']:
            factors.append({
                'factor': 'cold_temperature',
                'description': f"Cold temperatures ({temperature:.1f}°C) may increase heating emissions",
                'impact': 'negative',
                'confidence': 0.7
            })
        
        if self._detect_inversion(weather_data):
            factors.append({
                'factor': 'temperature_inversion',
                'description': "Temperature inversion is trapping pollutants near the ground",
                'impact': 'negative',
                'confidence': 0.8
            })
        
        # Humidity effects
        if humidity > 80 and pollutant in ['PM25', 'PM10']:
            factors.append({
                'factor': 'high_humidity',
                'description': f"High humidity ({humidity:.0f}%) may enhance particle formation",
                'impact': 'negative',
                'confidence': 0.6
            })
        
        return factors
    
    def _analyze_trends(self, aqi_data: Dict, trend_data: Dict) -> str:
        """Analyze air quality trends"""
        
        current_aqi = aqi_data.get('aqi', 0)
        yesterday_aqi = trend_data.get('yesterday_avg', current_aqi)
        last_week_avg = trend_data.get('week_avg', current_aqi)
        
        # Compare to yesterday
        daily_change = current_aqi - yesterday_aqi
        weekly_change = current_aqi - last_week_avg
        
        trend_explanations = []
        
        if abs(daily_change) > 10:
            if daily_change > 0:
                trend_explanations.append(
                    f"Air quality has worsened by {daily_change:.0f} AQI points since yesterday"
                )
            else:
                trend_explanations.append(
                    f"Air quality has improved by {abs(daily_change):.0f} AQI points since yesterday"
                )
        
        if abs(weekly_change) > 15:
            if weekly_change > 0:
                trend_explanations.append(
                    f"Air quality is {weekly_change:.0f} points worse than the weekly average"
                )
            else:
                trend_explanations.append(
                    f"Air quality is {abs(weekly_change):.0f} points better than the weekly average"
                )
        
        # Trend direction
        hourly_trend = trend_data.get('hourly_trend', 'stable')
        if hourly_trend == 'increasing':
            trend_explanations.append("Pollution levels are currently rising")
        elif hourly_trend == 'decreasing':
            trend_explanations.append("Pollution levels are currently decreasing")
        
        return '. '.join(trend_explanations) if trend_explanations else "Air quality is relatively stable"
    
    def _analyze_environment(self, aqi_data: Dict, location_data: Dict, weather_data: Dict) -> List[Dict]:
        """Analyze environmental and geographic factors"""
        
        factors = []
        
        # Urban vs rural
        if location_data.get('urban_area', False):
            factors.append({
                'factor': 'urban_emissions',
                'description': "Urban area with traffic and industrial emissions",
                'impact': 'negative',
                'confidence': 0.8
            })
        
        # Proximity to sources
        if location_data.get('near_highway', False):
            factors.append({
                'factor': 'traffic_proximity',
                'description': "Close proximity to major highways increases vehicle emissions",
                'impact': 'negative',
                'confidence': 0.9
            })
        
        if location_data.get('industrial_area', False):
            factors.append({
                'factor': 'industrial_sources',
                'description': "Industrial area with potential emissions sources",
                'impact': 'negative',
                'confidence': 0.8
            })
        
        if location_data.get('valley_location', False):
            factors.append({
                'factor': 'valley_topography',
                'description': "Valley location can trap pollutants during stable conditions",
                'impact': 'negative',
                'confidence': 0.7
            })
        
        # Special events
        if location_data.get('fire_nearby', False):
            factors.append({
                'factor': 'wildfire_smoke',
                'description': "Nearby wildfire activity is contributing smoke particles",
                'impact': 'negative',
                'confidence': 0.95
            })
        
        if location_data.get('dust_event', False):
            factors.append({
                'factor': 'dust_storm',
                'description': "Dust storm or high wind event is raising particulate levels",
                'impact': 'negative',
                'confidence': 0.9
            })
        
        return factors
    
    def _get_seasonal_context(self, aqi_data: Dict, weather_data: Dict) -> str:
        """Get seasonal context for air quality"""
        
        # Determine season
        month = datetime.now().month
        if month in [12, 1, 2]:
            season = 'winter'
        elif month in [3, 4, 5]:
            season = 'spring'
        elif month in [6, 7, 8]:
            season = 'summer'
        else:
            season = 'fall'
        
        pollutant = aqi_data.get('primary_pollutant', 'PM25')
        temperature = weather_data.get('temperature', 20)
        
        seasonal_explanations = {
            'winter': {
                'PM25': "Winter heating and reduced atmospheric mixing typically increase particulate matter",
                'PM10': "Winter conditions often trap larger particles near the surface",
                'O3': "Ozone levels are typically lower in winter due to reduced sunlight",
                'NO2': "Winter heating and traffic in cold conditions can elevate NO2 levels"
            },
            'summer': {
                'O3': "Summer heat and sunlight create ideal conditions for ozone formation",
                'PM25': "Summer wildfires and photochemical processes can increase fine particles",
                'PM10': "Dry summer conditions may increase dust and coarse particles"
            },
            'spring': {
                'PM25': "Spring weather patterns can bring variable air quality conditions",
                'PM10': "Spring winds may increase dust and pollen particles"
            },
            'fall': {
                'PM25': "Fall burning season and temperature inversions can worsen air quality",
                'PM10': "Leaf burning and agricultural activities may increase particles"
            }
        }
        
        base_explanation = seasonal_explanations.get(season, {}).get(pollutant, 
            f"{season.title()} weather patterns may influence current air quality conditions")
        
        if season == 'summer' and temperature > 35:
            base_explanation += ". Exceptionally hot temperatures are enhancing photochemical reactions"
        elif season == 'winter' and temperature < 0:
            base_explanation += ". Very cold temperatures increase heating demand and emissions"
        
        return base_explanation
    
    def _generate_main_explanation(self, explanation_data: Dict, aqi_data: Dict, weather_data: Dict) -> str:
        """Generate the main explanation text"""
        
        pollutant = aqi_data.get('primary_pollutant', 'PM25')
        aqi_value = aqi_data.get('aqi', 0)
        
        if aqi_value <= 50:
            level_context = "Air quality is good today"
        elif aqi_value <= 100:
            level_context = "Air quality is moderate today"
        elif aqi_value <= 150:
            level_context = "Air quality is unhealthy for sensitive groups today"
        elif aqi_value <= 200:
            level_context = "Air quality is unhealthy today"
        else:
            level_context = "Air quality is very unhealthy today"
        
        # Primary factors
        main_factors = []
        for factor in explanation_data['meteorological_factors']:
            if factor['confidence'] > 0.7:
                main_factors.append(factor['description'].lower())
        
        if main_factors:
            main_explanation = f"{level_context}. {main_factors[0].capitalize()}"
            if len(main_factors) > 1:
                main_explanation += f", and {main_factors[1]}"
        else:
            main_explanation = f"{level_context} due to current atmospheric conditions"
        
        if explanation_data['trend_explanation'] and 'worsened' in explanation_data['trend_explanation']:
            main_explanation += f". {explanation_data['trend_explanation'].split('.')[0]}"
        
        return main_explanation
    
    def _get_health_context(self, aqi_data: Dict) -> str:
        """Generate health context based on AQI level"""
        
        aqi_value = aqi_data.get('aqi', 0)
        pollutant = aqi_data.get('primary_pollutant', 'PM25')
        
        health_contexts = {
            'good': "Great day for outdoor activities for everyone",
            'moderate': "Unusually sensitive people should consider limiting prolonged outdoor exertion",
            'unhealthy_sensitive': "Sensitive groups should reduce outdoor exertion and avoid prolonged outdoor activities",
            'unhealthy': "Everyone should reduce outdoor exertion and avoid prolonged outdoor activities",
            'very_unhealthy': "Everyone should avoid outdoor exertion and stay indoors if possible"
        }
        
        if aqi_value <= 50:
            return health_contexts['good']
        elif aqi_value <= 100:
            return health_contexts['moderate']
        elif aqi_value <= 150:
            return health_contexts['unhealthy_sensitive']
        elif aqi_value <= 200:
            return health_contexts['unhealthy']
        else:
            return health_contexts['very_unhealthy']
    
    def _get_forecast_insight(self, weather_data: Dict, aqi_data: Dict) -> str:
        """Generate forecast insight based on current conditions"""
        
        wind_speed = weather_data.get('wind_speed', 0)
        temperature = weather_data.get('temperature', 20)
        
        insights = []
        
        # Wind forecast impact
        if wind_speed < 3:
            insights.append("Continued calm winds may keep pollution levels elevated")
        elif wind_speed > 10:
            insights.append("Strong winds should help disperse pollutants")
        
        # Temperature trends
        if temperature > 30:
            insights.append("High temperatures may continue to worsen ozone levels")
        
        # Default insight
        if not insights:
            insights.append("Monitor conditions as weather patterns may affect air quality")
        
        return insights[0]
    
    def _detect_inversion(self, weather_data: Dict) -> bool:
        """Detect temperature inversion conditions"""
        
        # Simple inversion detection based on available data
        wind_speed = weather_data.get('wind_speed', 10) or 10
        temperature = weather_data.get('temperature', 20) or 20
        time_of_day = datetime.now().hour
        
        # Conditions favoring inversion
        calm_winds = wind_speed < 2
        nighttime_or_early_morning = time_of_day < 8 or time_of_day > 20
        
        return calm_winds and nighttime_or_early_morning
    
    def _calculate_confidence(self, aqi_data: Dict, weather_data: Dict, 
                            trend_data: Dict = None, location_data: Dict = None) -> float:
        """Calculate confidence score for the explanation"""
        
        confidence = 0.5  # Base confidence
        
        # Data availability increases confidence
        if weather_data:
            confidence += 0.2
        if trend_data:
            confidence += 0.2
        if location_data:
            confidence += 0.1
        
        # Data quality factors
        if weather_data.get('wind_speed') is not None:
            confidence += 0.1
        if weather_data.get('temperature') is not None:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _get_pm25_rules(self) -> Dict:
        """PM2.5 specific explanation rules"""
        return {
            'calm_winds': "Calm winds allow fine particles to accumulate in the atmosphere",
            'high_humidity': "High humidity can enhance secondary particle formation",
            'temperature_inversion': "Temperature inversion traps fine particles near the ground",
            'urban_sources': "Vehicle emissions and urban activities generate fine particles",
            'wildfire': "Wildfire smoke contains high concentrations of fine particles"
        }
    
    def _get_pm10_rules(self) -> Dict:
        """PM10 specific explanation rules"""
        return {
            'dust_event': "Dust storms and high winds raise coarse particle levels",
            'construction': "Construction and road dust contribute to particle pollution",
            'calm_winds': "Light winds allow dust and particles to remain suspended",
            'dry_conditions': "Dry conditions increase dust and particle suspension"
        }
    
    def _get_ozone_rules(self) -> Dict:
        """Ozone specific explanation rules"""
        return {
            'high_temperature': "High temperatures accelerate ozone-forming chemical reactions",
            'sunlight': "Strong sunlight drives photochemical ozone formation",
            'stagnant_air': "Stagnant air allows ozone precursors to accumulate and react",
            'afternoon_peak': "Ozone typically peaks in the afternoon after morning emissions"
        }
    
    def _get_no2_rules(self) -> Dict:
        """NO2 specific explanation rules"""
        return {
            'traffic_rush': "Rush hour traffic increases nitrogen dioxide emissions",
            'calm_winds': "Light winds allow NO2 from vehicles to accumulate",
            'cold_weather': "Cold weather increases vehicle emissions and heating"
        }
    
    def _get_so2_rules(self) -> Dict:
        """SO2 specific explanation rules"""
        return {
            'industrial_sources': "Industrial facilities and power plants emit sulfur dioxide",
            'calm_winds': "Light winds allow SO2 emissions to accumulate locally",
            'coal_burning': "Coal combustion for heating increases SO2 levels"
        }
    
    def _get_co_rules(self) -> Dict:
        """CO specific explanation rules"""
        return {
            'traffic_congestion': "Heavy traffic and vehicle congestion increase carbon monoxide",
            'enclosed_areas': "Poor ventilation in urban areas can trap CO emissions",
            'cold_starts': "Cold vehicle engines produce more carbon monoxide"
        }

# Why Today explainer - import and use in other modules
