"""
Processors Package - Safer Skies
NASA Space Apps Challenge 2025
Data processing and analysis services for air quality monitoring
"""

# Import main processors
try:
    from .alert_engine import AirQualityAlertEngine
    from .why_today_explainer import WhyTodayExplainer
    from .aqi_calculator import EPAAQICalculator
    from .forecast_aqi_calculator import ForecastAQICalculator
    from .three_source_fusion import ThreeSourceFusionEngine
    from .fusion_bias_corrector import ProductionFusionEngine
    from .trend_processor import TrendProcessor
    # TrendIntegrator temporarily disabled
    # from .trend_integration import TrendIntegrator
except ImportError as e:
    # Handle cases where specific processors might not be available
    pass  # Silently handle missing processors

__all__ = [
    'AirQualityAlertEngine',
    'WhyTodayExplainer',
    'EPAAQICalculator',
    'ForecastAQICalculator', 
    'ThreeSourceFusionEngine',
    'ProductionFusionEngine',
    'TrendProcessor',
    # 'TrendIntegrator'  # Temporarily disabled
]
