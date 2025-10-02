"""
Collectors Package - Safer Skies
Team AURA - NASA Space Apps Challenge 2025
Data collection services for air quality monitoring
"""

# Import main collectors
try:
    from .smart_hourly_collector import SmartHourlyCollector
    from .northamerica_collector import MultiSourceLocationCollector
    from .global_realtime_collector import GlobalRealtimeCollector
    from .forecast_5day_collector import Forecast5DayCollector
    from .lambda_fetch_tempo_files import ProductionTempoFetcher
except ImportError as e:
    # Handle cases where specific collectors might not be available
    # Silently handle missing dependencies for production speed
    pass

__all__ = [
    'SmartHourlyCollector',
    'MultiSourceLocationCollector', 
    'GlobalRealtimeCollector',
    'Forecast5DayCollector',
    'ProductionTempoFetcher'
]
