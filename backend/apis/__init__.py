"""
APIs Package
============
Flask API endpoints for NASA Space Apps Challenge 2025 - Safer Skies

This package provides unified REST API endpoints through smart_location_api.py
which consolidates all air quality data services including:
- Real-time air quality data
- 5-day forecasts
- Location-based AQI queries
- Air quality alerts management
- Trend analysis
- "Why Today" explanations

For standalone usage, individual API modules are available but not auto-imported
to avoid Flask route conflicts.
"""

# Only import essential non-Flask components to avoid route conflicts
try:
    from .smart_data_manager import SmartDataManager
except ImportError as e:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"Some API services not available: {e}")
    SmartDataManager = None

__all__ = ['SmartDataManager']
__version__ = '1.0.0'
