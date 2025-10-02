#!/usr/bin/env python3
"""
🔮 5-Day Forecast API Endpoint
Serves hourly and daily forecast data for frontend Day Planner integration
"""

import json
import pandas as pd
import os
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from typing import Dict, List, Optional
import logging
import sys

# Add backend to path for timezone handler
backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(backend_path)
from utils.timezone_handler import NorthAmericaTimezones



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import smart data manager for automatic collection (after logger setup)
try:
    from apis.smart_data_manager import smart_data_manager
    SMART_DATA_AVAILABLE = True
    logger.info("✅ Smart data manager available")
except ImportError as e:
    SMART_DATA_AVAILABLE = False
    logger.warning(f"⚠️ Smart data manager not available: {e}")

class ForecastAPI:
    """5-Day Air Quality Forecast API"""
    
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)  # Enable CORS for frontend
        
        self.forecast_base_dir = os.getenv('FORECAST_DATA_DIR', '/app/backend/results/forecast_5day')
        
        # Timezone handler for local time conversion
        self.timezone_handler = NorthAmericaTimezones()
        
        self._setup_routes()
    
    def _get_local_timestamp(self, lat: float = None, lon: float = None) -> str:
        """Get timezone-aware timestamp based on coordinates or UTC"""
        try:
            utc_now = datetime.now(timezone.utc)
            
            if lat is not None and lon is not None:
                timezone_str = self.timezone_handler.get_timezone_for_coordinates(lat, lon)
                local_time = self.timezone_handler.utc_to_local(utc_now, timezone_str)
                return local_time.isoformat()
            
            # Fallback to UTC
            return utc_now.isoformat()
            
        except Exception as e:
            logger.error(f"Error getting local timestamp: {e}")
            return datetime.now(timezone.utc).isoformat()
            
    def _convert_forecast_times_to_local(self, forecast_data: pd.DataFrame, lat: float, lon: float) -> pd.DataFrame:
        """Convert forecast timestamps from UTC to local timezone"""
        try:
            timezone_str = self.timezone_handler.get_timezone_for_coordinates(lat, lon)
            
            if 'timestamp' in forecast_data.columns:
                forecast_data = forecast_data.copy()
                forecast_data['timestamp'] = pd.to_datetime(forecast_data['timestamp'], utc=True)
                
                import pytz
                local_tz = pytz.timezone(timezone_str)
                forecast_data['timestamp'] = forecast_data['timestamp'].dt.tz_convert(local_tz)
                forecast_data['local_time'] = forecast_data['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S %Z')
            
            return forecast_data
            
        except Exception as e:
            logger.error(f"Error converting forecast times to local: {e}")
            return forecast_data
        
    def _setup_routes(self):
        """Setup Flask routes for forecast API"""
        
        @self.app.route('/api/forecast/location', methods=['GET'])
        def get_forecast_by_location():
            """Get 5-day forecast by coordinates"""
            try:
                lat = float(request.args.get('lat', 0))
                lon = float(request.args.get('lon', 0))
                city_name = request.args.get('city', f"{lat:.3f},{lon:.3f}")
                
                forecast_data = self._load_forecast_by_location(lat, lon, city_name)
                
                if forecast_data is not None and not forecast_data.empty:
                    forecast_data_local = self._convert_forecast_times_to_local(forecast_data, lat, lon)
                    
                    formatted_data = self._format_for_day_planner(forecast_data_local, lat, lon)
                    
                    return jsonify({
                        'success': True,
                        'data': formatted_data,
                        'location': {'lat': lat, 'lon': lon},
                        'forecast_hours': len(formatted_data.get('hourly', [])),
                        'forecast_days': len(formatted_data.get('daily', [])),
                        'timestamp': self._get_local_timestamp(lat, lon)
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': 'No forecast data available for this location',
                        'timestamp': self._get_local_timestamp(lat, lon)
                    }), 404
                    
            except Exception as e:
                logger.error(f"Error getting forecast by location: {e}")
                return jsonify({
                    'success': False,
                    'error': 'Internal server error',
                    'timestamp': self._get_local_timestamp()  # UTC fallback for error
                }), 500
        
        @self.app.route('/api/forecast/city', methods=['GET'])
        def get_forecast_by_city():
            """Get 5-day forecast by city name"""
            try:
                city = request.args.get('city', '').lower()
                
                city_coords = {
                    'new york': (40.7128, -74.0060),
                    'los angeles': (34.0522, -118.2437),
                    'chicago': (41.8781, -87.6298),
                    'philadelphia': (39.9526, -75.1652),
                    'boston': (42.3601, -71.0589),
                    'washington': (38.8951, -77.0364),
                    'washington dc': (38.8951, -77.0364),
                    'rajshahi': (24.364, 88.624)
                }
                
                if city in city_coords:
                    lat, lon = city_coords[city]
                    forecast_data = self._load_forecast_by_location(lat, lon, city)
                    
                    if forecast_data is not None and not forecast_data.empty:
                        forecast_data_local = self._convert_forecast_times_to_local(forecast_data, lat, lon)
                        
                        formatted_data = self._format_for_day_planner(forecast_data_local, lat, lon)
                        
                        return jsonify({
                            'success': True,
                            'data': formatted_data,
                            'city': city.title(),
                            'location': {'lat': lat, 'lon': lon},
                            'forecast_hours': len(formatted_data.get('hourly', [])),
                            'forecast_days': len(formatted_data.get('daily', [])),
                            'timestamp': self._get_local_timestamp(lat, lon)
                        })
                
                return jsonify({
                    'success': False,
                    'error': f'No forecast data available for {city}',
                    'timestamp': self._get_local_timestamp()  # UTC fallback
                }), 404
                
            except Exception as e:
                logger.error(f"Error getting forecast by city: {e}")
                return jsonify({
                    'success': False,
                    'error': 'Internal server error',
                    'timestamp': self._get_local_timestamp()  # UTC fallback
                }), 500
        
        @self.app.route('/api/forecast/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'service': '5-Day Forecast API',
                'timestamp': self._get_local_timestamp()  # UTC for health check
            })
    
    def _load_forecast_by_location(self, lat: float, lon: float, city_name: str = None) -> Optional[pd.DataFrame]:
        """Load forecast data with smart collection if needed"""
        
        if SMART_DATA_AVAILABLE:
            try:
                import asyncio
                
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        logger.info(f"🔄 Checking existing forecast data for {city_name or f'{lat:.3f},{lon:.3f}'}")
                        has_data, existing_data = smart_data_manager.check_forecast_data_exists(lat, lon, city_name)
                        if has_data:
                            return existing_data
                        else:
                            logger.warning(f"⚠️ No forecast data - would trigger collection if not in async context")
                    else:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            forecast_data = loop.run_until_complete(
                                smart_data_manager.ensure_forecast_data(lat, lon, city_name)
                            )
                            if forecast_data is not None:
                                logger.info(f"✅ Smart data manager provided forecast data for {city_name or f'{lat:.3f},{lon:.3f}'}")
                                return forecast_data
                        finally:
                            loop.close()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        forecast_data = loop.run_until_complete(
                            smart_data_manager.ensure_forecast_data(lat, lon, city_name)
                        )
                        if forecast_data is not None:
                            logger.info(f"✅ Smart data manager provided forecast data for {city_name or f'{lat:.3f},{lon:.3f}'}")
                            return forecast_data
                    finally:
                        loop.close()
                        
            except Exception as e:
                logger.warning(f"⚠️ Smart data manager failed: {e} - falling back to direct database query")
        
        # Fallback to direct database query
        return self._load_forecast_from_database_direct(lat, lon)
    
    def _load_forecast_from_database_direct(self, lat: float, lon: float) -> Optional[pd.DataFrame]:
        """Direct database query without smart management"""
        try:
            import pandas as pd
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from utils.database_connection import get_db_connection
            
            conn = get_db_connection()
            
            query = """
            SELECT * FROM forecast_5day_data 
            WHERE location_lat BETWEEN %s - 0.1 AND %s + 0.1
            AND location_lng BETWEEN %s - 0.1 AND %s + 0.1
            AND forecast_timestamp >= NOW()
            ORDER BY ABS(location_lat - %s) + ABS(location_lng - %s) ASC,
                     forecast_timestamp ASC
            LIMIT 120
            """
            
            df = pd.read_sql(query, conn, params=[lat, lat, lon, lon, lat, lon])
            conn.close()
            
            if df.empty:
                logger.warning(f"No forecast data found in database for coordinates ({lat}, {lon})")
                return None
                
            logger.info(f"Found {len(df)} forecast records from database for ({lat}, {lon})")
            return df
            
        except ImportError:
            logger.warning("MySQL connector not available - falling back to file system")
            return self._load_forecast_from_files(lat, lon)
        except Exception as e:
            logger.error(f"Database query failed: {e} - falling back to file system")
            return self._load_forecast_from_files(lat, lon)
    
    def _load_forecast_from_files(self, lat: float, lon: float) -> Optional[pd.DataFrame]:
        """Fallback: Load forecast data from Parquet files"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            forecast_dir = f"{self.forecast_base_dir}/{today}"
            
            if not os.path.exists(forecast_dir):
                logger.warning(f"No forecast directory for {today}")
                return None
            
            parquet_files = [f for f in os.listdir(forecast_dir) if f.endswith('.parquet')]
            
            if not parquet_files:
                logger.warning(f"No parquet files found in {forecast_dir}")
                return None
            
            best_match_file = None
            min_distance = float('inf')
            
            for filename in parquet_files:
                filepath = os.path.join(forecast_dir, filename)
                try:
                    df = pd.read_parquet(filepath)
                    
                    if 'multi_city' in filename or len(df['location_name'].unique() if 'location_name' in df.columns else []) > 1:
                        # Multi-city file - find closest city
                        if 'latitude' in df.columns and 'longitude' in df.columns:
                            df['distance'] = ((df['latitude'] - lat) ** 2 + (df['longitude'] - lon) ** 2) ** 0.5
                            min_file_distance = df['distance'].min()
                            if min_file_distance < min_distance:
                                min_distance = min_file_distance
                                best_match_file = filepath
                                
                    else:
                        # Single location file - check if coordinates match (within ~0.1 degree tolerance)
                        if 'latitude' in df.columns and 'longitude' in df.columns:
                            file_lat = df['latitude'].iloc[0]
                            file_lon = df['longitude'].iloc[0]
                            distance = ((file_lat - lat) ** 2 + (file_lon - lon) ** 2) ** 0.5
                            if distance < min_distance:
                                min_distance = distance
                                best_match_file = filepath
                                
                except Exception as e:
                    logger.warning(f"Error reading {filename}: {e}")
                    continue
            
            if best_match_file:
                logger.info(f"Loading forecast from {best_match_file} (distance: {min_distance:.4f})")
                df = pd.read_parquet(best_match_file)
                
                if 'multi_city' in best_match_file:
                    if 'latitude' in df.columns and 'longitude' in df.columns:
                        df['distance'] = ((df['latitude'] - lat) ** 2 + (df['longitude'] - lon) ** 2) ** 0.5
                        closest_city = df.loc[df['distance'].idxmin(), 'location_name']
                        df = df[df['location_name'] == closest_city].copy()
                        logger.info(f"Filtered to closest city: {closest_city}")
                
                return df
            
            logger.warning(f"No forecast files found in {forecast_dir}")
            return None
            
        except Exception as e:
            logger.error(f"Error loading forecast data: {e}")
            return None
    
    def _format_for_day_planner(self, df: pd.DataFrame, lat: float = None, lon: float = None) -> Dict:
        """Format forecast data for Day Planner frontend consumption"""
        try:
            # Ensure we have a datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                if 'timestamp' in df.columns:
                    df.set_index('timestamp', inplace=True)
                else:
                    start_time = datetime.now(timezone.utc)
                    df.index = pd.date_range(start=start_time, periods=len(df), freq='H')
            
            # Hourly data for detailed view
            hourly_data = []
            for i, (timestamp, row) in enumerate(df.iterrows()):
                hourly_data.append({
                    'hour': i,
                    'timestamp': timestamp.isoformat(),
                    'datetime': timestamp.strftime('%Y-%m-%d %H:%M'),
                    'aqi': {
                        'overall': int(row.get('overall_aqi', row.get('O3_aqi', 50))),
                        'category': row.get('aqi_category', 'Good'),
                        'dominant_pollutant': row.get('dominant_pollutant', 'O3'),
                        'pollutants': {
                            'O3': {'aqi': int(row.get('O3_aqi', 0)), 'concentration': round(row.get('O3_ppb', 0), 1), 'units': 'ppb'},
                            'NO2': {'aqi': int(row.get('NO2_aqi', 0)), 'concentration': round(row.get('NO2_ppb', 0), 1), 'units': 'ppb'},
                            'SO2': {'aqi': int(row.get('SO2_aqi', 0)), 'concentration': round(row.get('SO2_ppb', 0), 1), 'units': 'ppb'},
                            'CO': {'aqi': int(row.get('CO_aqi', 0)), 'concentration': round(row.get('CO_ppm', 0), 2), 'units': 'ppm'},
                            'PM25': {'aqi': int(row.get('PM25_aqi', 0)), 'concentration': round(row.get('PM25_ugm3', 0), 1), 'units': 'μg/m³'}
                        }
                    },
                    'weather': {
                        'temperature': round(row.get('T2M_celsius', 20), 1),
                        'wind_speed': round(row.get('WIND_SPEED_ms', 3), 1),
                        'wind_direction': int(row.get('WIND_DIRECTION_deg', 180)),
                        'humidity': round(row.get('relative_humidity_2m', 60), 1),
                        'precipitation': round(row.get('TPREC_mm', 0), 1),
                        'cloud_cover': round(row.get('CLDTT_percent', 50), 1)
                    }
                })
            
            # Daily summaries (24-hour periods)
            daily_data = []
            for day in range(5):  # 5 days
                day_start = day * 24
                day_end = min((day + 1) * 24, len(hourly_data))
                day_hours = hourly_data[day_start:day_end]
                
                if day_hours:
                    day_aqis = [h['aqi']['overall'] for h in day_hours]
                    day_temps = [h['weather']['temperature'] for h in day_hours]
                    
                    daily_data.append({
                        'day': day,
                        'date': day_hours[0]['timestamp'][:10],  # YYYY-MM-DD
                        'date_formatted': pd.to_datetime(day_hours[0]['timestamp']).strftime('%a, %b %d'),
                        'aqi': {
                            'max': max(day_aqis),
                            'min': min(day_aqis),
                            'avg': round(sum(day_aqis) / len(day_aqis)),
                            'category': self._get_aqi_category(max(day_aqis))
                        },
                        'weather': {
                            'temp_max': round(max(day_temps), 1),
                            'temp_min': round(min(day_temps), 1),
                            'temp_avg': round(sum(day_temps) / len(day_temps), 1)
                        },
                        'hourly_count': len(day_hours)
                    })
            
            return {
                'hourly': hourly_data,
                'daily': daily_data,
                'summary': {
                    'total_hours': len(hourly_data),
                    'total_days': len(daily_data),
                    'data_quality': 'excellent' if len(hourly_data) >= 120 else 'partial',
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error formatting forecast data: {e}")
            return {'hourly': [], 'daily': [], 'summary': {'error': str(e)}}
    
    def _get_aqi_category(self, aqi: int) -> str:
        """Get AQI category from AQI value"""
        if aqi <= 50:
            return 'Good'
        elif aqi <= 100:
            return 'Moderate'
        elif aqi <= 150:
            return 'Unhealthy for Sensitive Groups'
        elif aqi <= 200:
            return 'Unhealthy'
        elif aqi <= 300:
            return 'Very Unhealthy'
        else:
            return 'Hazardous'
    
    def run_server(self, host='localhost', port=5006, debug=False):
        """Run the Flask server"""
        print(f"🔮 Starting 5-Day Forecast API server...")
        print(f"📍 Base URL: http://{host}:{port}")
        print(f"🔗 Endpoints:")
        print(f"   • GET /api/forecast/location?lat=LAT&lon=LON")
        print(f"   • GET /api/forecast/city?city=CITY")
        print(f"   • GET /api/forecast/health")
        print(f"\n📊 Serving 120-hour forecasts with AQI + weather data")
        self.app.run(host=host, port=port, debug=debug)

if __name__ == "__main__":
    api = ForecastAPI()
    api.run_server()
