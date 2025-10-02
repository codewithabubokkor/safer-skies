#!/usr/bin/env python3
"""
Why Today API Endpoint
Simple Flask API for serving "Why Today?" explanations
Ready for frontend integration
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
import os
import sys

# Add backend to path
backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(backend_path)

from processors.why_today_explainer import WhyTodayExplainer
from utils.timezone_handler import NorthAmericaTimezones

# Simple mock Flask app structure (install flask for production)
try:
    from flask import Flask, jsonify, request
    from flask_cors import CORS
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    print("Flask not available. Install with: pip install flask flask-cors")

logger = logging.getLogger(__name__)

class WhyTodayAPIEndpoint:
    """
    Simple API endpoint for Why Today explanations
    Can be integrated into existing API server
    """
    
    def __init__(self):
        self.explainer = WhyTodayExplainer()
        self.timezone_handler = NorthAmericaTimezones()
        
        if FLASK_AVAILABLE:
            self.app = Flask(__name__)
            CORS(self.app)  # Enable CORS for frontend
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
    
    def _load_aqi_data(self, city_name: str = None, lat: float = None, lon: float = None) -> Optional[Dict]:
        """Load AQI data from the current data files"""
        try:
            base_dir = os.getenv('AQI_DATA_PATH', '/app/data/aqi/current')
            
            if lat is not None and lon is not None:
                return self._find_closest_city_data(lat, lon, base_dir)
            
            if city_name:
                return self._find_city_by_name(city_name, base_dir)
            
            return None
            
        except Exception as e:
            logger.error(f"Error loading AQI data: {e}")
            return None
    
    def _find_closest_city_data(self, lat: float, lon: float, base_dir: str) -> Optional[Dict]:
        """Find the closest city data file based on coordinates"""
        import os
        import glob
        
        closest_city = None
        min_distance = float('inf')
        
        # Scan all city directories
        city_dirs = glob.glob(f"{base_dir}/*/")
        
        for city_dir in city_dirs:
            try:
                dir_name = os.path.basename(city_dir.rstrip('/'))
                parts = dir_name.split('_')
                if len(parts) >= 3:
                    city_lat = float(parts[-2])
                    city_lon = float(parts[-1])
                    
                    distance = ((lat - city_lat) ** 2 + (lon - city_lon) ** 2) ** 0.5
                    
                    if distance < min_distance:
                        min_distance = distance
                        closest_city = dir_name
                        
            except (ValueError, IndexError):
                continue
        
        if closest_city and min_distance < 1.0:  # Within ~100km
            aqi_file = f"{base_dir}/{closest_city}/aqi_current.json"
            if os.path.exists(aqi_file):
                with open(aqi_file, 'r') as f:
                    data = json.load(f)
                    data['_distance_km'] = min_distance * 111  # Rough km conversion
                    data['_source_city'] = closest_city
                    return data
        
        return None
    
    def _find_city_by_name(self, city_name: str, base_dir: str) -> Optional[Dict]:
        """Find city data by name"""
        import os
        import glob
        
        city_lower = city_name.lower().replace(' ', '_')
        
        # Scan all city directories
        city_dirs = glob.glob(f"{base_dir}/*/")
        
        for city_dir in city_dirs:
            dir_name = os.path.basename(city_dir.rstrip('/'))
            
            if city_lower in dir_name.lower():
                aqi_file = f"{city_dir}/aqi_current.json"
                if os.path.exists(aqi_file):
                    with open(aqi_file, 'r') as f:
                        data = json.load(f)
                        data['_source_city'] = dir_name
                        return data
        
        return None
    
    def _convert_aqi_format(self, data):
        """Convert AQI data file format to explainer format."""
        aqi_info = data.get('aqi', {}).get('overall', {})
        return {
            'aqi': aqi_info.get('value', 50),  # Explainer expects 'aqi' not 'aqi_value'
            'aqi_category': aqi_info.get('category', 'Good'),
            'primary_pollutant': aqi_info.get('dominant_pollutant', 'PM2.5'),  # Changed to primary_pollutant
            'location_name': data.get('location', {}).get('name', 'Unknown'),
            'lat': data.get('location', {}).get('coordinates', {}).get('lat', 0),
            'lon': data.get('location', {}).get('coordinates', {}).get('lon', 0),
            'timestamp': data.get('timestamp', ''),
            'pollutants': data.get('aqi', {}).get('pollutants', {}),
            'health_message': data.get('health', {}).get('message', '')
        }
    
    def _create_mock_weather_data(self, aqi_data: Dict) -> Dict:
        """Create mock weather data based on AQI data (for now)"""
        return {
            'temperature': 22.0,  # °C
            'humidity': 65.0,     # %
            'wind_speed': 3.5,    # m/s
            'wind_direction': 225, # degrees
            'pressure': 1013.25,  # hPa
            'visibility': 10.0,   # km
            'weather_condition': 'clear'
        }
    
    def _setup_routes(self):
        """Setup Flask routes"""
        
        @self.app.route('/api/why-today/location', methods=['GET'])
        def get_explanation_by_location():
            """Get explanation by coordinates"""
            try:
                lat = float(request.args.get('lat'))
                lon = float(request.args.get('lon'))
                city_name = request.args.get('city_name')  # Optional city name from frontend search
                
                aqi_data = self._load_aqi_data(lat=lat, lon=lon)
                if not aqi_data:
                    raise Exception(f"No AQI data available for location {lat}, {lon}")
                
                display_city = city_name or aqi_data['location']['name']
                
                explainer_aqi_data = self._convert_aqi_format(aqi_data)
                
                weather_data = self._create_mock_weather_data(aqi_data)
                explanation = self.explainer.generate_explanation(
                    explainer_aqi_data, weather_data, 
                    location_data={'city': display_city, 'lat': lat, 'lon': lon}
                )
                
                return jsonify({
                    'success': True,
                    'data': explanation,
                    'location': {'lat': lat, 'lon': lon, 'city': display_city},
                    'source_city': aqi_data.get('_source_city', 'unknown'),
                    'distance_km': round(aqi_data.get('_distance_km', 0), 1),
                    'timestamp': self._get_local_timestamp(lat, lon)
                })
                
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'timestamp': self._get_local_timestamp(lat, lon)
                }), 400
        
        @self.app.route('/api/why-today/city', methods=['GET'])
        def get_explanation_by_city():
            """Get explanation by city name"""
            try:
                city = request.args.get('city')
                state = request.args.get('state', None)
                
                if not city:
                    return jsonify({
                        'success': False,
                        'error': 'City parameter required',
                        'timestamp': self._get_local_timestamp()  # UTC fallback for error
                    }), 400
                
                aqi_data = self._load_aqi_data(city_name=city)
                if not aqi_data:
                    raise Exception(f"No AQI data available for {city}")
                
                explainer_aqi_data = self._convert_aqi_format(aqi_data)
                
                weather_data = self._create_mock_weather_data(aqi_data)
                explanation = self.explainer.generate_explanation(
                    explainer_aqi_data, weather_data, 
                    location_data={'city': city, 'state': state}
                )
                
                lat = aqi_data.get('location', {}).get('coordinates', {}).get('lat')
                lon = aqi_data.get('location', {}).get('coordinates', {}).get('lon')
                
                return jsonify({
                    'success': True,
                    'data': explanation,
                    'city': aqi_data['location']['name'],
                    'state': state,
                    'source_city': aqi_data.get('_source_city', city),
                    'timestamp': self._get_local_timestamp(lat, lon)
                })
                
            except Exception as e:
                lat = lon = None
                if 'aqi_data' in locals() and aqi_data:
                    lat = aqi_data.get('location', {}).get('coordinates', {}).get('lat')
                    lon = aqi_data.get('location', {}).get('coordinates', {}).get('lon')
                
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'timestamp': self._get_local_timestamp(lat, lon)
                }), 400
        
        @self.app.route('/api/why-today/demo', methods=['GET'])
        def get_demo_cards():
            """Get demo cards for testing"""
            try:
                demo_file = os.getenv('DEMO_DATA_PATH', '/app/data/why_today_demo_cards.json')
                if os.path.exists(demo_file):
                    with open(demo_file, 'r') as f:
                        demo_cards = json.load(f)
                    
                    return jsonify({
                        'success': True,
                        'data': demo_cards,
                        'count': len(demo_cards),
                        'timestamp': self._get_local_timestamp()  # UTC for demo
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Demo cards not found',
                        'timestamp': self._get_local_timestamp()
                    }), 404
                    
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e),
                    'timestamp': self._get_local_timestamp()  # UTC for error
                }), 500
        
        @self.app.route('/api/why-today/health', methods=['GET'])
        def health_check():
            """API health check"""
            return jsonify({
                'success': True,
                'service': 'Why Today API',
                'status': 'operational',
                'timestamp': self._get_local_timestamp(),  # UTC for health check
                'version': '1.0.0'
            })
    
    def get_explanation_dict(self, latitude: float, longitude: float) -> Dict:
        """Get explanation as dictionary (for non-Flask usage)"""
        raw_aqi_data = self._load_aqi_data(lat=latitude, lon=longitude)
        if not raw_aqi_data:
            raise Exception(f"No AQI data available for location {latitude}, {longitude}")
        
        aqi_data = self._convert_aqi_format(raw_aqi_data)
        weather_data = self._create_mock_weather_data(raw_aqi_data)
        
        return self.explainer.generate_explanation(
            aqi_data, weather_data,
            location_data={'city': aqi_data.get('location_name'), 'lat': latitude, 'lon': longitude}
        )
    
    def get_city_explanation_dict(self, city_name: str, state: str = None) -> Dict:
        """Get city explanation as dictionary (for non-Flask usage)"""
        raw_aqi_data = self._load_aqi_data(city_name=city_name)
        if not raw_aqi_data:
            raise Exception(f"No AQI data available for {city_name}")
        
        aqi_data = self._convert_aqi_format(raw_aqi_data)
        weather_data = self._create_mock_weather_data(raw_aqi_data)
        
        return self.explainer.generate_explanation(
            aqi_data, weather_data,
            location_data={'city': city_name, 'state': state}
        )
    
    def run_server(self, host='localhost', port=5001, debug=False):
        """Run the Flask development server"""
        if FLASK_AVAILABLE:
            print(f"🌐 Starting Why Today API server...")
            print(f"📍 Base URL: http://{host}:{port}")
            print(f"🔗 Endpoints:")
            print(f"   • GET /api/why-today/location?lat=LAT&lon=LON&city_name=CITY_NAME")
            print(f"   • GET /api/why-today/city?city=CITY&state=STATE")
            print(f"   • GET /api/why-today/demo")
            print(f"   • GET /api/why-today/health")
            print(f"")
            
            self.app.run(host=host, port=port, debug=debug)
        else:
            print("Flask not available. Cannot start server.")


