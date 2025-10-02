#!/usr/bin/env python3
"""
Alert Registration API Endpoint for Safer Skies
Team AURA - NASA Space Apps Challenge 2025

Integrates frontend alert setup with smart location collection optimization.
This endpoint receives alert data from the frontend and registers it for optimization.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import os
import uuid
import json
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from processors.location_optimizer import SmartLocationOptimizer
from utils.database_connection import get_db_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

optimizer = SmartLocationOptimizer()

@app.route('/api/alerts/register', methods=['POST'])
def register_alert():
    try:
        alert_data = request.get_json()
        
        if not alert_data:
            return jsonify({
                'success': False,
                'error': 'No alert data provided'
            }), 400
        
        user_details = alert_data.get('userDetails', {})
        locations = alert_data.get('locations', [])
        
        if not user_details.get('email'):
            return jsonify({
                'success': False,
                'error': 'User email is required'
            }), 400
            
        if not locations:
            return jsonify({
                'success': False,
                'error': 'At least one location is required'
            }), 400
        
        result = process_alert_registration(alert_data)
        
        if result['success']:
            optimizer_success = False
            optimizer_error = None
            
            if not result.get('updated'):  # Only for new alerts
                try:
                    optimizer.register_user_alert(alert_data)
                    optimizer_success = True
                    logger.info("✅ Successfully registered with SmartLocationOptimizer")
                except Exception as e:
                    optimizer_error = str(e)
                    logger.warning(f"⚠️ SmartLocationOptimizer failed (non-critical): {e}")
            else:
                logger.info("🔄 Skipping optimizer registration for alert update")
            
            action_message = f'Alert updated' if result.get('updated') else f'Alert registered for {len(locations)} locations'
            
            response_data = {
                'success': True,
                'message': action_message,
                'user_id': result['user_id'],
                'alert_ids': result['alert_ids'],
                'locations_count': len(locations),
                'database_inserts': result['database_inserts'],
                'updated': result.get('updated', False),
                'optimizer_status': {
                    'success': optimizer_success,
                    'error': optimizer_error
                }
            }
            
            return jsonify(response_data)
        else:
            return jsonify({
                'success': False,
                'error': result['error']
            }), 500
            
    except Exception as e:
        logger.error(f"❌ Error registering alert: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def process_alert_registration(alert_data):
    """Process complete alert registration with database integration"""
    try:
        conn = get_db_connection()
        if not conn:
            return {'success': False, 'error': 'Database connection failed'}
        
        cursor = conn.cursor()
        
        user_details = alert_data.get('userDetails', {})
        locations = alert_data.get('locations', [])
        threshold = alert_data.get('threshold', {})
        notifications = alert_data.get('notifications', {})
        health_conditions = alert_data.get('healthConditions', [])
        quiet_hours = alert_data.get('quietHours', {})
        alert_types = alert_data.get('alertTypes', {})
        editing_alert_id = alert_data.get('editing_alert_id')  # Check if we're editing an existing alert
        
        pollutants = threshold.get('pollutants', alert_data.get('pollutants', ['all']))
        
        user_id = str(uuid.uuid4())
        alert_ids = []
        database_inserts = []
        
        if editing_alert_id:
            logger.info(f"🔄 Updating existing alert: {editing_alert_id}")
            return process_alert_update(alert_data, editing_alert_id)
        
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (user_details.get('email'),))
        existing_user = cursor.fetchone()
        
        if existing_user:

            user_id = existing_user[0]
            user_query = """
                UPDATE users SET 
                    name = %s,
                    timezone = %s,
                    last_active = NOW()
                WHERE email = %s
            """
            cursor.execute(user_query, (
                user_details.get('name', ''),
                user_details.get('timezone', 'UTC'),
                user_details.get('email', '')
            ))
            database_inserts.append(f"User updated: {user_details.get('email')}")
        else:

            user_query = """
                INSERT INTO users (user_id, name, email, timezone, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """
            cursor.execute(user_query, (
                user_id,
                user_details.get('name', ''),
                user_details.get('email', ''),
                user_details.get('timezone', 'UTC')
            ))
            database_inserts.append(f"User created: {user_details.get('email')}")
        

        

        for location in locations:
            alert_id = str(uuid.uuid4())
            alert_ids.append(alert_id)
            
            coordinates = location.get('coordinates', [0, 0])
            lat = coordinates[1] if len(coordinates) > 1 else 0
            lng = coordinates[0] if len(coordinates) > 0 else 0
            location_name = location.get('name', location.get('displayName', 'Unknown Location'))
            
            clean_city = clean_location_name(location_name)
            
            alert_query = """
                INSERT INTO alerts (
                    alert_id, user_id, alert_type, location_city,
                    location_lat, location_lng, pollutant, aqi_value,
                    alert_level, frequency, threshold_type, message, status,
                    created_at, expires_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), DATE_ADD(NOW(), INTERVAL 1 YEAR))
            """
            
            cursor.execute(alert_query, (
                alert_id,
                user_id,
                json.dumps(alert_types) if alert_types else 'immediate',
                clean_city,
                lat,  # Fixed: use extracted lat
                lng,  # Fixed: use extracted lng
                json.dumps(pollutants) if isinstance(pollutants, list) else str(pollutants),
                threshold.get('aqi', 100),  # Fixed: use 'aqi' not 'value'
                threshold.get('alertLevel', 'moderate'),  # Fixed: use alertLevel
                notifications.get('frequency', 'every_time'),  # Fixed: get from notifications
                threshold.get('type', 'category'),  # NEW: Frontend threshold.type
                f"Alert for {clean_city} when AQI exceeds {threshold.get('aqi', 100)}",
                'active'
            ))
            database_inserts.append(f"Alert: {clean_city} (AQI > {threshold.get('aqi', 100)})")
        
        if notifications:
            notif_query = """
                INSERT INTO notification_preferences (
                    user_id, email, push, web_push, sms, quiet_hours_enabled,
                    quiet_hours_start, quiet_hours_end, daily_summary, forecast_warnings, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    email = VALUES(email),
                    push = VALUES(push),
                    web_push = VALUES(web_push),
                    quiet_hours_enabled = VALUES(quiet_hours_enabled),
                    quiet_hours_start = VALUES(quiet_hours_start),
                    quiet_hours_end = VALUES(quiet_hours_end),
                    daily_summary = VALUES(daily_summary),
                    forecast_warnings = VALUES(forecast_warnings),
                    updated_at = NOW()
            """
            
            cursor.execute(notif_query, (
                user_id,
                notifications.get('email', True),
                notifications.get('mobile_push', False),
                notifications.get('web_push', True),
                False,  # SMS default false
                quiet_hours.get('enabled', True),  # NEW: Frontend quietHours.enabled
                quiet_hours.get('start', '22:00'),
                quiet_hours.get('end', '07:00'),
                alert_types.get('daily_summary', False),  # NEW: From alertTypes
                alert_types.get('forecast', True)  # NEW: From alertTypes
            ))
            database_inserts.append("Notification preferences set")
        
        if health_conditions:
            cursor.execute("DELETE FROM user_health_conditions WHERE user_id = %s", (user_id,))
            
            condition_names = []
            for condition in health_conditions:
                if isinstance(condition, dict):
                    condition_name = condition.get('condition', 'unknown')
                    severity = condition.get('severity', 'moderate')
                else:
                    condition_name = str(condition)
                    severity = 'moderate'
                    
                health_query = """
                    INSERT INTO user_health_conditions (user_id, condition_name, severity, created_at)
                    VALUES (%s, %s, %s, NOW())
                """
                cursor.execute(health_query, (user_id, condition_name, severity))
                condition_names.append(condition_name)
            
            database_inserts.append(f"Health conditions: {', '.join(condition_names)}")
        
        for location in locations:
            coordinates = location.get('coordinates', [0, 0])
            lat = coordinates[1] if len(coordinates) > 1 else 0
            lng = coordinates[0] if len(coordinates) > 0 else 0
            
            if lat is None or lng is None or lat == 0 or lng == 0:
                print(f"⚠️ Warning: Invalid coordinates for location {location}: lat={lat}, lng={lng}")
                lat = 23.8103 if lat is None or lat == 0 else lat  # Default to Dhaka
                lng = 90.4125 if lng is None or lng == 0 else lng
            
            location_name = location.get('name', location.get('displayName', 'Unknown Location'))
            clean_city = clean_location_name(location_name)
            location_key = f"{lat},{lng}"
            
            alert_location_query = """
                INSERT INTO alert_locations (
                    user_email, location_key, city, latitude, longitude,
                    alert_threshold, active, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                ON DUPLICATE KEY UPDATE
                    alert_threshold = VALUES(alert_threshold),
                    active = VALUES(active)
            """
            
            cursor.execute(alert_location_query, (
                user_details.get('email'),
                location_key,
                clean_city,
                lat,
                lng,
                threshold.get('aqi', 100)  # Fixed: use 'aqi' not 'value'
            ))
            
            search_freq_query = """
                INSERT INTO search_frequency (
                    location_key, city, latitude, longitude,
                    search_count, user_demand_score, last_searched
                ) VALUES (%s, %s, %s, %s, 1, 1.5, NOW())
                ON DUPLICATE KEY UPDATE
                    search_count = search_count + 1,
                    user_demand_score = user_demand_score + 0.5,
                    last_searched = NOW()
            """
            
            cursor.execute(search_freq_query, (
                location_key,
                clean_city,
                lat,  # Fixed: use extracted lat variable
                lng   # Fixed: use extracted lng variable
            ))
            
            database_inserts.append(f"Location tracking: {clean_city}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Alert registration completed for {user_details.get('email')}")
        
        return {
            'success': True,
            'user_id': user_id,
            'alert_ids': alert_ids,
            'database_inserts': database_inserts
        }
        
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
            if 'cursor' in locals():
                cursor.close()
            conn.close()
        
        logger.error(f"❌ Database error in alert registration: {e}")
        return {
            'success': False,
            'error': f'Database error: {str(e)}'
        }

def process_alert_update(alert_data, editing_alert_id):
    """Update an existing alert instead of creating new one"""
    try:
        conn = get_db_connection()
        if not conn:
            return {'success': False, 'error': 'Database connection failed'}
        
        cursor = conn.cursor()
        
        user_details = alert_data.get('userDetails', {})
        locations = alert_data.get('locations', [])
        threshold = alert_data.get('threshold', {})
        notifications = alert_data.get('notifications', {})
        quiet_hours = alert_data.get('quietHours', {})
        alert_types = alert_data.get('alertTypes', {})
        pollutants = threshold.get('pollutants', alert_data.get('pollutants', ['all']))
        
        cursor.execute("SELECT user_id FROM users WHERE email = %s", (user_details.get('email'),))
        user_row = cursor.fetchone()
        if not user_row:
            cursor.close()
            conn.close()
            return {'success': False, 'error': 'User not found'}
        
        user_id = user_row[0]
        
        cursor.execute("SELECT user_id FROM alerts WHERE alert_id = %s", (editing_alert_id,))
        alert_row = cursor.fetchone()
        if not alert_row or alert_row[0] != user_id:
            cursor.close()
            conn.close()
            return {'success': False, 'error': 'Alert not found or unauthorized'}
        
        location = locations[0] if locations else {}
        coordinates = location.get('coordinates', [location.get('lng', 0), location.get('lat', 0)])
        lat = coordinates[1] if len(coordinates) > 1 else location.get('lat', 0)
        lng = coordinates[0] if len(coordinates) > 0 else location.get('lng', 0)
        location_name = location.get('city', location.get('name', location.get('displayName', 'Updated Location')))
        clean_city = clean_location_name(location_name)
        
        update_query = """
            UPDATE alerts SET
                location_city = %s,
                location_lat = %s,
                location_lng = %s,
                aqi_value = %s,
                alert_level = %s,
                pollutant = %s,
                message = %s
            WHERE alert_id = %s
        """
        
        cursor.execute(update_query, (
            clean_city,
            lat,
            lng,
            threshold.get('aqi', threshold.get('value', 100)),
            threshold.get('alertLevel', threshold.get('category', 'moderate')),
            json.dumps(pollutants) if isinstance(pollutants, list) else str(pollutants),
            f"Updated alert for {clean_city} when AQI exceeds {threshold.get('aqi', threshold.get('value', 100))}",
            editing_alert_id
        ))
        
        if notifications:
            notif_update_query = """
                UPDATE notification_preferences SET
                    email = %s,
                    push = %s,
                    web_push = %s,
                    quiet_hours_enabled = %s,
                    quiet_hours_start = %s,
                    quiet_hours_end = %s,
                    daily_summary = %s,
                    forecast_warnings = %s,
                    updated_at = NOW()
                WHERE user_id = %s
            """
            
            cursor.execute(notif_update_query, (
                notifications.get('email', True),
                notifications.get('mobile_push', False),
                notifications.get('web_push', True),
                quiet_hours.get('enabled', True),
                quiet_hours.get('start', '22:00'),
                quiet_hours.get('end', '07:00'),
                alert_types.get('daily_summary', False),
                alert_types.get('forecast', True),
                user_id
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Alert {editing_alert_id} updated successfully for user: {user_details.get('email')}")
        
        return {
            'success': True,
            'user_id': user_id,
            'alert_ids': [editing_alert_id],
            'database_inserts': [f"Updated alert: {clean_city}"],
            'updated': True  # Flag to indicate this was an update, not a new alert
        }
        
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
            if 'cursor' in locals():
                cursor.close()
            conn.close()
        
        logger.error(f"❌ Database error in alert update: {e}")
        return {
            'success': False,
            'error': f'Database error: {str(e)}'
        }

@app.route('/api/alerts/user-by-email/<email>', methods=['GET'])
def get_user_alerts_by_email(email):
    """Get all alerts for a user by email address"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT user_id, name FROM users WHERE email = %s", (email,))
        user_row = cursor.fetchone()
        
        if not user_row:
            cursor.close()
            conn.close()
            return jsonify({
                'success': True,
                'user_exists': False,
                'alerts': [],
                'count': 0,
                'message': 'No user found with this email'
            })
        
        user_id, user_name = user_row
        
        query = """
        SELECT 
            a.alert_id,
            a.user_id,
            a.location_city,
            a.location_lat,
            a.location_lng,
            a.aqi_value,
            a.alert_level,
            a.pollutant,
            a.status,
            a.created_at,
            a.timestamp,
            np.email,
            np.push,
            np.web_push,
            np.sms,
            np.quiet_hours_enabled,
            np.quiet_hours_start,
            np.quiet_hours_end,
            np.daily_summary,
            np.forecast_warnings
        FROM alerts a
        LEFT JOIN notification_preferences np ON a.user_id = np.user_id
        WHERE a.user_id = %s
        ORDER BY a.created_at DESC
        """
        
        cursor.execute(query, (user_id,))
        alerts = cursor.fetchall()
        
        alert_list = []
        for alert in alerts:
            alert_dict = {
                'alert_id': alert[0],
                'user_id': alert[1],
                'location_key': f"{alert[3]},{alert[4]}" if alert[3] and alert[4] else None,  # Create from lat,lng
                'city': alert[2],
                'latitude': float(alert[3]) if alert[3] else None,
                'longitude': float(alert[4]) if alert[4] else None,
                'aqi_threshold': alert[5],
                'alert_level': alert[6],
                'pollutants': alert[7],
                'is_active': alert[8] == 'active',
                'created_at': alert[9].isoformat() if alert[9] else None,
                'updated_at': alert[10].isoformat() if alert[10] else None,
                'notification_preferences': {
                    'email': alert[11] if alert[11] is not None else True,
                    'push': bool(alert[12]) if alert[12] is not None else False,
                    'web_push': bool(alert[13]) if alert[13] is not None else True,
                    'sms': bool(alert[14]) if alert[14] is not None else False,
                    'quiet_hours_enabled': bool(alert[15]) if alert[15] is not None else True,
                    'quiet_hours_start': alert[16] if alert[16] else '22:00',
                    'quiet_hours_end': alert[17] if alert[17] else '07:00',
                    'daily_summary_enabled': bool(alert[18]) if alert[18] is not None else False,
                    'forecast_warnings': bool(alert[19]) if alert[19] is not None else True,
                    'timezone': 'UTC'  # Default timezone since it's not in the table
                }
            }
            alert_list.append(alert_dict)
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Retrieved {len(alert_list)} alerts for user: {email}")
        
        return jsonify({
            'success': True,
            'user_exists': True,
            'user_name': user_name,
            'user_id': user_id,
            'alerts': alert_list,
            'count': len(alert_list),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        
        logger.error(f"❌ Error fetching alerts for email {email}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/alerts/delete/<alert_id>', methods=['DELETE'])
def delete_user_alert(alert_id):
    """Delete a specific alert by alert_id"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT user_id FROM alerts WHERE alert_id = %s", (alert_id,))
        alert_row = cursor.fetchone()
        
        if not alert_row:
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': 'Alert not found'
            }), 404
        
        user_id = alert_row[0]
        
        cursor.execute("SELECT location_city, location_lat, location_lng FROM alerts WHERE alert_id = %s", (alert_id,))
        alert_details = cursor.fetchone()
        
        cursor.execute("DELETE FROM alerts WHERE alert_id = %s", (alert_id,))
        
        if alert_details:
            location_key = f"{alert_details[1]},{alert_details[2]}" if alert_details[1] and alert_details[2] else None
            if location_key:
                cursor.execute("DELETE FROM alert_locations WHERE location_key = %s", (location_key,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Alert {alert_id} deleted successfully for user {user_id}")
        
        return jsonify({
            'success': True,
            'message': 'Alert deleted successfully',
            'alert_id': alert_id
        })
        
    except Exception as e:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        
        logger.error(f"❌ Error deleting alert {alert_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def clean_location_name(location_name):
    """Clean location name like MapboxGeolocationService"""
    if not location_name:
        return ''
    
    parts = [part.strip() for part in location_name.split(',')]
    unique_parts = []
    seen = set()
    
    for part in parts:
        if part.lower() not in seen:
            unique_parts.append(part)
            seen.add(part.lower())
    
    return ', '.join(unique_parts[:2]) if len(unique_parts) >= 2 else unique_parts[0] if unique_parts else location_name

@app.route('/api/search/register', methods=['POST'])
def register_search():
    """Register user search activity for optimization"""
    try:
        search_data = request.get_json()
        
        city = search_data.get('city', '')
        latitude = search_data.get('latitude')
        longitude = search_data.get('longitude')
        
        if not all([city, latitude, longitude]):
            return jsonify({
                'success': False,
                'error': 'City, latitude, and longitude are required'
            }), 400
        
        success = optimizer.register_search(city, float(latitude), float(longitude))
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Search registered for {city}'
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to register search'
            }), 500
            
    except Exception as e:
        logger.error(f"❌ Error registering search: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/optimization/stats', methods=['GET'])
def get_optimization_stats():
    """Get current optimization statistics"""
    try:
        stats = optimizer.get_collection_statistics()
        priority_locations = optimizer.get_priority_locations(20)  # Top 20
        
        return jsonify({
            'success': True,
            'statistics': stats,
            'top_priority_locations': [
                {
                    'city': loc.city,
                    'location_key': loc.location_key,
                    'priority_score': loc.priority_score,
                    'alert_count': loc.alert_count,
                    'search_frequency': loc.search_frequency,
                    'last_collected': loc.last_collected.isoformat() if loc.last_collected else None
                }
                for loc in priority_locations
            ]
        })
        
    except Exception as e:
        logger.error(f"❌ Error getting stats: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/optimization/priority-locations', methods=['GET'])
def get_priority_locations():
    """Get priority locations for collection"""
    try:
        limit = request.args.get('limit', 100, type=int)
        priority_locations = optimizer.get_priority_locations(limit)
        
        return jsonify({
            'success': True,
            'locations': [
                {
                    'location_key': loc.location_key,
                    'city': loc.city,
                    'latitude': loc.latitude,
                    'longitude': loc.longitude,
                    'priority_score': loc.priority_score,
                    'alert_count': loc.alert_count,
                    'search_frequency': loc.search_frequency,
                    'user_count': loc.user_count,
                    'collection_frequency': loc.collection_frequency,
                    'last_collected': loc.last_collected.isoformat() if loc.last_collected else None,
                    'should_collect': optimizer.should_collect_location(loc.location_key)
                }
                for loc in priority_locations
            ],
            'total_count': len(priority_locations)
        })
        
    except Exception as e:
        logger.error(f"❌ Error getting priority locations: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/optimization/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        stats = optimizer.get_collection_statistics()
        return jsonify({
            'success': True,
            'status': 'healthy',
            'service': 'Safer Skies Smart Location Optimizer',
            'team': 'Team AURA',
            'optimization_active': True,
            'total_locations': stats.get('total_unique_locations', 0),
            'active_users': stats.get('active_users', 0)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/api/alerts/test', methods=['POST'])
def test_alert_endpoint():
    """Test endpoint for creating sample EPA alerts"""
    try:
        data = request.get_json()
        location_name = data.get('location', 'Test Location')
        
        test_locations = {
            'New York': {
                'lat': 40.7128,
                'lng': -74.0060,
                'aqi': 85,
                'level': 'moderate',
                'pollutant': 'PM2.5'
            },
            'Rajshahi': {
                'lat': 24.3745,
                'lng': 88.6042,
                'aqi': 132,
                'level': 'unhealthy_sensitive',
                'pollutant': 'PM2.5'
            },
            'Test Location': {
                'lat': 40.0,
                'lng': -74.0,
                'aqi': 75,
                'level': 'moderate',
                'pollutant': 'PM2.5'
            }
        }
        
        location_data = test_locations.get(location_name, test_locations['Test Location'])
        
        alert_response = {
            'success': True,
            'alert': {
                'alert_id': f'test_alert_{uuid.uuid4().hex[:8]}',
                'location': {
                    'city': location_name,
                    'lat': location_data['lat'],
                    'lng': location_data['lng']
                },
                'aqi_value': location_data['aqi'],
                'alert_level': location_data['level'],
                'pollutant': location_data['pollutant'],
                'epa_message': f"Air quality in {location_name} is {location_data['level'].replace('_', ' ')}. Current AQI: {location_data['aqi']}",
                'timestamp': datetime.now().isoformat(),
                'recommendations': [
                    'Check current air quality before going outside',
                    'Consider reducing outdoor activities if sensitive',
                    'Keep windows closed if air quality is poor'
                ]
            }
        }
        
        logger.info(f"🧪 Test alert generated for {location_name}")
        return jsonify(alert_response)
        
    except Exception as e:
        logger.error(f"Error creating test alert: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/demo', methods=['GET'])
def get_demo_alerts():
    """Get demo alerts for testing frontend"""
    try:
        demo_scenarios = [
            {
                'alert_id': 'demo_moderate_pm25',
                'alert_level': 'moderate',
                'pollutant': 'PM25',
                'aqi_value': 85,
                'location_city': 'New York',
                'epa_message': 'Air quality is acceptable; however, there may be a risk for some people, particularly those who are unusually sensitive to air pollution.',
                'timestamp': datetime.now().isoformat()
            },
            {
                'alert_id': 'demo_unhealthy_sensitive_pm25',
                'alert_level': 'unhealthy_sensitive',
                'pollutant': 'PM25',
                'aqi_value': 125,
                'location_city': 'Rajshahi',
                'epa_message': 'Members of sensitive groups may experience health effects. The general public is less likely to be affected.',
                'timestamp': datetime.now().isoformat()
            }
        ]
        
        return jsonify({
            'success': True,
            'scenarios': demo_scenarios,
            'count': len(demo_scenarios)
        })
        
    except Exception as e:
        logger.error(f"Error getting demo alerts: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/test/create-demo-alert', methods=['POST'])
def create_demo_alert():
    """Create a demo alert for testing"""
    try:
        demo_user_id = 'demo_user_123'
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT IGNORE INTO users (user_id, name, email, timezone, created_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, (demo_user_id, 'Demo User', 'demo@saferskies.app', 'UTC'))
        
        cursor.execute("""
            INSERT IGNORE INTO user_health_conditions (user_id, condition_name, severity, created_at)
            VALUES (%s, %s, %s, NOW())
        """, (demo_user_id, 'asthma', 'moderate'))
        
        conn.commit()
        
        demo_alert_data = {
            'userDetails': {
                'name': 'Demo User',
                'email': 'demo@saferskies.app',
                'timezone': 'UTC'
            },
            'locations': [{
                'name': 'San Francisco',
                'coordinates': [-122.4194, 37.7749],  # [lng, lat]
                'displayName': 'San Francisco'
            }],
            'threshold': {
                'aqi': 125,
                'alertLevel': 'unhealthy_sensitive',
                'type': 'category'
            },
            'pollutants': ['PM25'],
            'notifications': {
                'email': True,
                'web_push': True,
                'mobile_push': False
            },
            'healthConditions': ['asthma'],
            'quietHours': {
                'enabled': True,
                'start': '22:00',
                'end': '07:00'
            },
            'alertTypes': {
                'immediate': True,
                'forecast': True,
                'daily_summary': False
            }
        }
        
        cursor.close()
        conn.close()
        
        result = process_alert_registration(demo_alert_data)
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': 'Demo alert created successfully',
                'alert_ids': result['alert_ids'],
                'user_id': result['user_id']
            })
        else:
            return jsonify({
                'success': False,
                'error': result['error']
            }), 500
        
    except Exception as e:
        logger.error(f"Error creating demo alert: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/history/<email>', methods=['GET'])
def get_user_notification_history(email):
    """Get notification history for user - simple API"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False, 'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT user_id FROM users WHERE email = %s", [email])
        user_row = cursor.fetchone()
        
        if not user_row:
            cursor.close()
            conn.close()
            return jsonify({
                'success': True,
                'user_email': email,
                'user_exists': False,
                'total_notifications': 0,
                'recent_notifications': [],
                'message': 'No user found with this email'
            })
        
        user_id = user_row['user_id']
        
        query = """
        SELECT 
            nh.notification_id,
            nh.alert_id,
            nh.channel as notification_type,
            nh.status,
            nh.sent_at,
            nh.error_message,
            a.location_city as location_name,
            a.location_lat as latitude,
            a.location_lng as longitude,
            a.aqi_value as alert_threshold,
            a.alert_level,
            a.message as alert_message
        FROM notification_history nh
        LEFT JOIN alerts a ON nh.alert_id = a.alert_id
        WHERE nh.user_id = %s
        ORDER BY nh.sent_at DESC
        LIMIT 50
        """
        
        cursor.execute(query, [user_id])
        history = cursor.fetchall()
        
        cursor.execute('SELECT COUNT(*) as total FROM notification_history WHERE user_id = %s', [user_id])
        total_count = cursor.fetchone()['total']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'user_email': email,
            'user_id': user_id,
            'total_notifications': total_count,
            'recent_notifications': history
        })
        
    except Exception as e:
        logger.error(f"Error fetching user notification history: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print("🚀 Starting Safer Skies Alert Registration API...")
    print("📡 Team AURA - NASA Space Apps Challenge 2025")
    print("🎯 Smart Location Collection Optimization")
    
    # Test the optimizer
    try:
        stats = optimizer.get_collection_statistics()
        print(f"✅ Optimizer initialized - {stats.get('total_unique_locations', 0)} locations tracked")
    except Exception as e:
        print(f"⚠️  Optimizer warning: {e}")
    
    port = int(os.getenv('PORT', 5003))
    debug_mode = os.getenv('DEBUG', 'false').lower() == 'true'
    host = os.getenv('HOST', '0.0.0.0')
    app.run(host=host, port=port, debug=debug_mode)