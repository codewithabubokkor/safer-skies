#!/usr/bin/env python3
"""
Alert Management API Endpoint
Manages air quality alerts and notification preferences
NASA Space Apps Challenge - Backend Implementation
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import mysql.connector
import json
import os
from dotenv import load_dotenv

load_dotenv()
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import uuid
import sys
import os

backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, backend_path)

# Import notification system
from notifications.notification_manager import NotificationManager
from notifications.email_service import EmailNotificationService
from notifications.push_service import PushNotificationService
from processors.alert_engine import AirQualityAlertEngine
from utils.database_connection import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=["*"])

notification_manager = NotificationManager()
alert_engine = AirQualityAlertEngine(local_mode=True)

def init_alerts_database():
    """Initialize alerts database connection (tables already exist in MySQL)"""
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to connect to MySQL database")
        return False
    
    try:
        # Test connection with a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        cursor.fetchone()
        conn.close()
        logger.info("✅ MySQL Alert database connection verified")
        return True
    except Exception as e:
        logger.error(f"Database verification failed: {e}")
        if conn:
            conn.close()
        return False
    
        # Tables already exist in MySQL - no need to create them

@app.route('/api/alerts/user/<user_id>', methods=['GET'])
def get_user_alerts(user_id):
    """Get active alerts for a user"""
    try:
        conn = get_db_connection()
        
        alerts = conn.execute('''
            SELECT * FROM alerts 
            WHERE user_id = ? AND status = 'active' 
            AND expires_at > datetime('now')
            ORDER BY timestamp DESC
            LIMIT 20
        ''', (user_id,)).fetchall()
        
        alert_list = []
        for alert in alerts:
            alert_dict = dict(alert)
            alert_list.append(alert_dict)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'alerts': alert_list,
            'count': len(alert_list),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching alerts for user {user_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/location', methods=['POST'])
def get_location_alerts():
    """Get alerts for a specific location"""
    try:
        data = request.get_json()
        lat = data.get('lat')
        lng = data.get('lng')
        radius = data.get('radius', 50)  # Default 50km radius
        
        if not lat or not lng:
            return jsonify({'success': False, 'error': 'Latitude and longitude required'}), 400
        
        conn = get_db_connection()
        
        alerts = conn.execute('''
            SELECT * FROM alerts
            WHERE status = 'active'
            AND expires_at > datetime('now')
            AND ABS(location_lat - ?) < ?
            AND ABS(location_lng - ?) < ?
            ORDER BY timestamp DESC
            LIMIT 10
        ''', (lat, radius/111, lng, radius/111)).fetchall()
        
        alert_list = [dict(alert) for alert in alerts]
        conn.close()
        
        return jsonify({
            'success': True,
            'alerts': alert_list,
            'location': {'lat': lat, 'lng': lng, 'radius': radius},
            'count': len(alert_list),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching location alerts: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/create', methods=['POST'])
def create_alert():
    """Create a new air quality or fire alert"""
    try:
        data = request.get_json()
        
        alert_type = data.get('alert_type', 'aqi')
        
        # Required fields based on alert type
        if alert_type == 'fire':
            required_fields = ['user_id', 'location', 'message']
        else:
            required_fields = ['user_id', 'location', 'pollutant', 'aqi_value', 'alert_level', 'epa_message']
        
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        alert_id = str(uuid.uuid4())
        
        hours = 48 if alert_type == 'fire' else 24
        expires_at = datetime.now() + timedelta(hours=hours)
        
        conn = get_db_connection()
        conn.execute('''
            INSERT INTO alerts (
                alert_id, user_id, alert_type, location_city, location_lat, location_lng,
                pollutant, aqi_value, alert_level, epa_message, severity, message,
                fire_data, aqi_data, timestamp, expires_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            alert_id, 
            data['user_id'],
            alert_type,
            data['location'].get('city', ''),
            data['location'].get('lat', 0),
            data['location'].get('lng', 0),
            data.get('pollutant', ''),
            data.get('aqi_value', 0),
            data.get('alert_level', ''),
            data.get('epa_message', ''),
            data.get('severity', 'medium'),
            data.get('message', ''),
            json.dumps(data.get('fire_data', {})) if alert_type == 'fire' else None,
            json.dumps(data.get('aqi_data', {})) if alert_type == 'aqi' else None,
            datetime.now().isoformat(),
            expires_at.isoformat(),
            'active'
        ))
        
        conn.commit()
        conn.close()
        
        try:
            user_profile = get_user_profile(data['user_id'])
            if user_profile:
                alert_data = {
                    'alert_id': alert_id,
                    'user_id': data['user_id'],
                    'timestamp': datetime.now().isoformat(),
                    'location': data['location'],
                    'pollutant': data['pollutant'],
                    'aqi_value': data['aqi_value'],
                    'alert_level': data['alert_level'],
                    'epa_message': data['epa_message'],
                    'user_conditions': user_profile.get('health_conditions', [])
                }
                
                notification_results = notification_manager.send_alert_notifications(
                    alert_data, user_profile
                )
                
                logger.info(f"Alert {alert_id} created and notifications sent: {notification_results}")
        
        except Exception as notification_error:
            logger.error(f"Error sending notifications for alert {alert_id}: {notification_error}")
        
        return jsonify({
            'success': True,
            'alert_id': alert_id,
            'message': 'Alert created successfully',
            'expires_at': expires_at.isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error creating alert: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<user_id>/preferences', methods=['GET'])
def get_notification_preferences(user_id):
    """Get user notification preferences"""
    try:
        conn = get_db_connection()
        
        prefs = conn.execute('''
            SELECT * FROM notification_preferences WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if prefs:
            prefs_dict = dict(prefs)
        else:
            prefs_dict = {
                'user_id': user_id,
                'email': True,
                'push': True,
                'web_push': True,
                'sms': False,
                'daily_summary': False,
                'forecast_warnings': True,
                'quiet_hours_start': '22:00',
                'quiet_hours_end': '07:00'
            }
        
        conditions = conn.execute('''
            SELECT condition, severity FROM user_health_conditions WHERE user_id = ?
        ''', (user_id,)).fetchall()
        
        prefs_dict['health_conditions'] = [
            {'condition': dict(c)['condition'], 'severity': dict(c)['severity']} 
            for c in conditions
        ]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'preferences': prefs_dict,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching preferences for user {user_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<user_id>/preferences', methods=['POST'])
def update_notification_preferences(user_id):
    """Update user notification preferences"""
    try:
        data = request.get_json()
        
        conn = get_db_connection()
        
        conn.execute('''
            INSERT OR REPLACE INTO notification_preferences (
                user_id, email, push, web_push, sms, daily_summary,
                forecast_warnings, quiet_hours_start, quiet_hours_end, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            data.get('email', True),
            data.get('push', True),
            data.get('web_push', True),
            data.get('sms', False),
            data.get('daily_summary', False),
            data.get('forecast_warnings', True),
            data.get('quiet_hours_start', '22:00'),
            data.get('quiet_hours_end', '07:00'),
            datetime.now().isoformat()
        ))
        
        if 'health_conditions' in data:
            # Clear existing conditions
            conn.execute('''
                DELETE FROM user_health_conditions WHERE user_id = ?
            ''', (user_id,))
            
            for condition in data['health_conditions']:
                conn.execute('''
                    INSERT INTO user_health_conditions (user_id, condition, severity)
                    VALUES (?, ?, ?)
                ''', (user_id, condition['condition'], condition.get('severity', 'moderate')))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Preferences updated successfully',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error updating preferences for user {user_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/<alert_id>/dismiss', methods=['POST'])
def dismiss_alert(alert_id):
    """Dismiss an alert"""
    try:
        conn = get_db_connection()
        
        result = conn.execute('''
            UPDATE alerts SET status = 'dismissed', expires_at = datetime('now')
            WHERE alert_id = ?
        ''', (alert_id,))
        
        if result.rowcount == 0:
            conn.close()
            return jsonify({'success': False, 'error': 'Alert not found'}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Alert dismissed successfully',
            'alert_id': alert_id
        })
        
    except Exception as e:
        logger.error(f"Error dismissing alert {alert_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/statistics', methods=['GET'])
def get_alert_statistics():
    """Get alert system statistics"""
    try:
        conn = get_db_connection()
        
        alert_stats = conn.execute('''
            SELECT alert_level, COUNT(*) as count
            FROM alerts
            WHERE status = 'active' AND expires_at > datetime('now')
            GROUP BY alert_level
        ''').fetchall()
        
        notification_stats = conn.execute('''
            SELECT channel, status, COUNT(*) as count
            FROM notification_history
            WHERE sent_at > datetime('now', '-24 hours')
            GROUP BY channel, status
        ''').fetchall()
        
        recent_alerts = conn.execute('''
            SELECT alert_level, location_city, timestamp, aqi_value
            FROM alerts
            WHERE created_at > datetime('now', '-24 hours')
            ORDER BY timestamp DESC
            LIMIT 10
        ''').fetchall()
        
        conn.close()
        
        stats = {
            'alert_levels': {dict(row)['alert_level']: dict(row)['count'] for row in alert_stats},
            'notifications': {
                f"{dict(row)['channel']}_{dict(row)['status']}": dict(row)['count'] 
                for row in notification_stats
            },
            'recent_alerts': [dict(row) for row in recent_alerts],
            'system_status': {
                'notification_manager': 'operational',
                'alert_engine': 'operational',
                'last_update': datetime.now().isoformat()
            }
        }
        
        return jsonify({
            'success': True,
            'statistics': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching alert statistics: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

def get_user_profile(user_id):
    """Helper function to get user profile for notifications"""
    try:
        conn = get_db_connection()
        
        user = conn.execute('''
            SELECT * FROM users WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if not user:
            return None
        
        user_dict = dict(user)
        
        prefs = conn.execute('''
            SELECT * FROM notification_preferences WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if prefs:
            prefs_dict = dict(prefs)
            user_dict['notification_preferences'] = {
                'email': bool(prefs_dict.get('email')),
                'push': bool(prefs_dict.get('push')),
                'web_push': bool(prefs_dict.get('web_push')),
                'sms': bool(prefs_dict.get('sms')),
                'daily_summary': bool(prefs_dict.get('daily_summary'))
            }
        else:
            user_dict['notification_preferences'] = {
                'email': True,
                'push': True,
                'web_push': True,
                'sms': False,
                'daily_summary': False
            }
        
        conditions = conn.execute('''
            SELECT condition FROM user_health_conditions WHERE user_id = ?
        ''', (user_id,)).fetchall()
        
        user_dict['health_conditions'] = [dict(c)['condition'] for c in conditions]
        
        conn.close()
        return user_dict
        
    except Exception as e:
        logger.error(f"Error fetching user profile {user_id}: {e}")
        return None

@app.route('/api/user/<user_id>/alert-preferences', methods=['POST'])
def save_alert_preferences(user_id):
    """Save comprehensive alert preferences for a user"""
    try:
        data = request.get_json()
        
        conn = get_db_connection()
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS alert_preferences (
                user_id TEXT PRIMARY KEY,
                locations TEXT,
                threshold_type TEXT,
                threshold_value INTEGER,
                threshold_category TEXT,
                pollutants TEXT,
                frequency TEXT,
                notification_methods TEXT,
                user_details TEXT,
                health_conditions TEXT,
                quiet_hours TEXT,
                alert_types TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        conn.execute('''
            INSERT OR REPLACE INTO users (user_id, name, email, phone)
            VALUES (?, ?, ?, ?)
        ''', (
            user_id, 
            data.get('userDetails', {}).get('name', 'Unknown'),
            data.get('userDetails', {}).get('email', ''),
            data.get('userDetails', {}).get('phone', '')
        ))
        
        conn.execute('''
            INSERT OR REPLACE INTO alert_preferences (
                user_id, locations, threshold_type, threshold_value, threshold_category,
                pollutants, frequency, notification_methods, user_details,
                health_conditions, quiet_hours, alert_types, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            json.dumps(data.get('locations', [])),
            data.get('threshold', {}).get('type', 'category'),
            data.get('threshold', {}).get('value', 100),
            data.get('thresholdCategory', 'moderate'),
            json.dumps(data.get('pollutants', ['all'])),
            data.get('frequency', 'every_time'),
            json.dumps(data.get('notifications', {})),
            json.dumps(data.get('userDetails', {})),
            json.dumps(data.get('healthConditions', [])),
            json.dumps(data.get('quietHours', {})),
            json.dumps(data.get('alertTypes', {})),
            datetime.now().isoformat()
        ))
        
        notifications = data.get('notifications', {})
        conn.execute('''
            INSERT OR REPLACE INTO notification_preferences (
                user_id, email, push, web_push, sms, daily_summary,
                forecast_warnings, quiet_hours_start, quiet_hours_end, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            notifications.get('email', True),
            notifications.get('mobile_push', False),
            notifications.get('web_push', True),
            False,  # SMS not used per requirement
            data.get('alertTypes', {}).get('daily_summary', False),
            data.get('alertTypes', {}).get('forecast', True),
            data.get('quietHours', {}).get('start', '22:00'),
            data.get('quietHours', {}).get('end', '07:00'),
            datetime.now().isoformat()
        ))
        
        health_conditions = data.get('healthConditions', [])
        conn.execute('DELETE FROM user_health_conditions WHERE user_id = ?', (user_id,))
        for condition in health_conditions:
            conn.execute('''
                INSERT INTO user_health_conditions (user_id, condition, severity)
                VALUES (?, ?, ?)
            ''', (user_id, condition, 'moderate'))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Alert preferences saved for user {user_id}")
        
        return jsonify({
            'success': True,
            'message': 'Alert preferences saved successfully',
            'user_id': user_id,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error saving alert preferences for user {user_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<user_id>/alert-preferences', methods=['GET'])
def get_alert_preferences(user_id):
    """Get comprehensive alert preferences for a user"""
    try:
        conn = get_db_connection()
        
        prefs = conn.execute('''
            SELECT * FROM alert_preferences WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        if not prefs:
            conn.close()
            return jsonify({
                'success': True,
                'preferences': {
                    'locations': [],
                    'threshold': {'type': 'category', 'value': 100},
                    'thresholdCategory': 'moderate',
                    'pollutants': ['all'],
                    'frequency': 'every_time',
                    'notifications': {'email': True, 'web_push': True, 'mobile_push': False},
                    'userDetails': {'name': '', 'email': ''},
                    'healthConditions': [],
                    'quietHours': {'enabled': True, 'start': '22:00', 'end': '07:00'},
                    'alertTypes': {'immediate': True, 'forecast': True, 'daily_summary': False}
                },
                'timestamp': datetime.now().isoformat()
            })
        
        prefs_dict = dict(prefs)
        
        parsed_prefs = {
            'locations': json.loads(prefs_dict.get('locations', '[]')),
            'threshold': {
                'type': prefs_dict.get('threshold_type', 'category'),
                'value': prefs_dict.get('threshold_value', 100)
            },
            'thresholdCategory': prefs_dict.get('threshold_category', 'moderate'),
            'pollutants': json.loads(prefs_dict.get('pollutants', '["all"]')),
            'frequency': prefs_dict.get('frequency', 'every_time'),
            'notifications': json.loads(prefs_dict.get('notification_methods', '{}')),
            'userDetails': json.loads(prefs_dict.get('user_details', '{}')),
            'healthConditions': json.loads(prefs_dict.get('health_conditions', '[]')),
            'quietHours': json.loads(prefs_dict.get('quiet_hours', '{}')),
            'alertTypes': json.loads(prefs_dict.get('alert_types', '{}'))
        }
        
        conn.close()
        
        return jsonify({
            'success': True,
            'preferences': parsed_prefs,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching alert preferences for user {user_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/user/<user_id>/saved-alerts', methods=['GET'])
def get_saved_alerts(user_id):
    """Get user's saved alert configurations"""
    try:
        conn = get_db_connection()
        
        prefs = conn.execute('''
            SELECT * FROM alert_preferences WHERE user_id = ?
        ''', (user_id,)).fetchone()
        
        saved_alerts = []
        
        if prefs:
            prefs_dict = dict(prefs)
            locations = json.loads(prefs_dict.get('locations', '[]'))
            
            for i, location in enumerate(locations):
                if location.get('city'):
                    threshold_display = (
                        f"{prefs_dict.get('threshold_category', 'moderate').replace('_', ' ').title()} "
                        f"({prefs_dict.get('threshold_value', 100)} AQI)"
                        if prefs_dict.get('threshold_type') == 'category'
                        else f"AQI > {prefs_dict.get('threshold_value', 100)}"
                    )
                    
                    notifications = json.loads(prefs_dict.get('notification_methods', '{}'))
                    notification_methods = [
                        method for method, enabled in notifications.items() if enabled
                    ]
                    
                    saved_alerts.append({
                        'id': f"{user_id}_{i}",
                        'location': location['city'],
                        'threshold': threshold_display,
                        'notifications': ' + '.join(notification_methods).title(),
                        'status': 'Active',
                        'created_at': prefs_dict.get('created_at'),
                        'updated_at': prefs_dict.get('updated_at')
                    })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'alerts': saved_alerts,
            'count': len(saved_alerts),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching saved alerts for user {user_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/monitor/trigger', methods=['POST'])
def trigger_alert_monitoring():
    """Manually trigger alert monitoring (for testing)"""
    try:
        from processors.aqi_alert_monitor import AQIAlertMonitor
        
        monitor = AQIAlertMonitor()
        monitor.monitor_and_create_alerts()
        
        return jsonify({
            'success': True,
            'message': 'Alert monitoring triggered successfully'
        })
        
    except Exception as e:
        logger.error(f"Error triggering alert monitoring: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
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

@app.route('/api/test/create-demo-alert', methods=['POST'])
def create_demo_alert():
    """Create a demo alert for testing"""
    try:
        demo_user_id = 'demo_user_123'
        
        conn = get_db_connection()
        conn.execute('''
            INSERT OR IGNORE INTO users (user_id, name, email, phone)
            VALUES (?, ?, ?, ?)
        ''', (demo_user_id, 'Demo User', 'demo@saferskies.app', '+1-555-0123'))
        
        conn.execute('''
            INSERT OR IGNORE INTO user_health_conditions (user_id, condition, severity)
            VALUES (?, ?, ?)
        ''', (demo_user_id, 'asthma', 'moderate'))
        
        conn.commit()
        conn.close()
        
        demo_alert = {
            'user_id': demo_user_id,
            'location': {
                'city': 'San Francisco',
                'lat': 37.7749,
                'lng': -122.4194
            },
            'pollutant': 'PM25',
            'aqi_value': 125,
            'alert_level': 'unhealthy_sensitive',
            'epa_message': 'Sensitive groups: Make outdoor activities shorter and less intense. People with asthma should follow their asthma action plan.'
        }
        
        with app.test_request_context(json=demo_alert):
            request.get_json = lambda: demo_alert
            response = create_alert()
            
        return response
        
    except Exception as e:
        logger.error(f"Error creating demo alert: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print("🚨 Initializing Alert Management API...")
    
    init_alerts_database()
    
    print("✅ Alert API ready!")
    print("📡 Endpoints available:")
    print("   GET  /api/alerts/user/<user_id>")
    print("   POST /api/alerts/location")  
    print("   POST /api/alerts/create")
    print("   POST /api/alerts/test")
    print("   GET  /api/user/<user_id>/preferences")
    print("   POST /api/user/<user_id>/preferences")
    print("   POST /api/user/<user_id>/alert-preferences")
    print("   GET  /api/user/<user_id>/alert-preferences")
    print("   GET  /api/user/<user_id>/saved-alerts")
    print("   POST /api/alerts/<alert_id>/dismiss")
    print("   GET  /api/alerts/statistics")
    print("   POST /api/test/create-demo-alert")
    print("\n🌍 Starting Flask server...")
    
    app.run(debug=os.getenv('DEBUG', 'false').lower() == 'true', host='0.0.0.0', port=5003)
