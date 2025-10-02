#!/usr/bin/env python3
"""
Alert Monitor - Check AQI data and trigger notifications
This script monitors current AQI data and sends email/push notifications when user thresholds are exceeded
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.database_connection import get_db_connection
from notifications.notification_manager import NotificationManager
import logging
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertMonitor:
    """Monitor AQI data and trigger notifications for registered alerts"""
    
    def __init__(self):
        self.notification_manager = NotificationManager({
            'use_aws_ses': False,  # Using Gmail SMTP
            'smtp_config': {
                'smtp_server': os.getenv('SMTP_SERVER'),
                'smtp_port': int(os.getenv('SMTP_PORT')),
                'sender_email': os.getenv('SENDER_EMAIL'),
                'sender_password': os.getenv('SENDER_PASSWORD')
            }
        })
    
    def check_alerts(self):
        """Check all active alerts against current AQI data"""
        
        connection = get_db_connection()
        if not connection:
            logger.error("❌ Database connection failed")
            return False
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            logger.info("🔍 Checking active alerts...")
            
            cursor.execute("""
                SELECT 
                    al.id as alert_location_id,
                    al.user_email,
                    al.city,
                    al.latitude,
                    al.longitude,
                    al.alert_threshold,
                    u.name as user_name,
                    u.timezone,
                    up.notification_channels,
                    up.alert_types,
                    up.health_conditions,
                    up.quiet_hours_start,
                    up.quiet_hours_end,
                    up.timezone as user_timezone,
                    np.email as email_enabled,
                    np.push as push_enabled,
                    np.quiet_hours_enabled,
                    np.quiet_hours_start as np_quiet_start,
                    np.quiet_hours_end as np_quiet_end
                FROM alert_locations al
                LEFT JOIN users u ON al.user_email = u.email
                LEFT JOIN user_preferences up ON al.user_email = up.user_email
                LEFT JOIN notification_preferences np ON u.user_id = np.user_id
                WHERE al.active = 1
                  AND (np.email = 1 OR np.email IS NULL)
                ORDER BY al.created_at DESC
            """)
            
            active_locations = cursor.fetchall()
            logger.info(f"📋 Found {len(active_locations)} active alert locations")
            
            if not active_locations:
                logger.info("ℹ️ No active alert locations to check")
                return True
            
            alerts_triggered = 0
            
            for location in active_locations:
                try:
                    current_aqi = self.get_current_aqi_for_location(
                        cursor, 
                        location['latitude'], 
                        location['longitude']
                    )
                    
                    if current_aqi:
                        if current_aqi['overall_aqi'] > location['alert_threshold']:
                            logger.info(f"🚨 Alert triggered for {location['user_email']} at {location['city']}")
                            logger.info(f"    AQI: {current_aqi['overall_aqi']} > {location['alert_threshold']}")
                            
                            if self.should_send_alert_based_on_location_preferences(location, current_aqi):
                                if self.should_send_notification_for_location(location['alert_location_id'], location['user_email']):
                                    self.send_and_save_alert_notification(location, current_aqi)
                                    alerts_triggered += 1
                                else:
                                    logger.info(f"⏰ Skipping notification due to rate limiting")
                            else:
                                logger.info(f"⏰ Skipping notification due to user preferences (quiet hours/alert types)")
                    
                except Exception as e:
                    logger.error(f"❌ Error checking location {location['alert_location_id']}: {e}")
                    continue
            
            logger.info(f"📤 Sent {alerts_triggered} alert notifications")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error checking alerts: {e}")
            return False
        finally:
            cursor.close()
            connection.close()
    
    def get_current_aqi_for_location(self, cursor, lat, lng):
        """Get the most recent AQI data for a specific location"""
        
        cursor.execute("""
            SELECT 
                overall_aqi,
                dominant_pollutant,
                aqi_category,
                pm25_aqi,
                o3_aqi,
                no2_aqi,
                so2_aqi,
                co_aqi,
                timestamp,
                city
            FROM comprehensive_aqi_hourly
            WHERE ABS(location_lat - %s) < 0.1
            AND ABS(location_lng - %s) < 0.1
            ORDER BY timestamp DESC
            LIMIT 1
        """, (lat, lng))
        
        result = cursor.fetchone()
        if result:
            logger.info(f"📊 Found AQI data for location ({lat}, {lng}): AQI {result['overall_aqi']}")
        else:
            logger.warning(f"⚠️ No recent AQI data found for location ({lat}, {lng})")
        
        return result
    
    def is_threshold_exceeded(self, alert, current_aqi):
        """Check if current AQI exceeds the user's threshold"""
        
        threshold = alert['threshold_value']
        current_value = current_aqi['overall_aqi']
        
        try:
            if isinstance(alert['pollutants'], str):
                if alert['pollutants'].startswith('['):
                    pollutants = json.loads(alert['pollutants'])
                else:
                    pollutants = [alert['pollutants']]
            else:
                pollutants = alert['pollutants']
        except:
            pollutants = ['all']
        
        if 'all' in pollutants or len(pollutants) == 0:
            exceeded = current_value > threshold
            logger.info(f"🔍 Overall AQI check: {current_value} vs threshold {threshold} = {'EXCEEDED' if exceeded else 'OK'}")
            return exceeded
        
        for pollutant in pollutants:
            pollutant_key = f"{pollutant.lower()}_aqi"
            if pollutant_key in current_aqi and current_aqi[pollutant_key]:
                if current_aqi[pollutant_key] > threshold:
                    logger.info(f"🔍 {pollutant} AQI check: {current_aqi[pollutant_key]} vs threshold {threshold} = EXCEEDED")
                    return True
        
        return False
    
    def should_send_notification(self, alert_id, user_id):
        """Check if we should send a notification (rate limiting)"""
        
        connection = get_db_connection()
        if not connection:
            return True  # Default to sending if we can't check
        
        try:
            cursor = connection.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT MAX(sent_at) as last_sent
                FROM notification_log
                WHERE alert_id = %s AND sent_at > NOW() - INTERVAL 1 HOUR
            """, (alert_id,))
            
            result = cursor.fetchone()
            
            if result and result['last_sent']:
                logger.info(f"⏰ Last notification sent at {result['last_sent']} - rate limiting")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error checking notification rate limit: {e}")
            return True  # Default to sending on error
        finally:
            cursor.close()
            connection.close()
    
    def send_alert_notifications(self, alert, current_aqi):
        """Send email and push notifications for triggered alert"""
        
        try:
            alert_data = {
                'alert_id': alert['alert_id'],
                'location': alert['location_name'],
                'current_aqi': current_aqi['overall_aqi'],
                'threshold': alert['threshold_value'],
                'category': current_aqi['aqi_category'],
                'dominant_pollutant': current_aqi['dominant_pollutant'],
                'timestamp': current_aqi['timestamp'].isoformat() if current_aqi['timestamp'] else datetime.now().isoformat(),
                'user_timezone': alert['timezone'] or 'UTC'
            }
            
            if alert['email_enabled']:
                try:
                    email_result = self.notification_manager.send_email_alert(
                        user_email=alert['user_email'],
                        user_name=alert['user_name'],
                        alert_data=alert_data
                    )
                    
                    if email_result.get('success'):
                        logger.info(f"📧 Email sent to {alert['user_email']}")
                        self.log_notification(alert['alert_id'], 'email', True)
                    else:
                        logger.error(f"❌ Email failed: {email_result.get('error')}")
                        self.log_notification(alert['alert_id'], 'email', False, email_result.get('error'))
                        
                except Exception as e:
                    logger.error(f"❌ Email notification error: {e}")
                    self.log_notification(alert['alert_id'], 'email', False, str(e))
            
            if alert['push_enabled'] and alert['push_endpoint']:
                try:
                    push_result = self.notification_manager.send_push_alert(
                        push_endpoint=alert['push_endpoint'],
                        push_keys=json.loads(alert['push_keys']) if alert['push_keys'] else None,
                        user_name=alert['user_name'],
                        alert_data=alert_data
                    )
                    
                    if push_result.get('success'):
                        logger.info(f"📱 Push notification sent")
                        self.log_notification(alert['alert_id'], 'push', True)
                    else:
                        logger.error(f"❌ Push notification failed: {push_result.get('error')}")
                        self.log_notification(alert['alert_id'], 'push', False, push_result.get('error'))
                        
                except Exception as e:
                    logger.error(f"❌ Push notification error: {e}")
                    self.log_notification(alert['alert_id'], 'push', False, str(e))
                    
        except Exception as e:
            logger.error(f"❌ Error sending notifications: {e}")
    
    def log_notification(self, alert_id, notification_type, success, error_message=None):
        """Log notification attempt to database"""
        
        connection = get_db_connection()
        if not connection:
            return
        
        try:
            cursor = connection.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notification_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    alert_id VARCHAR(36),
                    notification_type ENUM('email', 'push', 'sms'),
                    success BOOLEAN,
                    error_message TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_alert_sent (alert_id, sent_at)
                );
            """)
            
            cursor.execute("""
                INSERT INTO notification_log (alert_id, notification_type, success, error_message)
                VALUES (%s, %s, %s, %s)
            """, (alert_id, notification_type, success, error_message))
            
            connection.commit()
            
        except Exception as e:
            logger.error(f"❌ Error logging notification: {e}")
        finally:
            cursor.close()
            connection.close()
    
    def should_send_alert_based_on_location_preferences(self, location, current_aqi):
        """Check if alert should be sent based on user preferences from existing schema"""
        
        try:
            import json
            from datetime import datetime, time
            
            if location.get('quiet_hours_enabled') or location.get('np_quiet_start'):
                current_time = datetime.now().time()
                quiet_start_str = location.get('np_quiet_start', location.get('quiet_hours_start', '22:00'))
                quiet_end_str = location.get('np_quiet_end', location.get('quiet_hours_end', '07:00'))
                
                if ':' in str(quiet_start_str):
                    quiet_start = datetime.strptime(str(quiet_start_str), '%H:%M').time()
                    quiet_end = datetime.strptime(str(quiet_end_str), '%H:%M').time()
                    
                    if quiet_start <= current_time or current_time <= quiet_end:
                        # Allow emergency alerts during quiet hours
                        if current_aqi['overall_aqi'] < 200:  # Not very unhealthy/hazardous
                            logger.info(f"Quiet hours active, skipping non-emergency alert")
                            return False
            
            if location.get('alert_types'):
                try:
                    alert_types = json.loads(location['alert_types'])
                    if not alert_types.get('realtime', True) and not alert_types.get('immediate', True):
                        logger.info(f"Realtime alerts disabled for user")
                        return False
                except:
                    pass  # If JSON parsing fails, allow alert
            
            return True
            
        except Exception as e:
            logger.warning(f"Error checking user preferences: {e}")
            return True  # Default to sending alert if preferences check fails
    
    def should_send_notification_for_location(self, location_id, user_email):
        """Check rate limiting for location-based alerts (30 minute interval)"""
        
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            
            cursor.execute("""
                SELECT COUNT(*) FROM notification_history 
                WHERE alert_id = %s 
                  AND user_id = %s 
                  AND sent_at > DATE_SUB(NOW(), INTERVAL 30 MINUTE)
                  AND status = 'sent'
            """, (str(location_id), user_email))
            
            recent_count = cursor.fetchone()[0]
            cursor.close()
            connection.close()
            
            return recent_count == 0
            
        except Exception as e:
            logger.warning(f"Error checking notification history: {e}")
            return True  # Default to allowing notification
    
    def send_and_save_alert_notification(self, location, current_aqi):
        """Send alert notification and save to history using existing schema"""
        
        try:
            import uuid
            import json
            
            alert_data = {
                'user_id': location['user_email'],
                'timestamp': datetime.now().isoformat(),
                'location': {
                    'city': location['city'],
                    'latitude': float(location['latitude']),
                    'longitude': float(location['longitude'])
                },
                'aqi_value': current_aqi['overall_aqi'],
                'alert_level': current_aqi['aqi_category'].lower().replace(' ', '_') if current_aqi.get('aqi_category') else 'moderate',
                'pollutant': current_aqi.get('dominant_pollutant', 'PM25'),
                'epa_message': current_aqi.get('health_message', 'Air quality may affect sensitive individuals.'),
                'user_conditions': []
            }
            
            if location.get('health_conditions'):
                try:
                    health_conditions = json.loads(location['health_conditions'])
                    alert_data['user_conditions'] = [condition['condition'] for condition in health_conditions]
                except:
                    pass
            
            user_name = location.get('user_name', location['user_email'].split('@')[0])
            success = self.notification_manager.email_service.send_alert_email(
                alert=alert_data,
                user_email=location['user_email'],
                user_name=user_name
            )
            
            if success:
                self.save_notification_history(location, current_aqi, 'email', 'sent')
                logger.info(f"✅ Alert sent and saved for {location['user_email']}")
            else:
                self.save_notification_history(location, current_aqi, 'email', 'failed')
                logger.error(f"❌ Alert failed for {location['user_email']}")
                
        except Exception as e:
            logger.error(f"Error sending alert notification: {e}")
    
    def save_notification_history(self, location, current_aqi, channel, status):
        """Save notification to history table"""
        
        try:
            import uuid
            connection = get_db_connection()
            cursor = connection.cursor()
            
            notification_id = str(uuid.uuid4())
            alert_id = str(location['alert_location_id'])
            user_id = location['user_email']
            
            cursor.execute("""
                INSERT INTO notification_history 
                (notification_id, alert_id, user_id, channel, status, sent_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (notification_id, alert_id, user_id, channel, status))
            
            connection.commit()
            cursor.close()
            connection.close()
            
        except Exception as e:
            logger.warning(f"Error saving notification history: {e}")

def main():
    """Main function to run alert monitoring"""
    
    logger.info("🚨 Starting Alert Monitor")
    logger.info("=" * 50)
    
    monitor = AlertMonitor()
    success = monitor.check_alerts()
    
    if success:
        logger.info("✅ Alert monitoring completed successfully")
    else:
        logger.error("❌ Alert monitoring completed with errors")
    
    return success

if __name__ == "__main__":
    main()