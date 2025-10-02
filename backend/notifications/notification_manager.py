#!/usr/bin/env python3
"""
Notification Manager
Orchestrates all notification services for air quality alerts
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
import sys

# Add backend to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from notifications.email_service import EmailNotificationService
from notifications.push_service import PushNotificationService

logger = logging.getLogger(__name__)

class NotificationManager:
    """
    Central manager for all air quality notifications
    Coordinates email, push, and SMS alerts
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        
        self.email_service = EmailNotificationService(
            use_aws_ses=self.config.get('use_aws_ses', False),
            smtp_config=self.config.get('smtp_config', {})
        )
        
        self.push_service = PushNotificationService(
            use_aws_sns=self.config.get('use_aws_sns', False)
        )
        
        # Notification rate limiting
        self.rate_limits = {
            'email': timedelta(hours=1),      # Max 1 email per hour per user
            'push': timedelta(minutes=30),    # Max 1 push per 30 min per user
            'sms': timedelta(hours=6)         # Max 1 SMS per 6 hours per user
        }
        
        # Track last notifications
        self.last_notifications = {}
        
        # Quiet hours (no notifications except emergencies)
        self.default_quiet_hours = {
            'start': '22:00',
            'end': '07:00'
        }
    
    def should_send_notification(self, user_id: str, channel: str, alert_level: str) -> bool:
        """Check if notification should be sent based on rate limiting and quiet hours"""
        
        # Emergency alerts always go through
        if alert_level in ['hazardous', 'very_unhealthy']:
            return True
        
        last_key = f"{user_id}_{channel}"
        if last_key in self.last_notifications:
            time_since_last = datetime.now() - self.last_notifications[last_key]
            if time_since_last < self.rate_limits.get(channel, timedelta(0)):
                logger.info(f"Rate limit hit for {user_id} on {channel}")
                return False
        
        current_time = datetime.now().time()
        quiet_start = datetime.strptime(self.default_quiet_hours['start'], '%H:%M').time()
        quiet_end = datetime.strptime(self.default_quiet_hours['end'], '%H:%M').time()
        
        if quiet_start <= current_time or current_time <= quiet_end:
            if alert_level not in ['hazardous', 'very_unhealthy']:
                logger.info(f"Quiet hours active, skipping non-emergency alert for {user_id}")
                return False
        
        return True
    
    def send_alert_notifications(self, alert: Dict, user_profile: Dict) -> Dict:
        """Send all configured notifications for an alert"""
        
        results = {
            'email': {'sent': False, 'error': None},
            'push': {'sent': False, 'error': None},
            'sms': {'sent': False, 'error': None},
            'total_sent': 0
        }
        
        user_id = user_profile['user_id']
        user_name = user_profile['name']
        user_email = user_profile['email']
        notification_prefs = user_profile.get('notification_preferences', {})
        
        # Email notifications
        if notification_prefs.get('email', True):
            if self.should_send_notification(user_id, 'email', alert['alert_level']):
                try:
                    success = self.email_service.send_alert_email(
                        alert=alert,
                        user_email=user_email,
                        user_name=user_name
                    )
                    
                    if success:
                        results['email']['sent'] = True
                        results['total_sent'] += 1
                        self.last_notifications[f"{user_id}_email"] = datetime.now()
                        logger.info(f"Email alert sent to {user_name}")
                    else:
                        results['email']['error'] = "Failed to send email"
                        
                except Exception as e:
                    results['email']['error'] = str(e)
                    logger.error(f"Email notification error for {user_name}: {e}")
        
        # Push notifications
        if notification_prefs.get('push', True) or notification_prefs.get('web_push', True):
            if self.should_send_notification(user_id, 'push', alert['alert_level']):
                try:
                    subscriptions = user_profile.get('push_subscriptions', [
                        {'endpoint': 'demo-endpoint-web', 'type': 'web'},
                        {'endpoint': 'demo-endpoint-mobile', 'type': 'mobile'}
                    ])
                    
                    push_results = self.push_service.send_browser_notification(
                        user_subscriptions=subscriptions,
                        alert=alert,
                        user_name=user_name
                    )
                    
                    if push_results['sent'] > 0:
                        results['push']['sent'] = True
                        results['total_sent'] += 1
                        self.last_notifications[f"{user_id}_push"] = datetime.now()
                        logger.info(f"Push notification sent to {user_name} ({push_results['sent']} devices)")
                    
                    if push_results['failed'] > 0:
                        results['push']['error'] = f"Failed on {push_results['failed']} devices"
                        
                except Exception as e:
                    results['push']['error'] = str(e)
                    logger.error(f"Push notification error for {user_name}: {e}")
        
        # SMS notifications (emergency only or explicit preference)
        if (notification_prefs.get('sms', False) or 
            alert['alert_level'] in ['hazardous', 'very_unhealthy']):
            
            if self.should_send_notification(user_id, 'sms', alert['alert_level']):
                try:
                    # SMS would be implemented here (AWS SNS, Twilio, etc.)
                    sms_success = self.send_sms_alert(user_profile, alert)
                    
                    if sms_success:
                        results['sms']['sent'] = True
                        results['total_sent'] += 1
                        self.last_notifications[f"{user_id}_sms"] = datetime.now()
                        logger.info(f"SMS alert sent to {user_name}")
                    else:
                        results['sms']['error'] = "SMS service unavailable"
                        
                except Exception as e:
                    results['sms']['error'] = str(e)
                    logger.error(f"SMS notification error for {user_name}: {e}")
        
        return results
    
    def send_push_notification(self, alert_data: Dict, user_preferences: Dict = None) -> Dict:
        """Send push notification for alert data"""
        try:
            user_name = alert_data.get('user_name', 'User')
            
            subscriptions = [
                {
                    'endpoint': f"https://fcm.googleapis.com/fcm/send/test-{alert_data.get('user_id', 'demo')}", 
                    'type': 'web',
                    'keys': {
                        'p256dh': 'demo-key',
                        'auth': 'demo-auth'
                    }
                }
            ]
            
            alert = {
                'alert_id': alert_data.get('alert_id'),
                'location': alert_data.get('location', {}),
                'current_aqi': alert_data.get('current_aqi', 100),
                'threshold_aqi': alert_data.get('threshold_aqi', 80),
                'pollutant': alert_data.get('pollutant', ['PM2.5']),
                'alert_level': alert_data.get('alert_level', 'moderate'),
                'message': alert_data.get('message', 'Air quality alert'),
                'timestamp': alert_data.get('timestamp', datetime.now().isoformat())
            }
            
            push_results = self.push_service.send_browser_notification(
                user_subscriptions=subscriptions,
                alert=alert,
                user_name=user_name
            )
            
            notification_data = self.push_service.create_push_notification(alert, user_name)
            
            return {
                'success': push_results.get('sent', 0) > 0,
                'sent_count': push_results.get('sent', 0),
                'failed_count': push_results.get('failed', 0),
                'error': push_results.get('error') if push_results.get('failed', 0) > 0 else None,
                'notification_data': notification_data
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'sent_count': 0,
                'failed_count': 1
            }
    
    def send_sms_alert(self, user_profile: Dict, alert: Dict) -> bool:
        """Send SMS alert (demo implementation)"""
        
        phone = user_profile.get('phone')
        if not phone:
            return False
        
        message = (f"🚨 AIR QUALITY ALERT\n"
                  f"{alert['location']['city']}: {alert['pollutant']} "
                  f"AQI {alert['aqi_value']} ({alert['alert_level'].replace('_', ' ').title()})\n"
                  f"EPA: {alert['epa_message'][:100]}...\n"
                  f"Reply STOP to unsubscribe")
        
        logger.info(f"SMS ALERT (Demo):")
        logger.info(f"To: {phone}")
        logger.info(f"Message: {message[:100]}...")
        
        """
        import boto3
        sns = boto3.client('sns')
        response = sns.publish(
            PhoneNumber=phone,
            Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional'
                }
            }
        )
        return response['ResponseMetadata']['HTTPStatusCode'] == 200
        """
        
        return True
    
    def send_daily_summary(self, user_profile: Dict, daily_data: Dict) -> bool:
        """Send daily air quality summary"""
        
        summary_alert = {
            'alert_id': f"daily_summary_{datetime.now().strftime('%Y%m%d')}",
            'user_id': user_profile['user_id'],
            'timestamp': datetime.now().isoformat(),
            'location': daily_data['location'],
            'alert_level': 'summary',
            'epa_message': self.create_daily_summary_message(daily_data),
            'user_conditions': user_profile.get('health_conditions', [])
        }
        
        if user_profile.get('notification_preferences', {}).get('daily_summary', False):
            try:
                success = self.email_service.send_daily_summary_email(
                    summary_data=daily_data,
                    user_email=user_profile['email'],
                    user_name=user_profile['name']
                )
                
                if success:
                    logger.info(f"Daily summary sent to {user_profile['name']}")
                    return True
                    
            except Exception as e:
                logger.error(f"Daily summary error for {user_profile['name']}: {e}")
        
        return False
    
    def create_daily_summary_message(self, daily_data: Dict) -> str:
        """Create EPA-compliant daily summary message"""
        
        avg_aqi = daily_data.get('average_aqi', 0)
        max_aqi = daily_data.get('max_aqi', 0)
        dominant_pollutant = daily_data.get('dominant_pollutant', 'PM25')
        
        if avg_aqi <= 50:
            message = "Today's air quality was good. Great day for outdoor activities."
        elif avg_aqi <= 100:
            message = "Today's air quality was moderate. Generally acceptable for most people."
        elif avg_aqi <= 150:
            message = "Today's air quality was unhealthy for sensitive groups. Sensitive individuals should have limited outdoor activities."
        else:
            message = "Today's air quality was unhealthy. Outdoor activities should have been limited."
        
        forecast = daily_data.get('tomorrow_forecast', {})
        if forecast:
            forecast_aqi = forecast.get('aqi', 0)
            if forecast_aqi > avg_aqi + 20:
                message += " Tomorrow's air quality is expected to worsen."
            elif forecast_aqi < avg_aqi - 20:
                message += " Tomorrow's air quality is expected to improve."
            else:
                message += " Tomorrow's air quality is expected to be similar."
        
        return message
    
    def send_forecast_warning(self, user_profile: Dict, forecast_alert: Dict) -> Dict:
        """Send advance warning based on forecast data"""
        
        # Modify alert level for forecast warnings
        forecast_alert['alert_level'] = 'forecast_' + forecast_alert['alert_level']
        forecast_alert['epa_message'] = (
            f"FORECAST WARNING: {forecast_alert['epa_message']} "
            f"Expected at {datetime.fromisoformat(forecast_alert['timestamp']).strftime('%I:%M %p')}."
        )
        
        return self.send_alert_notifications(forecast_alert, user_profile)
    
    def get_notification_stats(self) -> Dict:
        """Get notification delivery statistics"""
        
        stats = {
            'total_notifications_sent': len(self.last_notifications),
            'notifications_by_channel': {
                'email': len([k for k in self.last_notifications.keys() if 'email' in k]),
                'push': len([k for k in self.last_notifications.keys() if 'push' in k]),
                'sms': len([k for k in self.last_notifications.keys() if 'sms' in k])
            },
            'recent_notifications': list(self.last_notifications.keys())[-10:],
            'last_hour_count': len([
                t for t in self.last_notifications.values()
                if datetime.now() - t < timedelta(hours=1)
            ])
        }
        
        return stats

def test_notification_manager():
    """Test the notification manager"""
    
    print("📨 TESTING NOTIFICATION MANAGER")
    print("=" * 50)
    
    notification_manager = NotificationManager()
    
    # Sample user profile
    user_profile = {
        'user_id': 'test_user_123',
        'name': 'Sarah Johnson',
        'email': 'sarah.johnson@example.com',
        'phone': '+1-555-0123',
        'health_conditions': ['asthma', 'pregnant'],
        'notification_preferences': {
            'email': True,
            'push': True,
            'web_push': True,
            'sms': False,
            'daily_summary': True
        },
        'push_subscriptions': [
            {'endpoint': 'web-push-endpoint-123', 'type': 'web'},
            {'endpoint': 'mobile-push-endpoint-456', 'type': 'mobile'}
        ]
    }
    
    # Sample alert
    alert = {
        'alert_id': 'alert_123',
        'user_id': 'test_user_123',
        'timestamp': datetime.now().isoformat(),
        'location': {'city': 'Chicago', 'latitude': 41.8781, 'longitude': -87.6298},
        'pollutant': 'PM25',
        'aqi_value': 125,
        'alert_level': 'unhealthy_sensitive',
        'epa_message': 'Sensitive groups: Make outdoor activities shorter and less intense. It\'s OK to be active outdoors but take more breaks. Watch for symptoms such as coughing or shortness of breath. People with asthma: Follow your asthma action plan and keep quick relief medicine handy.',
        'user_conditions': user_profile['health_conditions']
    }
    
    print("📢 Sending alert notifications...")
    results = notification_manager.send_alert_notifications(alert, user_profile)
    
    print(f"📊 Notification Results:")
    for channel, result in results.items():
        if channel != 'total_sent':
            status = "✅ Sent" if result['sent'] else f"❌ Failed: {result.get('error', 'Unknown error')}"
            print(f"   {channel.title()}: {status}")
    
    print(f"📈 Total notifications sent: {results['total_sent']}")
    
    # Test daily summary
    print(f"\n📅 Testing daily summary...")
    daily_data = {
        'location': {'city': 'Chicago'},
        'average_aqi': 78,
        'max_aqi': 95,
        'dominant_pollutant': 'PM25',
        'tomorrow_forecast': {'aqi': 85}
    }
    
    summary_sent = notification_manager.send_daily_summary(user_profile, daily_data)
    print(f"Daily summary: {'✅ Sent' if summary_sent else '❌ Failed'}")
    
    print(f"\n📊 Notification Statistics:")
    stats = notification_manager.get_notification_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    print(f"\n🎉 Notification manager test completed!")

if __name__ == "__main__":
    test_notification_manager()
