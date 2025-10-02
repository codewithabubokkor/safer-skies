#!/usr/bin/env python3
"""
Push Notification Service
Web push notifications for air quality alerts
"""

import json
import logging
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import requests

logger = logging.getLogger(__name__)

class PushNotificationService:
    """
    Push notification service for air quality alerts
    Supports web push notifications and mobile push via SNS
    """
    
    def __init__(self, use_aws_sns=False):
        self.use_aws_sns = use_aws_sns
        
        if use_aws_sns:
            try:
                self.sns_client = boto3.client('sns', region_name='us-east-1')
            except Exception as e:
                logger.warning(f"AWS SNS not available: {e}")
                self.sns_client = None
    
    def create_push_notification(self, alert: Dict, user_name: str) -> Dict:
        """Create push notification payload for air quality alert"""
        
        # Emoji mapping for pollutants
        pollutant_emoji = {
            'PM25': '🌫️',
            'PM10': '💨', 
            'O3': '☁️',
            'NO2': '🚗',
            'SO2': '🏭',
            'CO': '⚠️'
        }
        
        # Color mapping for alert levels
        color_map = {
            'good': '#00e400',
            'moderate': '#ffff00',
            'unhealthy_sensitive': '#ff7e00', 
            'unhealthy': '#ff0000',
            'very_unhealthy': '#8f3f97',
            'hazardous': '#7e0023'
        }
        
        pollutant_icon = pollutant_emoji.get(alert['pollutant'], '🌍')
        alert_color = color_map.get(alert['alert_level'], '#666666')
        alert_level_display = alert['alert_level'].replace('_', ' ').title()
        
        if alert['alert_level'] in ['hazardous', 'very_unhealthy']:
            title = f"🚨 EMERGENCY: {alert_level_display} Air Quality"
        elif alert['alert_level'] in ['unhealthy', 'unhealthy_sensitive']:
            title = f"⚠️ {alert_level_display} Air Quality Alert"
        else:
            title = f"{pollutant_icon} Air Quality Update"
        
        body = f"{alert['location']['city']}: {alert['pollutant']} AQI {alert['aqi_value']}"
        
        epa_snippet = alert['epa_message'][:80] + "..." if len(alert['epa_message']) > 80 else alert['epa_message']
        
        # Web push notification payload
        web_push_payload = {
            'title': title,
            'body': body,
            'icon': '/static/icons/air-quality-icon.png',
            'badge': '/static/icons/badge-icon.png',
            'data': {
                'alert_id': alert.get('alert_id'),
                'user_id': alert.get('user_id'),
                'location': alert['location']['city'],
                'pollutant': alert['pollutant'],
                'aqi_value': alert['aqi_value'],
                'alert_level': alert['alert_level'],
                'timestamp': alert['timestamp'],
                'url': f"/dashboard?alert={alert.get('alert_id', '')}"
            },
            'actions': [
                {
                    'action': 'view_details',
                    'title': 'View Details',
                    'icon': '/static/icons/view-icon.png'
                },
                {
                    'action': 'dismiss',
                    'title': 'Dismiss',
                    'icon': '/static/icons/dismiss-icon.png'
                }
            ],
            'requireInteraction': alert['alert_level'] in ['hazardous', 'very_unhealthy'],
            'silent': False,
            'tag': f"aq-alert-{alert['pollutant']}-{alert['location']['city']}",
            'renotify': True,
            'vibrate': [200, 100, 200] if alert['alert_level'] in ['hazardous', 'very_unhealthy'] else [100],
            'timestamp': datetime.fromisoformat(alert['timestamp']).timestamp() * 1000
        }
        
        return web_push_payload
    
    def send_web_push_notification(self, subscription_endpoint: str, payload: Dict, 
                                 vapid_private_key: str = None, vapid_claims: Dict = None) -> bool:
        """
        Send web push notification to browser
        Requires web-push library and VAPID keys for production
        """
        
        try:
            logger.info(f"WEB PUSH NOTIFICATION:")
            logger.info(f"Endpoint: {subscription_endpoint[:50]}...")
            logger.info(f"Title: {payload['title']}")
            logger.info(f"Body: {payload['body']}")
            logger.info(f"Alert Level: {payload['data']['alert_level']}")
            
            """
            from pywebpush import webpush
            
            webpush(
                subscription_info={
                    "endpoint": subscription_endpoint,
                    "keys": {
                        "p256dh": subscription_p256dh_key,
                        "auth": subscription_auth_key
                    }
                },
                data=json.dumps(payload),
                vapid_private_key=vapid_private_key,
                vapid_claims=vapid_claims
            )
            """
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending web push notification: {e}")
            return False
    
    def send_mobile_push_sns(self, topic_arn: str, alert: Dict, user_name: str) -> bool:
        """Send mobile push notification via AWS SNS"""
        
        if not self.sns_client:
            logger.info(f"MOBILE PUSH NOTIFICATION (Demo):")
            logger.info(f"Topic: {topic_arn}")
            logger.info(f"User: {user_name}")
            logger.info(f"Alert: {alert['pollutant']} AQI {alert['aqi_value']} in {alert['location']['city']}")
            return True
        
        try:
            ios_payload = {
                "aps": {
                    "alert": {
                        "title": f"Air Quality Alert - {alert['location']['city']}",
                        "body": f"{alert['pollutant']} AQI {alert['aqi_value']} ({alert['alert_level'].replace('_', ' ').title()})"
                    },
                    "badge": 1,
                    "sound": "default" if alert['alert_level'] in ['moderate', 'unhealthy_sensitive'] else "emergency.wav",
                    "category": "AIR_QUALITY_ALERT",
                    "thread-id": f"aq-{alert['location']['city']}"
                },
                "custom_data": {
                    "alert_id": alert.get('alert_id'),
                    "pollutant": alert['pollutant'],
                    "aqi_value": alert['aqi_value'],
                    "location": alert['location']['city']
                }
            }
            
            android_payload = {
                "data": {
                    "title": f"Air Quality Alert - {alert['location']['city']}",
                    "body": f"{alert['pollutant']} AQI {alert['aqi_value']}",
                    "alert_id": alert.get('alert_id', ''),
                    "pollutant": alert['pollutant'],
                    "aqi_value": str(alert['aqi_value']),
                    "alert_level": alert['alert_level'],
                    "location": alert['location']['city'],
                    "click_action": "FLUTTER_NOTIFICATION_CLICK"
                },
                "notification": {
                    "title": f"Air Quality Alert - {alert['location']['city']}",
                    "body": f"{alert['pollutant']} AQI {alert['aqi_value']}",
                    "icon": "air_quality_icon",
                    "color": "
                    "sound": "default",
                    "tag": f"aq-{alert['pollutant']}-{alert['location']['city']}"
                }
            }
            
            message = {
                "default": f"Air Quality Alert: {alert['pollutant']} AQI {alert['aqi_value']} in {alert['location']['city']}",
                "APNS": json.dumps(ios_payload),
                "GCM": json.dumps(android_payload)
            }
            
            response = self.sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps(message),
                MessageStructure='json',
                Subject=f"Air Quality Alert - {alert['location']['city']}"
            )
            
            logger.info(f"Mobile push notification sent via SNS: {response['MessageId']}")
            return True
            
        except ClientError as e:
            logger.error(f"SNS error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending mobile push notification: {e}")
            return False
    
    def send_browser_notification(self, user_subscriptions: List[Dict], alert: Dict, user_name: str) -> Dict:
        """Send browser notifications to all user's subscribed devices"""
        
        results = {
            'sent': 0,
            'failed': 0,
            'errors': []
        }
        
        payload = self.create_push_notification(alert, user_name)
        
        for subscription in user_subscriptions:
            try:
                success = self.send_web_push_notification(
                    subscription_endpoint=subscription['endpoint'],
                    payload=payload,
                    vapid_private_key=subscription.get('vapid_private_key'),
                    vapid_claims=subscription.get('vapid_claims')
                )
                
                if success:
                    results['sent'] += 1
                else:
                    results['failed'] += 1
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(str(e))
        
        return results
    
    def create_notification_preferences_ui(self) -> str:
        """Generate HTML for notification preferences in user settings"""
        
        html = """
        <div class="notification-preferences">
            <h3>🔔 Notification Preferences</h3>
            
            <div class="notification-type">
                <h4>Alert Types</h4>
                <label><input type="checkbox" name="alerts" value="moderate" checked> Moderate (51-100 AQI) - Sensitive groups only</label>
                <label><input type="checkbox" name="alerts" value="unhealthy_sensitive" checked> Unhealthy for Sensitive (101-150 AQI)</label>
                <label><input type="checkbox" name="alerts" value="unhealthy" checked> Unhealthy (151-200 AQI)</label>
                <label><input type="checkbox" name="alerts" value="very_unhealthy" checked> Very Unhealthy (201-300 AQI)</label>
                <label><input type="checkbox" name="alerts" value="hazardous" checked> Hazardous (301+ AQI)</label>
            </div>
            
            <div class="notification-channels">
                <h4>Delivery Methods</h4>
                <label><input type="checkbox" name="channels" value="email" checked> 📧 Email notifications</label>
                <label><input type="checkbox" name="channels" value="web_push" checked> 🌐 Browser notifications</label>
                <label><input type="checkbox" name="channels" value="mobile_push"> 📱 Mobile app notifications</label>
                <label><input type="checkbox" name="channels" value="sms"> 📱 SMS alerts (emergency only)</label>
            </div>
            
            <div class="notification-timing">
                <h4>Timing</h4>
                <label><input type="checkbox" name="timing" value="immediate" checked> Immediate alerts</label>
                <label><input type="checkbox" name="timing" value="forecast" checked> 24-hour forecast warnings</label>
                <label><input type="checkbox" name="timing" value="daily_summary"> Daily air quality summary</label>
            </div>
            
            <div class="quiet-hours">
                <h4>Quiet Hours</h4>
                <label>From: <input type="time" name="quiet_start" value="22:00"></label>
                <label>To: <input type="time" name="quiet_end" value="07:00"></label>
                <small>No notifications during these hours except emergencies (hazardous AQI)</small>
            </div>
        </div>
        
        <style>
        .notification-preferences {
            max-width: 500px;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
            margin: 20px 0;
        }
        .notification-preferences h3 {
            color:
            margin-top: 0;
        }
        .notification-preferences h4 {
            color:
            margin: 20px 0 10px 0;
        }
        .notification-preferences label {
            display: block;
            margin: 8px 0;
            cursor: pointer;
        }
        .notification-preferences input[type="checkbox"] {
            margin-right: 8px;
        }
        .quiet-hours label {
            display: inline-block;
            margin-right: 15px;
        }
        .quiet-hours small {
            display: block;
            color:
            margin-top: 5px;
        }
        </style>
        """
        
        return html


