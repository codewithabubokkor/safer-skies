#!/usr/bin/env python3
"""
Email Notification Service
EPA-compliant email alerts for air quality notifications
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import logging
from datetime import datetime
from typing import Dict, Optional
import os
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class EmailNotificationService:
    """
    Email notification service for air quality alerts
    Safer Skies - NASA Space Apps Challenge 2025
    Supports both AWS SES and SMTP
    """
    
    def __init__(self, use_aws_ses=False, smtp_config=None):
        self.use_aws_ses = use_aws_ses
        self.smtp_config = smtp_config or {}
        
        if use_aws_ses:
            try:
                self.ses_client = boto3.client('ses', region_name='us-east-1')
            except Exception as e:
                logger.warning(f"AWS SES not available: {e}")
                self.ses_client = None
    
    def create_alert_email_html(self, alert: Dict, user_name: str) -> str:
        """Create EPA-compliant HTML email for air quality alert"""
        
        # Color mapping for AQI levels
        color_map = {
            'good': '#00e400',
            'moderate': '#ffff00', 
            'unhealthy_sensitive': '#ff7e00',
            'unhealthy': '#ff0000',
            'very_unhealthy': '#8f3f97',
            'hazardous': '#7e0023'
        }
        
        alert_color = color_map.get(alert['alert_level'], '#666666')
        alert_level_display = alert['alert_level'].replace('_', ' ').title()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Air Quality Alert - {alert['location']['city']}</title>
        </head>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto;">
            
            <!-- Header -->
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; text-align: center;">
                <h1 style="margin: 0; font-size: 28px;">🌍 Safer Skies Alert</h1>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">NASA Space Apps Challenge - Air Quality Monitoring</p>
            </div>
            
            <!-- Alert Banner -->
            <div style="background-color: {alert_color}; color: {'white' if alert['alert_level'] in ['unhealthy', 'very_unhealthy', 'hazardous'] else 'black'}; padding: 15px; text-align: center; font-weight: bold; font-size: 18px;">
                ⚠️ {alert_level_display} Air Quality Alert
            </div>
            
            <!-- Main Content -->
            <div style="padding: 20px; background-color: #f8f9fa;">
                
                <!-- Greeting -->
                <h2 style="color: #333; margin-top: 0;">Hello {user_name},</h2>
                
                <!-- Alert Details -->
                <div style="background: white; padding: 20px; border-radius: 8px; margin: 15px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                    <h3 style="color: #333; margin-top: 0; border-bottom: 2px solid #eee; padding-bottom: 10px;">
                        📍 Alert Details for {alert['location']['city']}
                    </h3>
                    
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 15px 0;">
                        <div>
                            <strong>🔬 Pollutant:</strong><br>
                            <span style="font-size: 18px; color: {alert_color};">{alert['pollutant']}</span>
                        </div>
                        <div>
                            <strong>📊 AQI Value:</strong><br>
                            <span style="font-size: 24px; font-weight: bold; color: {alert_color};">{alert['aqi_value']}</span>
                        </div>
                        <div>
                            <strong>⚠️ Alert Level:</strong><br>
                            <span style="color: {alert_color};">{alert_level_display}</span>
                        </div>
                        <div>
                            <strong>🕐 Time:</strong><br>
                            {datetime.fromisoformat(alert['timestamp']).strftime('%B %d, %Y at %I:%M %p')}
                        </div>
                    </div>
                </div>
                
                <!-- EPA Health Guidance -->
                <div style="background: #e7f3ff; padding: 20px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #2196F3;">
                    <h4 style="color: #1976D2; margin-top: 0;">🏥 EPA Health Guidance</h4>
                    <p style="margin: 0; font-size: 16px; line-height: 1.6;">{alert['epa_message']}</p>
                </div>
                
                <!-- Personal Health Info -->
                {self._get_personal_health_section(alert)}
                
                <!-- Action Items -->
                <div style="background: #fff3cd; padding: 20px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #ffc107;">
                    <h4 style="color: #856404; margin-top: 0;">💡 Recommended Actions</h4>
                    <ul style="margin: 0; padding-left: 20px;">
                        {self._get_action_items(alert)}
                    </ul>
                </div>
                
                <!-- Data Sources -->
                <div style="background: #d4edda; padding: 15px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #28a745;">
                    <h4 style="color: #155724; margin-top: 0;">📡 Data Sources</h4>
                    <p style="margin: 0; font-size: 14px;">
                        This alert is based on official EPA health guidance and uses NASA Earth observation data including:
                    </p>
                    <ul style="margin: 10px 0 0 0; padding-left: 20px; font-size: 14px;">
                        <li>NASA TEMPO satellite observations</li>
                        <li>NASA GEOS-CF atmospheric chemistry forecasts</li>
                        <li>EPA AirNow ground monitoring network</li>
                        <li>Meteorological analysis from GFS weather model</li>
                    </ul>
                </div>
                
                <!-- Footer -->
                <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd;">
                    <p style="color: #666; font-size: 14px; margin: 0;">
                        <strong>Safer Skies</strong> - NASA Space Apps Challenge 2025<br>
                        Protecting public health with real-time air quality monitoring
                    </p>
                    <p style="color: #999; font-size: 12px; margin: 10px 0 0 0;">
                        This system uses EPA-compliant health recommendations and NASA Earth science data.<br>
                        For medical emergencies, contact your healthcare provider immediately.
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _get_personal_health_section(self, alert: Dict) -> str:
        """Generate personalized health information based on user conditions"""
        if not alert.get('user_conditions'):
            return ""
        
        conditions = alert['user_conditions']
        section = '<div style="background: #f8d7da; padding: 20px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #dc3545;">'
        section += '<h4 style="color: #721c24; margin-top: 0;">🏥 Personal Health Considerations</h4>'
        
        if 'asthma' in conditions or 'lung_disease' in conditions:
            section += '<p style="margin: 0 0 10px 0;"><strong>Asthma/Respiratory:</strong> Have your rescue inhaler readily available. Follow your asthma action plan.</p>'
        
        if 'heart_disease' in conditions or 'cardiovascular' in conditions:
            section += '<p style="margin: 0 0 10px 0;"><strong>Heart Disease:</strong> Watch for symptoms like palpitations, shortness of breath, or unusual fatigue. Contact your healthcare provider if symptoms occur.</p>'
        
        if 'elderly' in conditions:
            section += '<p style="margin: 0 0 10px 0;"><strong>Older Adult:</strong> You may be more sensitive to air pollution. Consider staying indoors during peak pollution hours.</p>'
        
        if 'children' in conditions:
            section += '<p style="margin: 0 0 10px 0;"><strong>Child/Teen:</strong> Reduce time playing outdoors. Watch for coughing or breathing difficulties.</p>'
        
        if 'pregnant' in conditions:
            section += '<p style="margin: 0 0 10px 0;"><strong>Pregnancy:</strong> Limit outdoor activities to protect both you and your developing baby.</p>'
        
        if 'outdoor_worker' in conditions:
            section += '<p style="margin: 0 0 10px 0;"><strong>Outdoor Worker:</strong> Take frequent breaks indoors. Consider wearing N95 masks if work cannot be postponed.</p>'
        
        section += '</div>'
        return section
    
    def _get_action_items(self, alert: Dict) -> str:
        """Generate action items based on alert level and pollutant"""
        items = []
        
        if alert['alert_level'] in ['hazardous', 'very_unhealthy']:
            items.append("Stay indoors and keep windows closed")
            items.append("Avoid all outdoor activities")
            items.append("Use air purifiers if available")
            items.append("Seek medical attention if you experience symptoms")
        
        elif alert['alert_level'] in ['unhealthy', 'unhealthy_sensitive']:
            items.append("Reduce prolonged outdoor activities")
            items.append("Move exercise indoors")
            items.append("Keep windows closed during peak hours")
            if alert['pollutant'] == 'O3':
                items.append("Plan outdoor activities for early morning")
        
        elif alert['alert_level'] == 'moderate':
            items.append("Limit prolonged outdoor exertion if you're sensitive")
            items.append("Watch for symptoms like coughing or shortness of breath")
            items.append("Have rescue medications available if needed")
        
        # Pollutant-specific advice
        if alert['pollutant'] == 'PM25':
            items.append("Close windows and use air conditioning")
            items.append("Avoid areas with heavy traffic")
        
        elif alert['pollutant'] == 'O3':
            items.append("Avoid outdoor activities between 10 AM - 6 PM")
            items.append("Choose early morning for outdoor exercise")
        
        elif alert['pollutant'] == 'NO2':
            items.append("Stay away from busy roads and highways")
            items.append("Limit time near traffic")
        
        return ''.join([f'<li>{item}</li>' for item in items])
    
    def send_alert_email(self, alert: Dict, user_email: str, user_name: str) -> bool:
        """Send air quality alert email"""
        
        subject = f"🚨 Air Quality Alert - {alert['location']['city']} ({alert['alert_level'].replace('_', ' ').title()})"
        
        html_body = self.create_alert_email_html(alert, user_name)
        
        text_body = self._create_text_version(alert, user_name)
        
        try:
            if self.use_aws_ses and self.ses_client:
                return self._send_via_ses(user_email, subject, html_body, text_body)
            else:
                return self._send_via_smtp(user_email, subject, html_body, text_body)
                
        except Exception as e:
            logger.error(f"Error sending email to {user_email}: {e}")
            return False
    
    def _create_text_version(self, alert: Dict, user_name: str) -> str:
        """Create plain text version of the email"""
        
        text = f"""
SAFER SKIES - AIR QUALITY ALERT
================================

Hello {user_name},

ALERT DETAILS:
--------------
Location: {alert['location']['city']}
Pollutant: {alert['pollutant']}
AQI Value: {alert['aqi_value']}
Alert Level: {alert['alert_level'].replace('_', ' ').title()}
Time: {datetime.fromisoformat(alert['timestamp']).strftime('%B %d, %Y at %I:%M %p')}

EPA HEALTH GUIDANCE:
-------------------
{alert['epa_message']}

PERSONAL HEALTH CONSIDERATIONS:
------------------------------
{self._get_text_health_info(alert)}

DATA SOURCES:
------------
This alert uses official EPA health guidance and NASA Earth observation data including TEMPO satellite observations, GEOS-CF forecasts, EPA AirNow monitoring, and GFS weather analysis.

For medical emergencies, contact your healthcare provider immediately.

---
Safer Skies - NASA Space Apps Challenge 2025
Team AURA | Powered by NASA TEMPO & AirNow data
        """
        
        return text.strip()
    
    def _get_text_health_info(self, alert: Dict) -> str:
        """Get text version of health information"""
        if not alert.get('user_conditions'):
            return "No specific health conditions on file."
        
        conditions = alert['user_conditions']
        info = []
        
        if 'asthma' in conditions:
            info.append("• Asthma: Have rescue inhaler available, follow action plan")
        if 'heart_disease' in conditions:
            info.append("• Heart Disease: Watch for symptoms, contact doctor if needed")
        if 'elderly' in conditions:
            info.append("• Older Adult: Consider staying indoors during peak hours")
        if 'children' in conditions:
            info.append("• Child/Teen: Reduce outdoor play, watch for symptoms")
        if 'pregnant' in conditions:
            info.append("• Pregnancy: Limit outdoor activities for safety")
        if 'outdoor_worker' in conditions:
            info.append("• Outdoor Worker: Take breaks, consider protective equipment")
        
        return '\n'.join(info) if info else "Based on your health profile, follow EPA guidance above."
    
    def _send_via_ses(self, to_email: str, subject: str, html_body: str, text_body: str) -> bool:
        """Send email via AWS SES"""
        try:
            response = self.ses_client.send_email(
                Source='noreply@saferskies.app',
                Destination={'ToAddresses': [to_email]},
                Message={
                    'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                    'Body': {
                        'Text': {'Data': text_body, 'Charset': 'UTF-8'},
                        'Html': {'Data': html_body, 'Charset': 'UTF-8'}
                    }
                }
            )
            logger.info(f"Email sent via SES to {to_email}, MessageId: {response['MessageId']}")
            return True
            
        except ClientError as e:
            logger.error(f"SES error: {e}")
            return False
    
    def _send_via_smtp(self, to_email: str, subject: str, html_body: str, text_body: str) -> bool:
        """Send email via SMTP - Production Implementation"""
        
        try:
            smtp_server = self.smtp_config.get('server', os.getenv('SMTP_SERVER', 'smtp.gmail.com'))
            smtp_port = int(self.smtp_config.get('port', os.getenv('SMTP_PORT', '587')))
            smtp_user = self.smtp_config.get('username', os.getenv('SENDER_EMAIL'))
            smtp_pass = self.smtp_config.get('password', os.getenv('SENDER_PASSWORD'))
            
            if not smtp_user or not smtp_pass:
                logger.error("SMTP credentials not configured")
                return False
            
            logger.info(f"Sending email to {to_email} via {smtp_server}:{smtp_port}")
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = smtp_user
            msg['To'] = to_email
            
            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))
            
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            try:
                server = smtplib.SMTP(smtp_server, smtp_port, timeout=60)
                server.starttls(context=context)
                server.login(smtp_user, smtp_pass)
                server.send_message(msg)
                server.quit()
                
                logger.info(f"✅ Email sent successfully to {to_email}")
                return True
                
            except Exception as e:
                logger.warning(f"Port 587 failed, trying port 465: {e}")
                
                # Fallback to port 465 with SSL
                try:
                    server = smtplib.SMTP_SSL(smtp_server, 465, context=context, timeout=60)
                    server.login(smtp_user, smtp_pass)
                    server.send_message(msg)
                    server.quit()
                    
                    logger.info(f"✅ Email sent successfully to {to_email} via SSL")
                    return True
                    
                except Exception as e2:
                    logger.error(f"Both SMTP methods failed: {e2}")
                    return False
            
        except Exception as e:
            logger.error(f"SMTP email sending failed: {e}")
            return False
    
    def send_daily_summary_email(self, summary_data: Dict, user_email: str, user_name: str) -> bool:
        """Send daily air quality summary email"""
        
        try:
            # Email subject
            city = summary_data.get('location', {}).get('city', 'Your Area')
            date_str = datetime.now().strftime('%B %d, %Y')
            subject = f"Daily Air Quality Summary - {city} - {date_str}"
            
            html_content = self._create_daily_summary_template(summary_data, user_name)
            
            text_content = self._create_daily_summary_text(summary_data, user_name)
            
            if self.use_aws_ses and self.ses_client:
                success = self._send_via_ses(user_email, subject, html_content, text_content)
            else:
                success = self._send_via_smtp(user_email, subject, html_content, text_content)
            
            if success:
                logger.info(f"Daily summary email sent to {user_name} at {user_email}")
            else:
                logger.error(f"Failed to send daily summary email to {user_name}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending daily summary email to {user_name}: {e}")
            return False
    
    def _create_daily_summary_template(self, summary_data: Dict, user_name: str) -> str:
        """Create HTML template for daily summary"""
        
        city = summary_data.get('location', {}).get('city', 'Your Area')
        avg_aqi = summary_data.get('average_aqi', 0)
        max_aqi = summary_data.get('max_aqi', 0)
        dominant_pollutant = summary_data.get('dominant_pollutant', 'PM25')
        
        if max_aqi <= 50:
            color = '#4CAF50'
            level = 'Good'
        elif max_aqi <= 100:
            color = '#FFEB3B'
            level = 'Moderate'
        elif max_aqi <= 150:
            color = '#FF9800'
            level = 'Unhealthy for Sensitive Groups'
        elif max_aqi <= 200:
            color = '
            level = 'Unhealthy'
        else:
            color = '
            level = 'Very Unhealthy'
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Daily Air Quality Summary</title>
        </head>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg,
                <h1 style="margin: 0; font-size: 24px;">📅 Daily Air Quality Summary</h1>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">Hello {user_name}!</p>
            </div>
            
            <div style="background: white; padding: 20px; border: 1px solid #ddd; border-radius: 0 0 10px 10px;">
                <!-- Summary Stats -->
                <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 15px 0;">
                    <h3 style="color: #333; margin-top: 0;">📊 {city} - {datetime.now().strftime('%B %d, %Y')}</h3>
                    
                    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 15px; margin: 15px 0;">
                        <div style="text-align: center;">
                            <strong>Average AQI</strong><br>
                            <span style="font-size: 24px; color: {color};">{avg_aqi}</span>
                        </div>
                        <div style="text-align: center;">
                            <strong>Peak AQI</strong><br>
                            <span style="font-size: 24px; font-weight: bold; color: {color};">{max_aqi}</span>
                        </div>
                        <div style="text-align: center;">
                            <strong>Status</strong><br>
                            <span style="color: {color}; font-weight: bold;">{level}</span>
                        </div>
                    </div>
                </div>
                
                <!-- Main Pollutant -->
                <div style="background: #e7f3ff; padding: 15px; border-radius: 8px; margin: 15px 0;">
                    <h4 style="color: #1976D2; margin-top: 0;">🔬 Primary Pollutant: {dominant_pollutant}</h4>
                </div>
                
                <!-- Tomorrow's Forecast -->
                {self._get_forecast_section(summary_data)}
                
                <!-- Health Summary -->
                <div style="background: #fff3cd; padding: 15px; border-radius: 8px; margin: 15px 0;">
                    <h4 style="color: #856404; margin-top: 0;">🏥 Health Summary</h4>
                    <p style="margin: 0;">{self._get_daily_health_message(summary_data)}</p>
                </div>
                
                <!-- Footer -->
                <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd;">
                    <p style="color: #666; font-size: 14px; margin: 0;">
                        <strong>Safer Skies</strong> - Daily Air Quality Insights<br>
                        NASA Space Apps Challenge 2025
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _get_forecast_section(self, summary_data: Dict) -> str:
        """Generate forecast section for daily summary"""
        forecast = summary_data.get('tomorrow_forecast', {})
        if not forecast:
            return ""
        
        forecast_aqi = forecast.get('aqi', 0)
        if forecast_aqi <= 50:
            forecast_color = '#4CAF50'
            forecast_level = 'Good'
        elif forecast_aqi <= 100:
            forecast_color = '#FFEB3B'
            forecast_level = 'Moderate'
        else:
            forecast_color = '#FF9800'
            forecast_level = 'Unhealthy for Sensitive Groups'
        
        return f"""
        <div style="background: #f0f8f0; padding: 15px; border-radius: 8px; margin: 15px 0;">
            <h4 style="color: #2e7d32; margin-top: 0;">🔮 Tomorrow's Forecast</h4>
            <p style="margin: 0;">
                Expected AQI: <strong style="color: {forecast_color};">{forecast_aqi}</strong> 
                ({forecast_level})
            </p>
        </div>
        """
    
    def _get_daily_health_message(self, summary_data: Dict) -> str:
        """Generate health message for daily summary"""
        avg_aqi = summary_data.get('average_aqi', 0)
        
        if avg_aqi <= 50:
            return "Air quality was excellent today. Perfect conditions for all outdoor activities."
        elif avg_aqi <= 100:
            return "Air quality was good to moderate today. Generally safe for outdoor activities."
        elif avg_aqi <= 150:
            return "Air quality reached unhealthy levels for sensitive groups. If you're sensitive to air pollution, you may have experienced some discomfort."
        else:
            return "Air quality was unhealthy today. We hope you were able to limit outdoor exposure."
    
    def _create_daily_summary_text(self, summary_data: Dict, user_name: str) -> str:
        """Create plain text version of daily summary"""
        city = summary_data.get('location', {}).get('city', 'Your Area')
        avg_aqi = summary_data.get('average_aqi', 0)
        max_aqi = summary_data.get('max_aqi', 0)
        
        text = f"""
SAFER SKIES - DAILY AIR QUALITY SUMMARY
========================================

Hello {user_name},

DAILY SUMMARY - {city} - {datetime.now().strftime('%B %d, %Y')}
---------------------------------------------------------------
Average AQI: {avg_aqi}
Peak AQI: {max_aqi}
Primary Pollutant: {summary_data.get('dominant_pollutant', 'PM25')}

HEALTH SUMMARY:
{self._get_daily_health_message(summary_data)}

Tomorrow's forecast: {summary_data.get('tomorrow_forecast', {}).get('aqi', 'Not available')}

---
Safer Skies - NASA Space Apps Challenge 2025
Daily Air Quality Insights
        """
        
        return text.strip()

def test_email_service():
    """Test the email notification service"""
    
    # Sample alert data
    sample_alert = {
        'user_id': 'test_user',
        'timestamp': datetime.now().isoformat(),
        'location': {'city': 'New York', 'latitude': 40.7128, 'longitude': -74.0060},
        'pollutant': 'PM25',
        'aqi_value': 125,
        'alert_level': 'unhealthy_sensitive',
        'epa_message': 'Sensitive groups: Make outdoor activities shorter and less intense. It\'s OK to be active outdoors but take more breaks. Watch for symptoms such as coughing or shortness of breath. People with asthma: Follow your asthma action plan and keep quick relief medicine handy.',
        'user_conditions': ['asthma', 'outdoor_worker']
    }
    
    email_service = EmailNotificationService(use_aws_ses=False)
    
    success = email_service.send_alert_email(
        alert=sample_alert,
        user_email='test@example.com',
        user_name='John Doe'
    )
    
    print(f"Email test {'passed' if success else 'failed'}")

if __name__ == "__main__":
    test_email_service()
