#!/usr/bin/env python3
"""
Safer Skies Automatic Alert System
NASA Space Apps Challenge 2025 - Team AURA

This script runs continuously to monitor AQI conditions and automatically send
Gmail SMTP emails and push notifications when alert thresholds are exceeded.
"""

import sys
import os
import time
import schedule
from datetime import datetime, timedelta
import logging

backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(backend_path)

from schedulers.alert_monitor import AlertMonitor
from notifications.email_service import EmailNotificationService
from utils.database_connection import get_db_connection
from dotenv import load_dotenv

load_dotenv(os.path.join(backend_path, '.env'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('safer_skies_alerts.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SaferSkiesAlertSystem:
    
    def __init__(self):
        self.alert_monitor = AlertMonitor()
        
        # Test email service
        self.email_service = EmailNotificationService(use_aws_ses=False)
        
        # Configuration
        self.check_interval_minutes = 30  # Check every 30 minutes
        self.last_health_check = datetime.now()
        
        logger.info("✅ Safer Skies Alert System initialized")
        
    def run_alert_check(self):
        """Run a single alert check cycle"""
        
        try:
            logger.info("🔍 Starting alert check cycle...")
            
            if not self.check_database_health():
                logger.error("❌ Database health check failed")
                return False
            
            success = self.alert_monitor.check_alerts()
            
            if success:
                logger.info("✅ Alert check completed successfully")
            else:
                logger.error("❌ Alert check failed")
                
            self.last_health_check = datetime.now()
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Critical error in alert check: {e}")
            return False
    
    def check_database_health(self):
        """Check if database is accessible"""
        
        try:
            conn = get_db_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM alert_locations WHERE active = 1")
            result = cursor.fetchone()
            
            active_locations = result[0] if result else 0
            logger.info(f"📊 Database health check: {active_locations} active alert locations")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            return False
    
    def send_system_status_email(self):
        """Send daily system status email"""
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("SELECT COUNT(*) as count FROM alerts WHERE is_active = 1")
            active_alerts = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM users")
            total_users = cursor.fetchone()['count']
            
            cursor.execute("""
                SELECT COUNT(*) as count FROM alert_history 
                WHERE sent_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """)
            alerts_sent_24h = cursor.fetchone()['count']
            
            cursor.close()
            conn.close()
            
            subject = "🛡️ Safer Skies Daily System Status"
            
            body = f"""
Safer Skies Alert System - Daily Status Report
NASA Space Apps Challenge 2025 - Team AURA

📊 SYSTEM STATISTICS:
- Active Alerts: {active_alerts}
- Registered Users: {total_users}
- Alerts Sent (24h): {alerts_sent_24h}
- Last Health Check: {self.last_health_check.strftime('%Y-%m-%d %H:%M:%S')}

🔧 SYSTEM STATUS:
- Alert Monitor: ✅ Running
- Database: ✅ Connected
- Gmail SMTP: ✅ Configured
- Push Service: ✅ Ready

⚡ AUTOMATIC MONITORING:
- Check Interval: {self.check_interval_minutes} minutes
- Email Service: Gmail SMTP (abubokkor.dev@gmail.com)
- Push Notifications: Web Push enabled
- Rate Limiting: Active (prevents spam)

🛡️ NASA DATA SOURCES:
- TEMPO Satellite: Air quality observations
- GEOS-CF: Atmospheric composition forecasts
- AirNow: EPA real-time monitoring
- GFS: Weather analysis

---
Safer Skies - NASA Space Apps Challenge 2025
Team AURA | Automated Alert System
            """
            
            success = self.email_service._send_via_smtp(
                to_email="abubokkor.cse@gmail.com",
                subject=subject,
                html_body=f"<pre>{body}</pre>",
                text_body=body
            )
            
            if success:
                logger.info("📧 Daily status email sent successfully")
            else:
                logger.error("❌ Failed to send daily status email")
                
        except Exception as e:
            logger.error(f"❌ Error sending system status email: {e}")
    
    def start_monitoring(self):
        """Start the continuous monitoring system"""
        
        print(f"⚡ Starting automatic alert monitoring")
        print(f"🕐 Check interval: {self.check_interval_minutes} minutes")
        print(f"📧 Email service: Gmail SMTP (abubokkor.dev@gmail.com)")
        print(f"📱 Push service: Web Push enabled")
        print(f"📊 Target: Registered users from frontend")
        print("-" * 55)
        
        # Schedule regular alert checks every 30 minutes
        schedule.every(self.check_interval_minutes).minutes.do(self.run_alert_check)
        
        # Schedule daily status email at 8 AM
        schedule.every().day.at("08:00").do(self.send_system_status_email)
        
        logger.info("🚀 Running initial alert check...")
        self.run_alert_check()
        
        logger.info("🔄 Starting continuous monitoring loop...")
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute for scheduled tasks
                
            except KeyboardInterrupt:
                logger.info("🛑 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Error in monitoring loop: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying

def main():
    """Main entry point"""
    
    alert_system = SaferSkiesAlertSystem()
    
    try:
        alert_system.start_monitoring()
    except KeyboardInterrupt:
        print("\n🛑 Safer Skies Alert System stopped")
        logger.info("🛑 System shutdown completed")

if __name__ == "__main__":
    main()