#!/usr/bin/env python3
"""
AWS Lambda Handler for NAQForecast Alert System
This runs the alert check as a scheduled Lambda function
"""

import sys
import os
import json

# Add the backend path
sys.path.append('/opt/python')
sys.path.append('.')

def lambda_handler(event, context):
    """Lambda handler for scheduled alert checks"""
    
    try:
        from notifications.safer_skies_auto_alerts import SaferSkiesAlertSystem
        
        print("🛡️ Starting NAQForecast Alert Check (AWS Lambda)")
        
        alert_system = SaferSkiesAlertSystem()
        
        success = alert_system.run_alert_check()
        
        if success:
            print("✅ Alert check completed successfully")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'message': 'Alert check completed successfully'
                })
            }
        else:
            print("❌ Alert check failed")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Alert check failed'
                })
            }
            
    except Exception as e:
        print(f"❌ Lambda error: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }

if __name__ == "__main__":
    result = lambda_handler({}, {})
    print(f"Result: {result}")