#!/usr/bin/env python3
"""
AWS EventBridge Configuration for Hourly TEMPO Downloads

This script sets up EventBridge (CloudWatch Events) to trigger the Lambda function
every hour for downloading latest TEMPO files.

Usage:
    python aws_eventbridge_hourly_tempo.py --create
    python aws_eventbridge_hourly_tempo.py --delete
"""

import boto3
import json
import argparse
from datetime import datetime, timezone

class TempoEventBridgeManager:
    """Manages EventBridge rules for hourly TEMPO downloads"""
    
    def __init__(self, region='us-east-1'):
        self.region = region
        self.events_client = boto3.client('events', region_name=region)
        self.lambda_client = boto3.client('lambda', region_name=region)
        
        # Configuration
        self.rule_name = 'hourly-tempo-downloads'
        self.lambda_function_name = 'tempo-file-fetcher'
        
    def create_hourly_schedule(self):
        """Create EventBridge rule for hourly TEMPO downloads"""
        
        print("🚀 Creating hourly TEMPO download schedule...")
        
        rule_response = self.events_client.put_rule(
            Name=self.rule_name,
            ScheduleExpression='rate(1 hour)',  # Every hour
            Description='Hourly TEMPO satellite data downloads',
            State='ENABLED'
        )
        
        print(f"✅ EventBridge rule created: {rule_response['RuleArn']}")
        
        event_payload = {
            "locations": [
                {"lat": 40.7128, "lon": -74.0060},  # NYC
                {"lat": 34.0522, "lon": -118.2437}, # Los Angeles
                {"lat": 41.8781, "lon": -87.6298},  # Chicago
                {"lat": 29.7604, "lon": -95.3698},  # Houston
                {"lat": 33.4484, "lon": -112.0740}, # Phoenix
                {"lat": 39.9526, "lon": -75.1652},  # Philadelphia
                {"lat": 32.7767, "lon": -96.7970},  # Dallas
                {"lat": 37.7749, "lon": -122.4194},
                {"lat": 47.6062, "lon": -122.3321},
                {"lat": 25.7617, "lon": -80.1918}
            ],
            "gases": ["NO2", "HCHO"],
            "trigger": "eventbridge_hourly",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        target_response = self.events_client.put_targets(
            Rule=self.rule_name,
            Targets=[
                {
                    'Id': '1',
                    'Arn': f'arn:aws:lambda:{self.region}:{self._get_account_id()}:function:{self.lambda_function_name}',
                    'Input': json.dumps(event_payload)
                }
            ]
        )
        
        print(f"✅ Lambda target added: {target_response}")
        
        try:
            permission_response = self.lambda_client.add_permission(
                FunctionName=self.lambda_function_name,
                StatementId=f'{self.rule_name}-permission',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=rule_response['RuleArn']
            )
            print(f"✅ Lambda permission added: {permission_response}")
        except self.lambda_client.exceptions.ResourceConflictException:
            print("⚠️ Lambda permission already exists")
        
        print(f"""
🎯 Hourly TEMPO download schedule created successfully!

📊 Configuration:
- Rule Name: {self.rule_name}
- Schedule: Every 1 hour
- Lambda Function: {self.lambda_function_name}
- Locations: 10 major US cities
- Gases: NO2, HCHO

🔄 Next Execution: Within 1 hour
📝 Logs: Check CloudWatch Logs for execution details
        """)
        
        return rule_response['RuleArn']
    
    def delete_schedule(self):
        """Delete the EventBridge rule and cleanup"""
        
        print("🗑️ Deleting hourly TEMPO download schedule...")
        
        try:
            self.events_client.remove_targets(
                Rule=self.rule_name,
                Ids=['1']
            )
            print("✅ Targets removed")
            
            self.events_client.delete_rule(
                Name=self.rule_name
            )
            print("✅ EventBridge rule deleted")
            
            try:
                self.lambda_client.remove_permission(
                    FunctionName=self.lambda_function_name,
                    StatementId=f'{self.rule_name}-permission'
                )
                print("✅ Lambda permission removed")
            except Exception as e:
                print(f"⚠️ Permission removal failed: {e}")
            
            print("🎯 Hourly schedule deleted successfully!")
            
        except Exception as e:
            print(f"❌ Deletion failed: {e}")
    
    def check_schedule_status(self):
        """Check current status of the schedule"""
        
        try:
            rule = self.events_client.describe_rule(Name=self.rule_name)
            targets = self.events_client.list_targets_by_rule(Rule=self.rule_name)
            
            print(f"""
📊 TEMPO Schedule Status:
- Rule: {rule['Name']}
- State: {rule['State']}
- Schedule: {rule['ScheduleExpression']}
- Description: {rule['Description']}
- Targets: {len(targets['Targets'])}
- ARN: {rule['Arn']}
            """)
            
            return True
            
        except self.events_client.exceptions.ResourceNotFoundException:
            print("❌ No hourly TEMPO schedule found")
            return False
    
    def _get_account_id(self):
        """Get AWS account ID"""
        sts = boto3.client('sts')
        return sts.get_caller_identity()['Account']

def main():
    """CLI interface for EventBridge management"""
    
    parser = argparse.ArgumentParser(description='Manage hourly TEMPO download schedule')
    parser.add_argument('--create', action='store_true', help='Create hourly schedule')
    parser.add_argument('--delete', action='store_true', help='Delete hourly schedule')
    parser.add_argument('--status', action='store_true', help='Check schedule status')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    
    args = parser.parse_args()
    
    if not any([args.create, args.delete, args.status]):
        parser.print_help()
        return
    
    manager = TempoEventBridgeManager(region=args.region)
    
    if args.create:
        manager.create_hourly_schedule()
    elif args.delete:
        manager.delete_schedule()
    elif args.status:
        manager.check_schedule_status()

if __name__ == "__main__":
    main()
