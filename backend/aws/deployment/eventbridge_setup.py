"""
EventBridge Setup for TEMPO File Downloads
Configures hourly scheduling and event-driven TEMPO file fetching
Part of Safer Skies AWS Infrastructure
"""

import boto3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventBridgeSetup:
    """Sets up EventBridge scheduling for TEMPO file downloads"""
    
    def __init__(self, region: str = 'us-east-1'):
        """
        Initialize EventBridge setup
        
        Args:
            region: AWS region
        """
        self.region = region
        self.events_client = boto3.client('events', region_name=region)
        self.lambda_client = boto3.client('lambda', region_name=region)
        
    def create_hourly_schedule(self, lambda_function_arn: str, 
                             rule_name: str = 'naq-tempo-hourly-download') -> str:
        """
        Create hourly schedule for TEMPO file downloads
        
        Args:
            lambda_function_arn: ARN of Lambda function to trigger
            rule_name: Name of EventBridge rule
            
        Returns:
            ARN of created rule
        """
        try:
            response = self.events_client.put_rule(
                Name=rule_name,
                ScheduleExpression='rate(1 hour)',
                Description='Hourly TEMPO satellite file download for Safer Skies',
                State='ENABLED'
            )
            
            rule_arn = response['RuleArn']
            logger.info(f"✅ Created hourly EventBridge rule: {rule_arn}")
            
            targets = [
                {
                    'Id': 'no2-download',
                    'Arn': lambda_function_arn,
                    'Input': json.dumps({
                        'gas': 'NO2',
                        'force_refresh': False,
                        'source': 'eventbridge-hourly'
                    })
                },
                {
                    'Id': 'hcho-download',
                    'Arn': lambda_function_arn,
                    'Input': json.dumps({
                        'gas': 'HCHO',
                        'force_refresh': False,
                        'source': 'eventbridge-hourly'
                    })
                }
            ]
            
            self.events_client.put_targets(
                Rule=rule_name,
                Targets=targets
            )
            
            logger.info(f"✅ Added {len(targets)} targets to EventBridge rule")
            
            function_name = lambda_function_arn.split(':')[-1]
            
            try:
                self.lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId=f'eventbridge-{rule_name}',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=rule_arn
                )
                logger.info(f"✅ Added EventBridge permission to Lambda function")
            except self.lambda_client.exceptions.ResourceConflictException:
                logger.info("✅ EventBridge permission already exists")
            
            return rule_arn
            
        except Exception as e:
            logger.error(f"❌ Failed to create hourly schedule: {e}")
            raise
    
    def create_on_demand_schedule(self, lambda_function_arn: str,
                                rule_name: str = 'naq-tempo-on-demand') -> str:
        """
        Create on-demand trigger for TEMPO file downloads
        
        Args:
            lambda_function_arn: ARN of Lambda function to trigger
            rule_name: Name of EventBridge rule
            
        Returns:
            ARN of created rule
        """
        try:
            response = self.events_client.put_rule(
                Name=rule_name,
                EventPattern=json.dumps({
                    "source": ["naq.forecast"],
                    "detail-type": ["TEMPO File Request"],
                    "detail": {
                        "action": ["download", "refresh"]
                    }
                }),
                Description='On-demand TEMPO file download trigger for Safer Skies',
                State='ENABLED'
            )
            
            rule_arn = response['RuleArn']
            logger.info(f"✅ Created on-demand EventBridge rule: {rule_arn}")
            
            self.events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': 'on-demand-download',
                        'Arn': lambda_function_arn,
                        'InputTransformer': {
                            'InputPathsMap': {
                                'gas': '$.detail.gas',
                                'force_refresh': '$.detail.force_refresh',
                                'nasa_s3_path': '$.detail.nasa_s3_path'
                            },
                            'InputTemplate': json.dumps({
                                'gas': '<gas>',
                                'force_refresh': '<force_refresh>',
                                'nasa_s3_path': '<nasa_s3_path>',
                                'source': 'eventbridge-on-demand'
                            })
                        }
                    }
                ]
            )
            
            function_name = lambda_function_arn.split(':')[-1]
            
            try:
                self.lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId=f'eventbridge-{rule_name}',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=rule_arn
                )
                logger.info(f"✅ Added on-demand EventBridge permission to Lambda function")
            except self.lambda_client.exceptions.ResourceConflictException:
                logger.info("✅ On-demand EventBridge permission already exists")
            
            return rule_arn
            
        except Exception as e:
            logger.error(f"❌ Failed to create on-demand schedule: {e}")
            raise
    
    def create_cache_cleanup_schedule(self, cache_manager_arn: str,
                                    rule_name: str = 'naq-tempo-cache-cleanup') -> str:
        """
        Create daily schedule for cache cleanup
        
        Args:
            cache_manager_arn: ARN of cache manager Lambda function
            rule_name: Name of EventBridge rule
            
        Returns:
            ARN of created rule
        """
        try:
            response = self.events_client.put_rule(
                Name=rule_name,
                ScheduleExpression='cron(0 6 * * ? *)',
                Description='Daily TEMPO cache cleanup for NAQ Forecast',
                State='ENABLED'
            )
            
            rule_arn = response['RuleArn']
            logger.info(f"✅ Created cache cleanup EventBridge rule: {rule_arn}")
            
            self.events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': 'cache-cleanup',
                        'Arn': cache_manager_arn,
                        'Input': json.dumps({
                            'operation': 'cleanup',
                            'max_age_days': 7,
                            'source': 'eventbridge-cleanup'
                        })
                    }
                ]
            )
            
            function_name = cache_manager_arn.split(':')[-1]
            
            try:
                self.lambda_client.add_permission(
                    FunctionName=function_name,
                    StatementId=f'eventbridge-{rule_name}',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=rule_arn
                )
                logger.info(f"✅ Added cache cleanup permission to Lambda function")
            except self.lambda_client.exceptions.ResourceConflictException:
                logger.info("✅ Cache cleanup permission already exists")
            
            return rule_arn
            
        except Exception as e:
            logger.error(f"❌ Failed to create cache cleanup schedule: {e}")
            raise
    
    def trigger_on_demand_download(self, gas: str, nasa_s3_path: str = None, 
                                 force_refresh: bool = False) -> bool:
        """
        Trigger on-demand TEMPO file download
        
        Args:
            gas: Target gas (NO2, HCHO, etc.)
            nasa_s3_path: Specific NASA S3 path to download
            force_refresh: Force refresh even if cache is fresh
            
        Returns:
            True if event was sent successfully
        """
        try:
            event = {
                'Source': 'naq.forecast',
                'DetailType': 'TEMPO File Request',
                'Detail': json.dumps({
                    'action': 'download',
                    'gas': gas,
                    'nasa_s3_path': nasa_s3_path,
                    'force_refresh': force_refresh,
                    'timestamp': datetime.now().isoformat()
                })
            }
            
            response = self.events_client.put_events(Entries=[event])
            
            if response['FailedEntryCount'] == 0:
                logger.info(f"✅ Triggered on-demand download for {gas}")
                return True
            else:
                logger.error(f"❌ Failed to trigger on-demand download: {response}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error triggering on-demand download: {e}")
            return False
    
    def list_tempo_schedules(self) -> List[Dict]:
        """
        List all TEMPO-related EventBridge rules
        
        Returns:
            List of rule information dictionaries
        """
        try:
            response = self.events_client.list_rules(NamePrefix='naq-tempo')
            
            rules = []
            for rule in response.get('Rules', []):
                rule_info = {
                    'name': rule['Name'],
                    'arn': rule['Arn'],
                    'state': rule['State'],
                    'description': rule.get('Description', ''),
                    'schedule': rule.get('ScheduleExpression', ''),
                    'event_pattern': rule.get('EventPattern', '')
                }
                
                targets_response = self.events_client.list_targets_by_rule(Rule=rule['Name'])
                rule_info['targets'] = targets_response.get('Targets', [])
                
                rules.append(rule_info)
            
            logger.info(f"📋 Found {len(rules)} TEMPO EventBridge rules")
            return rules
            
        except Exception as e:
            logger.error(f"❌ Failed to list TEMPO schedules: {e}")
            return []
    
    def disable_schedule(self, rule_name: str) -> bool:
        """
        Disable a specific EventBridge rule
        
        Args:
            rule_name: Name of rule to disable
            
        Returns:
            True if successful
        """
        try:
            self.events_client.put_rule(
                Name=rule_name,
                State='DISABLED'
            )
            
            logger.info(f"✅ Disabled EventBridge rule: {rule_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to disable rule {rule_name}: {e}")
            return False
    
    def enable_schedule(self, rule_name: str) -> bool:
        """
        Enable a specific EventBridge rule
        
        Args:
            rule_name: Name of rule to enable
            
        Returns:
            True if successful
        """
        try:
            self.events_client.put_rule(
                Name=rule_name,
                State='ENABLED'
            )
            
            logger.info(f"✅ Enabled EventBridge rule: {rule_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to enable rule {rule_name}: {e}")
            return False

def setup_complete_eventbridge_system(lambda_function_arn: str, 
                                     cache_manager_arn: str) -> Dict:
    """
    Set up complete EventBridge system for TEMPO file management
    
    Args:
        lambda_function_arn: ARN of TEMPO file fetcher Lambda
        cache_manager_arn: ARN of cache manager Lambda
        
    Returns:
        Dictionary with all created rule ARNs
    """
    try:
        eventbridge = EventBridgeSetup()
        
        logger.info("🚀 Setting up complete EventBridge system...")
        
        hourly_arn = eventbridge.create_hourly_schedule(lambda_function_arn)
        
        on_demand_arn = eventbridge.create_on_demand_schedule(lambda_function_arn)
        
        cleanup_arn = eventbridge.create_cache_cleanup_schedule(cache_manager_arn)
        
        results = {
            'hourly_schedule_arn': hourly_arn,
            'on_demand_schedule_arn': on_demand_arn,
            'cache_cleanup_arn': cleanup_arn,
            'status': 'success'
        }
        
        logger.info("✅ Complete EventBridge system setup successful!")
        return results
        
    except Exception as e:
        logger.error(f"❌ EventBridge system setup failed: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    eventbridge = EventBridgeSetup()
    
    # List existing schedules
    print("📋 Current TEMPO EventBridge Rules:")
    rules = eventbridge.list_tempo_schedules()
    
    for rule in rules:
        print(f"  {rule['name']}: {rule['state']} - {rule['description']}")
        if rule['schedule']:
            print(f"    Schedule: {rule['schedule']}")
        print(f"    Targets: {len(rule['targets'])}")
        print()
    
    # Example on-demand trigger
    # eventbridge.trigger_on_demand_download('NO2', force_refresh=True)
