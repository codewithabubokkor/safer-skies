"""
NASA SPACE APPS 2025: AWS STEP FUNCTIONS ETL WORKFLOW
====================================================
Complete ETL Workflow Definition for EPA-Compliant AQI System
AWS Production Architecture with Lambda Functions

🚀 Phase 6 Component 1/10: Step Functions ETL Orchestration

Key Features:
- Parallel data fetching (TEMPO + GEOS-CF + Ground stations)
- Sequential AQI processing with EPA compliance
- Dual-write pattern (summary.json + history/)
- Conditional notification triggers
- Comprehensive error handling and retry logic
- Cost optimization within AWS Free Tier
"""

import json
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AWSStepFunctionsETL:
    """
    AWS Step Functions ETL Workflow for NASA AQI System
    
    Orchestrates the complete data pipeline:
    1. Parallel data collection (NASA TEMPO + GEOS-CF + Ground)
    2. EPA AQI calculation with time averaging
    3. S3 dual-write (summary + history)
    4. Conditional alert notifications
    """
    
    def __init__(self, region_name: str = "us-east-1"):
        self.region_name = region_name
        
        # AWS clients (mock for demo)
        self.stepfunctions = None  # boto3.client('stepfunctions', region_name=region_name)
        self.s3 = None  # boto3.client('s3')
        self.dynamodb = None  # boto3.resource('dynamodb', region_name=region_name)
        
        # Step Function ARN
        self.state_machine_arn = f"arn:aws:states:{region_name}:123456789012:stateMachine:NAQForecastETL"
        
        logger.info("🔧 AWS Step Functions ETL initialized")
    
    def create_state_machine_definition(self) -> Dict[str, Any]:
        """
        Create Step Functions state machine definition
        
        Returns:
            Complete state machine definition JSON
        """
        
        state_machine = {
            "Comment": "NASA SPACE APPS 2025: EPA-Compliant AQI ETL Pipeline",
            "StartAt": "FetchDataParallel",
            "States": {
                # Step 1: Parallel Data Fetching
                "FetchDataParallel": {
                    "Type": "Parallel",
                    "Comment": "Fetch data from NASA TEMPO, GEOS-CF, and ground stations in parallel",
                    "Branches": [
                        {
                            "StartAt": "FetchTEMPOData",
                            "States": {
                                "FetchTEMPOData": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_fetch_tempo_data",
                                    "Comment": "Fetch NASA TEMPO satellite data (NO2, HCHO)",
                                    "Retry": [
                                        {
                                            "ErrorEquals": ["States.TaskFailed"],
                                            "IntervalSeconds": 30,
                                            "MaxAttempts": 3,
                                            "BackoffRate": 2.0
                                        }
                                    ],
                                    "Catch": [
                                        {
                                            "ErrorEquals": ["States.ALL"],
                                            "Next": "TEMPOFallback",
                                            "ResultPath": "$.error"
                                        }
                                    ],
                                    "End": True
                                },
                                "TEMPOFallback": {
                                    "Type": "Pass",
                                    "Comment": "TEMPO data unavailable, use GEOS-CF backup",
                                    "Result": {
                                        "source": "fallback",
                                        "data": {},
                                        "quality": "degraded"
                                    },
                                    "End": True
                                }
                            }
                        },
                        {
                            "StartAt": "FetchGEOSCFData",
                            "States": {
                                "FetchGEOSCFData": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_fetch_geos_cf_data",
                                    "Comment": "Fetch GEOS-CF forecast data (O3, CO, SO2, meteorology)",
                                    "Retry": [
                                        {
                                            "ErrorEquals": ["States.TaskFailed"],
                                            "IntervalSeconds": 20,
                                            "MaxAttempts": 2,
                                            "BackoffRate": 1.5
                                        }
                                    ],
                                    "Catch": [
                                        {
                                            "ErrorEquals": ["States.ALL"],
                                            "Next": "GEOSCFFallback",
                                            "ResultPath": "$.error"
                                        }
                                    ],
                                    "End": True
                                },
                                "GEOSCFFallback": {
                                    "Type": "Pass",
                                    "Comment": "GEOS-CF unavailable, use historical averages",
                                    "Result": {
                                        "source": "climatology",
                                        "data": {},
                                        "quality": "estimated"
                                    },
                                    "End": True
                                }
                            }
                        },
                        {
                            "StartAt": "FetchGroundData",
                            "States": {
                                "FetchGroundData": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_fetch_ground_data",
                                    "Comment": "Fetch ground station data (AirNow EPA, WAQI)",
                                    "Retry": [
                                        {
                                            "ErrorEquals": ["States.TaskFailed"],
                                            "IntervalSeconds": 15,
                                            "MaxAttempts": 3,
                                            "BackoffRate": 1.5
                                        }
                                    ],
                                    "Catch": [
                                        {
                                            "ErrorEquals": ["States.ALL"],
                                            "Next": "GroundDataFallback",
                                            "ResultPath": "$.error"
                                        }
                                    ],
                                    "End": True
                                },
                                "GroundDataFallback": {
                                    "Type": "Pass",
                                    "Comment": "Ground data unavailable, satellite-only mode",
                                    "Result": {
                                        "source": "satellite_only",
                                        "data": {},
                                        "quality": "satellite_only"
                                    },
                                    "End": True
                                }
                            }
                        }
                    ],
                    "Next": "ProcessAQICalculation",
                    "ResultPath": "$.fetch_results"
                },
                
                # Step 2: EPA AQI Processing
                "ProcessAQICalculation": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_process_aqi",
                    "Comment": "Calculate EPA-compliant AQI with time averaging and bias correction",
                    "TimeoutSeconds": 300,
                    "Retry": [
                        {
                            "ErrorEquals": ["States.TaskFailed"],
                            "IntervalSeconds": 10,
                            "MaxAttempts": 2,
                            "BackoffRate": 2.0
                        }
                    ],
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "ProcessingFailure",
                            "ResultPath": "$.processing_error"
                        }
                    ],
                    "Next": "SaveResultsParallel",
                    "ResultPath": "$.aqi_results"
                },
                
                # Step 3: Parallel S3 Writing
                "SaveResultsParallel": {
                    "Type": "Parallel",
                    "Comment": "Save results to S3 in parallel (summary + history)",
                    "Branches": [
                        {
                            "StartAt": "SaveSummaryJSON",
                            "States": {
                                "SaveSummaryJSON": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_save_summary",
                                    "Comment": "Write summary.json for CloudFront caching",
                                    "Retry": [
                                        {
                                            "ErrorEquals": ["States.TaskFailed"],
                                            "IntervalSeconds": 5,
                                            "MaxAttempts": 3,
                                            "BackoffRate": 2.0
                                        }
                                    ],
                                    "End": True
                                }
                            }
                        },
                        {
                            "StartAt": "SaveHistoryJSON",
                            "States": {
                                "SaveHistoryJSON": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_save_history",
                                    "Comment": "Write timestamped history file for trend analysis",
                                    "Retry": [
                                        {
                                            "ErrorEquals": ["States.TaskFailed"],
                                            "IntervalSeconds": 5,
                                            "MaxAttempts": 2,
                                            "BackoffRate": 1.5
                                        }
                                    ],
                                    "End": True
                                }
                            }
                        },
                        {
                            "StartAt": "UpdateDynamoDB",
                            "States": {
                                "UpdateDynamoDB": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_update_metadata",
                                    "Comment": "Update location metadata and user last_accessed",
                                    "Retry": [
                                        {
                                            "ErrorEquals": ["States.TaskFailed"],
                                            "IntervalSeconds": 3,
                                            "MaxAttempts": 3,
                                            "BackoffRate": 2.0
                                        }
                                    ],
                                    "End": True
                                }
                            }
                        }
                    ],
                    "Next": "CheckAlertConditions",
                    "ResultPath": "$.save_results"
                },
                
                # Step 4: Conditional Notifications
                "CheckAlertConditions": {
                    "Type": "Choice",
                    "Comment": "Determine if alerts should be sent based on AQI thresholds",
                    "Choices": [
                        {
                            "Variable": "$.aqi_results.alert_triggered",
                            "BooleanEquals": True,
                            "Next": "SendAlertNotifications"
                        }
                    ],
                    "Default": "ETLComplete"
                },
                
                "SendAlertNotifications": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:lambda_send_alerts",
                    "Comment": "Send email and push notifications for alert conditions",
                    "TimeoutSeconds": 120,
                    "Retry": [
                        {
                            "ErrorEquals": ["States.TaskFailed"],
                            "IntervalSeconds": 10,
                            "MaxAttempts": 2,
                            "BackoffRate": 1.5
                        }
                    ],
                    "Catch": [
                        {
                            "ErrorEquals": ["States.ALL"],
                            "Next": "AlertFailure",
                            "ResultPath": "$.alert_error"
                        }
                    ],
                    "Next": "ETLComplete"
                },
                
                "ETLComplete": {
                    "Type": "Pass",
                    "Comment": "ETL pipeline completed successfully",
                    "Result": {
                        "status": "success",
                        "message": "NASA AQI ETL pipeline completed",
                        "timestamp": "2025-08-19T21:30:00Z"
                    },
                    "End": True
                },
                
                "ProcessingFailure": {
                    "Type": "Fail",
                    "Comment": "AQI processing failed",
                    "Cause": "Unable to calculate EPA-compliant AQI"
                },
                
                "AlertFailure": {
                    "Type": "Pass",
                    "Comment": "Alert sending failed but ETL succeeded",
                    "Result": {
                        "status": "partial_success",
                        "message": "ETL completed but alerts failed",
                        "alert_error": "Notification system unavailable"
                    },
                    "End": True
                }
            }
        }
        
        return state_machine
    
    async def create_step_function(self) -> str:
        """
        Create Step Functions state machine in AWS
        
        Returns:
            State machine ARN
        """
        
        definition = self.create_state_machine_definition()
        
        # Mock creation (in production, would use boto3)
        logger.info("🔧 Creating Step Functions state machine...")
        logger.info(f"📋 Definition: {len(json.dumps(definition))} characters")
        
        # Would create actual state machine:
        # response = self.stepfunctions.create_state_machine(
        #     name='NAQForecastETL',
        #     definition=json.dumps(definition),
        #     roleArn='arn:aws:iam::123456789012:role/StepFunctionsExecutionRole'
        # )
        
        logger.info(f"✅ Step Functions state machine created: {self.state_machine_arn}")
        return self.state_machine_arn
    
    async def start_etl_execution(self, locations: List[Dict[str, float]]) -> str:
        """
        Start ETL execution for given locations
        
        Args:
            locations: List of {lat, lon} coordinates
            
        Returns:
            Execution ARN
        """
        
        execution_input = {
            "locations": locations,
            "timestamp": datetime.now().isoformat(),
            "config": {
                "data_sources": ["tempo", "geos_cf", "airnow", "waqi"],
                "quality_threshold": 0.7,
                "alert_enabled": True,
                "s3_bucket": "naq-aqi-cache",
                "cloudfront_distribution": "E1234567890ABC"
            }
        }
        
        # Mock execution start
        execution_arn = f"{self.state_machine_arn}:execution:{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"🚀 Starting ETL execution for {len(locations)} locations")
        logger.info(f"📍 Execution ARN: {execution_arn}")
        
        # Would start actual execution:
        # response = self.stepfunctions.start_execution(
        #     stateMachineArn=self.state_machine_arn,
        #     input=json.dumps(execution_input)
        # )
        
        return execution_arn
    
    async def get_execution_status(self, execution_arn: str) -> Dict[str, Any]:
        """
        Get execution status and results
        
        Args:
            execution_arn: Step Functions execution ARN
            
        Returns:
            Execution status and results
        """
        
        # Mock status response
        status = {
            "executionArn": execution_arn,
            "stateMachineArn": self.state_machine_arn,
            "status": "SUCCEEDED",
            "startDate": datetime.now() - timedelta(minutes=5),
            "stopDate": datetime.now(),
            "input": json.dumps({
                "locations": [{"lat": 40.7128, "lon": -74.0060}],
                "timestamp": datetime.now().isoformat()
            }),
            "output": json.dumps({
                "status": "success",
                "locations_processed": 1,
                "alerts_sent": 0,
                "data_quality": "high",
                "processing_time_seconds": 287
            })
        }
        
        logger.info(f"📊 Execution status: {status['status']}")
        return status
    
    def get_cloudformation_template(self) -> Dict[str, Any]:
        """
        Generate CloudFormation template for complete infrastructure
        
        Returns:
            CloudFormation template
        """
        
        template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": "NASA SPACE APPS 2025: EPA-Compliant AQI System Infrastructure",
            "Parameters": {
                "ProjectName": {
                    "Type": "String",
                    "Default": "NAQForecast",
                    "Description": "Project name for resource naming"
                },
                "Environment": {
                    "Type": "String",
                    "Default": "production",
                    "AllowedValues": ["development", "staging", "production"]
                }
            },
            "Resources": {
                # S3 Bucket for AQI cache
                "AQICacheBucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {
                        "BucketName": {"Fn::Sub": "${ProjectName}-aqi-cache-${Environment}"},
                        "VersioningConfiguration": {"Status": "Enabled"},
                        "LifecycleConfiguration": {
                            "Rules": [
                                {
                                    "Id": "HistoryCleanup",
                                    "Status": "Enabled",
                                    "ExpirationInDays": 7,
                                    "Prefix": "history/"
                                }
                            ]
                        },
                        "CorsConfiguration": {
                            "CorsRules": [
                                {
                                    "AllowedOrigins": ["*"],
                                    "AllowedMethods": ["GET", "HEAD"],
                                    "AllowedHeaders": ["*"],
                                    "MaxAge": 3600
                                }
                            ]
                        }
                    }
                },
                
                # DynamoDB Tables
                "UserProfilesTable": {
                    "Type": "AWS::DynamoDB::Table",
                    "Properties": {
                        "TableName": {"Fn::Sub": "${ProjectName}-user-profiles"},
                        "BillingMode": "PAY_PER_REQUEST",
                        "AttributeDefinitions": [
                            {"AttributeName": "user_id", "AttributeType": "S"}
                        ],
                        "KeySchema": [
                            {"AttributeName": "user_id", "KeyType": "HASH"}
                        ],
                        "TimeToLiveSpecification": {
                            "AttributeName": "ttl",
                            "Enabled": True
                        }
                    }
                },
                
                "UserSubscriptionsTable": {
                    "Type": "AWS::DynamoDB::Table",
                    "Properties": {
                        "TableName": {"Fn::Sub": "${ProjectName}-user-subscriptions"},
                        "BillingMode": "PAY_PER_REQUEST",
                        "AttributeDefinitions": [
                            {"AttributeName": "user_id", "AttributeType": "S"},
                            {"AttributeName": "location_id", "AttributeType": "S"}
                        ],
                        "KeySchema": [
                            {"AttributeName": "user_id", "KeyType": "HASH"},
                            {"AttributeName": "location_id", "KeyType": "RANGE"}
                        ],
                        "TimeToLiveSpecification": {
                            "AttributeName": "ttl",
                            "Enabled": True
                        }
                    }
                },
                
                # CloudFront Distribution
                "CloudFrontDistribution": {
                    "Type": "AWS::CloudFront::Distribution",
                    "Properties": {
                        "DistributionConfig": {
                            "Origins": [
                                {
                                    "Id": "S3Origin",
                                    "DomainName": {"Fn::GetAtt": ["AQICacheBucket", "DomainName"]},
                                    "S3OriginConfig": {
                                        "OriginAccessIdentity": ""
                                    }
                                }
                            ],
                            "DefaultCacheBehavior": {
                                "TargetOriginId": "S3Origin",
                                "ViewerProtocolPolicy": "redirect-to-https",
                                "CachePolicyId": "4135ea2d-6df8-44a3-9df3-4b5a84be39ad",
                                "Compress": True
                            },
                            "Enabled": True,
                            "PriceClass": "PriceClass_100"
                        }
                    }
                },
                
                # Step Functions State Machine
                "ETLStateMachine": {
                    "Type": "AWS::StepFunctions::StateMachine",
                    "Properties": {
                        "StateMachineName": {"Fn::Sub": "${ProjectName}-ETL"},
                        "DefinitionString": json.dumps(self.create_state_machine_definition()),
                        "RoleArn": {"Fn::GetAtt": ["StepFunctionsRole", "Arn"]}
                    }
                },
                
                # IAM Role for Step Functions
                "StepFunctionsRole": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Principal": {"Service": "states.amazonaws.com"},
                                    "Action": "sts:AssumeRole"
                                }
                            ]
                        },
                        "ManagedPolicyArns": [
                            "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
                        ]
                    }
                }
            },
            "Outputs": {
                "S3BucketName": {
                    "Value": {"Ref": "AQICacheBucket"},
                    "Description": "S3 bucket for AQI cache storage"
                },
                "CloudFrontDomain": {
                    "Value": {"Fn::GetAtt": ["CloudFrontDistribution", "DomainName"]},
                    "Description": "CloudFront distribution domain"
                },
                "StateMachineArn": {
                    "Value": {"Ref": "ETLStateMachine"},
                    "Description": "Step Functions state machine ARN"
                }
            }
        }
        
        return template

async def demo_step_functions_etl():
    """Demonstrate AWS Step Functions ETL workflow"""
    
    print("🔧 NASA SPACE APPS 2025: AWS STEP FUNCTIONS ETL DEMO")
    print("=" * 65)
    print("Complete ETL Workflow Definition for EPA-Compliant AQI System")
    print()
    
    etl = AWSStepFunctionsETL()
    
    print("📋 CREATING STEP FUNCTIONS STATE MACHINE:")
    print("=" * 45)
    
    state_machine_arn = await etl.create_step_function()
    
    definition = etl.create_state_machine_definition()
    print(f"✅ State Machine Definition:")
    print(f"   - States: {len(definition['States'])}")
    print(f"   - Parallel Branches: 3 (TEMPO, GEOS-CF, Ground)")
    print(f"   - Retry Policies: Configured for all Lambda functions")
    print(f"   - Error Handling: Fallback strategies for data unavailability")
    
    print(f"\n🚀 STARTING ETL EXECUTION:")
    print("=" * 35)
    
    test_locations = [
        {"lat": 40.7128, "lon": -74.0060},  # NYC
        {"lat": 34.0522, "lon": -118.2437}, # LA
        {"lat": 41.8781, "lon": -87.6298}   # Chicago
    ]
    
    execution_arn = await etl.start_etl_execution(test_locations)
    
    print(f"📍 Processing {len(test_locations)} locations:")
    for i, loc in enumerate(test_locations, 1):
        print(f"   {i}. {loc['lat']:.4f}, {loc['lon']:.4f}")
    
    print(f"\n📊 EXECUTION STATUS:")
    print("=" * 25)
    
    status = await etl.get_execution_status(execution_arn)
    print(f"Status: {status['status']}")
    print(f"Duration: {(status['stopDate'] - status['startDate']).total_seconds():.1f} seconds")
    
    output = json.loads(status['output'])
    print(f"Locations Processed: {output['locations_processed']}")
    print(f"Alerts Sent: {output['alerts_sent']}")
    print(f"Data Quality: {output['data_quality']}")
    
    print(f"\n🏗️ INFRASTRUCTURE AS CODE:")
    print("=" * 35)
    
    cf_template = etl.get_cloudformation_template()
    resources = cf_template['Resources']
    
    print(f"CloudFormation Resources: {len(resources)}")
    print(f"✅ S3 Bucket: AQI cache with lifecycle policies")
    print(f"✅ DynamoDB Tables: User profiles + subscriptions with TTL")
    print(f"✅ CloudFront Distribution: Global CDN with caching")
    print(f"✅ Step Functions: ETL orchestration state machine")
    print(f"✅ IAM Roles: Proper permissions for Lambda execution")
    
    print(f"\n💰 AWS FREE TIER OPTIMIZATION:")
    print("=" * 40)
    
    print("📊 Estimated Monthly Usage (Free Tier):")
    print("   - Lambda: 1M requests/month (100% free)")
    print("   - DynamoDB: 25GB storage (100% free)")
    print("   - S3: 5GB storage + 20,000 GET requests (100% free)")
    print("   - CloudFront: 50GB data transfer (100% free)")
    print("   - Step Functions: 4,000 state transitions (100% free)")
    print("   - CloudWatch: 10 metrics + 1GB logs (100% free)")
    print(f"\n💡 Total Monthly Cost: $0.00 (within Free Tier limits)")
    
    print(f"\n✅ AWS STEP FUNCTIONS ETL DEMO COMPLETE!")
    print("🚀 Phase 6 Component 1/10: ETL Orchestration Ready!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(demo_step_functions_etl())
