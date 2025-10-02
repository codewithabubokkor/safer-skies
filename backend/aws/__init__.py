"""
AWS Infrastructure Package
=========================
AWS deployment and infrastructure components for NASA Space Apps Challenge 2025

This package provides:
- NASA Earthdata credentials management
- S3 cache management and lifecycle policies
- CloudWatch monitoring and alarms
- CloudFront CDN distribution
- EventBridge scheduling for TEMPO downloads
- Step Functions ETL workflows
- Lambda deployment automation
- DynamoDB TTL configuration

Components:
- nasa_credentials: NASA Earthdata authentication
- s3_cache_manager: S3 lifecycle and caching
- cloudwatch_monitoring_alarms: System monitoring
- cloudfront_cdn_distribution: CDN setup
- aws_eventbridge_hourly_tempo: Scheduled data collection
- aws_step_functions_etl: ETL workflow orchestration
- tempo_file_fetcher: NASA TEMPO data fetching
- aws_deployment_scripts: Infrastructure as Code deployment
- dynamodb_ttl_config: Database TTL management
- s3_lifecycle_policies: S3 storage optimization

Deployment:
- deployment/lambda_deploy.py: Lambda function deployment
- deployment/eventbridge_setup.py: EventBridge configuration
"""

# Import main AWS components (with error handling for missing dependencies)
try:
    from .nasa_credentials import NASACredentialsManager
    from .s3_cache_manager import S3CacheManager
    from .tempo_file_fetcher import TempoFileFetcher
    from .aws_deployment_scripts import AWSDeploymentManager
    from .aws_step_functions_etl import AWSStepFunctionsETL
    from .aws_eventbridge_hourly_tempo import TempoEventBridgeManager
    from .cloudwatch_monitoring_alarms import CloudWatchManager
    from .cloudfront_cdn_distribution import CloudFrontManager
    from .dynamodb_ttl_config import DynamoDBTTLManager
    from .s3_lifecycle_policies import S3LifecycleManager
except ImportError as e:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"Some AWS services not available: {e}")
    
    # Provide dummy classes for testing
    NASACredentialsManager = None
    S3CacheManager = None
    TempoFileFetcher = None
    AWSDeploymentManager = None
    AWSStepFunctionsETL = None
    TempoEventBridgeManager = None
    CloudWatchManager = None
    CloudFrontManager = None
    DynamoDBTTLManager = None
    S3LifecycleManager = None

__all__ = [
    'NASACredentialsManager',
    'S3CacheManager',
    'TempoFileFetcher',
    'AWSDeploymentManager',
    'AWSStepFunctionsETL',
    'TempoEventBridgeManager',
    'CloudWatchMonitoringManager',
    'CloudFrontDistributionManager',
    'DynamoDBTTLManager',
    'S3LifecyclePolicyManager'
]

__version__ = '1.0.0'