"""
S3 Cache Manager for TEMPO Files
Handles S3 lifecycle, cleanup, and optimization for ultra-fast access
Part of NAQ Forecast AWS Infrastructure
"""

import boto3
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3CacheManager:
    """Manages S3 cache lifecycle for TEMPO files"""
    
    def __init__(self, cache_bucket: str):
        """
        Initialize S3 cache manager
        
        Args:
            cache_bucket: S3 bucket name for TEMPO file cache
        """
        self.s3_client = boto3.client('s3')
        self.cache_bucket = cache_bucket
        
    def setup_lifecycle_policy(self):
        """
        Configure S3 lifecycle policy for automatic cleanup
        
        - Delete files older than 7 days
        - Transition to IA after 1 day
        - Optimize storage costs
        """
        try:
            lifecycle_config = {
                'Rules': [
                    {
                        'ID': 'tempo-cache-lifecycle',
                        'Status': 'Enabled',
                        'Filter': {'Prefix': 'latest-'},
                        'Expiration': {'Days': 7},
                        'Transitions': [
                            {
                                'Days': 1,
                                'StorageClass': 'STANDARD_IA'
                            },
                            {
                                'Days': 3,
                                'StorageClass': 'GLACIER_IR'
                            }
                        ]
                    },
                    {
                        'ID': 'cleanup-old-versions',
                        'Status': 'Enabled',
                        'Filter': {},
                        'NoncurrentVersionExpiration': {'NoncurrentDays': 1}
                    }
                ]
            }
            
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.cache_bucket,
                LifecycleConfiguration=lifecycle_config
            )
            
            logger.info(f"✅ Lifecycle policy configured for bucket: {self.cache_bucket}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup lifecycle policy: {e}")
            return False
    
    def setup_bucket_optimization(self):
        """
        Configure bucket for optimal performance
        
        - Enable versioning for rollback capability
        - Set up CloudWatch metrics
        - Configure transfer acceleration if needed
        """
        try:
            self.s3_client.put_bucket_versioning(
                Bucket=self.cache_bucket,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            logger.info(f"✅ Versioning enabled for bucket: {self.cache_bucket}")
            
            # Enable CloudWatch request metrics
            self.s3_client.put_bucket_metrics_configuration(
                Bucket=self.cache_bucket,
                Id='tempo-cache-metrics',
                MetricsConfiguration={
                    'Id': 'tempo-cache-metrics',
                    'Filter': {'Prefix': 'latest-'}
                }
            )
            logger.info(f"✅ CloudWatch metrics configured for bucket: {self.cache_bucket}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup bucket optimization: {e}")
            return False
    
    def list_cached_files(self) -> List[Dict]:
        """
        List all cached TEMPO files with metadata
        
        Returns:
            List of file information dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.cache_bucket,
                Prefix='latest-'
            )
            
            files = []
            for obj in response.get('Contents', []):
                file_info = {
                    'key': obj['Key'],
                    'gas': obj['Key'].replace('latest-', '').replace('.nc', '').upper(),
                    'size_mb': round(obj['Size'] / (1024 * 1024), 2),
                    'last_modified': obj['LastModified'],
                    'age_hours': (datetime.now(obj['LastModified'].tzinfo) - obj['LastModified']).total_seconds() / 3600,
                    's3_path': f"s3://{self.cache_bucket}/{obj['Key']}"
                }
                files.append(file_info)
            
            logger.info(f"📁 Found {len(files)} cached TEMPO files")
            return files
            
        except Exception as e:
            logger.error(f"❌ Failed to list cached files: {e}")
            return []
    
    def cleanup_old_files(self, max_age_days: int = 7) -> int:
        """
        Manual cleanup of old cached files
        
        Args:
            max_age_days: Maximum age in days before deletion
            
        Returns:
            Number of files deleted
        """
        try:
            files = self.list_cached_files()
            cutoff_time = datetime.now() - timedelta(days=max_age_days)
            
            deleted_count = 0
            for file_info in files:
                if file_info['last_modified'].replace(tzinfo=None) < cutoff_time:
                    self.s3_client.delete_object(
                        Bucket=self.cache_bucket,
                        Key=file_info['key']
                    )
                    logger.info(f"🗑️ Deleted old file: {file_info['key']}")
                    deleted_count += 1
            
            logger.info(f"✅ Cleanup complete: {deleted_count} files deleted")
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ Cleanup failed: {e}")
            return 0
    
    def get_cache_stats(self) -> Dict:
        """
        Get comprehensive cache statistics
        
        Returns:
            Dictionary with cache statistics
        """
        try:
            files = self.list_cached_files()
            
            if not files:
                return {
                    'total_files': 0,
                    'total_size_mb': 0,
                    'gases_available': [],
                    'freshest_file_age_hours': None,
                    'oldest_file_age_hours': None,
                    'average_age_hours': None
                }
            
            stats = {
                'total_files': len(files),
                'total_size_mb': round(sum(f['size_mb'] for f in files), 2),
                'gases_available': sorted(list(set(f['gas'] for f in files))),
                'freshest_file_age_hours': round(min(f['age_hours'] for f in files), 2),
                'oldest_file_age_hours': round(max(f['age_hours'] for f in files), 2),
                'average_age_hours': round(sum(f['age_hours'] for f in files) / len(files), 2)
            }
            
            # Per-gas breakdown
            stats['per_gas'] = {}
            for gas in stats['gases_available']:
                gas_files = [f for f in files if f['gas'] == gas]
                stats['per_gas'][gas] = {
                    'count': len(gas_files),
                    'size_mb': round(sum(f['size_mb'] for f in gas_files), 2),
                    'age_hours': round(gas_files[0]['age_hours'], 2) if gas_files else None
                }
            
            logger.info(f"📊 Cache stats: {stats['total_files']} files, {stats['total_size_mb']} MB")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get cache stats: {e}")
            return {}
    
    def validate_cache_integrity(self) -> Dict:
        """
        Validate cache integrity and file accessibility
        
        Returns:
            Validation results dictionary
        """
        try:
            files = self.list_cached_files()
            results = {
                'total_files': len(files),
                'valid_files': 0,
                'invalid_files': 0,
                'errors': []
            }
            
            for file_info in files:
                try:
                    response = self.s3_client.head_object(
                        Bucket=self.cache_bucket,
                        Key=file_info['key']
                    )
                    
                    # Basic validation checks
                    if response['ContentLength'] > 1000:  # Minimum reasonable file size
                        results['valid_files'] += 1
                    else:
                        results['invalid_files'] += 1
                        results['errors'].append(f"File too small: {file_info['key']}")
                        
                except Exception as e:
                    results['invalid_files'] += 1
                    results['errors'].append(f"Cannot access {file_info['key']}: {str(e)}")
            
            logger.info(f"✅ Cache validation: {results['valid_files']}/{results['total_files']} files valid")
            return results
            
        except Exception as e:
            logger.error(f"❌ Cache validation failed: {e}")
            return {'error': str(e)}

# Lambda handler for cache management operations
def lambda_handler(event, context):
    """
    AWS Lambda handler for S3 cache management
    
    Event structure:
    {
        "operation": "stats|cleanup|validate|setup",
        "cache_bucket": "naq-forecast-tempo-cache",
        "max_age_days": 7
    }
    """
    try:
        operation = event.get('operation', 'stats')
        cache_bucket = event.get('cache_bucket', 'naq-forecast-tempo-cache')
        max_age_days = event.get('max_age_days', 7)
        
        cache_manager = S3CacheManager(cache_bucket)
        
        logger.info(f"🚀 Cache management operation: {operation}")
        
        if operation == 'stats':
            stats = cache_manager.get_cache_stats()
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'operation': 'stats',
                    'data': stats
                })
            }
            
        elif operation == 'cleanup':
            deleted_count = cache_manager.cleanup_old_files(max_age_days)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'operation': 'cleanup',
                    'deleted_files': deleted_count
                })
            }
            
        elif operation == 'validate':
            validation = cache_manager.validate_cache_integrity()
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'operation': 'validate',
                    'data': validation
                })
            }
            
        elif operation == 'setup':
            lifecycle_success = cache_manager.setup_lifecycle_policy()
            optimization_success = cache_manager.setup_bucket_optimization()
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'operation': 'setup',
                    'lifecycle_configured': lifecycle_success,
                    'optimization_configured': optimization_success
                })
            }
            
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'status': 'error',
                    'message': f'Unknown operation: {operation}'
                })
            }
            
    except Exception as e:
        logger.error(f"❌ Cache management failed: {e}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }

if __name__ == "__main__":
    # Local testing
    cache_manager = S3CacheManager('naq-forecast-tempo-cache')
    
    print("📊 Cache Statistics:")
    stats = cache_manager.get_cache_stats()
    print(json.dumps(stats, indent=2, default=str))
    
    print("\n📁 Cached Files:")
    files = cache_manager.list_cached_files()
    for file in files:
        print(f"  {file['gas']}: {file['size_mb']} MB, {file['age_hours']:.1f}h old")
