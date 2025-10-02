"""
TEMPO File Fetcher - AWS Lambda Optimized
Downloads TEMPO satellite files to S3 for ultra-fast access (10-30s vs 3+ minutes)
Part of Safer Skies AWS Infrastructure
"""

import os
import json
import boto3
import requests
import tempfile
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TempoGasData:
    """Extracted TEMPO gas measurement"""
    gas: str
    timestamp: str
    column_density: float
    surface_ppb: float
    quality_flag: int
    cloud_fraction: float
    latitude: float
    longitude: float
    data_valid: bool
    nasa_quality: str

class TempoFileFetcher:
    """AWS Lambda-optimized TEMPO file fetcher with S3 caching"""
    
    def __init__(self):
        """Initialize with NASA credentials and S3 client"""
        
        # NASA Earthdata credentials - using provided bearer token
        self.bearer_token = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImFidWJva2tvci5jc2UiLCJleHAiOjE3NjAxMTM3MTEsImlhdCI6MTc1NDkyOTcxMSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.rJhkWn4bxNeWipNFNUgQu4qZelDQ47czJBtgWPbGIg7Yujny2c19d6QFfEGWTIDOCiwDhLde9RsrNH2W_JVk190fuekdiBPMUXMH5gnn-OO0eiB4QO5kN2nVKCin6jZPo7HLAXvshs92Z6VXXpj3mKVjAPlxA3R0keR93R0gVl0bKYyjkps5AUA93qDKKS5iBh1-Azil5aKeIqmSWDG6iHyp6bIAoznrt5hkEqkLU0BYsVmWNpMHty0Legv7ZEit_zR414LlUpY-hyVa8hhgnJM1pNyR4bnHvqSnZh8rvpXNt6_qQHAR0RvgrgoFXqRMpy6-tVd3XyqdEkLgaVZccQ"
        self.s3_cred_endpoint = 'https://data.asdc.earthdata.nasa.gov/s3credentials'
        
        # S3 clients
        self.s3_client = boto3.client('s3')
        self.cache_bucket = os.getenv('TEMPO_CACHE_BUCKET', 'naq-forecast-tempo-cache')
        
        # Local storage configuration for testing
        self.local_cache_dir = os.getenv('TEMPO_LOCAL_CACHE', '/app/data/tempo_data')
        os.makedirs(self.local_cache_dir, exist_ok=True)
        
        self.setup_nasa_s3_credentials()
        
    def setup_nasa_s3_credentials(self):
        """Get temporary NASA S3 credentials using working pattern"""
        try:
            headers = {'Authorization': f'Bearer {self.bearer_token}'}
            response = requests.get(self.s3_cred_endpoint, headers=headers)
            
            if response.status_code == 200:
                creds = response.json()
                
                os.environ['AWS_ACCESS_KEY_ID'] = creds['accessKeyId']
                os.environ['AWS_SECRET_ACCESS_KEY'] = creds['secretAccessKey']
                os.environ['AWS_SESSION_TOKEN'] = creds['sessionToken']
                
                import s3fs
                self.s3fs_client = s3fs.S3FileSystem(anon=False)
                self.nasa_s3_client = boto3.client('s3')  # For NASA S3
                
                logger.info("✅ NASA S3 credentials obtained")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to get NASA S3 credentials: {e}")
            return False
    
    def download_tempo_to_s3(self, gas: str, nasa_s3_path: str) -> Optional[str]:
        """
        Download TEMPO file from NASA S3 to our cache S3
        
        Args:
            gas: Target gas (NO2, HCHO, etc.)
            nasa_s3_path: Full NASA S3 path (s3://tempo-tempo/...)
            
        Returns:
            Our S3 cache path or None if failed
        """
        try:
            bucket = nasa_s3_path.replace('s3://', '').split('/')[0]
            key = '/'.join(nasa_s3_path.replace('s3://', '').split('/')[1:])
            
            logger.info(f"📥 Downloading from NASA: {nasa_s3_path}")
            
            # Download to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.nc') as temp_file:
                temp_local_path = temp_file.name
                
            self.nasa_s3_client.download_file(bucket, key, temp_local_path)
            logger.info(f"📁 Downloaded to temp: {temp_local_path}")
            
            # Upload to our S3 bucket with standardized name
            target_key = f"latest-{gas.lower()}.nc"
            
            logger.info(f"📤 Uploading to cache bucket: {target_key}")
            self.s3_client.upload_file(temp_local_path, self.cache_bucket, target_key)
            
            os.remove(temp_local_path)
            
            cache_s3_path = f"s3://{self.cache_bucket}/{target_key}"
            logger.info(f"✅ File cached: {cache_s3_path}")
            
            return cache_s3_path
            
        except Exception as e:
            logger.error(f"❌ Download failed for {nasa_s3_path}: {e}")
            return None
    
    def download_tempo_file_local(self, nasa_s3_path: str, gas: str) -> Optional[str]:
        """
        Download TEMPO file using the EXACT working pattern
        
        Args:
            nasa_s3_path: NASA S3 path like 'asdc-prod-protected/TEMPO/...'
            gas: "NO2" or "HCHO"
            
        Returns:
            Local file path or None
        """
        try:
            bucket = "asdc-prod-protected"
            key = nasa_s3_path.replace("asdc-prod-protected/", "")
            filename = os.path.basename(key)
            
            # Download to local cache directory
            local_path = os.path.join(self.local_cache_dir, f"latest-{gas.lower()}.nc")
            
            logger.info(f"⬇️ Downloading {filename} from NASA S3...")
            start_time = time.time()
            
            # Download using boto3 - EXACT same pattern as working code
            self.nasa_s3_client.download_file(bucket, key, local_path)
            
            download_time = time.time() - start_time
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            
            logger.info(f"✅ Downloaded {file_size_mb:.1f}MB in {download_time:.1f}s → {local_path}")
            
            return local_path
            
        except Exception as e:
            logger.error(f"❌ Local download failed for {nasa_s3_path}: {e}")
            return None
    
    def get_latest_local_tempo_file(self, gas: str) -> Optional[str]:
        """
        Get the latest TEMPO file from local cache
        
        Args:
            gas: Target gas (NO2, HCHO, etc.)
            
        Returns:
            Local file path to latest cached file or None
        """
        try:
            filename = f"latest-{gas.lower()}.nc"
            local_path = os.path.join(self.local_cache_dir, filename)
            
            if os.path.exists(local_path):
                file_age = time.time() - os.path.getmtime(local_path)
                age_hours = file_age / 3600
                
                logger.info(f"✅ Found local cached file: {local_path} (Age: {age_hours:.1f}h)")
                return local_path
            else:
                logger.warning(f"⚠️ No local cached file found for {gas}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error checking local cache for {gas}: {e}")
            return None
    
    def is_local_cache_fresh(self, gas: str, max_age_hours: int = 2) -> bool:
        """
        Check if local cached TEMPO file is fresh enough
        
        Args:
            gas: Target gas (NO2, HCHO, etc.)
            max_age_hours: Maximum age in hours before considering stale
            
        Returns:
            True if cache is fresh, False otherwise
        """
        try:
            filename = f"latest-{gas.lower()}.nc"
            local_path = os.path.join(self.local_cache_dir, filename)
            
            if not os.path.exists(local_path):
                return False
            
            file_age = time.time() - os.path.getmtime(local_path)
            age_hours = file_age / 3600
            
            is_fresh = age_hours <= max_age_hours
            logger.info(f"📅 Local cache age for {gas}: {age_hours:.1f} hours (Fresh: {is_fresh})")
            
            return is_fresh
            
        except Exception as e:
            logger.error(f"❌ Error checking local cache freshness for {gas}: {e}")
            return False
    
    def get_latest_tempo_file(self, gas: str) -> Optional[str]:
        """
        Get the latest TEMPO file using working s3fs pattern
        
        Args:
            gas: Target gas (NO2, HCHO, etc.)
            
        Returns:
            NASA S3 path to latest file or None
        """
        try:
            for days in range(3):
                check_date = datetime.now() - timedelta(days=days)
                date_str = check_date.strftime('%Y.%m.%d')
                s3path = f's3://asdc-prod-protected/TEMPO/TEMPO_{gas}_L3_V03/{date_str}/*.nc'
                
                try:
                    files = self.s3fs_client.glob(s3path)
                    if files:
                        latest_file = sorted(files)[-1]
                        logger.info(f"✅ Found {gas} file: {latest_file}")
                        return latest_file
                except Exception as e:
                    logger.debug(f"No {gas} files for {date_str}: {e}")
                    continue
            
            logger.warning(f"❌ No {gas} files found in last 3 days")
            return None
                
        except Exception as e:
            logger.error(f"❌ Error finding latest {gas} file: {e}")
            return None
            logger.warning(f"⚠️ No cached file found for {gas}")
            return None
        except Exception as e:
            logger.error(f"❌ Error checking cache for {gas}: {e}")
            return None
    
    def is_cache_fresh(self, gas: str, max_age_hours: int = 2) -> bool:
        """
        Check if cached TEMPO file is fresh enough
        
        Args:
            gas: Target gas (NO2, HCHO, etc.)
            max_age_hours: Maximum age in hours before considering stale
            
        Returns:
            True if cache is fresh, False otherwise
        """
        try:
            target_key = f"latest-{gas.lower()}.nc"
            
            response = self.s3_client.head_object(
                Bucket=self.cache_bucket,
                Key=target_key
            )
            
            last_modified = response['LastModified']
            age_hours = (datetime.now(last_modified.tzinfo) - last_modified).total_seconds() / 3600
            
            is_fresh = age_hours <= max_age_hours
            logger.info(f"📅 Cache age for {gas}: {age_hours:.1f} hours (Fresh: {is_fresh})")
            
            return is_fresh
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"⚠️ No cached file found for {gas}")
            return False
        except Exception as e:
            logger.error(f"❌ Error checking cache freshness for {gas}: {e}")
            return False

# Lambda handler function
def lambda_handler(event, context):
    """
    AWS Lambda handler for TEMPO file fetching
    
    Event structure:
    {
        "gas": "NO2",
        "nasa_s3_path": "s3://tempo-tempo/TEMPO_NO2_L2_V03_20250820T100000Z_S013G01.nc",
        "force_refresh": false
    }
    """
    try:
        fetcher = TempoFileFetcher()
        
        gas = event.get('gas', 'NO2')
        nasa_s3_path = event.get('nasa_s3_path')
        force_refresh = event.get('force_refresh', False)
        
        logger.info(f"🚀 Lambda triggered for gas: {gas}")
        
        if not force_refresh and fetcher.is_cache_fresh(gas):
            cached_path = fetcher.get_latest_tempo_file(gas)
            logger.info(f"✅ Using fresh cache: {cached_path}")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'source': 'cache',
                    'file_path': cached_path,
                    'gas': gas
                })
            }
        
        # Download new file if path provided
        if nasa_s3_path:
            cached_path = fetcher.download_tempo_to_s3(gas, nasa_s3_path)
            
            if cached_path:
                logger.info(f"✅ New file downloaded: {cached_path}")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'success',
                        'source': 'download',
                        'file_path': cached_path,
                        'gas': gas
                    })
                }
            else:
                logger.error(f"❌ Download failed for {nasa_s3_path}")
                
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'status': 'error',
                        'message': 'Download failed',
                        'gas': gas
                    })
                }
        
        # No path provided and cache is stale
        logger.warning(f"⚠️ No NASA S3 path provided and cache is stale for {gas}")
        
        return {
            'statusCode': 400,
            'body': json.dumps({
                'status': 'error',
                'message': 'No NASA S3 path provided and cache is stale',
                'gas': gas
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Lambda execution failed: {e}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }


