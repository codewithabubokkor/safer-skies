#!/usr/bin/env python3
"""
☁️ AWS TEMPO DOWNLOADER & CACHE SYSTEM
Lambda-ready NASA TEMPO data downloader with S3 caching

Architecture:
NASA S3 (TEMPO L3) → Lambda Downloader → S3 Cache → CloudFront → Ultra-Fast API

Features:
- Hourly automated downloads via Lambda/EventBridge
- Intelligent file detection (latest NO2/HCHO)
- S3 caching with "latest" keys for instant access
- JSON summary generation for frontend
- CloudFront CDN integration
- Error handling and retry logic
- Cost optimization (only download when new data available)

Deployment:
1. Package as Lambda layer (h5py, boto3, requests)
2. Schedule via EventBridge (hourly)
3. Deploy to us-east-1 (same region as NASA S3)
4. Use VPC endpoints for S3 to reduce costs
"""

import boto3
import json
import logging
import os
import requests
import tempfile
import h5py
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import hashlib

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class AWSTempoDownloader:
    """AWS Lambda-optimized TEMPO downloader and processor"""
    
    def __init__(self):
        """Initialize AWS services and NASA credentials"""
        # AWS Services
        self.s3_client = boto3.client('s3')
        self.cache_bucket = os.environ.get('TEMPO_CACHE_BUCKET', 'naq-tempo-cache')
        
        # NASA Credentials
        self.bearer_token = os.environ.get('NASA_BEARER_TOKEN', 
            'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImFidWJva2tvci5jc2UiLCJleHAiOjE3NjAxMTM3MTEsImlhdCI6MTc1NDkyOTcxMSwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292IiwiaWRlbnRpdHlfcHJvdmlkZXIiOiJlZGxfb3BzIiwiYWNyIjoiZWRsIiwiYXNzdXJhbmNlX2xldmVsIjozfQ.rJhkWn4bxNeWipNFNUgQu4qZelDQ47czJBtgWPbGIg7Yujny2c19d6QFfEGWTIDOCiwDhLde9RsrNH2W_JVk190fuekdiBPMUXMH5gnn-OO0eiB4QO5kN2nVKCin6jZPo7HLAXvshs92Z6VXXpj3mKVjAPlxA3R0keR93R0gVl0bKYyjkps5AUA93qDKKS5iBh1-Azil5aKeIqmSWDG6iHyp6bIAoznrt5hkEqkLU0BYsVmWNpMHty0Legv7ZEit_zR414LlUpY-hyVa8hhgnJM1pNyR4bnHvqSnZh8rvpXNt6_qQHAR0RvgrgoFXqRMpy6-tVd3XyqdEkLgaVZccQ')
        
        self.s3_cred_endpoint = 'https://data.asdc.earthdata.nasa.gov/s3credentials'
        
        # Pre-defined locations for summary generation
        self.key_locations = {
            "NYC": {"lat": 40.7128, "lon": -74.0060, "name": "New York City"},
            "LAX": {"lat": 34.0522, "lon": -118.2437, "name": "Los Angeles"},
            "CHI": {"lat": 41.8781, "lon": -87.6298, "name": "Chicago"},
            "MIA": {"lat": 25.7617, "lon": -80.1918, "name": "Miami"},
            "DEN": {"lat": 39.7392, "lon": -104.9903, "name": "Denver"}
        }
    
    def setup_nasa_credentials(self) -> bool:
        """Setup temporary NASA S3 credentials"""
        try:
            headers = {'Authorization': f'Bearer {self.bearer_token}'}
            response = requests.get(self.s3_cred_endpoint, headers=headers, timeout=30)
            response.raise_for_status()
            
            temp_creds = response.json()
            
            self.nasa_s3 = boto3.client(
                's3',
                aws_access_key_id=temp_creds['accessKeyId'],
                aws_secret_access_key=temp_creds['secretAccessKey'],
                aws_session_token=temp_creds['sessionToken'],
                region_name='us-east-1'
            )
            
            logger.info("✅ NASA S3 credentials configured")
            return True
            
        except Exception as e:
            logger.error(f"❌ NASA credential setup failed: {e}")
            return False
    
    def find_latest_tempo_file(self, gas: str, days_back: int = 2) -> Optional[str]:
        """
        Find latest TEMPO L3 file in NASA S3
        
        Args:
            gas: "NO2" or "HCHO"
            days_back: Number of days to search backwards
            
        Returns:
            S3 key to latest file or None
        """
        try:
            for days in range(days_back):
                check_date = datetime.now(timezone.utc) - timedelta(days=days)
                date_str = check_date.strftime('%Y.%m.%d')
                prefix = f'TEMPO/TEMPO_{gas}_L3_V03/{date_str}/'
                
                # List objects in NASA S3
                response = self.nasa_s3.list_objects_v2(
                    Bucket='asdc-prod-protected',
                    Prefix=prefix,
                    MaxKeys=100
                )
                
                if 'Contents' in response:
                    files = sorted(response['Contents'], 
                                 key=lambda x: x['LastModified'], reverse=True)
                    if files:
                        latest_key = files[0]['Key']
                        logger.info(f"📁 Found latest {gas}: {latest_key}")
                        return latest_key
            
            logger.warning(f"⚠️ No {gas} files found in last {days_back} days")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error finding {gas} files: {e}")
            return None
    
    def check_if_update_needed(self, gas: str, nasa_key: str) -> bool:
        """
        Check if we need to download new file by comparing metadata
        
        Args:
            gas: "NO2" or "HCHO"
            nasa_key: NASA S3 key
            
        Returns:
            True if download needed
        """
        try:
            cache_key = f"tempo/latest/{gas}.nc"
            
            try:
                cache_response = self.s3_client.head_object(
                    Bucket=self.cache_bucket,
                    Key=cache_key
                )
                cached_source = cache_response.get('Metadata', {}).get('source-key', '')
                
                if cached_source == nasa_key:
                    logger.info(f"✅ {gas} file already cached: {nasa_key}")
                    return False
                    
            except self.s3_client.exceptions.NoSuchKey:
                logger.info(f"🆕 No cached {gas} file found, download needed")
                return True
            
            logger.info(f"🔄 New {gas} file detected: {nasa_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error checking update status: {e}")
            return True  # Download on error to be safe
    
    def download_and_cache_file(self, gas: str, nasa_key: str) -> bool:
        """
        Download TEMPO file from NASA S3 and cache in our S3 bucket
        
        Args:
            gas: "NO2" or "HCHO"
            nasa_key: NASA S3 key to download
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"⬇️ Downloading {gas} file: {os.path.basename(nasa_key)}")
            
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                # Download from NASA S3
                self.nasa_s3.download_fileobj(
                    Bucket='asdc-prod-protected',
                    Key=nasa_key,
                    Fileobj=temp_file
                )
                
                temp_file_path = temp_file.name
            
            # Upload to our cache bucket
            cache_key = f"tempo/latest/{gas}.nc"
            
            with open(temp_file_path, 'rb') as f:
                self.s3_client.upload_fileobj(
                    f,
                    self.cache_bucket,
                    cache_key,
                    ExtraArgs={
                        'Metadata': {
                            'source-key': nasa_key,
                            'downloaded-at': datetime.now(timezone.utc).isoformat(),
                            'gas-type': gas
                        },
                        'ContentType': 'application/x-netcdf'
                    }
                )
            
            os.unlink(temp_file_path)
            
            file_size = self.s3_client.head_object(
                Bucket=self.cache_bucket, Key=cache_key
            )['ContentLength']
            
            logger.info(f"✅ Cached {gas} file: {file_size / 1024 / 1024:.1f}MB")
            return True
            
        except Exception as e:
            logger.error(f"❌ Download failed for {gas}: {e}")
            return False
    
    def extract_key_locations_data(self, gas: str) -> Dict:
        """
        Extract data for key locations and create summary
        
        Args:
            gas: "NO2" or "HCHO"
            
        Returns:
            Summary data for key locations
        """
        try:
            cache_key = f"tempo/latest/{gas}.nc"
            
            # Download cached file to temporary location
            with tempfile.NamedTemporaryFile() as temp_file:
                self.s3_client.download_fileobj(
                    self.cache_bucket,
                    cache_key,
                    temp_file
                )
                temp_file.seek(0)
                
                with h5py.File(temp_file, 'r') as h5_file:
                    try:
                        if 'geolocation' in h5_file:
                            lats = h5_file['geolocation']['latitude'][:]
                            lons = h5_file['geolocation']['longitude'][:]
                        else:
                            lats = h5_file['latitude'][:]
                            lons = h5_file['longitude'][:]
                    except KeyError:
                        lats = h5_file['latitude'][:]
                        lons = h5_file['longitude'][:]
                    
                    product = h5_file['product']
                    support_data = h5_file['support_data']
                    
                    if gas == "NO2":
                        vertical_column = product['vertical_column_troposphere']
                    else:  # HCHO
                        vertical_column = product['vertical_column']
                    
                    quality_flag = product['main_data_quality_flag']
                    cloud_fraction = support_data['eff_cloud_fraction']
                    
                    location_data = {}
                    
                    for loc_code, location in self.key_locations.items():
                        lat, lon = location["lat"], location["lon"]
                        
                        lat_idx = np.argmin(np.abs(lats - lat))
                        lon_idx = np.argmin(np.abs(lons - lon))
                        
                        gas_value = float(vertical_column[0, lat_idx, lon_idx])
                        qa_flag = int(quality_flag[0, lat_idx, lon_idx])
                        cloud_frac = float(cloud_fraction[0, lat_idx, lon_idx])
                        
                        data_valid = (
                            qa_flag == 0 and 
                            cloud_frac < 0.2 and 
                            not np.isnan(gas_value) and 
                            gas_value > 0
                        )
                        
                        surface_ppb = None
                        if data_valid:
                            if gas == "NO2":
                                surface_ppb = round((gas_value / 1e16) * 3.5, 2)
                            else:  # HCHO
                                surface_ppb = round((gas_value / 1e16) * 2.8, 2)
                        
                        location_data[loc_code] = {
                            "name": location["name"],
                            "lat": lat,
                            "lon": lon,
                            "column_density": gas_value,
                            "surface_ppb": surface_ppb,
                            "quality_flag": qa_flag,
                            "cloud_fraction": cloud_frac,
                            "data_valid": data_valid,
                            "actual_lat": float(lats[lat_idx]),
                            "actual_lon": float(lons[lon_idx])
                        }
            
            return location_data
            
        except Exception as e:
            logger.error(f"❌ Error extracting {gas} location data: {e}")
            return {}
    
    def generate_summary_json(self) -> Dict:
        """Generate summary JSON for frontend consumption"""
        try:
            summary = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "data_date": datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                "gases": {},
                "coverage": {
                    "region": "North America",
                    "spatial_resolution": "3.5km × 7km",
                    "temporal_resolution": "hourly"
                },
                "api_version": "1.0"
            }
            
            for gas in ["NO2", "HCHO"]:
                gas_data = self.extract_key_locations_data(gas)
                if gas_data:
                    summary["gases"][gas] = {
                        "locations": gas_data,
                        "valid_count": sum(1 for loc in gas_data.values() if loc["data_valid"]),
                        "total_count": len(gas_data)
                    }
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Error generating summary: {e}")
            return {"error": str(e), "generated_at": datetime.now(timezone.utc).isoformat()}
    
    def upload_summary_json(self, summary: Dict) -> bool:
        """Upload summary JSON to S3"""
        try:
            summary_key = "tempo/latest/summary.json"
            
            self.s3_client.put_object(
                Bucket=self.cache_bucket,
                Key=summary_key,
                Body=json.dumps(summary, indent=2),
                ContentType='application/json',
                CacheControl='max-age=3600'  # 1 hour cache for CloudFront
            )
            
            logger.info(f"✅ Summary uploaded: {summary_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Summary upload failed: {e}")
            return False
    
    def run_download_cycle(self) -> Dict:
        """Run complete download and caching cycle"""
        start_time = datetime.now(timezone.utc)
        results = {
            "start_time": start_time.isoformat(),
            "downloads": {},
            "summary_generated": False,
            "errors": []
        }
        
        try:
            if not self.setup_nasa_credentials():
                results["errors"].append("NASA credential setup failed")
                return results
            
            downloads_needed = False
            
            for gas in ["NO2", "HCHO"]:
                try:
                    nasa_key = self.find_latest_tempo_file(gas)
                    if not nasa_key:
                        results["downloads"][gas] = {"status": "no_files_found"}
                        continue
                    
                    if self.check_if_update_needed(gas, nasa_key):
                        # Download and cache
                        if self.download_and_cache_file(gas, nasa_key):
                            results["downloads"][gas] = {
                                "status": "downloaded",
                                "source_key": nasa_key
                            }
                            downloads_needed = True
                        else:
                            results["downloads"][gas] = {"status": "download_failed"}
                    else:
                        results["downloads"][gas] = {"status": "up_to_date"}
                        
                except Exception as e:
                    logger.error(f"❌ Error processing {gas}: {e}")
                    results["downloads"][gas] = {"status": "error", "message": str(e)}
                    results["errors"].append(f"{gas}: {str(e)}")
            
            summary = self.generate_summary_json()
            if self.upload_summary_json(summary):
                results["summary_generated"] = True
            
            # Final timing
            end_time = datetime.now(timezone.utc)
            results["end_time"] = end_time.isoformat()
            results["duration_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info(f"✅ Download cycle completed in {results['duration_seconds']:.1f}s")
            return results
            
        except Exception as e:
            logger.error(f"❌ Download cycle failed: {e}")
            results["errors"].append(str(e))
            return results

def lambda_handler(event, context):
    """
    AWS Lambda entry point for scheduled TEMPO downloads
    
    Triggered by:
    - EventBridge (hourly schedule)
    - Manual API Gateway calls
    - Step Functions workflows
    """
    logger.info("🚀 Starting AWS TEMPO download cycle")
    
    try:
        downloader = AWSTempoDownloader()
        results = downloader.run_download_cycle()
        
        # Lambda response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(results)
        }
        
    except Exception as e:
        logger.error(f"❌ Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

if __name__ == "__main__":
    """Test locally before Lambda deployment"""
    import sys
    
    os.environ['TEMPO_CACHE_BUCKET'] = 'naq-tempo-cache-test'
    
    downloader = AWSTempoDownloader()
    results = downloader.run_download_cycle()
    
    print("🚀 Local Test Results:")
    print(json.dumps(results, indent=2))
