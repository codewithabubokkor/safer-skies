"""
NASA Earthdata Credentials Manager
Handles authentication and temporary S3 credentials for TEMPO data access
Part of Safer Skies AWS Infrastructure
"""

import os
import json
import requests
import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NASACredentialsManager:
    """Manages NASA Earthdata authentication and S3 access"""
    
    def __init__(self):
        """Initialize with NASA Earthdata credentials"""
        
        self.bearer_token = os.getenv('NASA_BEARER_TOKEN')
        self.username = os.getenv('NASA_USERNAME')
        self.password = os.getenv('NASA_PASSWORD')
        
        # NASA endpoints
        self.s3_cred_endpoint = 'https://data.asdc.earthdata.nasa.gov/s3credentials'
        self.token_endpoint = 'https://urs.earthdata.nasa.gov/oauth/token'
        
        # Credential cache
        self.cached_credentials = None
        self.credential_expiry = None
        
        if not self.bearer_token and not (self.username and self.password):
            logger.warning("⚠️ No NASA credentials found in environment variables")
    
    def get_bearer_token(self) -> Optional[str]:
        """
        Get or refresh bearer token from NASA Earthdata
        
        Returns:
            Bearer token string or None if failed
        """
        if self.bearer_token:
            logger.info("✅ Using existing bearer token from environment")
            return self.bearer_token
        
        if not (self.username and self.password):
            logger.error("❌ No NASA username/password provided")
            return None
        
        try:
            # Request new token
            data = {
                'grant_type': 'client_credentials',
                'username': self.username,
                'password': self.password
            }
            
            response = requests.post(self.token_endpoint, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                self.bearer_token = token_data['access_token']
                
                logger.info("✅ New bearer token obtained from NASA")
                return self.bearer_token
            else:
                logger.error(f"❌ Failed to get bearer token: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting bearer token: {e}")
            return None
    
    def get_nasa_s3_credentials(self, force_refresh: bool = False) -> Optional[Dict]:
        """
        Get temporary NASA S3 credentials
        
        Args:
            force_refresh: Force refresh even if cached credentials are valid
            
        Returns:
            S3 credentials dictionary or None if failed
        """
        if (not force_refresh and
            self.cached_credentials and
            self.credential_expiry and
            datetime.now() < self.credential_expiry):
            
            logger.info("✅ Using cached NASA S3 credentials")
            return self.cached_credentials
        
        bearer_token = self.get_bearer_token()
        if not bearer_token:
            logger.error("❌ Cannot get NASA S3 credentials without bearer token")
            return None
        
        try:
            headers = {'Authorization': f'Bearer {bearer_token}'}
            response = requests.get(self.s3_cred_endpoint, headers=headers)
            
            if response.status_code == 200:
                credentials = response.json()
                
                # Cache credentials with expiry buffer (5 minutes before actual expiry)
                expiry_time = datetime.fromisoformat(credentials['expiration'].replace('Z', '+00:00'))
                self.credential_expiry = expiry_time - timedelta(minutes=5)
                self.cached_credentials = credentials
                
                logger.info(f"✅ NASA S3 credentials obtained (expires: {expiry_time})")
                return credentials
            else:
                logger.error(f"❌ Failed to get NASA S3 credentials: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting NASA S3 credentials: {e}")
            return None
    
    def create_nasa_s3_client(self, force_refresh: bool = False) -> Optional[boto3.client]:
        """
        Create authenticated S3 client for NASA data access
        
        Args:
            force_refresh: Force refresh credentials
            
        Returns:
            Boto3 S3 client or None if failed
        """
        credentials = self.get_nasa_s3_credentials(force_refresh)
        if not credentials:
            return None
        
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['accessKeyId'],
                aws_secret_access_key=credentials['secretAccessKey'],
                aws_session_token=credentials['sessionToken'],
                region_name='us-west-2'  # NASA ASDC region
            )
            
            logger.info("✅ NASA S3 client created successfully")
            return s3_client
            
        except Exception as e:
            logger.error(f"❌ Failed to create NASA S3 client: {e}")
            return None
    
    def test_nasa_access(self) -> bool:
        """
        Test NASA S3 access with current credentials
        
        Returns:
            True if access works, False otherwise
        """
        try:
            s3_client = self.create_nasa_s3_client()
            if not s3_client:
                return False
            
            response = s3_client.list_objects_v2(
                Bucket='tempo-tempo',
                Prefix='TEMPO_NO2_L2_V03',
                MaxKeys=1
            )
            
            if 'Contents' in response:
                logger.info(f"✅ NASA access test successful (found {len(response['Contents'])} files)")
                return True
            else:
                logger.warning("⚠️ NASA access test: no files found")
                return False
                
        except Exception as e:
            logger.error(f"❌ NASA access test failed: {e}")
            return False
    
    def get_credential_status(self) -> Dict:
        """
        Get current credential status and expiry information
        
        Returns:
            Status dictionary with credential information
        """
        status = {
            'bearer_token_available': bool(self.bearer_token),
            'username_available': bool(self.username),
            'password_available': bool(self.password),
            'cached_credentials_available': bool(self.cached_credentials),
            'credentials_expired': False,
            'time_until_expiry': None,
            'nasa_access_working': False
        }
        
        if self.credential_expiry:
            now = datetime.now()
            if now > self.credential_expiry:
                status['credentials_expired'] = True
            else:
                status['time_until_expiry'] = str(self.credential_expiry - now)
        
        # Test NASA access
        status['nasa_access_working'] = self.test_nasa_access()
        
        return status

# Lambda handler for credential management
def lambda_handler(event, context):
    """
    AWS Lambda handler for NASA credential management
    
    Event structure:
    {
        "operation": "test|refresh|status",
        "force_refresh": false
    }
    """
    try:
        operation = event.get('operation', 'status')
        force_refresh = event.get('force_refresh', False)
        
        cred_manager = NASACredentialsManager()
        
        logger.info(f"🚀 Credential management operation: {operation}")
        
        if operation == 'test':
            access_working = cred_manager.test_nasa_access()
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'operation': 'test',
                    'nasa_access_working': access_working
                })
            }
            
        elif operation == 'refresh':
            credentials = cred_manager.get_nasa_s3_credentials(force_refresh=True)
            success = credentials is not None
            
            return {
                'statusCode': 200 if success else 500,
                'body': json.dumps({
                    'status': 'success' if success else 'error',
                    'operation': 'refresh',
                    'credentials_refreshed': success,
                    'expiry': credentials.get('expiration') if credentials else None
                })
            }
            
        elif operation == 'status':
            status = cred_manager.get_credential_status()
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'operation': 'status',
                    'data': status
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
        logger.error(f"❌ Credential management failed: {e}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }

# Environment variable setup guide
def print_environment_setup():
    """Print environment variable setup instructions"""
    print("""
🔐 NASA Earthdata Credentials Setup

Required Environment Variables:
1. NASA_BEARER_TOKEN (preferred method)
   - Get from: https://urs.earthdata.nasa.gov/
   - Long-lived token for automated access
   
2. Alternative: NASA_USERNAME + NASA_PASSWORD
   - Your Earthdata login credentials
   - Will automatically request bearer tokens

AWS Lambda Environment Variables:
```
NASA_BEARER_TOKEN=eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOi...
TEMPO_CACHE_BUCKET=naq-forecast-tempo-cache
```

Local Development (.env file):
```
NASA_BEARER_TOKEN=your_token_here
NASA_USERNAME=your_username
NASA_PASSWORD=your_password
TEMPO_CACHE_BUCKET=naq-forecast-tempo-cache
```

Test your setup:
```python
from nasa_credentials import NASACredentialsManager
manager = NASACredentialsManager()
print(manager.get_credential_status())
```
""")


