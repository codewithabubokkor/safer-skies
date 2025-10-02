# TEMPO File Downloading AWS Architecture

This directory contains the complete AWS infrastructure for ultra-fast TEMPO satellite data access through S3 caching.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   EventBridge   │───▶│  Lambda Fetcher  │───▶│   S3 Cache      │
│   (Hourly)      │    │                  │    │   Bucket        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Your Collector │◀───│   NASA S3 API    │    │  NASA TEMPO     │
│   (10-30s)      │    │   (Temp Creds)   │    │   Bucket        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📁 File Structure

```
/backend/aws/
├── tempo_file_fetcher.py      # Core TEMPO downloading logic
├── s3_cache_manager.py        # S3 lifecycle and caching
├── nasa_credentials.py        # NASA Earthdata authentication
└── deployment/
    ├── lambda_deploy.py       # AWS Lambda deployment scripts
    ├── eventbridge_setup.py   # Hourly scheduling setup
    └── README.md             # This file
```

## 🚀 Performance Benefits

| Method | Access Time | Use Case |
|--------|-------------|----------|
| **Direct NASA API** | 3+ minutes | Research/testing |
| **S3 Cache (Our System)** | **10-30 seconds** | **Production** |
| **Local Cache** | 2-5 seconds | Development |

## 🔧 Setup Instructions

### 1. Environment Variables

Create `.env` file in `/backend/aws/`:

```bash
# NASA Earthdata Credentials
NASA_BEARER_TOKEN=eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOi...
NASA_USERNAME=your_username  # Alternative to bearer token
NASA_PASSWORD=your_password  # Alternative to bearer token

# AWS Configuration
TEMPO_CACHE_BUCKET=naq-forecast-tempo-cache
AWS_REGION=us-east-1
```

### 2. AWS Prerequisites

```bash
# Install AWS CLI and configure credentials
aws configure

# Create S3 bucket for TEMPO cache
aws s3 mb s3://naq-forecast-tempo-cache --region us-east-1
```

### 3. Deploy Complete System

```bash
cd /backend/aws/deployment
python lambda_deploy.py
```

This will automatically:
- ✅ Create IAM roles with proper permissions
- ✅ Deploy 3 Lambda functions (fetcher, cache manager, credentials)
- ✅ Set up EventBridge hourly scheduling
- ✅ Configure S3 lifecycle policies
- ✅ Add proper permissions and policies

### 4. Test Deployment

```bash
# Test file fetcher
aws lambda invoke --function-name naq-tempo-file-fetcher \
  --payload '{"gas":"NO2","force_refresh":true}' \
  response.json

# Test cache manager
aws lambda invoke --function-name naq-tempo-cache-manager \
  --payload '{"operation":"stats"}' \
  cache_response.json

# View results
cat response.json
cat cache_response.json
```

## 📊 Components

### 1. **tempo_file_fetcher.py** - Core Downloader
- 🎯 **Purpose**: Downloads TEMPO files from NASA S3 to our cache
- ⚡ **Performance**: 10-30s access vs 3+ minutes direct
- 🔄 **Scheduling**: Hourly automatic downloads via EventBridge
- 💾 **Caching**: Stores latest files as `latest-no2.nc`, `latest-hcho.nc`

### 2. **s3_cache_manager.py** - Cache Lifecycle
- 🧹 **Cleanup**: Automatic deletion of files older than 7 days
- 📊 **Analytics**: Cache statistics and health monitoring
- 🔧 **Optimization**: Storage class transitions (Standard → IA → Glacier)
- ✅ **Validation**: File integrity and accessibility checks

### 3. **nasa_credentials.py** - Authentication
- 🔐 **NASA Access**: Manages Earthdata bearer tokens
- ⏰ **Auto-Refresh**: Handles credential expiration automatically
- 🧪 **Testing**: Validates NASA S3 access before operations
- 🔄 **Fallback**: Username/password backup for token generation

### 4. **lambda_deploy.py** - Deployment Automation
- 🚀 **One-Click Deploy**: Complete system setup with single command
- 🛡️ **IAM Management**: Creates roles with minimal required permissions
- 📦 **Packaging**: Handles Lambda deployment packages automatically
- 🔗 **Integration**: Connects all components seamlessly

### 5. **eventbridge_setup.py** - Scheduling System
- ⏰ **Hourly Downloads**: Automatic TEMPO file fetching
- 🎯 **On-Demand**: Manual triggers for immediate downloads
- 🧹 **Daily Cleanup**: Automated cache maintenance
- 📋 **Management**: Enable/disable schedules as needed

## 🎯 Integration with Your Collector

### Option A: Direct S3 Access (Recommended)
```python
from backend.aws.tempo_file_fetcher import TempoFileFetcher

# In your northamerica_collector.py
fetcher = TempoFileFetcher()

# Get cached file (10-30s access)
tempo_file_path = fetcher.get_latest_tempo_file('NO2')
if tempo_file_path:
    # Process ultra-fast cached data
    tempo_data = process_tempo_file(tempo_file_path)
```

### Option B: Lambda Invocation
```python
import boto3

lambda_client = boto3.client('lambda')

# Trigger file download if needed
response = lambda_client.invoke(
    FunctionName='naq-tempo-file-fetcher',
    Payload=json.dumps({
        'gas': 'NO2',
        'force_refresh': False
    })
)

result = json.loads(response['Payload'].read())
tempo_file_path = result['body']['file_path']
```

## 📈 Monitoring & Maintenance

### CloudWatch Metrics
- Lambda execution duration and errors
- S3 cache hit/miss ratios
- EventBridge rule execution status
- NASA API response times

### Daily Operations
```bash
# Check cache status
aws lambda invoke --function-name naq-tempo-cache-manager \
  --payload '{"operation":"stats"}' stats.json

# Manual cleanup if needed
aws lambda invoke --function-name naq-tempo-cache-manager \
  --payload '{"operation":"cleanup","max_age_days":3}' cleanup.json

# Test NASA credentials
aws lambda invoke --function-name naq-tempo-credential-manager \
  --payload '{"operation":"test"}' cred_test.json
```

### Troubleshooting

| Issue | Solution |
|-------|----------|
| NASA access fails | Check bearer token expiry in credentials manager |
| Cache files missing | Trigger manual download with `force_refresh: true` |
| EventBridge not firing | Check rule status and Lambda permissions |
| S3 access denied | Verify IAM role has proper S3 permissions |

## 💰 Cost Optimization

### S3 Storage Classes
- **Standard**: Active files (0-1 day) - ~$0.023/GB
- **Standard-IA**: Recent files (1-3 days) - ~$0.0125/GB  
- **Glacier IR**: Archive files (3-7 days) - ~$0.004/GB

### Lambda Pricing
- **File Fetcher**: ~$0.01 per download (300s max)
- **Cache Manager**: ~$0.001 per operation (30s avg)
- **Credential Manager**: ~$0.0005 per check (10s avg)

### EventBridge Pricing
- **Hourly Schedule**: ~$1/month for 24/7 operation
- **On-Demand Events**: $1 per million invocations

## 🔐 Security Best Practices

1. **Environment Variables**: Never commit NASA tokens to git
2. **IAM Policies**: Minimal required permissions only
3. **S3 Bucket**: Private access with lifecycle policies
4. **Lambda Functions**: VPC isolation if handling sensitive data
5. **CloudWatch Logs**: Monitor for unauthorized access attempts

## 🚦 Status Dashboard

Create a simple status check:

```python
from backend.aws.s3_cache_manager import S3CacheManager
from backend.aws.nasa_credentials import NASACredentialsManager

def check_tempo_system_health():
    cache_manager = S3CacheManager('naq-forecast-tempo-cache')
    cred_manager = NASACredentialsManager()
    
    return {
        'cache_stats': cache_manager.get_cache_stats(),
        'nasa_access': cred_manager.test_nasa_access(),
        'credential_status': cred_manager.get_credential_status()
    }
```

---

## 🎉 Result: Ultra-Fast TEMPO Access

Your data collection system now has **10-30 second** access to TEMPO satellite data instead of **3+ minutes**, enabling real-time air quality forecasting with NASA's latest satellite observations!
