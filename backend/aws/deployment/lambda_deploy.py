"""
AWS Lambda Deployment Script for TEMPO File Fetcher
Automates deployment of TEMPO downloading system to AWS Lambda
Part of Safer Skies AWS Infrastructure
"""

import os
import json
import boto3
import zipfile
import tempfile
import shutil
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LambdaDeployer:
    """Deploys TEMPO file fetcher to AWS Lambda"""
    
    def __init__(self, region: str = 'us-east-1'):
        """
        Initialize Lambda deployer
        
        Args:
            region: AWS region for deployment
        """
        self.region = region
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.iam_client = boto3.client('iam', region_name=region)
        self.events_client = boto3.client('events', region_name=region)
        
        # Deployment configuration
        self.function_name = 'naq-tempo-file-fetcher'
        self.cache_manager_function = 'naq-tempo-cache-manager'
        self.credential_manager_function = 'naq-tempo-credential-manager'
        
        self.runtime = 'python3.11'
        self.timeout = 300  # 5 minutes
        self.memory_size = 512  # MB
    
    def create_iam_role(self, role_name: str) -> str:
        """
        Create IAM role for Lambda functions
        
        Args:
            role_name: Name of IAM role to create
            
        Returns:
            ARN of created role
        """
        try:
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            response = self.iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description='IAM role for NAQ TEMPO file fetcher Lambda functions'
            )
            
            role_arn = response['Role']['Arn']
            logger.info(f"✅ Created IAM role: {role_arn}")
            
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            )
            
            s3_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket",
                            "s3:HeadObject",
                            "s3:PutBucketLifecycleConfiguration",
                            "s3:PutBucketVersioning",
                            "s3:PutBucketMetricsConfiguration"
                        ],
                        "Resource": [
                            "arn:aws:s3:::naq-forecast-tempo-cache",
                            "arn:aws:s3:::naq-forecast-tempo-cache/*",
                            "arn:aws:s3:::tempo-tempo",
                            "arn:aws:s3:::tempo-tempo/*"
                        ]
                    }
                ]
            }
            
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName='TempoS3Access',
                PolicyDocument=json.dumps(s3_policy)
            )
            
            logger.info(f"✅ Attached S3 access policy to role: {role_name}")
            return role_arn
            
        except self.iam_client.exceptions.EntityAlreadyExistsException:
            # Role already exists, get its ARN
            response = self.iam_client.get_role(RoleName=role_name)
            role_arn = response['Role']['Arn']
            logger.info(f"✅ Using existing IAM role: {role_arn}")
            return role_arn
            
        except Exception as e:
            logger.error(f"❌ Failed to create IAM role: {e}")
            raise
    
    def create_deployment_package(self, source_file: str, dependencies: list = None) -> str:
        """
        Create Lambda deployment package
        
        Args:
            source_file: Path to main Python file
            dependencies: List of additional files to include
            
        Returns:
            Path to created ZIP file
        """
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Copy main source file
                source_path = Path(source_file)
                shutil.copy2(source_path, temp_path / 'lambda_function.py')
                
                # Copy dependencies if provided
                if dependencies:
                    for dep in dependencies:
                        dep_path = Path(dep)
                        if dep_path.exists():
                            shutil.copy2(dep_path, temp_path / dep_path.name)
                
                requirements = [
                    'boto3>=1.34.0',
                    'requests>=2.31.0',
                    'netcdf4>=1.6.0',
                    'numpy>=1.24.0'
                ]
                
                with open(temp_path / 'requirements.txt', 'w') as f:
                    f.write('\n'.join(requirements))
                
                zip_path = temp_path / 'deployment_package.zip'
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for file_path in temp_path.rglob('*'):
                        if file_path.is_file() and file_path.suffix != '.zip':
                            zip_file.write(file_path, file_path.relative_to(temp_path))
                
                # Copy to permanent location
                final_zip_path = Path.cwd() / f'{self.function_name}-deployment.zip'
                shutil.copy2(zip_path, final_zip_path)
                
                logger.info(f"✅ Created deployment package: {final_zip_path}")
                return str(final_zip_path)
                
        except Exception as e:
            logger.error(f"❌ Failed to create deployment package: {e}")
            raise
    
    def deploy_lambda_function(self, function_name: str, source_file: str, 
                             role_arn: str, environment_vars: dict = None) -> str:
        """
        Deploy Lambda function
        
        Args:
            function_name: Name of Lambda function
            source_file: Path to source Python file
            role_arn: IAM role ARN
            environment_vars: Environment variables
            
        Returns:
            ARN of deployed function
        """
        try:
            zip_path = self.create_deployment_package(source_file)
            
            with open(zip_path, 'rb') as zip_file:
                zip_content = zip_file.read()
            
            env_vars = environment_vars or {}
            env_vars.update({
                'TEMPO_CACHE_BUCKET': 'naq-forecast-tempo-cache'
            })
            
            try:
                response = self.lambda_client.update_function_code(
                    FunctionName=function_name,
                    ZipFile=zip_content
                )
                
                self.lambda_client.update_function_configuration(
                    FunctionName=function_name,
                    Runtime=self.runtime,
                    Role=role_arn,
                    Handler='lambda_function.lambda_handler',
                    Timeout=self.timeout,
                    MemorySize=self.memory_size,
                    Environment={'Variables': env_vars}
                )
                
                logger.info(f"✅ Updated existing Lambda function: {function_name}")
                
            except self.lambda_client.exceptions.ResourceNotFoundException:
                response = self.lambda_client.create_function(
                    FunctionName=function_name,
                    Runtime=self.runtime,
                    Role=role_arn,
                    Handler='lambda_function.lambda_handler',
                    Code={'ZipFile': zip_content},
                    Timeout=self.timeout,
                    MemorySize=self.memory_size,
                    Environment={'Variables': env_vars},
                    Description=f'Safer Skies TEMPO {function_name.split("-")[-1]} system'
                )
                
                logger.info(f"✅ Created new Lambda function: {function_name}")
            
            os.remove(zip_path)
            
            function_arn = response['FunctionArn']
            return function_arn
            
        except Exception as e:
            logger.error(f"❌ Failed to deploy Lambda function {function_name}: {e}")
            raise
    
    def setup_eventbridge_schedule(self, function_arn: str) -> str:
        """
        Set up EventBridge schedule for hourly TEMPO file downloads
        
        Args:
            function_arn: ARN of Lambda function to trigger
            
        Returns:
            ARN of created EventBridge rule
        """
        try:
            rule_name = 'naq-tempo-hourly-download'
            
            response = self.events_client.put_rule(
                Name=rule_name,
                ScheduleExpression='rate(1 hour)',
                Description='Hourly TEMPO file download for Safer Skies',
                State='ENABLED'
            )
            
            rule_arn = response['RuleArn']
            logger.info(f"✅ Created EventBridge rule: {rule_arn}")
            
            self.events_client.put_targets(
                Rule=rule_name,
                Targets=[
                    {
                        'Id': '1',
                        'Arn': function_arn,
                        'Input': json.dumps({
                            'gas': 'NO2',
                            'force_refresh': False
                        })
                    }
                ]
            )
            
            try:
                self.lambda_client.add_permission(
                    FunctionName=self.function_name,
                    StatementId='eventbridge-invoke',
                    Action='lambda:InvokeFunction',
                    Principal='events.amazonaws.com',
                    SourceArn=rule_arn
                )
            except self.lambda_client.exceptions.ResourceConflictException:
                pass
            
            logger.info(f"✅ Configured EventBridge schedule for {function_arn}")
            return rule_arn
            
        except Exception as e:
            logger.error(f"❌ Failed to setup EventBridge schedule: {e}")
            raise
    
    def deploy_complete_system(self, nasa_bearer_token: str) -> dict:
        """
        Deploy complete TEMPO file system
        
        Args:
            nasa_bearer_token: NASA Earthdata bearer token
            
        Returns:
            Deployment results dictionary
        """
        try:
            logger.info("🚀 Starting complete TEMPO system deployment...")
            
            role_name = 'naq-tempo-lambda-role'
            role_arn = self.create_iam_role(role_name)
            
            import time
            time.sleep(10)
            
            # Environment variables
            env_vars = {
                'NASA_BEARER_TOKEN': nasa_bearer_token
            }
            
            # Deploy main file fetcher
            current_dir = Path(__file__).parent
            fetcher_source = current_dir / 'tempo_file_fetcher.py'
            
            fetcher_arn = self.deploy_lambda_function(
                self.function_name,
                str(fetcher_source),
                role_arn,
                env_vars
            )
            
            # Deploy cache manager
            cache_source = current_dir / 's3_cache_manager.py'
            
            cache_arn = self.deploy_lambda_function(
                self.cache_manager_function,
                str(cache_source),
                role_arn,
                env_vars
            )
            
            # Deploy credential manager
            cred_source = current_dir / 'nasa_credentials.py'
            
            cred_arn = self.deploy_lambda_function(
                self.credential_manager_function,
                str(cred_source),
                role_arn,
                env_vars
            )
            
            schedule_arn = self.setup_eventbridge_schedule(fetcher_arn)
            
            results = {
                'status': 'success',
                'iam_role_arn': role_arn,
                'file_fetcher_arn': fetcher_arn,
                'cache_manager_arn': cache_arn,
                'credential_manager_arn': cred_arn,
                'eventbridge_rule_arn': schedule_arn,
                'region': self.region
            }
            
            logger.info("✅ Complete TEMPO system deployment successful!")
            return results
            
        except Exception as e:
            logger.error(f"❌ Complete system deployment failed: {e}")
            raise

def main():
    """Main deployment script"""
    
    nasa_token = os.getenv('NASA_BEARER_TOKEN')
    if not nasa_token:
        logger.error("❌ NASA_BEARER_TOKEN environment variable not set")
        logger.info("Get your token from: https://urs.earthdata.nasa.gov/")
        return
    
    # Deploy system
    deployer = LambdaDeployer(region='us-east-1')
    
    try:
        results = deployer.deploy_complete_system(nasa_token)
        
        print("\n" + "="*60)
        print("🎉 TEMPO File System Deployment Complete!")
        print("="*60)
        print(f"File Fetcher: {results['file_fetcher_arn']}")
        print(f"Cache Manager: {results['cache_manager_arn']}")
        print(f"Credential Manager: {results['credential_manager_arn']}")
        print(f"EventBridge Schedule: {results['eventbridge_rule_arn']}")
        print(f"Region: {results['region']}")
        print("\n📋 Next Steps:")
        print("1. Test the deployment with manual invocation")
        print("2. Monitor CloudWatch logs for any issues")
        print("3. Verify S3 bucket permissions")
        print("4. Check EventBridge rule execution")
        
    except Exception as e:
        logger.error(f"❌ Deployment failed: {e}")

if __name__ == "__main__":
    main()
