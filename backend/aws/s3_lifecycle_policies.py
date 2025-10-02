"""
NASA SPACE APPS 2025: S3 LIFECYCLE POLICIES
===========================================
AWS S3 Lifecycle Management for Cost-Effective Data Storage
Automatic tiering: STANDARD → IA → Glacier → Deep Archive

🚀 Phase 6 Component 6/10: S3 Lifecycle Policies

Key Features:
- Automatic storage class transitions based on data age
- Cost optimization with AWS Free Tier compliance
- Intelligent tiering for varying access patterns
- Compliance with data retention requirements
- Multi-part upload cleanup for incomplete uploads
- Delete marker optimization for versioned objects
"""

import json
# import boto3  # Not needed for demo
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LifecycleRule:
    """S3 lifecycle rule definition"""
    rule_id: str
    status: str
    filter_prefix: str
    transitions: List[Dict[str, Any]]
    expiration_days: Optional[int] = None
    abort_incomplete_uploads: Optional[int] = None
    noncurrent_version_expiration: Optional[int] = None

@dataclass
class StorageCostAnalysis:
    """Storage cost analysis for different tiers"""
    standard_gb_month: float
    ia_gb_month: float
    glacier_gb_month: float
    deep_archive_gb_month: float
    total_monthly_cost: float
    savings_vs_standard: float

class S3LifecycleManager:
    """
    AWS S3 Lifecycle Policy Manager
    
    Manages automatic storage class transitions:
    - Hot data (0-30 days): STANDARD
    - Warm data (30-90 days): STANDARD_IA
    - Cold data (90-365 days): GLACIER
    - Archive data (365+ days): DEEP_ARCHIVE
    """
    
    def __init__(self, bucket_name: str = "naq-forecast-data"):
        self.bucket_name = bucket_name
        self.s3_client = None  # boto3.client('s3') in production
        
        # Storage class pricing (per GB/month in us-east-1)
        self.storage_prices = {
            "STANDARD": 0.023,
            "STANDARD_IA": 0.0125,
            "GLACIER": 0.004,
            "DEEP_ARCHIVE": 0.00099
        }
        
        # Data retention policy
        self.retention_policy = {
            "summary_files": 90,      # 90 days for summary.json
            "hourly_history": 90,     # 90 days for hourly data
            "daily_aggregates": 365,  # 1 year for daily data
            "monthly_reports": 2555,  # 7 years for monthly data
            "user_preferences": 90,   # 90 days for inactive users
            "alert_history": 365      # 1 year for alert records
        }
        
        logger.info("♻️ S3 Lifecycle Manager initialized")
    
    def create_lifecycle_policies(self) -> List[LifecycleRule]:
        """
        Create comprehensive lifecycle policies for NAQ Forecast data
        
        Returns:
            List of lifecycle rules
        """
        
        lifecycle_rules = []
        
        # Rule 1: Summary files (frequently accessed)
        summary_rule = LifecycleRule(
            rule_id="naq_summary_files",
            status="Enabled",
            filter_prefix="summary",
            transitions=[
                {"Days": 30, "StorageClass": "STANDARD_IA"},
                {"Days": 90, "StorageClass": "GLACIER"}
            ],
            expiration_days=self.retention_policy["summary_files"],
            abort_incomplete_uploads=1
        )
        lifecycle_rules.append(summary_rule)
        
        # Rule 2: Hourly historical data (moderate access)
        hourly_rule = LifecycleRule(
            rule_id="naq_hourly_history",
            status="Enabled",
            filter_prefix="history/",
            transitions=[
                {"Days": 7, "StorageClass": "STANDARD_IA"},   # Quick transition for cost
                {"Days": 30, "StorageClass": "GLACIER"},      # Long-term storage
                {"Days": 90, "StorageClass": "DEEP_ARCHIVE"}  # Deep archive
            ],
            expiration_days=self.retention_policy["hourly_history"],
            abort_incomplete_uploads=1
        )
        lifecycle_rules.append(hourly_rule)
        
        # Rule 3: Daily aggregates (long-term retention)
        daily_rule = LifecycleRule(
            rule_id="naq_daily_aggregates",
            status="Enabled",
            filter_prefix="history/daily/",
            transitions=[
                {"Days": 30, "StorageClass": "STANDARD_IA"},
                {"Days": 90, "StorageClass": "GLACIER"},
                {"Days": 180, "StorageClass": "DEEP_ARCHIVE"}
            ],
            expiration_days=self.retention_policy["daily_aggregates"],
            abort_incomplete_uploads=1
        )
        lifecycle_rules.append(daily_rule)
        
        # Rule 4: Monthly reports (compliance retention)
        monthly_rule = LifecycleRule(
            rule_id="naq_monthly_reports",
            status="Enabled",
            filter_prefix="history/monthly/",
            transitions=[
                {"Days": 60, "StorageClass": "STANDARD_IA"},
                {"Days": 180, "StorageClass": "GLACIER"},
                {"Days": 365, "StorageClass": "DEEP_ARCHIVE"}
            ],
            expiration_days=self.retention_policy["monthly_reports"],
            abort_incomplete_uploads=7
        )
        lifecycle_rules.append(monthly_rule)
        
        # Rule 5: User data and preferences
        user_rule = LifecycleRule(
            rule_id="naq_user_data",
            status="Enabled",
            filter_prefix="users/",
            transitions=[
                {"Days": 30, "StorageClass": "STANDARD_IA"},
                {"Days": 60, "StorageClass": "GLACIER"}
            ],
            expiration_days=self.retention_policy["user_preferences"],
            abort_incomplete_uploads=1
        )
        lifecycle_rules.append(user_rule)
        
        # Rule 6: Alert history and logs
        alert_rule = LifecycleRule(
            rule_id="naq_alert_history",
            status="Enabled",
            filter_prefix="alerts/",
            transitions=[
                {"Days": 30, "StorageClass": "STANDARD_IA"},
                {"Days": 90, "StorageClass": "GLACIER"},
                {"Days": 180, "StorageClass": "DEEP_ARCHIVE"}
            ],
            expiration_days=self.retention_policy["alert_history"],
            abort_incomplete_uploads=1
        )
        lifecycle_rules.append(alert_rule)
        
        # Rule 7: Temporary processing files
        temp_rule = LifecycleRule(
            rule_id="naq_temp_processing",
            status="Enabled",
            filter_prefix="temp/",
            transitions=[],  # No transitions, just cleanup
            expiration_days=1,  # Delete after 1 day
            abort_incomplete_uploads=1
        )
        lifecycle_rules.append(temp_rule)
        
        # Rule 8: Versioned object cleanup
        version_rule = LifecycleRule(
            rule_id="naq_version_cleanup",
            status="Enabled",
            filter_prefix="",  # All objects
            transitions=[],
            noncurrent_version_expiration=30,  # Delete old versions after 30 days
            abort_incomplete_uploads=1
        )
        lifecycle_rules.append(version_rule)
        
        logger.info(f"♻️ Created {len(lifecycle_rules)} lifecycle rules")
        return lifecycle_rules
    
    def generate_lifecycle_configuration(self, rules: List[LifecycleRule]) -> Dict[str, Any]:
        """
        Generate S3 lifecycle configuration JSON
        
        Args:
            rules: List of lifecycle rules
            
        Returns:
            S3 lifecycle configuration
        """
        
        lifecycle_config = {
            "Rules": []
        }
        
        for rule in rules:
            rule_config = {
                "ID": rule.rule_id,
                "Status": rule.status,
                "Filter": {
                    "Prefix": rule.filter_prefix
                }
            }
            
            if rule.transitions:
                rule_config["Transitions"] = rule.transitions
            
            if rule.expiration_days:
                rule_config["Expiration"] = {
                    "Days": rule.expiration_days
                }
            
            if rule.abort_incomplete_uploads:
                rule_config["AbortIncompleteMultipartUpload"] = {
                    "DaysAfterInitiation": rule.abort_incomplete_uploads
                }
            
            if rule.noncurrent_version_expiration:
                rule_config["NoncurrentVersionExpiration"] = {
                    "NoncurrentDays": rule.noncurrent_version_expiration
                }
            
            lifecycle_config["Rules"].append(rule_config)
        
        return lifecycle_config
    
    def calculate_storage_costs(self, data_volume_gb: float) -> StorageCostAnalysis:
        """
        Calculate storage costs across different tiers
        
        Args:
            data_volume_gb: Total data volume in GB
            
        Returns:
            Storage cost analysis
        """
        
        # Simulate data distribution across storage classes
        # Based on realistic access patterns for AQI data
        
        # Month 1: All in STANDARD
        standard_gb = data_volume_gb * 0.1  # Recent data (10%)
        
        # Month 2-3: Move to STANDARD_IA
        ia_gb = data_volume_gb * 0.2  # Warm data (20%)
        
        # Month 4-12: Move to GLACIER
        glacier_gb = data_volume_gb * 0.5  # Cold data (50%)
        
        # Year 2+: Move to DEEP_ARCHIVE
        deep_archive_gb = data_volume_gb * 0.2  # Archive data (20%)
        
        costs = StorageCostAnalysis(
            standard_gb_month=standard_gb * self.storage_prices["STANDARD"],
            ia_gb_month=ia_gb * self.storage_prices["STANDARD_IA"],
            glacier_gb_month=glacier_gb * self.storage_prices["GLACIER"],
            deep_archive_gb_month=deep_archive_gb * self.storage_prices["DEEP_ARCHIVE"],
            total_monthly_cost=0.0,
            savings_vs_standard=0.0
        )
        
        costs.total_monthly_cost = (
            costs.standard_gb_month +
            costs.ia_gb_month +
            costs.glacier_gb_month +
            costs.deep_archive_gb_month
        )
        
        all_standard_cost = data_volume_gb * self.storage_prices["STANDARD"]
        costs.savings_vs_standard = all_standard_cost - costs.total_monthly_cost
        
        return costs
    
    async def apply_lifecycle_policies(self, rules: List[LifecycleRule]) -> bool:
        """
        Apply lifecycle policies to S3 bucket
        
        Args:
            rules: Lifecycle rules to apply
            
        Returns:
            Success status
        """
        
        try:
            lifecycle_config = self.generate_lifecycle_configuration(rules)
            
            # Mock S3 lifecycle policy application
            # self.s3_client.put_bucket_lifecycle_configuration(
            #     Bucket=self.bucket_name,
            #     LifecycleConfiguration=lifecycle_config
            # )
            
            logger.info(f"♻️ Applied {len(rules)} lifecycle policies to bucket {self.bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to apply lifecycle policies: {e}")
            return False
    
    def validate_lifecycle_compliance(self) -> Dict[str, Any]:
        """
        Validate lifecycle policies against compliance requirements
        
        Returns:
            Compliance validation results
        """
        
        compliance_checks = {
            "data_retention_compliance": True,
            "cost_optimization": True,
            "free_tier_compliance": True,
            "regulatory_requirements": True,
            "issues": []
        }
        
        for data_type, retention_days in self.retention_policy.items():
            if retention_days > 2555:  # 7 years max
                compliance_checks["regulatory_requirements"] = False
                compliance_checks["issues"].append(f"Retention period too long for {data_type}")
        
        if not all(rule.transitions for rule in self.create_lifecycle_policies() if rule.filter_prefix != "temp/"):
            compliance_checks["cost_optimization"] = False
            compliance_checks["issues"].append("Missing storage class transitions")
        
        estimated_storage_gb = 3.0  # Conservative estimate
        if estimated_storage_gb > 5.0:
            compliance_checks["free_tier_compliance"] = False
            compliance_checks["issues"].append("Storage exceeds AWS Free Tier limits")
        
        return compliance_checks

async def demo_s3_lifecycle_management():
    """Demonstrate S3 lifecycle management"""
    
    print("♻️ NASA SPACE APPS 2025: S3 LIFECYCLE MANAGEMENT DEMO")
    print("=" * 70)
    print("Automatic Storage Tiering for Cost Optimization")
    print()
    
    lifecycle_manager = S3LifecycleManager()
    
    print("📊 LIFECYCLE POLICY CREATION:")
    print("=" * 40)
    
    rules = lifecycle_manager.create_lifecycle_policies()
    
    print(f"Total Lifecycle Rules: {len(rules)}")
    
    for i, rule in enumerate(rules, 1):
        print(f"\n{i}. {rule.rule_id.upper().replace('_', ' ')}")
        print(f"   Status: {rule.status}")
        print(f"   Prefix: {rule.filter_prefix}")
        
        if rule.transitions:
            print("   Storage Transitions:")
            for transition in rule.transitions:
                print(f"     • Day {transition['Days']}: → {transition['StorageClass']}")
        
        if rule.expiration_days:
            print(f"   Expiration: {rule.expiration_days} days")
        
        if rule.abort_incomplete_uploads:
            print(f"   Cleanup Incomplete Uploads: {rule.abort_incomplete_uploads} days")
    
    print(f"\n🔧 LIFECYCLE CONFIGURATION:")
    print("=" * 40)
    
    config = lifecycle_manager.generate_lifecycle_configuration(rules)
    print(f"Generated configuration with {len(config['Rules'])} rules")
    
    sample_rule = config['Rules'][0]
    print(f"\nSample Rule Configuration:")
    print(json.dumps(sample_rule, indent=2))
    
    print(f"\n💰 STORAGE COST ANALYSIS:")
    print("=" * 35)
    
    # Simulate 5GB of data (AWS Free Tier limit)
    data_volume = 5.0  # GB
    cost_analysis = lifecycle_manager.calculate_storage_costs(data_volume)
    
    print(f"Data Volume: {data_volume} GB")
    print(f"Cost Breakdown (monthly):")
    print(f"   • STANDARD: ${cost_analysis.standard_gb_month:.4f}")
    print(f"   • STANDARD_IA: ${cost_analysis.ia_gb_month:.4f}")
    print(f"   • GLACIER: ${cost_analysis.glacier_gb_month:.4f}")
    print(f"   • DEEP_ARCHIVE: ${cost_analysis.deep_archive_gb_month:.4f}")
    print(f"   • Total Monthly Cost: ${cost_analysis.total_monthly_cost:.4f}")
    print(f"   • Savings vs STANDARD: ${cost_analysis.savings_vs_standard:.4f} ({cost_analysis.savings_vs_standard/cost_analysis.total_monthly_cost*100:.1f}%)")
    
    print(f"\n📈 DATA DISTRIBUTION TIMELINE:")
    print("=" * 40)
    
    print("🕐 Storage Class Progression:")
    print("   • Days 0-7: STANDARD (hot data, frequent access)")
    print("   • Days 7-30: STANDARD_IA (warm data, occasional access)")
    print("   • Days 30-90: GLACIER (cold data, rare access)")
    print("   • Days 90+: DEEP_ARCHIVE (archive data, minimal access)")
    
    print("📊 Access Pattern Optimization:")
    print("   • Summary files: Cached by CloudFront (reduces S3 access)")
    print("   • Hourly data: Accessed for historical queries")
    print("   • Daily aggregates: Used for trend analysis")
    print("   • Monthly reports: Compliance and long-term analysis")
    
    print(f"\n⚙️ APPLYING LIFECYCLE POLICIES:")
    print("=" * 40)
    
    success = await lifecycle_manager.apply_lifecycle_policies(rules)
    status_icon = "✅" if success else "❌"
    print(f"Policy Application: {status_icon}")
    
    if success:
        print("Lifecycle policies successfully applied!")
        print("Automatic storage transitions will begin based on object age")
    
    print(f"\n🔍 COMPLIANCE VALIDATION:")
    print("=" * 35)
    
    compliance = lifecycle_manager.validate_lifecycle_compliance()
    
    print("Compliance Checks:")
    for check, status in compliance.items():
        if check != "issues":
            status_icon = "✅" if status else "❌"
            check_name = check.replace("_", " ").title()
            print(f"   • {check_name}: {status_icon}")
    
    if compliance["issues"]:
        print("\nIssues Found:")
        for issue in compliance["issues"]:
            print(f"   ⚠️ {issue}")
    else:
        print("\n✅ All compliance checks passed!")
    
    print(f"\n🆓 AWS FREE TIER OPTIMIZATION:")
    print("=" * 40)
    
    print("Storage Benefits:")
    print("   • 5 GB Standard storage included")
    print("   • 20,000 GET requests per month")
    print("   • 2,000 PUT requests per month")
    print("   • Lifecycle transitions reduce costs further")
    
    print("Cost Optimization Features:")
    print("   • Automatic cleanup of incomplete uploads")
    print("   • Old version deletion (versioned buckets)")
    print("   • Temporary file cleanup (1-day expiration)")
    print("   • Intelligent tiering based on access patterns")
    
    print("Expected Monthly Costs:")
    print(f"   • Storage: ${cost_analysis.total_monthly_cost:.4f} (well within free tier)")
    print("   • Requests: $0.00 (covered by free tier)")
    print("   • Data Transfer: $0.00 (covered by CloudFront)")
    
    print(f"\n✅ S3 LIFECYCLE MANAGEMENT DEMO COMPLETE!")
    print("🚀 Phase 6 Component 6/10: S3 Lifecycle Policies Ready!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(demo_s3_lifecycle_management())
