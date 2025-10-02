"""
Notification Services Package
============================
Air quality notification services for Safer Skies NASA Space Apps Challenge 2025

This package provides:
- Email notifications with EPA-compliant health guidance
- Push notifications for web and mobile
- Centralized notification management with rate limiting
- Support for AWS SES/SNS and local SMTP/demo modes

Services:
- EmailNotificationService: EPA-compliant email alerts with rich HTML templates
- PushNotificationService: Web push and mobile notifications via SNS
- NotificationManager: Orchestrates all notification channels with smart delivery
"""

# Import main notification services
try:
    from .email_service import EmailNotificationService  
    from .push_service import PushNotificationService
    from .notification_manager import NotificationManager
except ImportError as e:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"Some notification services not available: {e}")
    
    # Provide dummy classes for testing
    class EmailNotificationService:
        def __init__(self, *args, **kwargs):
            logger.warning("EmailNotificationService not available - using dummy class")
    
    class PushNotificationService:
        def __init__(self, *args, **kwargs):
            logger.warning("PushNotificationService not available - using dummy class")
            
    class NotificationManager:
        def __init__(self, *args, **kwargs):
            logger.warning("NotificationManager not available - using dummy class")

__all__ = [
    'EmailNotificationService',
    'PushNotificationService', 
    'NotificationManager'
]

__version__ = '1.0.0'