"""
EPA-Compliant Air Quality Alert Engine
NASA Space Apps Challenge - Backend Implementation
"""

import boto3
import pandas as pd
from datetime import datetime, timedelta
import uuid
from typing import List, Dict, Optional
import logging
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirQualityAlertEngine:
    """
    EPA-compliant air quality alert system
    Integrates with existing forecast system
    """
    
    def __init__(self, local_mode=True):
        """
        Initialize alert engine
        local_mode: If True, use local storage instead of DynamoDB
        """
        self.local_mode = local_mode
        
        if not local_mode:
            self.dynamodb = boto3.resource('dynamodb')
            self.users_table = self.dynamodb.Table('AQUsers')
            self.alerts_table = self.dynamodb.Table('AQForecastAlerts')
            self.notifications_table = self.dynamodb.Table('AQNotificationHistory')
        else:
            # Local storage for development
            self.local_data_dir = 'data/alerts'
            os.makedirs(self.local_data_dir, exist_ok=True)
        
        # EPA thresholds by sensitive group (from official EPA document)
        self.epa_thresholds = {
            'general_public': {
                'warning': 101,    # Unhealthy for Sensitive Groups
                'emergency': 151   # Unhealthy
            },
            'sensitive_groups': {
                'warning': 51,     # Moderate
                'emergency': 101   # Unhealthy for Sensitive Groups
            }
        }
        
        # EPA sensitive group mappings (from Table 4 in EPA document)
        self.pollutant_sensitive_groups = {
            'O3': ['lung_disease', 'children', 'active_outdoors', 'elderly'],
            'PM25': ['heart_disease', 'lung_disease', 'elderly', 'children', 'outdoor_workers', 'minority_groups', 'low_income'],
            'PM10': ['heart_disease', 'lung_disease', 'elderly', 'children', 'outdoor_workers', 'minority_groups', 'low_income'],
            'CO': ['heart_disease'],
            'NO2': ['lung_disease', 'children', 'elderly'],
            'SO2': ['lung_disease', 'children', 'elderly']
        }
        
        # EPA health condition mappings
        self.condition_mapping = {
            'asthma': 'lung_disease',
            'copd': 'lung_disease',
            'respiratory': 'lung_disease',
            'lung_disease': 'lung_disease',
            'heart_disease': 'heart_disease',
            'cardiovascular': 'heart_disease',
            'children': 'children',
            'elderly': 'elderly',
            'outdoor_worker': 'outdoor_workers',
            'active_outdoors': 'active_outdoors',
            'pregnant': 'pregnant',
            'minority_groups': 'minority_groups',
            'low_income': 'low_income'
        }
    
    def check_user_sensitivity(self, user_conditions: List[str], pollutant: str) -> bool:
        """
        Check if user is sensitive to specific pollutant per EPA Table 4 guidelines
        """
        sensitive_groups = self.pollutant_sensitive_groups.get(pollutant, [])
        
        user_epa_groups = [self.condition_mapping.get(condition, condition) 
                          for condition in user_conditions]
        
        is_sensitive = any(group in sensitive_groups for group in user_epa_groups)
        
        logger.debug(f"User conditions: {user_conditions}")
        logger.debug(f"Pollutant {pollutant} sensitive groups: {sensitive_groups}")
        logger.debug(f"User EPA groups: {user_epa_groups}")
        logger.debug(f"Is sensitive: {is_sensitive}")
        
        return is_sensitive
    
    def get_epa_message(self, pollutant: str, aqi_value: int, 
                       is_sensitive: bool, user_conditions: List[str]) -> str:
        """
        Generate EPA-compliant health message
        Based on official EPA Table 5 cautionary statements
        """
        if aqi_value <= 50:
            return "It's a great day to be active outside."
        
        elif aqi_value <= 100:
            if is_sensitive:
                if pollutant == 'O3':
                    return "Unusually sensitive people: Consider making outdoor activities shorter and less intense. Watch for symptoms such as coughing or shortness of breath. These are signs to take it easier."
                elif pollutant in ['PM25', 'PM10']:
                    return "Unusually sensitive people: Consider making outdoor activities shorter and less intense. Go inside if you have symptoms such as coughing or shortness of breath."
                elif pollutant == 'NO2':
                    return "Unusually sensitive people: Consider limiting prolonged exertion especially near busy roads."
                else:
                    return "Unusually sensitive people should consider reducing outdoor activities."
            else:
                return "Air quality is acceptable for most people."
        
        elif aqi_value <= 150:
            base_message = "Sensitive groups: Make outdoor activities shorter and less intense. Take more breaks. Watch for symptoms such as coughing or shortness of breath."
            
            if any(cond in user_conditions for cond in ['asthma', 'lung_disease', 'respiratory']):
                base_message += " People with asthma: Follow your asthma action plan and keep quick-relief medicine handy."
            
            if any(cond in user_conditions for cond in ['heart_disease', 'cardiovascular']) and pollutant in ['PM25', 'PM10']:
                base_message += " People with heart disease: Symptoms such as palpitations, shortness of breath, or unusual fatigue may indicate a serious problem. If you have any of these, contact your health care provider."
            
            if pollutant == 'O3':
                base_message += " Plan outdoor activities in the morning when ozone is lower."
            
            return base_message
        
        elif aqi_value <= 200:
            return "Sensitive groups: Do not do long or intense outdoor activities. Schedule outdoor activities in the morning when ozone is lower. Consider moving activities indoors. Everyone else should reduce prolonged or heavy outdoor exertion."
        
        elif aqi_value <= 300:
            return "Very Unhealthy: Health warnings of emergency conditions. Everyone should avoid prolonged or heavy outdoor exertion. Sensitive groups should avoid all outdoor activities."
        
        else:
            return "Hazardous: Health warnings of emergency conditions. Everyone should avoid all outdoor activities."
    
    def get_alert_level(self, aqi_value: int, is_sensitive: bool) -> str:
        """
        Determine alert level based on EPA AQI categories
        """
        if aqi_value <= 50:
            return "good"
        elif aqi_value <= 100:
            return "moderate" if is_sensitive else "good"
        elif aqi_value <= 150:
            return "unhealthy_sensitive"
        elif aqi_value <= 200:
            return "unhealthy"
        elif aqi_value <= 300:
            return "very_unhealthy"
        else:
            return "hazardous"
    
    def create_sample_users(self) -> List[Dict]:
        """
        Create sample users for testing
        """
        sample_users = [
            {
                'user_id': str(uuid.uuid4()),
                'name': 'John Doe',
                'email': 'john.doe@email.com',
                'latitude': 40.7128,
                'longitude': -74.0060,
                'age': 35,
                'health_conditions': ['asthma', 'outdoor_worker'],
                'notification_preferences': {
                    'email': True,
                    'push': True,
                    'sms': False
                },
                'created_at': datetime.utcnow().isoformat(),
                'is_active': True
            },
            {
                'user_id': str(uuid.uuid4()),
                'name': 'Maria Garcia',
                'email': 'maria.garcia@email.com',
                'latitude': 34.0522,
                'longitude': -118.2437,
                'age': 72,
                'health_conditions': ['elderly', 'heart_disease'],
                'notification_preferences': {
                    'email': True,
                    'push': False,
                    'sms': True
                },
                'created_at': datetime.utcnow().isoformat(),
                'is_active': True
            },
            {
                'user_id': str(uuid.uuid4()),
                'name': 'Sarah Johnson',
                'email': 'sarah.johnson@email.com',
                'latitude': 41.8781,
                'longitude': -87.6298,
                'age': 28,
                'health_conditions': ['pregnant'],
                'notification_preferences': {
                    'email': True,
                    'push': True,
                    'sms': False
                },
                'created_at': datetime.utcnow().isoformat(),
                'is_active': True
            },
            {
                'user_id': str(uuid.uuid4()),
                'name': 'Tommy Chen',
                'email': 'tommy.chen@email.com',
                'latitude': 40.7128,
                'longitude': -74.0060,
                'age': 12,
                'health_conditions': ['children'],
                'notification_preferences': {
                    'email': False,
                    'push': True,
                    'sms': False
                },
                'created_at': datetime.utcnow().isoformat(),
                'is_active': True
            }
        ]
        
        if self.local_mode:
            users_file = os.path.join(self.local_data_dir, 'users.json')
            with open(users_file, 'w') as f:
                json.dump(sample_users, f, indent=2)
        
        return sample_users
    
    def get_all_active_users(self) -> List[Dict]:
        """
        Get all active users
        """
        if self.local_mode:
            users_file = os.path.join(self.local_data_dir, 'users.json')
            if os.path.exists(users_file):
                with open(users_file, 'r') as f:
                    users = json.load(f)
                return [user for user in users if user.get('is_active', True)]
            else:
                return self.create_sample_users()
        else:
            response = self.users_table.scan(
                FilterExpression='is_active = :active',
                ExpressionAttributeValues={':active': True}
            )
            return response.get('Items', [])
    
    def get_user_location_forecast(self, user: Dict, 
                                  forecast_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get forecast data for user's location
        For demo: find nearest city in forecast data
        """
        user_lat, user_lon = user['latitude'], user['longitude']
        
        locations = forecast_df[['location_name', 'latitude', 'longitude']].drop_duplicates()
        
        if locations.empty:
            logger.warning("No location data in forecast")
            return pd.DataFrame()
        
        locations['distance'] = (
            (locations['latitude'] - user_lat) ** 2 + 
            (locations['longitude'] - user_lon) ** 2
        ) ** 0.5
        
        nearest_location = locations.loc[locations['distance'].idxmin(), 'location_name']
        
        logger.info(f"User at ({user_lat:.3f}, {user_lon:.3f}) mapped to {nearest_location}")
        
        return forecast_df[forecast_df['location_name'] == nearest_location].copy()
    
    def check_user_alerts(self, user: Dict, forecast_df: pd.DataFrame) -> List[Dict]:
        """
        Check alerts for specific user based on their location and conditions
        """
        alerts = []
        
        user_location_data = self.get_user_location_forecast(user, forecast_df)
        
        if user_location_data.empty:
            logger.warning(f"No forecast data for user {user['name']}")
            return alerts
        
        logger.info(f"Checking alerts for {user['name']} with {len(user_location_data)} forecast rows")
        
        for _, row in user_location_data.iterrows():
            for pollutant in ['PM25', 'O3', 'NO2', 'SO2', 'CO']:
                aqi_col = f'{pollutant}_aqi'
                conc_col = f'{pollutant}_concentration'
                
                if aqi_col not in row or pd.isna(row[aqi_col]):
                    continue
                
                aqi_value = float(row[aqi_col])
                
                is_sensitive = self.check_user_sensitivity(
                    user['health_conditions'], pollutant
                )
                
                # Determine alert threshold
                threshold = (self.epa_thresholds['sensitive_groups']['warning'] 
                           if is_sensitive 
                           else self.epa_thresholds['general_public']['warning'])
                
                if aqi_value >= threshold:
                    forecast_time = row.get('forecast_time', row.get('datetime', datetime.utcnow().isoformat()))
                    
                    alert = {
                        'alert_id': str(uuid.uuid4()),
                        'user_id': user['user_id'],
                        'user_name': user['name'],
                        'timestamp': forecast_time,
                        'location': {
                            'latitude': user['latitude'],
                            'longitude': user['longitude'],
                            'city': row.get('location_name', 'Unknown')
                        },
                        'pollutant': pollutant,
                        'aqi_value': int(aqi_value),
                        'concentration': float(row.get(conc_col, 0)),
                        'alert_level': self.get_alert_level(aqi_value, is_sensitive),
                        'epa_message': self.get_epa_message(
                            pollutant, aqi_value, is_sensitive,
                            user['health_conditions']
                        ),
                        'forecast_hour': row.get('forecast_hour', 0),
                        'is_sensitive_user': is_sensitive,
                        'user_conditions': user['health_conditions'],
                        'expires_at': (
                            pd.to_datetime(forecast_time) +
                            timedelta(hours=6)
                        ).isoformat(),
                        'created_at': datetime.utcnow().isoformat()
                    }
                    
                    alerts.append(alert)
                    
                    logger.info(f"Alert generated: {user['name']} - {pollutant} AQI {aqi_value} ({alert['alert_level']})")
        
        return alerts
    
    def save_alerts(self, alerts: List[Dict]):
        """
        Save alerts to storage
        """
        if self.local_mode:
            alerts_file = os.path.join(self.local_data_dir, f'alerts_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
            with open(alerts_file, 'w') as f:
                json.dump(alerts, f, indent=2)
            logger.info(f"Saved {len(alerts)} alerts to {alerts_file}")
        else:
            for alert in alerts:
                self.alerts_table.put_item(Item=alert)
    
    def send_mock_notifications(self, alerts: List[Dict]):
        """
        Send mock notifications (for demo purposes)
        """
        notifications_sent = []
        
        for alert in alerts:
            user_id = alert['user_id']
            
            # Mock email notification
            email_notification = {
                'notification_id': str(uuid.uuid4()),
                'user_id': user_id,
                'alert_id': alert['alert_id'],
                'channel': 'email',
                'status': 'sent',
                'sent_at': datetime.utcnow().isoformat(),
                'subject': f"Air Quality Alert - {alert['alert_level'].replace('_', ' ').title()}",
                'message_preview': alert['epa_message'][:100] + "..."
            }
            notifications_sent.append(email_notification)
            
            # Mock push notification
            push_notification = {
                'notification_id': str(uuid.uuid4()),
                'user_id': user_id,
                'alert_id': alert['alert_id'],
                'channel': 'push',
                'status': 'sent',
                'sent_at': datetime.utcnow().isoformat(),
                'title': f"{alert['pollutant']} Alert",
                'body': f"AQI {alert['aqi_value']} in {alert['location']['city']}"
            }
            notifications_sent.append(push_notification)
        
        if self.local_mode:
            notifications_file = os.path.join(self.local_data_dir, f'notifications_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
            with open(notifications_file, 'w') as f:
                json.dump(notifications_sent, f, indent=2)
            logger.info(f"Sent {len(notifications_sent)} notifications")
        
        return notifications_sent
    
    def process_forecast_alerts(self, forecast_file_path: str) -> Dict:
        """
        Main function: Process forecast data and generate alerts for all users
        """
        logger.info(f"Processing forecast alerts from {forecast_file_path}")
        
        try:
            df = pd.read_parquet(forecast_file_path)
            logger.info(f"Loaded forecast data: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")
            
            users = self.get_all_active_users()
            logger.info(f"Found {len(users)} active users")
            
            all_alerts = []
            
            for user in users:
                user_alerts = self.check_user_alerts(user, df)
                all_alerts.extend(user_alerts)
                
                if user_alerts:
                    logger.info(f"Generated {len(user_alerts)} alerts for {user['name']}")
            
            if all_alerts:
                self.save_alerts(all_alerts)
                
                notifications = self.send_mock_notifications(all_alerts)
                
                logger.info(f"Total alerts generated: {len(all_alerts)}")
                logger.info(f"Notifications sent: {len(notifications)}")
            else:
                logger.info("No alerts generated - air quality is good!")
            
            return {
                'success': True,
                'alerts_count': len(all_alerts),
                'users_processed': len(users),
                'forecast_rows': len(df)
            }
            
        except Exception as e:
            logger.error(f"Alert processing failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_alerts_summary(self) -> Dict:
        """
        Get summary of recent alerts
        """
        if self.local_mode:
            alert_files = [f for f in os.listdir(self.local_data_dir) if f.startswith('alerts_')]
            if not alert_files:
                return {'total_alerts': 0, 'recent_files': []}
            
            latest_file = sorted(alert_files)[-1]
            alerts_file = os.path.join(self.local_data_dir, latest_file)
            
            with open(alerts_file, 'r') as f:
                alerts = json.load(f)
            
            # Summarize alerts
            summary = {
                'total_alerts': len(alerts),
                'alerts_by_level': {},
                'alerts_by_pollutant': {},
                'users_alerted': set(),
                'latest_file': latest_file
            }
            
            for alert in alerts:
                level = alert['alert_level']
                pollutant = alert['pollutant']
                
                summary['alerts_by_level'][level] = summary['alerts_by_level'].get(level, 0) + 1
                summary['alerts_by_pollutant'][pollutant] = summary['alerts_by_pollutant'].get(pollutant, 0) + 1
                summary['users_alerted'].add(alert['user_name'])
            
            summary['users_alerted'] = list(summary['users_alerted'])
            
            return summary
        
        return {'message': 'DynamoDB summary not implemented yet'}


# Alert engine - import and use in other modules
