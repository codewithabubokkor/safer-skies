#!/usr/bin/env python3
"""
Simple Database ConnectioNASA Space Apps Challenge 2025 - Team AURA
"""

import os
import mysql.connector
from dotenv import load_dotenv
import logging
from pathlib import Path

# Load environment variables from backend/.env specifically
backend_env_path = Path(__file__).parent.parent / '.env'
load_dotenv(backend_env_path)

logger = logging.getLogger(__name__)

class SimpleDatabase:
    """Simple database connection - fast and direct"""
    
    def __init__(self):
        self.connection_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'safer_skies')
        }
    
    def get_connection(self):
        """Get a simple direct database connection"""
        try:
            return mysql.connector.connect(**self.connection_config)
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return None
    
# Global database instance
_db = None

def get_db():
    """Get the simple database instance"""
    global _db
    if _db is None:
        _db = SimpleDatabase()
    return _db

def get_db_connection():
    """Get a database connection (convenience function)"""
    return get_db().get_connection()

if __name__ == "__main__":
    # Test the database connection
    print("🔄 Testing simple database connection...")
    
    db = get_db()
    conn = db.get_connection()
    
    if conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT VERSION() as version, DATABASE() as db_name")
        result = cursor.fetchone()
        
        print("✅ Database connection successful!")
        print(f"📊 MySQL Version: {result['version']}")
        print(f"� Database: {result['db_name']}")
        
        cursor.close()
        conn.close()
    else:
        print("❌ Database connection failed")