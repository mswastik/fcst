"""
Database Service - Now uses Enhanced Database Service with Connection Pooling
This file now imports and exposes the EnhancedDatabaseService for backward compatibility
"""
from .enhanced_db_service import EnhancedDatabaseService, get_enhanced_database_service

# Export the enhanced database service for backward compatibility
DatabaseService = EnhancedDatabaseService
get_database_service = get_enhanced_database_service
