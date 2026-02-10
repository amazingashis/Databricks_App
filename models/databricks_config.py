"""
Enhanced database configuration for Databricks Apps
Supports multiple database backends including Databricks SQL
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from flask_sqlalchemy import SQLAlchemy
from models.database import db, MappingSession, MappingResult, AgentExecutionLog

class DatabaseConfig:
    """
    Database configuration manager that supports multiple backends
    optimized for Databricks Apps environment
    """
    
    def __init__(self):
        self.database_type = self._detect_database_type()
        self.connection_string = self._build_connection_string()
    
    def _detect_database_type(self) -> str:
        """Detect which database configuration is available"""
        
        # Priority order: explicit DATABASE_URL first, then Databricks SQL if configured
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            if 'postgresql' in database_url:
                return 'postgresql'
            elif 'mysql' in database_url:
                return 'mysql'
            elif 'sqlite' in database_url:
                return 'sqlite'
            elif 'databricks+' in database_url:
                return 'databricks_sql'
        
        # If no DATABASE_URL but Databricks SQL is configured
        if os.getenv('DATABRICKS_SQL_WAREHOUSE_ID'):
            return 'databricks_sql'
        
        # Default to SQLite for development
        return 'sqlite'
    
    def _build_connection_string(self) -> str:
        """Build appropriate connection string based on database type"""
        
        if self.database_type == 'databricks_sql':
            return self._build_databricks_sql_connection()
        elif self.database_type in ['postgresql', 'mysql']:
            return os.getenv('DATABASE_URL')
        elif self.database_type == 'sqlite':
            # Only for local development
            return os.getenv('DATABASE_URL', 'sqlite:///mappings.db')
        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")
    
    def _build_databricks_sql_connection(self) -> str:
        """Build Databricks SQL connection string"""
        
        # Required parameters
        databricks_host = os.getenv('DATABRICKS_HOST', 'https://your-workspace.cloud.databricks.com')
        sql_warehouse_id = os.getenv('DATABRICKS_SQL_WAREHOUSE_ID')
        catalog = os.getenv('DATABRICKS_CATALOG', 'main')
        schema = os.getenv('DATABRICKS_SCHEMA', 'mapping_system')
        token = os.getenv('DATABRICKS_TOKEN')
        
        if not sql_warehouse_id:
            raise ValueError("DATABRICKS_SQL_WAREHOUSE_ID is required for Databricks SQL")
        
        if not token:
            raise ValueError("DATABRICKS_TOKEN is required for Databricks SQL")
        
        # Databricks SQL connection string using databricks-sql-connector
        return f"databricks+connector://token:{token}@{databricks_host.replace('https://', '')}?http_path=/sql/1.0/warehouses/{sql_warehouse_id}&catalog={catalog}&schema={schema}"
    
    def configure_flask_app(self, app):
        """Configure Flask app with appropriate database settings"""
        
        if self.database_type == 'databricks_sql':
            # Databricks SQL configuration
            app.config['SQLALCHEMY_DATABASE_URI'] = self.connection_string
            app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
            app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
                'pool_pre_ping': True,
                'pool_recycle': 3600,  # Recycle connections every hour
                'connect_args': {
                    "auth_type": "pat",  # Personal Access Token
                    "warehouse_id": os.getenv('DATABRICKS_SQL_WAREHOUSE_ID')
                }
            }
        else:
            # Standard SQL database configuration
            app.config['SQLALCHEMY_DATABASE_URI'] = self.connection_string
            app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
            app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
                'pool_pre_ping': True,
                'pool_recycle': 3600
            }
    
    def create_tables_if_needed(self, app):
        """Create database tables if they don't exist"""
        
        with app.app_context():
            if self.database_type == 'databricks_sql':
                # For Databricks SQL, we might need to create catalog and schema first
                self._ensure_databricks_catalog_schema()
            
            try:
                db.create_all()
                print(f"✅ Database tables created/verified for {self.database_type}")
            except Exception as e:
                print(f"❌ Failed to create tables: {e}")
                raise
    
    def _ensure_databricks_catalog_schema(self):
        """Ensure catalog and schema exist in Databricks"""
        
        catalog = os.getenv('DATABRICKS_CATALOG', 'main')
        schema = os.getenv('DATABRICKS_SCHEMA', 'mapping_system')
        
        try:
            # Create catalog if it doesn't exist (might require permissions)
            if catalog != 'main':  # Don't try to create main catalog
                db.session.execute(f"CREATE CATALOG IF NOT EXISTS {catalog}")
            
            # Create schema if it doesn't exist
            db.session.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            db.session.commit()
            
        except Exception as e:
            print(f"⚠️ Could not create catalog/schema (might already exist): {e}")
            db.session.rollback()


class DataBricksTableManager:
    """
    Manages tables specifically for Databricks SQL backend
    """
    
    @staticmethod
    def create_optimized_tables():
        """Create tables optimized for Databricks SQL with Delta Lake"""
        
        # Mapping Sessions table with partitioning
        sessions_table_sql = """
        CREATE TABLE IF NOT EXISTS mapping_sessions (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY,
            notebook_path STRING,
            status STRING,
            created_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_message STRING,
            gitlab_project_id STRING,
            gitlab_branch STRING,
            total_mappings INT,
            high_confidence_count INT,
            needs_review_count INT,
            agent_execution_log STRING
        ) USING DELTA
        PARTITIONED BY (DATE(created_at))
        """
        
        # Mapping Results table with clustering
        results_table_sql = """
        CREATE TABLE IF NOT EXISTS mapping_results (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY,
            session_id BIGINT,
            source_table STRING,
            source_column STRING,
            transformation_rule STRING,
            target_field STRING,
            array_field STRING,
            confidence_score DOUBLE,
            needs_review BOOLEAN,
            agent_notes STRING,
            user_notes STRING,
            user_validated BOOLEAN,
            extraction_method STRING,
            extraction_source STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
        CLUSTER BY (session_id, source_table)
        """
        
        # Agent Execution Logs table
        logs_table_sql = """
        CREATE TABLE IF NOT EXISTS agent_execution_logs (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY,
            session_id BIGINT,
            agent_name STRING,
            agent_version STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status STRING,
            error_message STRING,
            input_data_size INT,
            output_data_size INT,
            processing_time_seconds DOUBLE,
            llm_model_used STRING,
            llm_tokens_used INT,
            llm_cost_estimate DOUBLE,
            confidence_scores STRING,
            review_flags_count INT,
            created_at TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (DATE(created_at))
        """
        
        try:
            db.session.execute(sessions_table_sql)
            db.session.execute(results_table_sql)
            db.session.execute(logs_table_sql)
            db.session.commit()
            print("✅ Databricks-optimized tables created successfully")
            
        except Exception as e:
            print(f"❌ Failed to create Databricks tables: {e}")
            db.session.rollback()
            raise


def get_database_config():
    """Factory function to get the appropriate database configuration"""
    return DatabaseConfig()