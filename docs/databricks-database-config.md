# Database Configuration for Databricks Apps

## Overview

The agentic mapping system supports multiple database backends, with special optimizations for Databricks Apps environment.

## ⚠️ **Important for Databricks Apps**

**SQLite is NOT suitable for Databricks Apps** because:
- File system is ephemeral (resets on cluster restart)
- Data would be lost when the app restarts
- No persistence across sessions

## 🎯 **Recommended Database Options**

### 1. **Databricks SQL (Recommended)**

Best choice for Databricks Apps as it's native and integrated.

**Configuration:**
```env
DATABRICKS_SQL_WAREHOUSE_ID=your-sql-warehouse-id
DATABRICKS_CATALOG=your-catalog-name  
DATABRICKS_SCHEMA=mapping_system
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-databricks-token
```

**Benefits:**
- ✅ Native Databricks integration
- ✅ Automatic scaling
- ✅ Delta Lake optimizations
- ✅ Unity Catalog support
- ✅ High performance for analytics workloads

**Setup Steps:**
1. Create a SQL warehouse in your Databricks workspace
2. Create a catalog and schema for the app
3. Configure the environment variables
4. The app will automatically create optimized Delta tables

### 2. **External Cloud Databases**

For enterprise environments requiring external databases.

**PostgreSQL (Azure Database, AWS RDS):**
```env
DATABASE_URL=postgresql://username:password@hostname:5432/database_name
```

**MySQL (Azure Database, AWS RDS):**
```env
DATABASE_URL=mysql://username:password@hostname:3306/database_name
```

**Benefits:**
- ✅ Fully managed service
- ✅ Automated backups
- ✅ High availability
- ✅ Enterprise security

### 3. **SQLite (Development Only)**

Only use for local development, never in Databricks Apps.

```env
DATABASE_URL=sqlite:///mappings.db
```

## 📊 **Database Schema**

The system creates these tables:

### `mapping_sessions`
- Tracks notebook analysis sessions
- Stores metadata about each run
- Partitioned by date (Databricks SQL)

### `mapping_results` 
- Individual mapping transformations
- Source → Target field mappings
- Confidence scores and review flags
- Clustered by session_id and source_table (Databricks SQL)

### `agent_execution_logs`
- Detailed agent performance metrics
- LLM usage tracking
- Processing time and costs
- Partitioned by date (Databricks SQL)

## 🔧 **Databricks App Deployment**

### Step 1: Create SQL Warehouse
```sql
-- In Databricks SQL, create a warehouse for the app
CREATE WAREHOUSE IF NOT EXISTS mapping_system_warehouse
WITH AUTO_STOP = 10;
```

### Step 2: Setup Catalog and Schema
```sql
-- Create dedicated catalog (optional)
CREATE CATALOG IF NOT EXISTS mapping_apps;

-- Create schema for the application
CREATE SCHEMA IF NOT EXISTS mapping_apps.mapping_system;
```

### Step 3: Configure Environment
```python
# In your Databricks App configuration
import os

os.environ.update({
    'DATABRICKS_SQL_WAREHOUSE_ID': 'your-warehouse-id',
    'DATABRICKS_CATALOG': 'mapping_apps',
    'DATABRICKS_SCHEMA': 'mapping_system',
    'DATABRICKS_HOST': 'https://your-workspace.cloud.databricks.com',
    'DATABRICKS_TOKEN': dbutils.secrets.get('scope', 'databricks-token')
})
```

### Step 4: Deploy App
```python
# app.py will automatically detect Databricks SQL configuration
from app import app
app.run(host='0.0.0.0', port=8080)
```

## 🚀 **Performance Optimizations**

### Databricks SQL Optimizations
- **Delta Lake Tables**: Automatic versioning and time travel
- **Clustering**: Query performance optimization
- **Partitioning**: Efficient data pruning
- **Auto-scaling**: Warehouse scales based on demand

### Connection Pooling
```python
# Automatic connection pool management
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_pre_ping': True,
    'pool_recycle': 3600,  # 1 hour
}
```

## 🔍 **Monitoring and Maintenance**

### Query Performance
```sql
-- Monitor table size and performance
DESCRIBE DETAIL mapping_apps.mapping_system.mapping_results;

-- Check query history
SELECT * FROM system.query.history 
WHERE warehouse_id = 'your-warehouse-id'
ORDER BY start_time DESC;
```

### Storage Optimization
```sql
-- Optimize Delta tables periodically
OPTIMIZE mapping_apps.mapping_system.mapping_results
ZORDER BY (session_id, source_table);

-- Vacuum old versions (optional)
VACUUM mapping_apps.mapping_system.mapping_results RETAIN 168 HOURS;
```

## 💰 **Cost Considerations**

### Databricks SQL Costs
- **Warehouse compute**: Based on DBU usage
- **Storage**: Delta Lake storage costs
- **Auto-stop**: Configure warehouses to stop when idle

### Optimization Tips
1. Use **Serverless SQL** for cost-effective small workloads
2. Set appropriate **auto-stop** times
3. Use **clustering** to reduce scan costs
4. Monitor and **optimize query patterns**

## 🔒 **Security**

### Access Control
```sql
-- Grant permissions to app service principal
GRANT USE CATALOG ON CATALOG mapping_apps TO `service-principal-name`;
GRANT USE SCHEMA ON SCHEMA mapping_apps.mapping_system TO `service-principal-name`;
GRANT ALL PRIVILEGES ON SCHEMA mapping_apps.mapping_system TO `service-principal-name`;
```

### Token Management
- Use **service principals** instead of user tokens
- Store tokens in **Databricks secrets**
- Rotate tokens regularly

## 🧪 **Testing Database Configuration**

```python
# Test database connectivity
from models.databricks_config import get_database_config

db_config = get_database_config()
print(f"Database type: {db_config.database_type}")
print(f"Connection string: {db_config.connection_string}")
```

Run the included test:
```bash
python -c "from models.databricks_config import get_database_config; print(get_database_config().database_type)"
```