# Agentic Mapping Document Generator

## Overview

This project implements an AI-powered agentic system that automatically generates standard mapping documents from Databricks notebooks containing PySpark and SQL transformations. The system uses multiple AI agents orchestrated through the Model Context Protocol (MCP) to analyze code, extract transformations, validate results, and generate standardized documentation.

## Architecture

The system consists of four main AI agents:

1. **Code Analysis Agent** - Analyzes PySpark/SQL code from GitLab repositories
2. **Legacy Mapping Agent** - Runs the existing DocumentExtractorV5 for comparison
3. **Validation Agent** - Compares outputs and validates/corrects mappings using Claude LLM
4. **Document Generation Agent** - Generates final standardized mapping documents

### System Architecture Diagram

```mermaid
graph TB
    subgraph "📱 User Interface Layer"
        UI[Flask Web UI<br/>- Upload notebooks<br/>- Review mappings<br/>- Download Excel]
    end
    
    subgraph "🤖 Agent Orchestra (MCP)"
        A1[Code Analysis Agent<br/>- Parse PySpark/SQL<br/>- Extract transformations<br/>- GitLab integration]
        A2[Legacy Mapping Agent<br/>- Run DocumentExtractorV5<br/>- Extract current mappings]
        A3[Validation & Correction Agent<br/>- Compare outputs<br/>- Validate transformations<br/>- Flag uncertainties]
        A4[Document Generation Agent<br/>- Generate standard format<br/>- Create confidence scores<br/>- Export to Excel]
    end
    
    subgraph "🧠 LLM Services (Databricks)"
        LLM1[Claude Sonnet 4<br/>Endpoint: .../databricks-claude-sonnet-4/invocations]
        LLM2[Llama 3.3 70B<br/>Endpoint: .../databricks-meta-llama-3-3-70b-instruct/invocations]
    end
    
    subgraph "📂 Data Sources"
        GL[GitLab Repository<br/>- Databricks Notebooks<br/>- PySpark/SQL Code]
        LE[Legacy Extractor<br/>- DocumentExtractorV5.py<br/>- AST-based analysis]
    end
    
    subgraph "💾 Storage & Output"
        DB[(Multi-Backend Database<br/>- Databricks SQL (Prod)<br/>- PostgreSQL/Azure SQL<br/>- SQLite (Dev only)]
        EX[Excel Output<br/>- Standard mapping format<br/>- User annotations]
    end

    UI --> A1
    A1 --> GL
    A1 --> LLM1
    A2 --> LE
    A2 --> LLM2
    A1 --> A3
    A2 --> A3
    A3 --> LLM1
    A3 --> A4
    A4 --> DB
    A4 --> EX
    A4 --> UI
```

### Agent Workflow

The agents work in a coordinated workflow:
1. **Code Analysis Agent** fetches and analyzes the notebook using AST parsing and Claude LLM
2. **Legacy Mapping Agent** runs the existing DocumentExtractorV5 for comparison baseline
3. **Validation Agent** compares both outputs, resolves conflicts using Claude LLM intelligence
4. **Document Generation Agent** creates the final standardized mapping document with confidence scores

## Features

- **Multi-Agent Workflow**: Coordinated AI agents for comprehensive analysis
- **GitLab Integration**: Fetches notebooks directly from GitLab repositories
- **Dual LLM Support**: Uses both Claude Sonnet 4 and Llama 3.3 70B for accuracy
- **Interactive UI**: Flask-based web interface for review and editing
- **Confidence Scoring**: AI-generated confidence scores for each mapping
- **Review Flagging**: Automatically flags uncertain mappings for human review
- **Excel Export**: Export results to Excel format matching existing templates

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd Databricks_App
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your actual configuration
   ```

4. **Initialize the database** (Development only - see Database section for production):
   ```bash
   python -c "from app import app; from models.database import db; app.app_context().push(); db.create_all()"
   ```

## Database Configuration

### For Databricks Apps Deployment

**⚠️ Important**: This application supports multiple database backends optimized for Databricks Apps:

#### Why SQLite doesn't work with Databricks Apps:
- Databricks Apps use ephemeral file systems that reset on restart
- SQLite databases would be lost when the app restarts  
- No data persistence across cluster restarts

#### Recommended Options:

1. **Databricks SQL (Recommended)**:
   ```env
   DATABASE_URL=databricks+sql://<token>@<hostname>/<warehouse_id>?catalog=<catalog>&schema=<schema>
   DATABRICKS_SQL_WAREHOUSE_ID=your-sql-warehouse-id
   DATABRICKS_CATALOG=mapping_apps
   DATABRICKS_SCHEMA=mapping_system
   ```

2. **External Database** (Azure SQL, PostgreSQL, MySQL):
   ```env
   # PostgreSQL
   DATABASE_URL=postgresql://user:password@host:port/database
   
   # Azure SQL
   DATABASE_URL=mssql+pyodbc://user:password@server.database.windows.net/db?driver=ODBC+Driver+18+for+SQL+Server
   
   # MySQL
   DATABASE_URL=mysql://user:password@host:port/database
   ```

3. **Development Only** (SQLite - NOT for production):
   ```env
   DATABASE_URL=sqlite:///mapping_system.db
   ```

The application automatically detects the database backend and configures accordingly. See [Database Configuration Guide](docs/databricks-database-config.md) for detailed setup.

## Databricks Apps Deployment

### Prerequisites
1. **Database Setup**: Configure a persistent database (Databricks SQL or external database)
2. **Cluster Configuration**: Ensure the cluster has required libraries installed
3. **Environment Variables**: Set all required environment variables in cluster configuration

### Deployment Steps

1. **Upload application files** to Databricks workspace
2. **Install dependencies** on the cluster:
   ```bash
   pip install flask sqlalchemy databricks-sql-connector flask-cors openpyxl gitpython
   ```
3. **Configure environment variables** in cluster settings:
   ```
   DATABRICKS_TOKEN=dapi-xxx...
   CLAUDE_ENDPOINT=https://your-databricks-instance.cloud.databricks.com/serving-endpoints/databricks-claude-sonnet-4/invocations
   LLAMA_ENDPOINT=https://your-databricks-instance.cloud.databricks.com/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations
   DATABASE_URL=databricks+sql://...
   SECRET_KEY=your-secret-key
   ```
4. **Run the Flask application** as a Databricks App:
   ```python
   # In a notebook cell:
   %run /path/to/app.py
   ```

### Important Notes:
- **File System**: Use DBFS or external storage for persistent files
- **Database**: Never use SQLite - use Databricks SQL or external database
- **Secrets**: Store sensitive credentials in Databricks secrets
- **Scaling**: Configure cluster auto-scaling for production workloads

## Configuration

### Environment Variables

- `DATABRICKS_TOKEN`: Your Databricks API token for LLM access
- `CLAUDE_ENDPOINT`: Claude Sonnet 4 endpoint URL
- `LLAMA_ENDPOINT`: Llama 3.3 70B endpoint URL
- `DATABASE_URL`: Database connection string (default: SQLite)
- `SECRET_KEY`: Flask secret key for sessions

### GitLab Integration

For GitLab integration, you'll need:
- GitLab URL (e.g., `https://gitlab.example.com`)
- Project ID
- Access token with read permissions

## Usage

1. **Start the application**:
   ```bash
   python app.py
   ```

2. **Access the web interface**:
   Open `http://localhost:5000` in your browser

3. **Analyze a notebook**:
   - Enter the notebook path (e.g., `load_silver_provider.py`)
   - Optionally configure GitLab credentials
   - Click "Start Analysis"

4. **Review results**:
   - View generated mappings in the interface
   - Review flagged items requiring human validation
   - Edit mappings as needed
   - Export to Excel when ready

## API Endpoints

### Analysis
- `POST /api/analyze` - Start notebook analysis
- `GET /api/sessions/<id>/results` - Get analysis results
- `POST /api/sessions/<id>/update` - Update mapping results

### Export
- `GET /api/sessions/<id>/export` - Export to Excel format

### Health Check
- `GET /api/health` - System health status

## Output Format

The system generates mapping documents in the standard format:

| Source Table | Source Column | Transformation / Mapping Rules | Field | Array Field |
|--------------|---------------|--------------------------------|-------|-------------|
| provider_drname | nationalid | nationalid | service_provider_id | |
| provider_drname | dr_fname | dr_fname | name_first_name | |

## Technology Stack

- **Backend**: Flask 3.0.0, SQLAlchemy, AsyncIO
- **Frontend**: Vue.js 3, Axios  
- **Database**: Multi-backend support (Databricks SQL, PostgreSQL, Azure SQL, MySQL, SQLite for dev)
- **AI/LLM**: Databricks-hosted Claude Sonnet 4 & Llama 3.3 70B models
- **Code Analysis**: Python AST, Regex patterns
- **Orchestration**: Custom agent coordination (MCP-inspired)