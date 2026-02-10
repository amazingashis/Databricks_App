import os
from flask import Flask, request, jsonify, render_template, send_file
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from agents.agent_orchestrator import AgentOrchestrator
from services.gitlab_service import GitLabService
from services.llm_service import LLMService
from models.database import db, MappingSession, MappingResult
from models.databricks_config import get_database_config
import asyncio
import json
from datetime import datetime
import tempfile

app = Flask(__name__)
CORS(app)

# Enhanced configuration for Databricks Apps
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')

# Database configuration - supports multiple backends
db_config = get_database_config()
db_config.configure_flask_app(app)

# Databricks LLM endpoints
app.config['CLAUDE_ENDPOINT'] = 'https://dbc-3735add4-1cb6.cloud.databricks.com/serving-endpoints/databricks-claude-sonnet-4/invocations'
app.config['LLAMA_ENDPOINT'] = 'https://dbc-3735add4-1cb6.cloud.databricks.com/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations'
app.config['DATABRICKS_TOKEN'] = os.getenv('DATABRICKS_TOKEN')

# Initialize database
db.init_app(app)

# Initialize services  
llm_service = LLMService(
    claude_endpoint=app.config['CLAUDE_ENDPOINT'],
    llama_endpoint=app.config['LLAMA_ENDPOINT'],
    api_token=app.config['DATABRICKS_TOKEN']
)

agent_orchestrator = AgentOrchestrator(llm_service=llm_service)
gitlab_service = GitLabService()

@app.route('/')
def index():
    """Main interface for the mapping generation system"""
    return render_template('index.html')

@app.route('/api/analyze', methods=['POST'])
async def analyze_notebook():
    """
    Analyze a notebook and generate mapping document
    
    Expected input:
    {
        "notebook_path": "path/to/notebook.py",
        "gitlab_credentials": {...},
        "target_format": "standard"
    }
    """
    try:
        data = request.get_json()
        
        # Create new mapping session
        session = MappingSession(
            notebook_path=data['notebook_path'],
            status='processing',
            created_at=datetime.utcnow()
        )
        db.session.add(session)
        db.session.commit()
        
        # Run agent orchestration workflow
        result = await agent_orchestrator.execute_mapping_workflow(
            notebook_path=data['notebook_path'],
            gitlab_credentials=data.get('gitlab_credentials'),
            session_id=session.id
        )
        
        # Update session status
        session.status = 'completed' if result['success'] else 'failed'
        session.completed_at = datetime.utcnow()
        db.session.commit()
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/sessions/<int:session_id>/results')
def get_session_results(session_id):
    """Get results for a specific mapping session"""
    session = MappingSession.query.get_or_404(session_id)
    results = MappingResult.query.filter_by(session_id=session_id).all()
    
    return jsonify({
        'session': {
            'id': session.id,
            'status': session.status,
            'notebook_path': session.notebook_path,
            'created_at': session.created_at.isoformat(),
            'completed_at': session.completed_at.isoformat() if session.completed_at else None
        },
        'results': [{
            'id': result.id,
            'source_table': result.source_table,
            'source_column': result.source_column,
            'transformation_rule': result.transformation_rule,
            'target_field': result.target_field,
            'array_field': result.array_field,
            'confidence_score': result.confidence_score,
            'needs_review': result.needs_review,
            'agent_notes': result.agent_notes
        } for result in results]
    })

@app.route('/api/sessions/<int:session_id>/update', methods=['POST'])
def update_mapping_result():
    """Update a specific mapping result after user review"""
    data = request.get_json()
    result_id = data['result_id']
    
    result = MappingResult.query.get_or_404(result_id)
    
    # Update fields
    if 'transformation_rule' in data:
        result.transformation_rule = data['transformation_rule']
    if 'needs_review' in data:
        result.needs_review = data['needs_review']
    if 'user_notes' in data:
        result.user_notes = data['user_notes']
    
    db.session.commit()
    
    return jsonify({'success': True})

@app.route('/api/sessions/<int:session_id>/export')
def export_to_excel(session_id):
    """Export mapping results to Excel format"""
    session = MappingSession.query.get_or_404(session_id)
    results = MappingResult.query.filter_by(session_id=session_id).all()
    
    # Create Excel file
    import pandas as pd
    df_data = []
    
    for result in results:
        df_data.append({
            'Source Table': result.source_table,
            'Source Column': result.source_column,
            'Transformation / Mapping Rules': result.transformation_rule,
            'Field': result.target_field,
            'Array Field': result.array_field,
            'Confidence Score': result.confidence_score,
            'Needs Review': result.needs_review,
            'Agent Notes': result.agent_notes,
            'User Notes': result.user_notes or ''
        })
    
    df = pd.DataFrame(df_data)
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        df.to_excel(tmp.name, index=False)
        return send_file(
            tmp.name,
            as_attachment=True,
            download_name=f'mapping_document_{session_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})

if __name__ == '__main__':
    # Initialize database with enhanced configuration
    db_config.create_tables_if_needed(app)
    
    print(f"🚀 Starting Agentic Mapping System")
    print(f"Database Type: {db_config.database_type}")
    print(f"Environment: {os.getenv('FLASK_ENV', 'production')}")
    
    # Run Flask app (in Databricks Apps, this runs on the cluster)
    app.run(host='0.0.0.0', port=5000, debug=os.getenv('DEBUG', 'False').lower() == 'true')