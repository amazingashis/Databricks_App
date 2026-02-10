from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship

db = SQLAlchemy()


class MappingSession(db.Model):
    """
    Database model for mapping generation sessions
    Each session represents one notebook analysis run
    """
    __tablename__ = 'mapping_sessions'
    
    id = Column(Integer, primary_key=True)
    notebook_path = Column(String(500), nullable=False)
    status = Column(String(50), nullable=False, default='pending')  # pending, processing, completed, failed
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # GitLab information
    gitlab_project_id = Column(String(100), nullable=True)
    gitlab_branch = Column(String(100), nullable=True, default='main')
    
    # Session metadata
    total_mappings = Column(Integer, nullable=True, default=0)
    high_confidence_count = Column(Integer, nullable=True, default=0)
    needs_review_count = Column(Integer, nullable=True, default=0)
    
    # Agent execution metadata
    agent_execution_log = Column(Text, nullable=True)  # JSON string of agent execution details
    
    # Relationship to mapping results
    mapping_results = relationship("MappingResult", back_populates="session", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f'<MappingSession {self.id}: {self.notebook_path}>'
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'notebook_path': self.notebook_path,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error_message': self.error_message,
            'total_mappings': self.total_mappings,
            'high_confidence_count': self.high_confidence_count,
            'needs_review_count': self.needs_review_count,
            'gitlab_project_id': self.gitlab_project_id,
            'gitlab_branch': self.gitlab_branch
        }


class MappingResult(db.Model):
    """
    Database model for individual mapping results
    Each record represents one source -> target mapping
    """
    __tablename__ = 'mapping_results'
    
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('mapping_sessions.id'), nullable=False)
    
    # Mapping information (matches CSV format)
    source_table = Column(String(200), nullable=False)
    source_column = Column(String(200), nullable=False)
    transformation_rule = Column(Text, nullable=False)  # The actual PySpark/SQL transformation
    target_field = Column(String(200), nullable=False)
    array_field = Column(String(200), nullable=True)
    
    # AI-generated metadata
    confidence_score = Column(Float, nullable=False, default=0.5)  # 0.0 to 1.0
    needs_review = Column(Boolean, nullable=False, default=False)
    agent_notes = Column(Text, nullable=True)  # Notes from the AI agents
    
    # User annotations
    user_notes = Column(Text, nullable=True)  # User can add their own notes
    user_validated = Column(Boolean, nullable=False, default=False)  # User has reviewed this mapping
    
    # Extraction metadata
    extraction_method = Column(String(100), nullable=True)  # ast, pattern_matching, llm_enhanced
    extraction_source = Column(String(100), nullable=True)  # code_analysis, legacy, manual
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True, onupdate=datetime.utcnow)
    
    # Relationship to session
    session = relationship("MappingSession", back_populates="mapping_results")
    
    def __repr__(self):
        return f'<MappingResult {self.id}: {self.source_table}.{self.source_column} -> {self.target_field}>'
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'session_id': self.session_id,
            'source_table': self.source_table,
            'source_column': self.source_column,
            'transformation_rule': self.transformation_rule,
            'target_field': self.target_field,
            'array_field': self.array_field,
            'confidence_score': self.confidence_score,
            'needs_review': self.needs_review,
            'agent_notes': self.agent_notes,
            'user_notes': self.user_notes,
            'user_validated': self.user_validated,
            'extraction_method': self.extraction_method,
            'extraction_source': self.extraction_source,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def to_csv_row(self):
        """Convert to CSV row format matching the original mapping document"""
        return {
            'Source Table': self.source_table,
            'Source Column': self.source_column,
            'Transformation / Mapping Rules': self.transformation_rule,
            'Field': self.target_field,
            'Array Field': self.array_field or ''
        }


class AgentExecutionLog(db.Model):
    """
    Database model for detailed agent execution logs
    Tracks performance and debugging information for each agent
    """
    __tablename__ = 'agent_execution_logs'
    
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('mapping_sessions.id'), nullable=False)
    
    # Agent information
    agent_name = Column(String(100), nullable=False)  # code_analysis, legacy_mapping, validation, document_generation
    agent_version = Column(String(50), nullable=True)
    
    # Execution details
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    status = Column(String(50), nullable=False)  # success, error, timeout
    error_message = Column(Text, nullable=True)
    
    # Input/Output information
    input_data_size = Column(Integer, nullable=True)  # Size of input data in characters
    output_data_size = Column(Integer, nullable=True)  # Size of output data in characters
    processing_time_seconds = Column(Float, nullable=True)
    
    # LLM usage tracking
    llm_model_used = Column(String(50), nullable=True)  # claude, llama
    llm_tokens_used = Column(Integer, nullable=True)
    llm_cost_estimate = Column(Float, nullable=True)  # Estimated cost in USD
    
    # Quality metrics
    confidence_scores = Column(Text, nullable=True)  # JSON array of confidence scores
    review_flags_count = Column(Integer, nullable=True, default=0)
    
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<AgentExecutionLog {self.id}: {self.agent_name} - {self.status}>'
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'id': self.id,
            'session_id': self.session_id,
            'agent_name': self.agent_name,
            'agent_version': self.agent_version,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status,
            'error_message': self.error_message,
            'processing_time_seconds': self.processing_time_seconds,
            'llm_model_used': self.llm_model_used,
            'llm_tokens_used': self.llm_tokens_used,
            'llm_cost_estimate': self.llm_cost_estimate,
            'review_flags_count': self.review_flags_count,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }


def init_database(app):
    """
    Initialize database with the Flask app
    """
    db.init_app(app)
    
    with app.app_context():
        # Create all tables
        db.create_all()
        
        # Optionally add some sample data for testing
        if app.config.get('TESTING') or app.config.get('DEBUG'):
            _create_sample_data()


def _create_sample_data():
    """
    Create sample data for development and testing
    """
    # Check if sample data already exists
    existing_session = MappingSession.query.first()
    if existing_session:
        return  # Sample data already exists
    
    # Create a sample session
    sample_session = MappingSession(
        notebook_path='load_silver_provider.py',
        status='completed',
        created_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
        total_mappings=5,
        high_confidence_count=3,
        needs_review_count=2,
        gitlab_project_id='sample-project',
        gitlab_branch='main'
    )
    
    db.session.add(sample_session)
    db.session.commit()
    
    # Create sample mapping results
    sample_mappings = [
        {
            'source_table': 'provider_drname',
            'source_column': 'nationalid',
            'transformation_rule': 'nationalid',
            'target_field': 'service_provider_id',
            'array_field': '',
            'confidence_score': 0.95,
            'needs_review': False,
            'agent_notes': 'Simple direct mapping',
            'extraction_method': 'ast',
            'extraction_source': 'code_analysis'
        },
        {
            'source_table': 'provider_drname',
            'source_column': 'dr_fname',
            'transformation_rule': 'dr_fname',
            'target_field': 'name_first_name',
            'array_field': '',
            'confidence_score': 0.90,
            'needs_review': False,
            'agent_notes': 'Direct column mapping',
            'extraction_method': 'pattern_matching',
            'extraction_source': 'code_analysis'
        },
        {
            'source_table': 'provider_drname',
            'source_column': 'drsal, dr_fname, dr_iname, dr_lname, drsuffix',
            'transformation_rule': 'trim(concat(coalesce(drsal,"" ""),coalesce(dr_fname,"" ""),coalesce(dr_iname,"" ""),coalesce(dr_lname,"" ""),coalesce(drsuffix,"" "")))',
            'target_field': 'alternate_name_name',
            'array_field': '',
            'confidence_score': 0.85,
            'needs_review': True,
            'agent_notes': 'Complex concatenation transformation',
            'extraction_method': 'llm_enhanced',
            'extraction_source': 'validation'
        }
    ]
    
    for mapping_data in sample_mappings:
        mapping = MappingResult(
            session_id=sample_session.id,
            **mapping_data
        )
        db.session.add(mapping)
    
    db.session.commit()
    print("Sample data created successfully")