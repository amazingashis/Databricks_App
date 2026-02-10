#!/usr/bin/env python3
"""
Test script for the Agentic Mapping Document Generator
Run this to test the basic functionality of the system
"""

import asyncio
import os
import sys
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.llm_service import LLMService
from services.gitlab_service import GitlabService  
from agents.code_analysis_agent import CodeAnalysisAgent
from agents.legacy_mapping_agent import LegacyMappingAgent
from agents.validation_agent import ValidationAgent
from agents.document_generation_agent import DocumentGenerationAgent


async def test_llm_connections():
    """Test connections to Databricks LLM endpoints"""
    print("🔧 Testing LLM Connections...")
    
    # Mock credentials for testing (replace with real ones)
    claude_endpoint = "https://dbc-3735add4-1cb6.cloud.databricks.com/serving-endpoints/databricks-claude-sonnet-4/invocations"
    llama_endpoint = "https://dbc-3735add4-1cb6.cloud.databricks.com/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations" 
    api_token = os.getenv('DATABRICKS_TOKEN', 'test-token')
    
    llm_service = LLMService(claude_endpoint, llama_endpoint, api_token)
    
    try:
        # Test connection (this will fail without real credentials)
        results = await llm_service.test_connection()
        print(f"Claude Status: {results['claude']['status']}")
        print(f"Llama Status: {results['llama']['status']}")
        
        if results['claude']['error']:
            print(f"Claude Error: {results['claude']['error']}")
        if results['llama']['error']:
            print(f"Llama Error: {results['llama']['error']}")
            
    except Exception as e:
        print(f"LLM connection test failed: {e}")
        print("Note: This is expected without valid Databricks credentials")
    
    return llm_service


async def test_gitlab_service():
    """Test GitLab service functionality"""
    print("\n📁 Testing GitLab Service...")
    
    gitlab_service = GitlabService()
    
    try:
        # Test local file reading
        content = await gitlab_service.fetch_notebook_content('load_silver_provider.py')
        print(f"✅ Successfully read local file: {len(content)} characters")
        return True
        
    except Exception as e:
        print(f"❌ GitLab service test failed: {e}")
        return False


async def test_code_analysis_agent(llm_service):
    """Test the code analysis agent"""
    print("\n🤖 Testing Code Analysis Agent...")
    
    code_agent = CodeAnalysisAgent(llm_service)
    
    try:
        # Test with local file
        result = await code_agent.analyze_notebook('load_silver_provider.py')
        
        print(f"Agent Status: {result['status']}")
        print(f"Transformations Found: {result.get('transformations_count', 0)}")
        
        if result['status'] == 'success':
            print("✅ Code Analysis Agent test passed")
        else:
            print(f"❌ Code Analysis Agent failed: {result.get('error', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        print(f"❌ Code Analysis Agent test failed: {e}")
        return {'status': 'error', 'error': str(e)}


async def test_legacy_mapping_agent(llm_service):
    """Test the legacy mapping agent"""
    print("\n📜 Testing Legacy Mapping Agent...")
    
    legacy_agent = LegacyMappingAgent(llm_service)
    
    try:
        # Test with local file
        result = await legacy_agent.extract_mappings('load_silver_provider.py')
        
        print(f"Agent Status: {result['status']}")
        print(f"Transformations Found: {result.get('transformations_count', 0)}")
        
        if result['status'] == 'success':
            print("✅ Legacy Mapping Agent test passed")
        else:
            print(f"❌ Legacy Mapping Agent failed: {result.get('error', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        print(f"❌ Legacy Mapping Agent test failed: {e}")
        return {'status': 'error', 'error': str(e)}


async def test_validation_agent(llm_service, code_results, legacy_results):
    """Test the validation agent"""
    print("\n✅ Testing Validation Agent...")
    
    validation_agent = ValidationAgent(llm_service)
    
    try:
        result = await validation_agent.validate_and_correct(
            code_results, legacy_results, session_id=1
        )
        
        print(f"Agent Status: {result['status']}")
        print(f"Validated Mappings: {len(result.get('validated_mappings', []))}")
        
        if result['status'] == 'success':
            print("✅ Validation Agent test passed")
        else:
            print(f"❌ Validation Agent failed: {result.get('error', 'Unknown error')}")
            
        return result
        
    except Exception as e:
        print(f"❌ Validation Agent test failed: {e}")
        return {'status': 'error', 'error': str(e)}


def test_database_models():
    """Test database model creation"""
    print("\n🗄️ Testing Database Models...")
    
    try:
        from models.database import MappingSession, MappingResult, db
        
        # Create sample objects (don't save to avoid DB dependency)
        session = MappingSession(
            notebook_path='test.py',
            status='completed'
        )
        
        mapping = MappingResult(
            session_id=1,
            source_table='test_table',
            source_column='test_column', 
            transformation_rule='col("test_column")',
            target_field='test_field',
            confidence_score=0.8
        )
        
        # Test serialization
        session_dict = session.to_dict()
        mapping_dict = mapping.to_dict() 
        csv_row = mapping.to_csv_row()
        
        print("✅ Database models test passed")
        print(f"Session fields: {len(session_dict)}")
        print(f"Mapping fields: {len(mapping_dict)}")
        print(f"CSV fields: {list(csv_row.keys())}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database models test failed: {e}")
        return False


async def main():
    """Main test function"""
    print("🚀 Starting Agentic Mapping Document Generator Tests")
    print("=" * 60)
    
    # Test individual components
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test LLM connections
    llm_service = await test_llm_connections()
    
    # Test GitLab service
    gitlab_success = await test_gitlab_service()
    
    # Test database models
    db_success = test_database_models()
    
    if gitlab_success:
        # Test agents
        code_results = await test_code_analysis_agent(llm_service)
        legacy_results = await test_legacy_mapping_agent(llm_service)
        
        if code_results['status'] == 'success' and legacy_results['status'] == 'success':
            validation_results = await test_validation_agent(llm_service, code_results, legacy_results)
        else:
            print("\n⚠️ Skipping validation agent test due to previous failures")
    
    print("\n" + "=" * 60)
    print("🏁 Test Summary:")
    print(f"✅ GitLab Service: {'PASS' if gitlab_success else 'FAIL'}")
    print(f"✅ Database Models: {'PASS' if db_success else 'FAIL'}")
    print("💡 LLM services require valid Databricks credentials to test fully")
    print("\nTo run the full system:")
    print("1. Set up your .env file with valid credentials")
    print("2. Run: python app.py")
    print("3. Open http://localhost:5000 in your browser")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())