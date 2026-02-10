#!/usr/bin/env python3
"""
Simple test for core system components
"""

import asyncio
import os
import sys
from datetime import datetime

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_imports():
    """Test that core modules can be imported"""
    print("🔧 Testing Core Module Imports...")
    
    try:
        from services.llm_service import LLMService
        print("✅ LLM Service imported successfully")
    except Exception as e:
        print(f"❌ LLM Service import failed: {e}")
        return False
    
    try:
        from services.gitlab_service import GitLabService  
        print("✅ GitLab Service imported successfully")
    except Exception as e:
        print(f"❌ GitLab Service import failed: {e}")
        return False
    
    try:
        from models.database import MappingSession, MappingResult, db
        print("✅ Database models imported successfully")
    except Exception as e:
        print(f"❌ Database models import failed: {e}")
        return False
    
    return True


async def test_gitlab_service():
    """Test GitLab service functionality"""
    print("\n📁 Testing GitLab Service...")
    
    try:
        from services.gitlab_service import GitLabService
        gitlab_service = GitLabService()
        
        # Test local file reading
        content = await gitlab_service.fetch_notebook_content('load_silver_provider.py')
        print(f"✅ Successfully read local file: {len(content)} characters")
        return True
        
    except Exception as e:
        print(f"❌ GitLab service test failed: {e}")
        return False


def test_database_models():
    """Test database model creation"""
    print("\n🗄️ Testing Database Models...")
    
    try:
        from models.database import MappingSession, MappingResult
        
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


async def test_llm_service():
    """Test LLM service structure"""
    print("\n🤖 Testing LLM Service Structure...")
    
    try:
        from services.llm_service import LLMService
        
        # Test service initialization
        claude_endpoint = "https://test.endpoint.com/claude"
        llama_endpoint = "https://test.endpoint.com/llama" 
        api_token = "test-token"
        
        llm_service = LLMService(claude_endpoint, llama_endpoint, api_token)
        
        # Test method exists
        model_recommendation = await llm_service.get_model_choice_recommendation('code_analysis')
        print(f"✅ LLM Service structure test passed")
        print(f"Model recommendation for code_analysis: {model_recommendation}")
        
        return True
        
    except Exception as e:
        print(f"❌ LLM service structure test failed: {e}")
        return False


def test_flask_app_structure():
    """Test Flask app can be imported and basic structure is correct"""
    print("\n🌐 Testing Flask App Structure...")
    
    try:
        # Set required environment variables
        os.environ['DATABRICKS_TOKEN'] = 'test-token'
        
        # Import Flask app
        from app import app
        
        print("✅ Flask app imported successfully")
        
        # Test app configuration
        with app.app_context():
            # Test that routes are registered
            routes = [rule.rule for rule in app.url_map.iter_rules()]
            print(f"Registered routes: {routes}")
            
            if '/' in routes and '/api/analyze' in routes:
                print("✅ Core routes registered correctly")
                return True
            else:
                print("❌ Missing core routes")
                return False
        
    except Exception as e:
        print(f"❌ Flask app structure test failed: {e}")
        return False


async def main():
    """Main test function"""
    print("🚀 Agentic Mapping Document Generator - Core Tests")
    print("=" * 60)
    
    test_results = {}
    
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test imports
    test_results['imports'] = test_imports()
    
    if test_results['imports']:
        # Test individual components
        test_results['gitlab'] = await test_gitlab_service()
        test_results['database'] = test_database_models()
        test_results['llm_service'] = await test_llm_service()
        test_results['flask_app'] = test_flask_app_structure()
    
    print("\n" + "=" * 60)
    print("🏁 Test Summary:")
    for test_name, result in test_results.items():
        status = "PASS" if result else "FAIL"
        print(f"✅ {test_name.title()}: {status}")
    
    print(f"\nOverall: {sum(test_results.values())}/{len(test_results)} tests passed")
    
    if all(test_results.values()):
        print("\n🎉 All core tests PASSED! System is ready.")
        print("Next steps:")
        print("1. Set up your .env file with valid Databricks credentials")
        print("2. Run: python app.py")
        print("3. Open http://localhost:5000 in your browser")
    else:
        print("\n⚠️ Some tests failed. Please check the errors above.")
    
    return all(test_results.values())


if __name__ == "__main__":
    # Run the async main function
    result = asyncio.run(main())
    sys.exit(0 if result else 1)