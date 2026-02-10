import httpx
import asyncio
import json
from typing import Dict, Any, Optional
import os


class LLMService:
    """
    Service for communicating with Databricks-hosted LLM endpoints.
    Supports both Claude Sonnet 4 and Llama 3.3 70B models.
    """
    
    def __init__(self, claude_endpoint: str, llama_endpoint: str, api_token: str):
        self.claude_endpoint = claude_endpoint
        self.llama_endpoint = llama_endpoint
        self.api_token = api_token
        
        # HTTP client configuration
        self.timeout = httpx.Timeout(60.0)  # 60 seconds timeout
        self.headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }
    
    async def call_claude(self, prompt: str, system_prompt: str = None, max_tokens: int = 4000) -> str:
        """
        Call Claude Sonnet 4 model for high-accuracy code analysis
        
        Args:
            prompt: The main prompt for Claude
            system_prompt: Optional system prompt for context
            max_tokens: Maximum tokens in response
            
        Returns:
            String response from Claude
        """
        try:
            # Prepare request payload
            payload = {
                'messages': [
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'max_tokens': max_tokens,
                'temperature': 0.1,  # Low temperature for consistent, accurate responses
            }
            
            if system_prompt:
                payload['messages'].insert(0, {
                    'role': 'system',
                    'content': system_prompt
                })
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.claude_endpoint,
                    headers=self.headers,
                    json=payload
                )
                
                response.raise_for_status()
                
                # Parse response
                response_data = response.json()
                
                # Extract content based on Databricks response format
                if 'choices' in response_data and response_data['choices']:
                    return response_data['choices'][0]['message']['content']
                elif 'content' in response_data:
                    return response_data['content']
                else:
                    raise ValueError(f"Unexpected response format: {response_data}")
        
        except httpx.HTTPStatusError as e:
            error_msg = f"Claude API error {e.response.status_code}: {e.response.text}"
            raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"Claude API call failed: {str(e)}")
    
    async def call_llama(self, prompt: str, system_prompt: str = None, max_tokens: int = 3000) -> str:
        """
        Call Llama 3.3 70B model for secondary analysis and validation
        
        Args:
            prompt: The main prompt for Llama
            system_prompt: Optional system prompt for context
            max_tokens: Maximum tokens in response
            
        Returns:
            String response from Llama
        """
        try:
            # Prepare request payload
            payload = {
                'messages': [
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'max_tokens': max_tokens,
                'temperature': 0.2,  # Slightly higher temperature than Claude for diverse perspectives
            }
            
            if system_prompt:
                payload['messages'].insert(0, {
                    'role': 'system',  
                    'content': system_prompt
                })
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.llama_endpoint,
                    headers=self.headers,
                    json=payload
                )
                
                response.raise_for_status()
                
                # Parse response
                response_data = response.json()
                
                # Extract content based on Databricks response format
                if 'choices' in response_data and response_data['choices']:
                    return response_data['choices'][0]['message']['content']
                elif 'content' in response_data:
                    return response_data['content']
                else:
                    raise ValueError(f"Unexpected response format: {response_data}")
        
        except httpx.HTTPStatusError as e:
            error_msg = f"Llama API error {e.response.status_code}: {e.response.text}"
            raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"Llama API call failed: {str(e)}")
    
    async def test_connection(self) -> Dict[str, Any]:
        """
        Test connectivity to both LLM endpoints
        
        Returns:
            Dict with connection test results
        """
        results = {
            'claude': {'status': 'unknown', 'error': None},
            'llama': {'status': 'unknown', 'error': None}
        }
        
        # Test Claude endpoint
        try:
            claude_response = await self.call_claude(
                "Test connectivity. Respond with just 'Connected successfully.'",
                max_tokens=50
            )
            
            if 'connected' in claude_response.lower():
                results['claude']['status'] = 'connected'
            else:
                results['claude']['status'] = 'error'
                results['claude']['error'] = 'Unexpected response format'
                
        except Exception as e:
            results['claude']['status'] = 'error'
            results['claude']['error'] = str(e)
        
        # Test Llama endpoint
        try:
            llama_response = await self.call_llama(
                "Test connectivity. Respond with just 'Connected successfully.'",
                max_tokens=50
            )
            
            if 'connected' in llama_response.lower():
                results['llama']['status'] = 'connected'
            else:
                results['llama']['status'] = 'error'
                results['llama']['error'] = 'Unexpected response format'
                
        except Exception as e:
            results['llama']['status'] = 'error'
            results['llama']['error'] = str(e)
        
        return results
    
    async def get_model_choice_recommendation(self, task_type: str) -> str:
        """
        Recommend which model to use for specific tasks
        
        Args:
            task_type: Type of task ('code_analysis', 'validation', 'generation', etc.)
            
        Returns:
            Recommended model ('claude' or 'llama')
        """
        
        # Task-specific model recommendations
        recommendations = {
            'code_analysis': 'claude',  # Claude is better for code understanding
            'validation': 'claude',     # Claude is better for complex reasoning
            'generation': 'claude',     # Claude is better for structured output
            'enhancement': 'llama',     # Llama can provide different perspectives
            'fallback': 'llama'         # Llama as backup when Claude fails
        }
        
        return recommendations.get(task_type, 'claude')
    
    async def call_with_fallback(
        self, 
        prompt: str, 
        system_prompt: str = None, 
        preferred_model: str = 'claude',
        max_tokens: int = 3000
    ) -> Dict[str, Any]:
        """
        Call LLM with automatic fallback to secondary model
        
        Args:
            prompt: The prompt text
            system_prompt: Optional system prompt
            preferred_model: 'claude' or 'llama'
            max_tokens: Maximum response tokens
            
        Returns:
            Dict with response and metadata
        """
        
        primary_model = preferred_model
        fallback_model = 'llama' if preferred_model == 'claude' else 'claude'
        
        # Try primary model first
        try:
            if primary_model == 'claude':
                response = await self.call_claude(prompt, system_prompt, max_tokens)
            else:
                response = await self.call_llama(prompt, system_prompt, max_tokens)
            
            return {
                'response': response,
                'model_used': primary_model,
                'used_fallback': False,
                'success': True
            }
            
        except Exception as primary_error:
            print(f"Primary model ({primary_model}) failed: {str(primary_error)}")
            
            # Try fallback model
            try:
                if fallback_model == 'claude':
                    response = await self.call_claude(prompt, system_prompt, max_tokens)
                else:
                    response = await self.call_llama(prompt, system_prompt, max_tokens)
                
                return {
                    'response': response,
                    'model_used': fallback_model,
                    'used_fallback': True,
                    'success': True,
                    'primary_error': str(primary_error)
                }
                
            except Exception as fallback_error:
                return {
                    'response': None,
                    'model_used': None,
                    'used_fallback': True,
                    'success': False,
                    'primary_error': str(primary_error),
                    'fallback_error': str(fallback_error)
                }