import httpx
import asyncio
import base64
from typing import Dict, Any, Optional, List
import os


class GitlabService:
    """
    Service for fetching Databricks notebooks and code from GitLab repositories
    """
    
    def __init__(self):
        self.timeout = httpx.Timeout(30.0)
    
    async def fetch_notebook_content(
        self, 
        notebook_path: str, 
        gitlab_credentials: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Fetch notebook content from GitLab repository
        
        Args:
            notebook_path: Path to the notebook in GitLab repo
            gitlab_credentials: GitLab authentication info
            
        Returns:
            String content of the notebook
        """
        
        # For development/testing, if no GitLab credentials provided,
        # try to read from local file system first
        if not gitlab_credentials or not gitlab_credentials.get('gitlab_url'):
            return await self._fetch_from_local_file(notebook_path)
        
        try:
            # Extract GitLab connection details
            gitlab_url = gitlab_credentials['gitlab_url']
            project_id = gitlab_credentials['project_id']
            access_token = gitlab_credentials.get('access_token')
            username = gitlab_credentials.get('username')
            password = gitlab_credentials.get('password')
            
            # Prepare headers
            headers = {}
            
            if access_token:
                headers['Authorization'] = f'Bearer {access_token}'
            elif username and password:
                # Basic authentication
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers['Authorization'] = f'Basic {credentials}'
            else:
                raise ValueError("No valid authentication method provided")
            
            # Construct GitLab API URL for file content
            # Format: https://gitlab.example.com/api/v4/projects/{project_id}/repository/files/{file_path}/raw?ref=main
            encoded_path = self._encode_gitlab_path(notebook_path)
            api_url = f"{gitlab_url}/api/v4/projects/{project_id}/repository/files/{encoded_path}/raw"
            
            # Add branch/ref parameter
            branch = gitlab_credentials.get('branch', 'main')
            params = {'ref': branch}
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    api_url,
                    headers=headers,
                    params=params
                )
                
                response.raise_for_status()
                return response.text
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(f"Notebook not found at path: {notebook_path}")
            else:
                raise Exception(f"GitLab API error {e.response.status_code}: {e.response.text}")
        
        except Exception as e:
            raise Exception(f"Failed to fetch notebook from GitLab: {str(e)}")
    
    async def _fetch_from_local_file(self, file_path: str) -> str:
        """
        Fallback method to fetch from local file system
        """
        try:
            # Try absolute path first
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            # Try relative to current working directory
            local_path = os.path.join(os.getcwd(), file_path)
            if os.path.exists(local_path):
                with open(local_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            # Try in the workspace directory
            workspace_path = os.path.join('/workspaces/Databricks_App', file_path)
            if os.path.exists(workspace_path):
                with open(workspace_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            raise FileNotFoundError(f"File not found: {file_path}")
            
        except Exception as e:
            raise Exception(f"Failed to read local file: {str(e)}")
    
    def _encode_gitlab_path(self, file_path: str) -> str:
        """
        Encode file path for GitLab API URL
        GitLab requires URL encoding of file paths
        """
        import urllib.parse
        
        # Remove leading slash if present
        if file_path.startswith('/'):
            file_path = file_path[1:]
        
        # URL encode the path
        encoded_path = urllib.parse.quote(file_path, safe='')
        
        return encoded_path
    
    async def list_repository_files(
        self, 
        gitlab_credentials: Dict[str, str],
        path: str = '',
        file_extension: str = '.py'
    ) -> List[Dict[str, Any]]:
        """
        List files in GitLab repository
        
        Args:
            gitlab_credentials: GitLab authentication info
            path: Directory path to list (optional)
            file_extension: Filter by file extension (optional)
            
        Returns:
            List of file information dictionaries
        """
        try:
            gitlab_url = gitlab_credentials['gitlab_url']
            project_id = gitlab_credentials['project_id']
            access_token = gitlab_credentials.get('access_token')
            
            headers = {}
            if access_token:
                headers['Authorization'] = f'Bearer {access_token}'
            
            # GitLab API endpoint for repository tree
            api_url = f"{gitlab_url}/api/v4/projects/{project_id}/repository/tree"
            
            params = {
                'recursive': True,
                'per_page': 100
            }
            
            if path:
                params['path'] = path
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    api_url,
                    headers=headers,
                    params=params
                )
                
                response.raise_for_status()
                files_data = response.json()
                
                # Filter files by extension if specified
                filtered_files = []
                for file_info in files_data:
                    if file_info['type'] == 'blob':  # Only files, not directories
                        if not file_extension or file_info['name'].endswith(file_extension):
                            filtered_files.append({
                                'name': file_info['name'],
                                'path': file_info['path'],
                                'id': file_info['id'],
                                'mode': file_info['mode']
                            })
                
                return filtered_files
                
        except Exception as e:
            raise Exception(f"Failed to list repository files: {str(e)}")
    
    async def validate_credentials(self, gitlab_credentials: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate GitLab credentials and connection
        
        Args:
            gitlab_credentials: GitLab authentication info
            
        Returns:
            Validation results dictionary
        """
        try:
            gitlab_url = gitlab_credentials['gitlab_url']
            project_id = gitlab_credentials['project_id']
            access_token = gitlab_credentials.get('access_token')
            
            headers = {}
            if access_token:
                headers['Authorization'] = f'Bearer {access_token}'
            
            # Test connection by getting project info
            api_url = f"{gitlab_url}/api/v4/projects/{project_id}"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(api_url, headers=headers)
                response.raise_for_status()
                
                project_info = response.json()
                
                return {
                    'valid': True,
                    'project_name': project_info.get('name', 'Unknown'),
                    'project_description': project_info.get('description', ''),
                    'default_branch': project_info.get('default_branch', 'main'),
                    'web_url': project_info.get('web_url', '')
                }
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                return {
                    'valid': False,
                    'error': 'Invalid authentication credentials'
                }
            elif e.response.status_code == 404:
                return {
                    'valid': False,
                    'error': 'Project not found or access denied'
                }
            else:
                return {
                    'valid': False,
                    'error': f'GitLab API error: {e.response.status_code}'
                }
        except Exception as e:
            return {
                'valid': False,
                'error': f'Connection failed: {str(e)}'
            }