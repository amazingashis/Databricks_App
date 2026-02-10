import ast
import re
import asyncio
from typing import Dict, List, Any, Optional
from services.gitlab_service import GitlabService
from services.llm_service import LLMService


class CodeAnalysisAgent:
    """
    Agent responsible for analyzing PySpark and SQL code from GitLab notebooks
    and extracting data transformation mappings using Claude LLM
    """
    
    def __init__(self, llm_service: LLMService):
        self.llm_service = llm_service
        self.gitlab_service = GitlabService()
        
        # Patterns for different types of transformations
        self.transformation_patterns = {
            'column_mapping': r'\.withColumn\([\'\"](.*?)[\'\"], ?(.*?)\)',
            'select_mapping': r'\.select\((.*?)\)',
            'when_mapping': r'\.when\((.*?), ?(.*?)\)',
            'coalesce_mapping': r'coalesce\((.*?)\)',
            'lit_mapping': r'lit\([\'\"](.*?)[\'\"]',
            'col_mapping': r'col\([\'\"](.*?)[\'\"]',
            'sql_select': r'SELECT\s+(.*?)\s+FROM',
            'sql_case': r'CASE\s+WHEN\s+(.*?)\s+THEN\s+(.*?)\s+END',
        }
    
    async def analyze_notebook(self, notebook_path: str, gitlab_credentials: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Main method to analyze a notebook and extract transformations
        
        Args:
            notebook_path: Path to the Databricks notebook
            gitlab_credentials: GitLab authentication credentials
            
        Returns:
            Dict containing extracted transformations and metadata
        """
        try:
            # Fetch notebook content from GitLab
            notebook_content = await self.gitlab_service.fetch_notebook_content(
                notebook_path, gitlab_credentials
            )
            
            # Parse the notebook content
            parsed_code = self._parse_notebook_content(notebook_content)
            
            # Extract transformations using both AST and pattern matching
            ast_transformations = self._extract_ast_transformations(parsed_code)
            pattern_transformations = self._extract_pattern_transformations(parsed_code)
            
            # Use Claude LLM to enhance and validate transformations
            enhanced_transformations = await self._enhance_with_llm(
                notebook_content, ast_transformations, pattern_transformations
            )
            
            return {
                'agent': 'code_analysis',
                'status': 'success',
                'notebook_path': notebook_path,
                'transformations_count': len(enhanced_transformations),
                'transformations': enhanced_transformations,
                'raw_ast_count': len(ast_transformations),
                'raw_pattern_count': len(pattern_transformations),
                'confidence_level': 'high'  # Will be calculated by LLM
            }
            
        except Exception as e:
            return {
                'agent': 'code_analysis',
                'status': 'error',
                'error': str(e),
                'notebook_path': notebook_path
            }
    
    def _parse_notebook_content(self, content: str) -> Dict[str, Any]:
        """
        Parse notebook content and separate PySpark and SQL sections
        """
        lines = content.split('\n')
        cells = {'pyspark': [], 'sql': [], 'markdown': []}
        current_cell = []
        current_type = 'pyspark'
        
        for line in lines:
            if line.strip().startswith('# COMMAND ----------'):
                # Save previous cell
                if current_cell:
                    cells[current_type].append('\n'.join(current_cell))
                    current_cell = []
            elif line.strip().startswith('# MAGIC %sql'):
                current_type = 'sql'
            elif line.strip().startswith('# MAGIC %md'):
                current_type = 'markdown'
            elif line.strip().startswith('# MAGIC'):
                # Remove magic command prefix
                cleaned_line = line.replace('# MAGIC', '').strip()
                if cleaned_line:
                    current_cell.append(cleaned_line)
            else:
                current_type = 'pyspark'
                current_cell.append(line)
        
        # Save last cell
        if current_cell:
            cells[current_type].append('\n'.join(current_cell))
        
        return cells
    
    def _extract_ast_transformations(self, parsed_code: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract transformations using AST parsing for PySpark code
        """
        transformations = []
        
        for cell in parsed_code['pyspark']:
            try:
                tree = ast.parse(cell)
                visitor = TransformationVisitor()
                visitor.visit(tree)
                transformations.extend(visitor.transformations)
            except SyntaxError:
                # Skip cells with syntax errors
                continue
        
        return transformations
    
    def _extract_pattern_transformations(self, parsed_code: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract transformations using regex patterns for both PySpark and SQL
        """
        transformations = []
        
        # Process PySpark cells
        for cell in parsed_code['pyspark']:
            transformations.extend(self._extract_pyspark_patterns(cell))
        
        # Process SQL cells
        for cell in parsed_code['sql']:
            transformations.extend(self._extract_sql_patterns(cell))
        
        return transformations
    
    def _extract_pyspark_patterns(self, code: str) -> List[Dict[str, Any]]:
        """Extract PySpark transformation patterns"""
        transformations = []
        
        # withColumn transformations
        column_matches = re.finditer(self.transformation_patterns['column_mapping'], code, re.IGNORECASE)
        for match in column_matches:
            target_column = match.group(1).strip()
            transformation_expr = match.group(2).strip()
            
            transformations.append({
                'type': 'column_transformation',
                'target_column': target_column,
                'transformation': transformation_expr,
                'source_line': match.group(0),
                'extraction_method': 'pattern_matching'
            })
        
        # Select transformations
        select_matches = re.finditer(self.transformation_patterns['select_mapping'], code, re.IGNORECASE)
        for match in select_matches:
            select_expr = match.group(1).strip()
            
            transformations.append({
                'type': 'select_transformation',
                'transformation': select_expr,
                'source_line': match.group(0),
                'extraction_method': 'pattern_matching'
            })
        
        return transformations
    
    def _extract_sql_patterns(self, code: str) -> List[Dict[str, Any]]:
        """Extract SQL transformation patterns"""
        transformations = []
        
        # SQL SELECT patterns
        select_matches = re.finditer(self.transformation_patterns['sql_select'], code, re.IGNORECASE | re.DOTALL)
        for match in select_matches:
            select_clause = match.group(1).strip()
            
            transformations.append({
                'type': 'sql_select',
                'transformation': select_clause,
                'source_line': match.group(0),
                'extraction_method': 'pattern_matching'
            })
        
        # SQL CASE patterns
        case_matches = re.finditer(self.transformation_patterns['sql_case'], code, re.IGNORECASE | re.DOTALL)
        for match in case_matches:
            condition = match.group(1).strip()
            result = match.group(2).strip()
            
            transformations.append({
                'type': 'sql_case',
                'condition': condition,
                'result': result,
                'source_line': match.group(0),
                'extraction_method': 'pattern_matching'
            })
        
        return transformations
    
    async def _enhance_with_llm(
        self, 
        notebook_content: str, 
        ast_transformations: List[Dict[str, Any]], 
        pattern_transformations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Use Claude LLM to enhance and validate extracted transformations
        """
        
        prompt = f"""
        As an expert in PySpark and SQL data transformations, analyze the following Databricks notebook code and provide enhanced mapping information.

        NOTEBOOK CONTENT:
        {notebook_content[:8000]}  # Limit content to avoid token limits

        EXTRACTED AST TRANSFORMATIONS:
        {ast_transformations}

        EXTRACTED PATTERN TRANSFORMATIONS:
        {pattern_transformations}

        TASK:
        1. Analyze all the transformations in the code
        2. Identify source tables, source columns, transformation logic, and target fields
        3. Provide a standardized mapping format for each transformation
        4. Include confidence scores (0.0-1.0) for each mapping
        5. Flag any transformations that need human review

        REQUIRED OUTPUT FORMAT:
        {{
            "mappings": [
                {{
                    "source_table": "table_name",
                    "source_column": "column_name", 
                    "transformation_rule": "actual PySpark/SQL transformation code",
                    "target_field": "target_column_name",
                    "array_field": "array_field_if_applicable",
                    "confidence_score": 0.95,
                    "needs_review": false,
                    "reasoning": "explanation of the transformation",
                    "code_location": "line or section where found"
                }}
            ],
            "summary": {{
                "total_transformations": 10,
                "high_confidence_count": 8,
                "needs_review_count": 2
            }}
        }}

        Focus on accuracy and provide detailed transformation rules that match the standard mapping document format.
        """

        try:
            response = await self.llm_service.call_claude(prompt)
            
            # Parse LLM response
            import json
            enhanced_data = json.loads(response)
            
            return enhanced_data.get('mappings', [])
            
        except Exception as e:
            print(f"LLM enhancement failed: {str(e)}")
            # Fall back to combined raw transformations
            return self._fallback_format_transformations(ast_transformations, pattern_transformations)
    
    def _fallback_format_transformations(
        self, 
        ast_transformations: List[Dict[str, Any]], 
        pattern_transformations: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Fallback method to format transformations when LLM fails
        """
        formatted = []
        
        # Process AST transformations
        for trans in ast_transformations:
            formatted.append({
                'source_table': trans.get('source_table', 'unknown'),
                'source_column': trans.get('source_column', 'unknown'),
                'transformation_rule': str(trans.get('transformation', '')),
                'target_field': trans.get('target_column', ''),
                'array_field': '',
                'confidence_score': 0.6,  # Medium confidence for fallback
                'needs_review': True,
                'reasoning': 'Extracted via AST parsing - needs validation',
                'code_location': trans.get('source_line', '')
            })
        
        # Process pattern transformations  
        for trans in pattern_transformations:
            formatted.append({
                'source_table': 'unknown',
                'source_column': trans.get('target_column', 'unknown'),
                'transformation_rule': trans.get('transformation', ''),
                'target_field': trans.get('target_column', ''),
                'array_field': '',
                'confidence_score': 0.5,  # Lower confidence for pattern matching
                'needs_review': True,
                'reasoning': 'Extracted via pattern matching - needs validation',
                'code_location': trans.get('source_line', '')
            })
        
        return formatted


class TransformationVisitor(ast.NodeVisitor):
    """
    AST visitor to extract transformation information from PySpark code
    """
    
    def __init__(self):
        self.transformations = []
        self.current_dataframe = None
        self.table_mappings = {}
    
    def visit_Call(self, node):
        """Visit function call nodes to identify transformations"""
        
        if isinstance(node.func, ast.Attribute):
            # Handle dataframe.method() calls
            if node.func.attr == 'withColumn':
                self._extract_with_column(node)
            elif node.func.attr == 'select':
                self._extract_select(node)
            elif node.func.attr in ['when', 'otherwise']:
                self._extract_when(node)
        
        self.generic_visit(node)
    
    def _extract_with_column(self, node):
        """Extract withColumn transformation"""
        if len(node.args) >= 2:
            # Get column name
            if isinstance(node.args[0], ast.Constant):
                column_name = node.args[0].value
            elif isinstance(node.args[0], ast.Str):  # Python 3.7 compatibility
                column_name = node.args[0].s
            else:
                column_name = ast.unparse(node.args[0]) if hasattr(ast, 'unparse') else 'unknown'
            
            # Get transformation expression
            transformation = ast.unparse(node.args[1]) if hasattr(ast, 'unparse') else str(node.args[1])
            
            self.transformations.append({
                'type': 'withColumn',
                'target_column': column_name,
                'transformation': transformation,
                'source_line': f"withColumn('{column_name}', {transformation})",
                'extraction_method': 'ast'
            })
    
    def _extract_select(self, node):
        """Extract select transformation"""
        select_items = []
        
        for arg in node.args:
            if isinstance(arg, ast.Constant):
                select_items.append(arg.value)
            elif isinstance(arg, ast.Str):
                select_items.append(arg.s)
            else:
                select_items.append(ast.unparse(arg) if hasattr(ast, 'unparse') else str(arg))
        
        self.transformations.append({
            'type': 'select',
            'transformation': ', '.join(select_items),
            'source_line': f"select({', '.join(select_items)})",
            'extraction_method': 'ast'
        })
    
    def _extract_when(self, node):
        """Extract when/otherwise transformation"""
        if len(node.args) >= 2:
            condition = ast.unparse(node.args[0]) if hasattr(ast, 'unparse') else str(node.args[0])
            result = ast.unparse(node.args[1]) if hasattr(ast, 'unparse') else str(node.args[1])
            
            self.transformations.append({
                'type': 'when',
                'condition': condition,
                'result': result,
                'transformation': f"when({condition}, {result})",
                'source_line': f"when({condition}, {result})",
                'extraction_method': 'ast'
            })