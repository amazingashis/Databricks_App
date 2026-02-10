import asyncio
import subprocess
import tempfile
import os
# import pandas as pd  # Commented out for demo
from typing import Dict, List, Any, Optional
from services.llm_service import LLMService
from DocumentExtractorV5 import MappingExtractor  # Import the existing extractor


class LegacyMappingAgent:
    """
    Agent responsible for running the existing DocumentExtractorV5.py script
    and extracting mappings using the current AST-based approach
    """
    
    def __init__(self, llm_service: LLMService):
        self.llm_service = llm_service
        self.extractor = MappingExtractor()
    
    async def extract_mappings(self, notebook_path: str) -> Dict[str, Any]:
        """
        Extract mappings using the legacy DocumentExtractorV5 approach
        
        Args:
            notebook_path: Path to the Databricks notebook
            
        Returns:
            Dict containing extracted mappings and metadata
        """
        try:
            # Read the notebook content
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook_content = f.read()
            
            # Use the existing DocumentExtractorV5 logic
            legacy_mappings = await self._run_legacy_extraction(notebook_content)
            
            # Format results to match our standard format
            formatted_mappings = self._format_legacy_mappings(legacy_mappings)
            
            # Enhance with LLM understanding
            enhanced_mappings = await self._enhance_legacy_with_llm(
                notebook_content, formatted_mappings
            )
            
            return {
                'agent': 'legacy_mapping',
                'status': 'success',
                'notebook_path': notebook_path,
                'transformations_count': len(enhanced_mappings),
                'transformations': enhanced_mappings,
                'extraction_method': 'DocumentExtractorV5',
                'confidence_level': 'medium'  # Legacy system has known accuracy issues
            }
            
        except Exception as e:
            return {
                'agent': 'legacy_mapping',
                'status': 'error',
                'error': str(e),
                'notebook_path': notebook_path
            }
    
    async def _run_legacy_extraction(self, notebook_content: str) -> List[Dict[str, Any]]:
        """
        Run the legacy DocumentExtractorV5 extraction logic
        """
        import ast
        
        mappings = []
        
        try:
            # Parse the notebook content as Python AST
            tree = ast.parse(notebook_content)
            
            # Use the existing MappingExtractor
            self.extractor.visit(tree)
            
            # Extract the mappings from the extractor
            for mapping in self.extractor.mappings:
                mappings.append({
                    'source_table': getattr(mapping, 'source_table', 'unknown'),
                    'source_column': getattr(mapping, 'source_column', 'unknown'),
                    'transformation': getattr(mapping, 'transformation', ''),
                    'target_field': getattr(mapping, 'target_field', ''),
                    'array_field': getattr(mapping, 'array_field', ''),
                    'extraction_details': getattr(mapping, 'details', {})
                })
            
            # Also extract dataframe mappings if available
            for df_name, df_mapping in self.extractor.dataframe_mappings.items():
                mappings.append({
                    'source_table': df_name,
                    'source_column': 'dataframe_operation',
                    'transformation': str(df_mapping),
                    'target_field': df_name,
                    'array_field': '',
                    'extraction_details': {'type': 'dataframe_mapping'}
                })
        
        except Exception as e:
            print(f"Legacy extraction error: {str(e)}")
            # If AST parsing fails, try a simpler approach
            mappings = self._fallback_legacy_extraction(notebook_content)
        
        return mappings
    
    def _fallback_legacy_extraction(self, notebook_content: str) -> List[Dict[str, Any]]:
        """
        Fallback extraction method if main legacy extraction fails
        """
        import re
        
        mappings = []
        
        # Simple regex-based extraction as fallback
        patterns = {
            'withColumn': r'\.withColumn\([\'\"](.*?)[\'\"], ?(.*?)\)',
            'select': r'\.select\((.*?)\)',
            'alias': r'\.alias\([\'\"](.*?)[\'\"]',
        }
        
        for pattern_name, pattern in patterns.items():
            matches = re.finditer(pattern, notebook_content, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                if pattern_name == 'withColumn':
                    column_name = match.group(1).strip()
                    transformation = match.group(2).strip()
                    
                    mappings.append({
                        'source_table': 'unknown',
                        'source_column': 'unknown',
                        'transformation': transformation,
                        'target_field': column_name,
                        'array_field': '',
                        'extraction_details': {'method': 'fallback_regex', 'pattern': pattern_name}
                    })
                
                elif pattern_name == 'select':
                    select_expr = match.group(1).strip()
                    
                    mappings.append({
                        'source_table': 'unknown',
                        'source_column': 'select_expression',
                        'transformation': select_expr,
                        'target_field': 'selected_columns',
                        'array_field': '',
                        'extraction_details': {'method': 'fallback_regex', 'pattern': pattern_name}
                    })
        
        return mappings
    
    def _format_legacy_mappings(self, legacy_mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Format legacy mapping results to match our standard format
        """
        formatted_mappings = []
        
        for mapping in legacy_mappings:
            formatted_mappings.append({
                'source_table': mapping.get('source_table', 'unknown'),
                'source_column': mapping.get('source_column', 'unknown'),
                'transformation_rule': mapping.get('transformation', ''),
                'target_field': mapping.get('target_field', ''),
                'array_field': mapping.get('array_field', ''),
                'confidence_score': 0.7,  # Legacy system baseline confidence
                'needs_review': True,  # Always flag for review due to known inaccuracies
                'reasoning': 'Extracted using legacy DocumentExtractorV5',
                'extraction_method': 'legacy',
                'extraction_details': mapping.get('extraction_details', {})
            })
        
        return formatted_mappings
    
    async def _enhance_legacy_with_llm(
        self, 
        notebook_content: str, 
        formatted_mappings: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Use Llama model to enhance and validate legacy mappings
        """
        
        prompt = f"""
        As a data transformation expert, review and improve these mappings extracted from a Databricks notebook using a legacy extraction tool.

        NOTEBOOK CONTENT (first 6000 chars):
        {notebook_content[:6000]}

        LEGACY EXTRACTED MAPPINGS:
        {formatted_mappings}

        The legacy tool has known accuracy issues. Your task:

        1. Review each mapping for accuracy
        2. Correct any obvious errors in source tables, source columns, or transformation rules
        3. Identify missing transformations that the legacy tool might have missed
        4. Improve confidence scores based on your analysis
        5. Flag mappings that definitely need human review

        REQUIRED OUTPUT FORMAT:
        {{
            "corrected_mappings": [
                {{
                    "source_table": "corrected_table_name",
                    "source_column": "corrected_column_name",
                    "transformation_rule": "corrected_transformation_code",
                    "target_field": "target_column_name",
                    "array_field": "array_field_if_applicable",
                    "confidence_score": 0.85,
                    "needs_review": false,
                    "reasoning": "explanation of corrections made",
                    "changes_made": ["list", "of", "corrections"]
                }}
            ],
            "additional_mappings": [
                // Any mappings missed by legacy tool
            ],
            "summary": {{
                "corrections_made": 5,
                "mappings_added": 2,
                "high_confidence_count": 8
            }}
        }}

        Focus on accuracy and provide detailed explanations for your corrections.
        """

        try:
            response = await self.llm_service.call_llama(prompt)
            
            # Parse LLM response
            import json
            enhanced_data = json.loads(response)
            
            corrected_mappings = enhanced_data.get('corrected_mappings', formatted_mappings)
            additional_mappings = enhanced_data.get('additional_mappings', [])
            
            # Combine corrected and additional mappings
            all_mappings = corrected_mappings + additional_mappings
            
            # Add enhancement metadata
            for mapping in all_mappings:
                mapping['enhanced_by_llm'] = True
                mapping['llm_model'] = 'llama'
            
            return all_mappings
            
        except Exception as e:
            print(f"Legacy LLM enhancement failed: {str(e)}")
            # Return formatted mappings without enhancement
            return formatted_mappings
    
    def _extract_table_definitions(self, notebook_content: str) -> Dict[str, List[str]]:
        """
        Extract table definitions and column lists from notebook content
        This helps improve accuracy of the legacy extraction
        """
        table_definitions = {}
        
        # Look for table_list definitions
        import re
        table_list_pattern = r'table_list\s*=\s*\[(.*?)\]'
        matches = re.finditer(table_list_pattern, notebook_content, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            tables_str = match.group(1)
            # Extract individual table names
            table_names = re.findall(r'[\'\"](.*?)[\'\"]', tables_str)
            table_definitions['main_tables'] = table_names
        
        # Look for dataframe creations
        df_pattern = r'(\w+)_df\s*=\s*dm_df\[[\'\"](.*?)[\'\"]'
        df_matches = re.finditer(df_pattern, notebook_content, re.IGNORECASE)
        
        for match in df_matches:
            df_name = match.group(1) + '_df'
            table_name = match.group(2)
            table_definitions[df_name] = table_name
        
        return table_definitions