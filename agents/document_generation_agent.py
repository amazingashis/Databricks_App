import asyncio
from typing import Dict, List, Any
from services.llm_service import LLMService
from models.database import db, MappingResult
from datetime import datetime
import json


class DocumentGenerationAgent:
    """
    Agent responsible for generating the final standardized mapping document
    and storing results in the database with proper formatting
    """
    
    def __init__(self, llm_service: LLMService):
        self.llm_service = llm_service
    
    async def generate_document(
        self, 
        validated_mappings: Dict[str, Any], 
        session_id: int
    ) -> Dict[str, Any]:
        """
        Generate final mapping document from validated mappings
        
        Args:
            validated_mappings: Output from ValidationAgent
            session_id: Database session ID
            
        Returns:
            Dict containing generation results and metadata
        """
        try:
            # Extract validated mapping data
            mappings = validated_mappings.get('validated_mappings', [])
            
            # Enhance mappings with Claude for final standardization
            standardized_mappings = await self._standardize_mappings_with_claude(mappings)
            
            # Store results in database
            storage_results = await self._store_mappings_in_database(standardized_mappings, session_id)
            
            # Generate summary statistics
            summary_stats = self._generate_summary_statistics(standardized_mappings)
            
            return {
                'agent': 'document_generation',
                'status': 'success',
                'session_id': session_id,
                'mappings': standardized_mappings,
                'storage_results': storage_results,
                'confidence_summary': summary_stats['confidence_summary'],
                'review_required_count': summary_stats['review_required_count'],
                'total_mappings': len(standardized_mappings)
            }
            
        except Exception as e:
            return {
                'agent': 'document_generation',
                'status': 'error',
                'error': str(e),
                'session_id': session_id
            }
    
    async def _standardize_mappings_with_claude(self, mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Use Claude to standardize the final mapping format according to the template
        """
        
        # Load the sample mapping format from the existing CSV
        sample_format = """
        Source Table,Source Column,Transformation / Mapping Rules,Field,Array Field
        provider_drname,nationalid,nationalid,service_provider_id,
        provider_drname,dr_no_ext,dr_no_ext,source_service_provider_id,
        provider_drname,dr_fname,dr_fname,name_first_name,
        provider_drname,"drsal, dr_fname, dr_iname, dr_lname, drsuffix","trim(concat(coalesce(drsal,"" ""),coalesce(dr_fname,"" ""),coalesce(dr_iname,"" ""),coalesce(dr_lname,"" ""),coalesce(drsuffix,"" ""))",alternate_name_name,
        """
        
        prompt = f"""
        As an expert in data mapping standardization, format these validated mappings into the final standardized mapping document format.

        VALIDATED MAPPINGS:
        {json.dumps(mappings[:20], indent=2)}  # Limit to prevent token overflow

        REQUIRED STANDARD FORMAT (based on existing template):
        {sample_format}

        STANDARDIZATION REQUIREMENTS:
        1. Ensure all transformation rules use proper PySpark/SQL syntax
        2. Clean up any malformed transformation expressions
        3. Standardize column naming conventions
        4. Ensure source tables and columns are properly identified
        5. Format complex transformations like concat, coalesce, when clauses properly
        6. Add appropriate array field values where applicable
        7. Maintain consistency with the existing mapping document style

        TRANSFORMATION RULE FORMATTING:
        - Use proper PySpark functions: col(), coalesce(), when(), lit(), concat(), trim()
        - For concatenations: trim(concat(coalesce(field1,"" ""), coalesce(field2,"" "")))
        - For conditionals: when(condition, result).otherwise(alternative)
        - For literals: lit("value")
        - For column references: col("column_name") or just column_name

        REQUIRED OUTPUT FORMAT:
        {{
            "standardized_mappings": [
                {{
                    "source_table": "table_name",
                    "source_column": "column_name or complex_expression",
                    "transformation_rule": "properly formatted PySpark/SQL code",
                    "target_field": "target_column_name",
                    "array_field": "array_field_if_applicable or empty",
                    "confidence_score": 0.95,
                    "needs_review": false,
                    "final_notes": "any important notes for the user"
                }}
            ],
            "standardization_summary": {{
                "rules_cleaned": 5,
                "syntax_corrections": 3,
                "formatting_applied": true
            }}
        }}

        Focus on creating clean, readable, and consistent transformation rules that match the standard format.
        """

        try:
            response = await self.llm_service.call_claude(prompt)
            
            # Parse Claude response
            standardized_data = json.loads(response)
            return standardized_data.get('standardized_mappings', mappings)
            
        except Exception as e:
            print(f"Claude standardization failed: {str(e)}")
            # Return original mappings with basic cleanup
            return self._fallback_standardization(mappings)
    
    def _fallback_standardization(self, mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Fallback standardization when Claude fails
        """
        standardized = []
        
        for mapping in mappings:
            # Basic cleanup and standardization
            standardized_mapping = {
                'source_table': self._clean_table_name(mapping.get('source_table', '')),
                'source_column': self._clean_column_name(mapping.get('source_column', '')),
                'transformation_rule': self._clean_transformation_rule(mapping.get('transformation_rule', '')),
                'target_field': self._clean_field_name(mapping.get('target_field', '')),
                'array_field': mapping.get('array_field', ''),
                'confidence_score': mapping.get('confidence_score', 0.5),
                'needs_review': mapping.get('needs_review', True),
                'final_notes': 'Standardized using fallback method'
            }
            
            standardized.append(standardized_mapping)
        
        return standardized
    
    def _clean_table_name(self, table_name: str) -> str:
        """Clean and standardize table names"""
        if not table_name or table_name == 'unknown':
            return 'unknown'
        
        # Remove common prefixes/suffixes
        cleaned = table_name.strip().lower()
        if cleaned.endswith('_df'):
            cleaned = cleaned[:-3]
        
        return cleaned
    
    def _clean_column_name(self, column_name: str) -> str:
        """Clean and standardize column names"""
        if not column_name or column_name == 'unknown':
            return 'unknown'
        
        # Basic cleanup
        cleaned = column_name.strip()
        
        # Remove quotes if present
        if cleaned.startswith('"') and cleaned.endswith('"'):
            cleaned = cleaned[1:-1]
        elif cleaned.startswith("'") and cleaned.endswith("'"):
            cleaned = cleaned[1:-1]
        
        return cleaned
    
    def _clean_transformation_rule(self, transformation: str) -> str:
        """Clean and standardize transformation rules"""
        if not transformation:
            return ''
        
        # Basic cleanup
        cleaned = transformation.strip()
        
        # Common replacements for standardization
        replacements = {
            'F.': '',  # Remove F. prefixes
            'pyspark.sql.functions.': '',  # Remove full module paths
        }
        
        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)
        
        return cleaned
    
    def _clean_field_name(self, field_name: str) -> str:
        """Clean and standardize target field names"""
        if not field_name:
            return ''
        
        return field_name.strip()
    
    async def _store_mappings_in_database(
        self, 
        mappings: List[Dict[str, Any]], 
        session_id: int
    ) -> Dict[str, Any]:
        """
        Store standardized mappings in the database
        """
        try:
            stored_count = 0
            error_count = 0
            
            for mapping in mappings:
                try:
                    # Create MappingResult instance
                    mapping_result = MappingResult(
                        session_id=session_id,
                        source_table=mapping.get('source_table', ''),
                        source_column=mapping.get('source_column', ''),
                        transformation_rule=mapping.get('transformation_rule', ''),
                        target_field=mapping.get('target_field', ''),
                        array_field=mapping.get('array_field', ''),
                        confidence_score=float(mapping.get('confidence_score', 0.5)),
                        needs_review=bool(mapping.get('needs_review', False)),
                        agent_notes=mapping.get('final_notes', ''),
                        created_at=datetime.utcnow()
                    )
                    
                    db.session.add(mapping_result)
                    stored_count += 1
                    
                except Exception as e:
                    print(f"Failed to store mapping: {str(e)}")
                    error_count += 1
            
            # Commit all changes
            db.session.commit()
            
            return {
                'stored_count': stored_count,
                'error_count': error_count,
                'total_attempted': len(mappings),
                'success_rate': stored_count / len(mappings) if mappings else 0
            }
            
        except Exception as e:
            db.session.rollback()
            return {
                'stored_count': 0,
                'error_count': len(mappings),
                'error_message': str(e)
            }
    
    def _generate_summary_statistics(self, mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary statistics for the final mappings
        """
        if not mappings:
            return {
                'confidence_summary': {},
                'review_required_count': 0
            }
        
        # Confidence score distribution
        confidence_scores = [m.get('confidence_score', 0) for m in mappings]
        high_confidence = sum(1 for score in confidence_scores if score >= 0.8)
        medium_confidence = sum(1 for score in confidence_scores if 0.5 <= score < 0.8)
        low_confidence = sum(1 for score in confidence_scores if score < 0.5)
        
        # Review requirements
        review_required = sum(1 for m in mappings if m.get('needs_review', False))
        
        # Transformation complexity
        complex_transformations = sum(1 for m in mappings 
                                    if len(m.get('transformation_rule', '')) > 50)
        
        return {
            'confidence_summary': {
                'high_confidence_count': high_confidence,
                'medium_confidence_count': medium_confidence,
                'low_confidence_count': low_confidence,
                'average_confidence': sum(confidence_scores) / len(confidence_scores),
                'complex_transformations': complex_transformations
            },
            'review_required_count': review_required,
            'transformation_types': self._analyze_transformation_types(mappings)
        }
    
    def _analyze_transformation_types(self, mappings: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Analyze types of transformations found
        """
        types = {
            'simple_mapping': 0,
            'concatenation': 0,
            'conditional': 0,
            'coalesce': 0,
            'literal': 0,
            'complex': 0
        }
        
        for mapping in mappings:
            rule = mapping.get('transformation_rule', '').lower()
            
            if 'concat(' in rule:
                types['concatenation'] += 1
            elif 'when(' in rule:
                types['conditional'] += 1
            elif 'coalesce(' in rule:
                types['coalesce'] += 1
            elif 'lit(' in rule:
                types['literal'] += 1
            elif len(rule) > 100 or '(' in rule:
                types['complex'] += 1
            else:
                types['simple_mapping'] += 1
        
        return types