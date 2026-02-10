import asyncio
from typing import Dict, List, Any, Tuple
from services.llm_service import LLMService
import difflib
import json


class ValidationAgent:
    """
    Agent responsible for validating and correcting differences between 
    Code Analysis Agent and Legacy Mapping Agent outputs using Claude LLM
    """
    
    def __init__(self, llm_service: LLMService):
        self.llm_service = llm_service
    
    async def validate_and_correct(
        self, 
        code_analysis_results: Dict[str, Any], 
        legacy_mapping_results: Dict[str, Any],
        session_id: int
    ) -> Dict[str, Any]:
        """
        Compare outputs from both agents and generate corrected mappings
        
        Args:
            code_analysis_results: Results from CodeAnalysisAgent
            legacy_mapping_results: Results from LegacyMappingAgent  
            session_id: Database session ID
            
        Returns:
            Dict containing validated and corrected mappings
        """
        try:
            # Extract transformations from both agents
            code_mappings = code_analysis_results.get('transformations', [])
            legacy_mappings = legacy_mapping_results.get('transformations', [])
            
            # Perform comparison analysis
            comparison_analysis = self._compare_mappings(code_mappings, legacy_mappings)
            
            # Use Claude LLM for intelligent validation and correction
            validated_mappings = await self._validate_with_claude(
                code_analysis_results,
                legacy_mapping_results,
                comparison_analysis
            )
            
            # Calculate confidence scores and review flags
            final_mappings = self._calculate_confidence_scores(validated_mappings)
            
            return {
                'agent': 'validation',
                'status': 'success',
                'session_id': session_id,
                'validated_mappings': final_mappings,
                'comparison_summary': {
                    'code_analysis_count': len(code_mappings),
                    'legacy_count': len(legacy_mappings),
                    'final_count': len(final_mappings),
                    'conflicts_resolved': comparison_analysis['conflicts_count'],
                    'new_mappings_added': comparison_analysis['new_mappings_count'],
                    'duplicates_removed': comparison_analysis['duplicates_removed']
                }
            }
            
        except Exception as e:
            return {
                'agent': 'validation',
                'status': 'error',
                'error': str(e),
                'session_id': session_id
            }
    
    def _compare_mappings(
        self, 
        code_mappings: List[Dict[str, Any]], 
        legacy_mappings: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Perform detailed comparison of mappings from both agents
        """
        analysis = {
            'exact_matches': [],
            'partial_matches': [],
            'conflicts': [],
            'code_only': [],
            'legacy_only': [],
            'conflicts_count': 0,
            'new_mappings_count': 0,
            'duplicates_removed': 0
        }
        
        # Create lookup dictionaries for comparison
        code_lookup = self._create_mapping_lookup(code_mappings, 'code_analysis')
        legacy_lookup = self._create_mapping_lookup(legacy_mappings, 'legacy')
        
        # Find matches and conflicts
        all_keys = set(code_lookup.keys()) | set(legacy_lookup.keys())
        
        for key in all_keys:
            code_mapping = code_lookup.get(key)
            legacy_mapping = legacy_lookup.get(key)
            
            if code_mapping and legacy_mapping:
                # Both agents found this mapping
                similarity = self._calculate_similarity(code_mapping, legacy_mapping)
                
                if similarity > 0.9:
                    analysis['exact_matches'].append({
                        'key': key,
                        'code_mapping': code_mapping,
                        'legacy_mapping': legacy_mapping,
                        'similarity': similarity
                    })
                elif similarity > 0.5:
                    analysis['partial_matches'].append({
                        'key': key,
                        'code_mapping': code_mapping,
                        'legacy_mapping': legacy_mapping,
                        'similarity': similarity
                    })
                else:
                    analysis['conflicts'].append({
                        'key': key,
                        'code_mapping': code_mapping,
                        'legacy_mapping': legacy_mapping,
                        'similarity': similarity
                    })
                    analysis['conflicts_count'] += 1
                    
            elif code_mapping:
                analysis['code_only'].append(code_mapping)
                analysis['new_mappings_count'] += 1
                
            elif legacy_mapping:
                analysis['legacy_only'].append(legacy_mapping)
        
        return analysis
    
    def _create_mapping_lookup(self, mappings: List[Dict[str, Any]], source: str) -> Dict[str, Dict[str, Any]]:
        """
        Create a lookup dictionary for mappings based on source table + column
        """
        lookup = {}
        
        for mapping in mappings:
            key = f"{mapping.get('source_table', 'unknown')}_{mapping.get('source_column', 'unknown')}"
            
            # Add source information
            mapping['extraction_source'] = source
            lookup[key] = mapping
        
        return lookup
    
    def _calculate_similarity(self, mapping1: Dict[str, Any], mapping2: Dict[str, Any]) -> float:
        """
        Calculate similarity score between two mappings
        """
        # Compare transformation rules
        rule1 = mapping1.get('transformation_rule', '')
        rule2 = mapping2.get('transformation_rule', '')
        
        # Use difflib to calculate similarity
        rule_similarity = difflib.SequenceMatcher(None, rule1, rule2).ratio()
        
        # Compare target fields
        target1 = mapping1.get('target_field', '')
        target2 = mapping2.get('target_field', '')
        target_similarity = difflib.SequenceMatcher(None, target1, target2).ratio()
        
        # Weighted average (transformation rule is more important)
        overall_similarity = (rule_similarity * 0.7) + (target_similarity * 0.3)
        
        return overall_similarity
    
    async def _validate_with_claude(
        self, 
        code_analysis_results: Dict[str, Any],
        legacy_mapping_results: Dict[str, Any], 
        comparison_analysis: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Use Claude LLM to intelligently validate and correct mapping conflicts
        """
        
        prompt = f"""
        As an expert in PySpark and SQL data transformations, you need to validate and correct mapping conflicts between two extraction methods.

        EXTRACTION RESULTS:

        CODE ANALYSIS AGENT (High accuracy, modern approach):
        Status: {code_analysis_results.get('status')}
        Transformations Count: {code_analysis_results.get('transformations_count')}
        Confidence Level: {code_analysis_results.get('confidence_level')}

        LEGACY MAPPING AGENT (Known inaccuracies, AST-based):
        Status: {legacy_mapping_results.get('status')}
        Transformations Count: {legacy_mapping_results.get('transformations_count')}
        Confidence Level: {legacy_mapping_results.get('confidence_level')}

        COMPARISON ANALYSIS:
        Exact Matches: {len(comparison_analysis['exact_matches'])}
        Partial Matches: {len(comparison_analysis['partial_matches'])}
        Conflicts: {len(comparison_analysis['conflicts'])}
        Code Analysis Only: {len(comparison_analysis['code_only'])}
        Legacy Only: {len(comparison_analysis['legacy_only'])}

        CONFLICTS TO RESOLVE:
        {json.dumps(comparison_analysis['conflicts'][:10], indent=2)}  # Limit to first 10 conflicts

        PARTIAL MATCHES TO VALIDATE:
        {json.dumps(comparison_analysis['partial_matches'][:10], indent=2)}

        YOUR TASK:
        1. For CONFLICTS: Choose the more accurate mapping or create a corrected version
        2. For PARTIAL MATCHES: Merge the best parts from both mappings
        3. For EXACT MATCHES: Keep as-is (they're already validated)
        4. For single-agent mappings: Validate accuracy and set appropriate confidence
        5. Set needs_review=True for any mappings you're uncertain about

        VALIDATION CRITERIA:
        - Prioritize Code Analysis Agent results (more accurate)
        - Look for complete transformation rules (not partial/incomplete)
        - Ensure source table and column names are accurate
        - Validate that target fields make sense
        - Check for proper PySpark/SQL syntax in transformation rules

        REQUIRED OUTPUT FORMAT:
        {{
            "validated_mappings": [
                {{
                    "source_table": "table_name",
                    "source_column": "column_name",
                    "transformation_rule": "complete PySpark/SQL code",
                    "target_field": "target_column_name",
                    "array_field": "array_field_if_applicable",
                    "confidence_score": 0.95,
                    "needs_review": false,
                    "validation_reasoning": "explanation of validation decision",
                    "sources_used": ["code_analysis", "legacy", "merged"],
                    "conflict_resolution": "how conflicts were resolved if any"
                }}
            ],
            "validation_summary": {{
                "total_validated": 15,
                "high_confidence": 12,
                "needs_review": 3,
                "conflicts_resolved": 4,
                "methodology": "prioritized code analysis with legacy validation"
            }}
        }}

        Be thorough and accurate. When in doubt, flag for human review.
        """

        try:
            response = await self.llm_service.call_claude(prompt)
            
            # Parse Claude's response
            validated_data = json.loads(response)
            
            # Add exact matches and validated unique mappings
            validated_mappings = validated_data.get('validated_mappings', [])
            
            # Add exact matches (already validated by high similarity)
            for exact_match in comparison_analysis['exact_matches']:
                mapping = exact_match['code_mapping'].copy()  # Prefer code analysis
                mapping['validation_reasoning'] = 'Exact match between both agents'
                mapping['sources_used'] = ['code_analysis', 'legacy']
                mapping['confidence_score'] = min(mapping.get('confidence_score', 0.8) + 0.1, 1.0)
                mapping['needs_review'] = False
                validated_mappings.append(mapping)
            
            return validated_mappings
            
        except Exception as e:
            print(f"Claude validation failed: {str(e)}")
            # Fallback: prefer code analysis results
            return self._fallback_validation(comparison_analysis)
    
    def _fallback_validation(self, comparison_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fallback validation when Claude LLM fails
        """
        validated_mappings = []
        
        # Add exact matches
        for match in comparison_analysis['exact_matches']:
            mapping = match['code_mapping'].copy()
            mapping['validation_reasoning'] = 'Fallback: Exact match validation'
            mapping['needs_review'] = False
            validated_mappings.append(mapping)
        
        # Add partial matches (prefer code analysis)
        for match in comparison_analysis['partial_matches']:
            mapping = match['code_mapping'].copy()
            mapping['validation_reasoning'] = 'Fallback: Partial match, preferred code analysis'
            mapping['needs_review'] = True  # Flag for review
            mapping['confidence_score'] = max(mapping.get('confidence_score', 0.5) - 0.2, 0.1)
            validated_mappings.append(mapping)
        
        # Add conflicts (prefer code analysis with low confidence)
        for conflict in comparison_analysis['conflicts']:
            mapping = conflict['code_mapping'].copy()
            mapping['validation_reasoning'] = 'Fallback: Conflict resolved by preferring code analysis'
            mapping['needs_review'] = True
            mapping['confidence_score'] = 0.4  # Low confidence due to conflict
            validated_mappings.append(mapping)
        
        # Add code-only mappings
        for mapping in comparison_analysis['code_only']:
            mapping = mapping.copy()
            mapping['validation_reasoning'] = 'Fallback: Code analysis only'
            mapping['needs_review'] = False
            validated_mappings.append(mapping)
        
        # Add legacy-only mappings with low confidence
        for mapping in comparison_analysis['legacy_only']:
            mapping = mapping.copy()
            mapping['validation_reasoning'] = 'Fallback: Legacy only (lower accuracy expected)'
            mapping['needs_review'] = True
            mapping['confidence_score'] = 0.3  # Low confidence for legacy-only
            validated_mappings.append(mapping)
        
        return validated_mappings
    
    def _calculate_confidence_scores(self, mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate final confidence scores and review flags for validated mappings
        """
        for mapping in mappings:
            # Base confidence from existing score
            base_confidence = mapping.get('confidence_score', 0.5)
            
            # Adjust based on validation criteria
            adjustments = 0
            
            # Boost confidence if transformation rule is complete
            transformation = mapping.get('transformation_rule', '')
            if len(transformation) > 10 and ('when(' in transformation.lower() or 'col(' in transformation.lower()):
                adjustments += 0.1
            
            # Boost confidence if source information is complete
            if mapping.get('source_table', 'unknown') != 'unknown' and mapping.get('source_column', 'unknown') != 'unknown':
                adjustments += 0.1
            
            # Reduce confidence if flagged for review
            if mapping.get('needs_review', False):
                adjustments -= 0.1
            
            # Final confidence score
            mapping['confidence_score'] = max(min(base_confidence + adjustments, 1.0), 0.1)
            
            # Set review flag based on final confidence
            if mapping['confidence_score'] < 0.6:
                mapping['needs_review'] = True
        
        return mappings