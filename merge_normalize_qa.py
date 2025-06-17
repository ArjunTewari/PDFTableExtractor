import json
import re
import os
from typing import Dict, Any, List, Tuple, Union
from openai import OpenAI
import asyncio
from collections import defaultdict

# the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
# do not change this unless explicitly requested by the user
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY)

class MergeNormalizeQA:
    def __init__(self):
        """Initialize the merge, normalize and QA processor"""
        self.unit_normalization_patterns = {
            # Number patterns
            r'(\d+\.?\d*)\s*thousand': (r'\1', 'thousand'),
            r'(\d+\.?\d*)\s*million': (r'\1', 'million'),
            r'(\d+\.?\d*)\s*billion': (r'\1', 'billion'),
            r'(\d+\.?\d*)\s*k\b': (r'\1', 'thousand'),
            r'(\d+\.?\d*)\s*m\b': (r'\1', 'million'),
            r'(\d+\.?\d*)\s*b\b': (r'\1', 'billion'),
            
            # Currency patterns
            r'\$(\d+\.?\d*)\s*million': (r'\1', 'million USD'),
            r'\$(\d+\.?\d*)\s*billion': (r'\1', 'billion USD'),
            r'\$(\d+\.?\d*)\s*thousand': (r'\1', 'thousand USD'),
            r'\$(\d+\.?\d*)\s*m\b': (r'\1', 'million USD'),
            r'\$(\d+\.?\d*)\s*b\b': (r'\1', 'billion USD'),
            r'\$(\d+\.?\d*)\s*k\b': (r'\1', 'thousand USD'),
            r'\$(\d+\.?\d*)': (r'\1', 'USD'),
            
            # Percentage patterns
            r'(\d+\.?\d*)\s*%': (r'\1', 'percent'),
            r'(\d+\.?\d*)\s*percent': (r'\1', 'percent'),
            
            # Other currency patterns
            r'€(\d+\.?\d*)': (r'\1', 'EUR'),
            r'£(\d+\.?\d*)': (r'\1', 'GBP'),
        }
    
    def concatenate_arrays(self, extraction_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Step 1: Concatenate the three arrays from extraction results"""
        print("Step 1: Concatenating extraction arrays...")
        
        combined_data = []
        
        # Add tables data
        tables_data = extraction_results.get('tables_extracted', [])
        combined_data.extend(tables_data)
        print(f"Added {len(tables_data)} table items")
        
        # Add key-values data
        keyvalues_data = extraction_results.get('keyvalues_extracted', [])
        combined_data.extend(keyvalues_data)
        print(f"Added {len(keyvalues_data)} key-value items")
        
        # Add narrative data
        narrative_data = extraction_results.get('narrative_extracted', [])
        combined_data.extend(narrative_data)
        print(f"Added {len(narrative_data)} narrative items")
        
        print(f"Total concatenated items: {len(combined_data)}")
        return combined_data
    
    def normalize_units_regex(self, value: str) -> Tuple[str, str]:
        """Normalize units using regex patterns"""
        value_lower = value.lower().strip()
        
        for pattern, (replacement, unit) in self.unit_normalization_patterns.items():
            match = re.search(pattern, value_lower, re.IGNORECASE)
            if match:
                normalized_value = re.sub(pattern, replacement, value_lower, flags=re.IGNORECASE)
                return normalized_value.strip(), unit
        
        # If no pattern matches, return original
        return value, ""
    
    async def normalize_units_gpt(self, value: str) -> Tuple[str, str]:
        """Normalize units using GPT-3.5-turbo for complex cases"""
        prompt = f"""Extract the numeric value and unit from this text. Return only JSON.

Text: "{value}"

Extract:
- value: the numeric part only
- unit: the unit (e.g., "thousand", "million USD", "percent", etc.)

Examples:
"457 thousand" → {{"value": "457", "unit": "thousand"}}
"$115.5 million" → {{"value": "115.5", "unit": "million USD"}}
"25%" → {{"value": "25", "unit": "percent"}}

JSON:"""
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a data normalization expert. Extract numeric values and units. Return only valid JSON."
                        },
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"},
                    max_tokens=100,
                    temperature=0
                )
            )
            
            content = response.choices[0].message.content
            if content:
                result = json.loads(content)
                return result.get('value', value), result.get('unit', '')
            else:
                return value, ""
                
        except Exception as e:
            print(f"GPT normalization failed for '{value}': {e}")
            return value, ""
    
    async def normalize_units(self, combined_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Step 2: Normalize units by regex or small GPT-3.5-turbo call"""
        print("Step 2: Normalizing units...")
        
        normalized_data = []
        gpt_tasks = []
        
        for item in combined_data:
            # Try regex normalization first
            original_value = item.get('value', '')
            normalized_value, normalized_unit = self.normalize_units_regex(original_value)
            
            # If regex didn't find a unit and value looks like it might have one, use GPT
            if not normalized_unit and any(char in original_value.lower() for char in ['$', '%', 'million', 'thousand', 'billion', 'k', 'm', 'b']):
                gpt_tasks.append((len(normalized_data), original_value))
            
            # Create normalized item
            normalized_item = item.copy()
            normalized_item['value'] = normalized_value
            
            # Update unit field with normalized unit if found
            if normalized_unit:
                normalized_item['unit'] = normalized_unit
            
            normalized_data.append(normalized_item)
        
        # Process GPT tasks if any
        if gpt_tasks:
            print(f"Processing {len(gpt_tasks)} items with GPT-3.5-turbo for unit normalization...")
            
            async def process_gpt_task(index: int, value: str):
                normalized_value, normalized_unit = await self.normalize_units_gpt(value)
                return index, normalized_value, normalized_unit
            
            gpt_results = await asyncio.gather(*[
                process_gpt_task(index, value) for index, value in gpt_tasks
            ], return_exceptions=True)
            
            # Apply GPT results
            for result in gpt_results:
                if isinstance(result, tuple) and len(result) == 3:
                    index, normalized_value, normalized_unit = result
                    normalized_data[index]['value'] = normalized_value
                    if normalized_unit:
                        normalized_data[index]['unit'] = normalized_unit
        
        print(f"Unit normalization completed for {len(normalized_data)} items")
        return normalized_data
    
    def deduplicate_and_sort(self, normalized_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Step 3: Dedupe & Sort by (page, section, row_id)"""
        print("Step 3: Deduplicating and sorting...")
        
        # Create deduplication key
        seen_items = set()
        deduplicated_data = []
        
        for item in normalized_data:
            # Create unique key based on page, section, column, and value
            dedup_key = (
                item.get('page', 0),
                item.get('section', ''),
                item.get('column', ''),
                item.get('value', '')
            )
            
            if dedup_key not in seen_items:
                seen_items.add(dedup_key)
                deduplicated_data.append(item)
        
        duplicates_removed = len(normalized_data) - len(deduplicated_data)
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate items")
        
        # Sort by (page, section, row_id)
        sorted_data = sorted(deduplicated_data, key=lambda x: (
            x.get('page', 0),
            x.get('section', ''),
            x.get('row_id', 0)
        ))
        
        print(f"Sorted {len(sorted_data)} unique items")
        return sorted_data
    
    def run_qa_checks(self, sorted_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Step 4: Run local QA checks"""
        print("Step 4: Running QA checks...")
        
        qa_results = {
            'total_items': len(sorted_data),
            'checks': {
                'unit_present': {'passed': 0, 'failed': 0, 'failed_items': []},
                'numeric_parseable': {'passed': 0, 'failed': 0, 'failed_items': []},
                'known_context': {'passed': 0, 'failed': 0, 'failed_items': []},
                'required_fields': {'passed': 0, 'failed': 0, 'failed_items': []}
            },
            'warnings': [],
            'errors': []
        }
        
        known_contexts = {'header', 'label', 'data', 'total', 'summary', ''}
        required_fields = ['page', 'section', 'column', 'value']
        
        for i, item in enumerate(sorted_data):
            item_id = f"Item {i+1} (Page {item.get('page', '?')}, {item.get('section', '?')})"
            
            # Check 1: Unit present (for numeric-looking values)
            value = str(item.get('value', ''))
            unit = item.get('unit', '')
            
            if re.search(r'\d', value):  # If value contains digits
                if unit and unit.strip():
                    qa_results['checks']['unit_present']['passed'] += 1
                else:
                    qa_results['checks']['unit_present']['failed'] += 1
                    qa_results['checks']['unit_present']['failed_items'].append({
                        'item_id': item_id,
                        'value': value,
                        'issue': 'Missing unit for numeric value'
                    })
            else:
                qa_results['checks']['unit_present']['passed'] += 1
            
            # Check 2: Numeric parseable (for values that should be numeric)
            if unit in ['thousand', 'million', 'billion', 'percent', 'USD', 'EUR', 'GBP']:
                try:
                    float(value.replace(',', ''))
                    qa_results['checks']['numeric_parseable']['passed'] += 1
                except ValueError:
                    qa_results['checks']['numeric_parseable']['failed'] += 1
                    qa_results['checks']['numeric_parseable']['failed_items'].append({
                        'item_id': item_id,
                        'value': value,
                        'unit': unit,
                        'issue': 'Value not parseable as number despite having numeric unit'
                    })
            else:
                qa_results['checks']['numeric_parseable']['passed'] += 1
            
            # Check 3: Known context
            context = item.get('context', '')
            if context in known_contexts:
                qa_results['checks']['known_context']['passed'] += 1
            else:
                qa_results['checks']['known_context']['failed'] += 1
                qa_results['checks']['known_context']['failed_items'].append({
                    'item_id': item_id,
                    'context': context,
                    'issue': f'Unknown context: "{context}"'
                })
            
            # Check 4: Required fields present
            missing_fields = [field for field in required_fields if not item.get(field)]
            if not missing_fields:
                qa_results['checks']['required_fields']['passed'] += 1
            else:
                qa_results['checks']['required_fields']['failed'] += 1
                qa_results['checks']['required_fields']['failed_items'].append({
                    'item_id': item_id,
                    'missing_fields': missing_fields,
                    'issue': f'Missing required fields: {missing_fields}'
                })
        
        # Generate summary
        total_checks = sum(check['passed'] + check['failed'] for check in qa_results['checks'].values())
        total_passed = sum(check['passed'] for check in qa_results['checks'].values())
        total_failed = sum(check['failed'] for check in qa_results['checks'].values())
        
        qa_results['summary'] = {
            'total_checks': total_checks,
            'total_passed': total_passed,
            'total_failed': total_failed,
            'success_rate': round((total_passed / total_checks * 100) if total_checks > 0 else 0, 2)
        }
        
        # Log failures
        for check_name, check_data in qa_results['checks'].items():
            if check_data['failed'] > 0:
                qa_results['errors'].append(f"{check_name}: {check_data['failed']} failures")
                print(f"QA Check '{check_name}': {check_data['failed']} failures")
        
        print(f"QA completed: {qa_results['summary']['success_rate']}% success rate")
        print(f"Total checks: {total_checks}, Passed: {total_passed}, Failed: {total_failed}")
        
        return qa_results
    
    async def process_merge_normalize_qa(self, extraction_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Complete Phase 5: Merge, Normalize & QA workflow
        
        Args:
            extraction_results: Results from Phase 4 LLM extraction
            
        Returns:
            Complete merged, normalized and QA'd results
        """
        print("Starting Phase 5: Merge, Normalize & QA...")
        
        # Step 1: Concatenate the three arrays
        combined_data = self.concatenate_arrays(extraction_results)
        
        # Step 2: Normalize units
        normalized_data = await self.normalize_units(combined_data)
        
        # Step 3: Dedupe & Sort
        final_data = self.deduplicate_and_sort(normalized_data)
        
        # Step 4: Run QA checks
        qa_results = self.run_qa_checks(final_data)
        
        # Compile final results
        results = {
            'success': True,
            'phase': 'Phase 5: Merge, Normalize & QA',
            'processing_stats': {
                'original_items': len(combined_data),
                'normalized_items': len(normalized_data),
                'final_items': len(final_data),
                'duplicates_removed': len(normalized_data) - len(final_data)
            },
            'final_data': final_data,
            'qa_results': qa_results,
            'metadata': extraction_results.get('metadata', {})
        }
        
        print(f"Phase 5 completed: {len(final_data)} items processed and validated")
        return results
    
    def save_results(self, results: Dict[str, Any], output_path: str) -> bool:
        """Save the final merged and normalized results"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            
            print(f"Phase 5 results saved to: {output_path}")
            return True
            
        except Exception as e:
            print(f"Error saving Phase 5 results: {e}")
            return False


# Synchronous wrapper
def merge_normalize_qa_from_extraction(extraction_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synchronous wrapper for merge, normalize and QA processing
    
    Args:
        extraction_results: Results from Phase 4 LLM extraction
        
    Returns:
        Merged, normalized and QA'd results
    """
    processor = MergeNormalizeQA()
    return asyncio.run(processor.process_merge_normalize_qa(extraction_results))


# Testing function
async def test_merge_normalize_qa():
    """Test the merge, normalize and QA processor with sample data"""
    print("Testing Merge, Normalize & QA Processor")
    print("=" * 50)
    
    # Sample extraction results from Phase 4
    sample_extraction_results = {
        'tables_extracted': [
            {
                'page': 1,
                'section': 'Table',
                'row_id': 1,
                'column': 'Company',
                'value': 'Life360',
                'unit': '',
                'context': 'header'
            },
            {
                'page': 1,
                'section': 'Table',
                'row_id': 2,
                'column': 'Revenue',
                'value': '$115.5 million',
                'unit': '',
                'context': 'data'
            },
            {
                'page': 1,
                'section': 'Table',
                'row_id': 3,
                'column': 'Growth',
                'value': '35%',
                'unit': '',
                'context': 'data'
            }
        ],
        'keyvalues_extracted': [
            {
                'page': 1,
                'section': 'KeyValue',
                'row_id': 1,
                'column': 'Total Users',
                'value': '66.9 million',
                'unit': '',
                'context': ''
            },
            {
                'page': 1,
                'section': 'KeyValue',
                'row_id': 2,
                'column': 'MAU Growth',
                'value': '33%',
                'unit': '',
                'context': ''
            }
        ],
        'narrative_extracted': [
            {
                'page': 1,
                'section': 'Narrative',
                'row_id': 1,
                'column': 'text',
                'value': 'Life360 is a leading family safety company.',
                'unit': '',
                'context': ''
            },
            # Duplicate for testing deduplication
            {
                'page': 1,
                'section': 'Table',
                'row_id': 1,
                'column': 'Company',
                'value': 'Life360',
                'unit': '',
                'context': 'header'
            }
        ],
        'metadata': {
            'job_id': 'test-merge-qa-123',
            'total_pages': 1
        }
    }
    
    processor = MergeNormalizeQA()
    results = await processor.process_merge_normalize_qa(sample_extraction_results)
    
    print(f"\nMerge & QA completed:")
    print(f"  Original items: {results['processing_stats']['original_items']}")
    print(f"  Final items: {results['processing_stats']['final_items']}")
    print(f"  Duplicates removed: {results['processing_stats']['duplicates_removed']}")
    print(f"  QA success rate: {results['qa_results']['summary']['success_rate']}%")
    
    if results['qa_results']['summary']['total_failed'] > 0:
        print(f"  QA failures: {results['qa_results']['summary']['total_failed']}")
        for error in results['qa_results']['errors']:
            print(f"    - {error}")
    
    # Save results
    success = processor.save_results(results, "output/test_merge_qa_results.json")
    print(f"Results saved: {success}")
    
    return results


if __name__ == "__main__":
    # Test the processor
    asyncio.run(test_merge_normalize_qa())