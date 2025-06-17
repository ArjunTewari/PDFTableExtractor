import json
import os
import asyncio
from typing import Dict, Any, List, Union
from openai import OpenAI
import concurrent.futures

# the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
# do not change this unless explicitly requested by the user
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY)

class LLMStructuredExtractor:
    def __init__(self, schema_path: str = "schema.json"):
        """Initialize the LLM structured extractor with schema"""
        self.schema_path = schema_path
        self.schema = self._load_schema()
        
    def _load_schema(self) -> Dict[str, Any]:
        """Load the canonical JSON schema"""
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
    
    def _create_table_extraction_prompt(self, table_data: Dict[str, Any]) -> str:
        """Create optimized prompt for table extraction"""
        schema_example = {
            "page": 1,
            "section": "Table",
            "row_id": 1,
            "column": "header",
            "value": "example_value",
            "unit": "USD",
            "context": "label"
        }
        
        prompt = f"""You are a financial document analyst. Extract ALL data from this table into the canonical schema format.

CRITICAL INSTRUCTIONS:
- Output ONLY valid JSON matching the provided schema
- Extract EVERY cell value, including headers and data
- Do not skip any rows or columns
- Use descriptive column names for headers
- Assign sequential row_id numbers starting from 1
- Include page number and section information
- Extract units from values (e.g., "$100M" → value="100M", unit="USD")
- Use table context as labels for the context field

CANONICAL SCHEMA FORMAT:
Each item must have: page (integer), section (string), row_id (integer), column (string), value (string), unit (string), context (string)

EXAMPLE OUTPUT FORMAT:
{json.dumps([schema_example], indent=2)}

TABLE DATA TO EXTRACT:
Page: {table_data.get('page', 1)}
Table ID: {table_data.get('table_id', 'unknown')}
Raw Data: {json.dumps(table_data.get('raw_data', {}), indent=2)}
Relationships: {json.dumps(table_data.get('relationships', []), indent=2)}

EXTRACT ALL TABLE CELLS INTO JSON ARRAY:"""
        
        return prompt
    
    def _create_keyvalue_extraction_prompt(self, kv_data: Dict[str, Any]) -> str:
        """Create optimized prompt for key-value extraction"""
        schema_example = {
            "page": 1,
            "section": "KeyValue",
            "row_id": 1,
            "column": "key_name",
            "value": "key_value",
            "unit": "",
            "context": ""
        }
        
        prompt = f"""You are a financial document analyst. Extract ALL key-value pairs into the canonical schema format.

CRITICAL INSTRUCTIONS:
- Output ONLY valid JSON matching the provided schema
- Extract EVERY key-value pair completely
- Use the key as the column name
- Use the value as the value field
- Assign sequential row_id numbers starting from 1
- Extract units from values if present
- Leave context empty unless there's additional descriptive text

CANONICAL SCHEMA FORMAT:
Each item must have: page (integer), section (string), row_id (integer), column (string), value (string), unit (string), context (string)

EXAMPLE OUTPUT FORMAT:
{json.dumps([schema_example], indent=2)}

KEY-VALUE DATA TO EXTRACT:
Page: {kv_data.get('page', 1)}
KV ID: {kv_data.get('kv_id', 'unknown')}
Raw Data: {json.dumps(kv_data.get('raw_data', {}), indent=2)}

EXTRACT ALL KEY-VALUE PAIRS INTO JSON ARRAY:"""
        
        return prompt
    
    def _create_narrative_extraction_prompt(self, narrative_data: Dict[str, Any]) -> str:
        """Create optimized prompt for narrative text extraction"""
        schema_example = {
            "page": 1,
            "section": "Narrative",
            "row_id": 1,
            "column": "text",
            "value": "line_content",
            "unit": "",
            "context": ""
        }
        
        prompt = f"""You are a financial document analyst. Extract narrative text lines into the canonical schema format.

CRITICAL INSTRUCTIONS:
- Output ONLY valid JSON matching the provided schema
- Extract EACH text line as a separate entry
- Use "text" as the column name for all entries
- Use the actual line content as the value
- Assign sequential row_id numbers starting from 1
- Leave unit empty unless specific units are mentioned in the text
- Leave context empty for plain narrative text

CANONICAL SCHEMA FORMAT:
Each item must have: page (integer), section (string), row_id (integer), column (string), value (string), unit (string), context (string)

EXAMPLE OUTPUT FORMAT:
{json.dumps([schema_example], indent=2)}

NARRATIVE DATA TO EXTRACT:
Page: {narrative_data.get('page', 1)}
Chunk ID: {narrative_data.get('chunk_id', 'unknown')}
Text Lines: {json.dumps(narrative_data.get('text_lines', []), indent=2)}

EXTRACT ALL TEXT LINES INTO JSON ARRAY:"""
        
        return prompt
    
    async def extract_tables_async(self, tables_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract data from tables batch with streaming support"""
        print(f"Processing {len(tables_batch)} tables with LLM...")
        all_results = []
        
        async def process_single_table(table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
            """Process a single table"""
            try:
                prompt = self._create_table_extraction_prompt(table_data)
                
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: openai_client.chat.completions.create(
                        model="gpt-4o",
                        messages=[
                            {
                                "role": "system", 
                                "content": "You are a precise data extraction specialist. Output only JSON matching the provided schema. No prose."
                            },
                            {"role": "user", "content": prompt}
                        ],
                        response_format={"type": "json_object"},
                        stream=True
                    )
                )
                
                # Collect streaming response
                content = ""
                for chunk in response:
                    if chunk.choices[0].delta.content:
                        content += chunk.choices[0].delta.content
                
                # Parse and validate JSON
                if content.strip():
                    result = json.loads(content)
                    if isinstance(result, dict) and "data" in result:
                        return result["data"]
                    elif isinstance(result, list):
                        return result
                    else:
                        # Wrap single object in array
                        return [result] if result else []
                else:
                    print(f"Empty response for table {table_data.get('table_id', 'unknown')}")
                    return []
                    
            except Exception as e:
                print(f"Error processing table {table_data.get('table_id', 'unknown')}: {e}")
                return []
        
        # Process tables concurrently
        tasks = [process_single_table(table) for table in tables_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Flatten results
        for result in results:
            if isinstance(result, list):
                all_results.extend(result)
            elif isinstance(result, Exception):
                print(f"Table processing exception: {result}")
        
        return all_results
    
    async def extract_keyvalues_async(self, kvs_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract data from key-values batch with streaming support"""
        print(f"Processing {len(kvs_batch)} key-value pairs with LLM...")
        all_results = []
        
        async def process_single_kv(kv_data: Dict[str, Any]) -> List[Dict[str, Any]]:
            """Process a single key-value pair"""
            try:
                prompt = self._create_keyvalue_extraction_prompt(kv_data)
                
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: openai_client.chat.completions.create(
                        model="gpt-4o",
                        messages=[
                            {
                                "role": "system", 
                                "content": "You are a precise data extraction specialist. Output only JSON matching the provided schema. No prose."
                            },
                            {"role": "user", "content": prompt}
                        ],
                        response_format={"type": "json_object"},
                        stream=True
                    )
                )
                
                # Collect streaming response
                content = ""
                for chunk in response:
                    if chunk.choices[0].delta.content:
                        content += chunk.choices[0].delta.content
                
                # Parse and validate JSON
                if content.strip():
                    result = json.loads(content)
                    if isinstance(result, dict) and "data" in result:
                        return result["data"]
                    elif isinstance(result, list):
                        return result
                    else:
                        return [result] if result else []
                else:
                    print(f"Empty response for KV {kv_data.get('kv_id', 'unknown')}")
                    return []
                    
            except Exception as e:
                print(f"Error processing KV {kv_data.get('kv_id', 'unknown')}: {e}")
                return []
        
        # Process key-values concurrently
        tasks = [process_single_kv(kv) for kv in kvs_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Flatten results
        for result in results:
            if isinstance(result, list):
                all_results.extend(result)
            elif isinstance(result, Exception):
                print(f"KV processing exception: {result}")
        
        return all_results
    
    async def extract_narrative_async(self, narrative_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract data from narrative batch with streaming support"""
        print(f"Processing {len(narrative_batch)} narrative chunks with LLM...")
        all_results = []
        
        async def process_single_narrative(narrative_data: Dict[str, Any]) -> List[Dict[str, Any]]:
            """Process a single narrative chunk"""
            try:
                prompt = self._create_narrative_extraction_prompt(narrative_data)
                
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: openai_client.chat.completions.create(
                        model="gpt-4o",
                        messages=[
                            {
                                "role": "system", 
                                "content": "You are a precise data extraction specialist. Output only JSON matching the provided schema. No prose."
                            },
                            {"role": "user", "content": prompt}
                        ],
                        response_format={"type": "json_object"},
                        stream=True
                    )
                )
                
                # Collect streaming response
                content = ""
                for chunk in response:
                    if chunk.choices[0].delta.content:
                        content += chunk.choices[0].delta.content
                
                # Parse and validate JSON
                if content.strip():
                    result = json.loads(content)
                    if isinstance(result, dict) and "data" in result:
                        return result["data"]
                    elif isinstance(result, list):
                        return result
                    else:
                        return [result] if result else []
                else:
                    print(f"Empty response for narrative {narrative_data.get('chunk_id', 'unknown')}")
                    return []
                    
            except Exception as e:
                print(f"Error processing narrative {narrative_data.get('chunk_id', 'unknown')}: {e}")
                return []
        
        # Process narrative chunks concurrently
        tasks = [process_single_narrative(chunk) for chunk in narrative_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Flatten results
        for result in results:
            if isinstance(result, list):
                all_results.extend(result)
            elif isinstance(result, Exception):
                print(f"Narrative processing exception: {result}")
        
        return all_results
    
    async def process_structured_extraction(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Phase 4: LLM - Structured Extraction
        Process the chunked payload through three parallel GPT calls
        """
        print("Starting Phase 4: LLM - Structured Extraction...")
        
        raw_data = payload.get("raw", {})
        tables = raw_data.get("tables", [])
        kvs = raw_data.get("kvs", [])
        text_chunks = raw_data.get("text_chunks", [])
        
        # Create concurrent tasks for all three extraction types
        tasks = []
        
        if tables:
            tasks.append(("tables", self.extract_tables_async(tables)))
        
        if kvs:
            tasks.append(("kvs", self.extract_keyvalues_async(kvs)))
        
        if text_chunks:
            tasks.append(("narrative", self.extract_narrative_async(text_chunks)))
        
        # Execute all tasks concurrently
        print(f"Executing {len(tasks)} parallel LLM extraction tasks...")
        task_results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
        
        # Organize results
        structured_results = {
            "tables_extracted": [],
            "keyvalues_extracted": [],
            "narrative_extracted": [],
            "total_extracted_items": 0,
            "processing_stats": {
                "tables_processed": len(tables),
                "kvs_processed": len(kvs),
                "narrative_chunks_processed": len(text_chunks),
                "extraction_errors": 0
            }
        }
        
        # Map results back to categories
        for i, (category, _) in enumerate(tasks):
            result = task_results[i]
            if isinstance(result, list):
                structured_results[f"{category}_extracted"] = result
                structured_results["total_extracted_items"] += len(result)
            elif isinstance(result, Exception):
                print(f"Error in {category} extraction: {result}")
                structured_results["processing_stats"]["extraction_errors"] += 1
        
        # Combine all extracted data
        all_extracted_data = []
        all_extracted_data.extend(structured_results["tables_extracted"])
        all_extracted_data.extend(structured_results["keyvalues_extracted"])
        all_extracted_data.extend(structured_results["narrative_extracted"])
        
        # Add metadata
        structured_results["combined_extracted_data"] = all_extracted_data
        structured_results["metadata"] = payload.get("metadata", {})
        structured_results["schema"] = self.schema
        
        print(f"Phase 4 completed: Extracted {len(all_extracted_data)} total items")
        return structured_results
    
    def save_extraction_results(self, results: Dict[str, Any], output_path: str) -> bool:
        """Save extraction results to JSON file"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            
            print(f"Extraction results saved to: {output_path}")
            return True
            
        except Exception as e:
            print(f"Error saving extraction results: {e}")
            return False


# Synchronous wrapper
def extract_structured_data_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synchronous wrapper for structured extraction
    
    Args:
        payload: Chunked payload from Phase 3
        
    Returns:
        Structured extraction results
    """
    extractor = LLMStructuredExtractor()
    return asyncio.run(extractor.process_structured_extraction(payload))


# Testing function
async def test_llm_structured_extractor():
    """Test the LLM structured extractor with sample data"""
    print("Testing LLM Structured Extractor")
    print("=" * 50)
    
    # Sample payload from Phase 3
    sample_payload = {
        "schema": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "page": {"type": "integer"},
                    "section": {"type": "string"},
                    "row_id": {"type": "integer"},
                    "column": {"type": "string"},
                    "value": {"type": "string"},
                    "unit": {"type": "string"},
                    "context": {"type": "string"}
                },
                "required": ["page", "section", "column", "value"]
            }
        },
        "raw": {
            "tables": [
                {
                    "page": 1,
                    "table_id": "page_1_table_1",
                    "block_type": "TABLE",
                    "raw_data": {
                        "rows": [
                            ["Company", "Revenue", "Growth"],
                            ["Life360", "$115.5M", "35%"],
                            ["Competitor A", "$85M", "20%"]
                        ]
                    },
                    "relationships": [
                        {"type": "CELL", "row": 0, "column": 0, "value": "Company"},
                        {"type": "CELL", "row": 0, "column": 1, "value": "Revenue"},
                        {"type": "CELL", "row": 0, "column": 2, "value": "Growth"}
                    ]
                }
            ],
            "kvs": [
                {
                    "page": 1,
                    "kv_id": "page_1_kv_1",
                    "block_type": "KEY_VALUE_SET",
                    "raw_data": {"key": "Total Users", "value": "66.9 million"}
                }
            ],
            "text_chunks": [
                {
                    "page": 1,
                    "chunk_id": "page_1_chunk_1",
                    "block_type": "NARRATIVE",
                    "text_lines": [
                        "Life360 is a leading family safety company.",
                        "The company reported strong Q4 results."
                    ]
                }
            ]
        },
        "metadata": {
            "total_pages": 1,
            "job_id": "test-job-123"
        }
    }
    
    extractor = LLMStructuredExtractor()
    results = await extractor.process_structured_extraction(sample_payload)
    
    print(f"Extraction completed:")
    print(f"  Tables extracted: {len(results['tables_extracted'])}")
    print(f"  Key-values extracted: {len(results['keyvalues_extracted'])}")
    print(f"  Narrative extracted: {len(results['narrative_extracted'])}")
    print(f"  Total items: {results['total_extracted_items']}")
    
    # Save results
    success = extractor.save_extraction_results(results, "output/test_extraction_results.json")
    print(f"Results saved: {success}")
    
    return results


if __name__ == "__main__":
    # Test the extractor
    asyncio.run(test_llm_structured_extractor())