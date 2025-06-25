import json
import os
from openai import OpenAI
from typing import Dict, Any, List
import asyncio
import aiohttp
import concurrent.futures

# Using gpt-4o-mini for optimal performance and cost efficiency
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# GPT-4o-mini pricing per 1M tokens (as of 2024)
GPT_4O_MINI_INPUT_COST = 0.150  # $0.150 per 1M input tokens
GPT_4O_MINI_OUTPUT_COST = 0.600  # $0.600 per 1M output tokens

class CostTracker:
    def __init__(self):
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost = 0.0
        self.api_calls = 0
    
    def add_usage(self, input_tokens, output_tokens):
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        self.api_calls += 1
        
        input_cost = (input_tokens / 1_000_000) * GPT_4O_MINI_INPUT_COST
        output_cost = (output_tokens / 1_000_000) * GPT_4O_MINI_OUTPUT_COST
        call_cost = input_cost + output_cost
        self.total_cost += call_cost
        
        return call_cost
    
    def get_summary(self):
        return {
            'total_input_tokens': self.total_input_tokens,
            'total_output_tokens': self.total_output_tokens,
            'total_tokens': self.total_input_tokens + self.total_output_tokens,
            'total_cost_usd': round(self.total_cost, 6),
            'api_calls': self.api_calls,
            'input_cost_usd': round((self.total_input_tokens / 1_000_000) * GPT_4O_MINI_INPUT_COST, 6),
            'output_cost_usd': round((self.total_output_tokens / 1_000_000) * GPT_4O_MINI_OUTPUT_COST, 6)
        }

# Global cost tracker instance
cost_tracker = CostTracker()

def split_text_section(text_lines, max_lines=25):
    """Split text lines into manageable chunks with sentence boundary preservation"""
    chunks = []
    current_chunk = []
    
    for i, line in enumerate(text_lines):
        current_chunk.append(line)
        
        # Check if we should create a chunk
        if len(current_chunk) >= max_lines:
            # Try to end at a sentence boundary
            if line.strip().endswith(('.', '!', '?', ':')):
                chunks.append(current_chunk)
                current_chunk = []
            elif len(current_chunk) >= max_lines + 5:  # Force split if too long
                chunks.append(current_chunk)
                current_chunk = []
    
    # Add remaining lines
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks

async def process_table_data(table_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process table data with GPT-4o-mini asynchronously - simple format"""
    prompt = f"""Extract key data points from this table as simple field-value pairs.

Table data:
{json.dumps(table_data, indent=2)}

Instructions:
1. Extract important data points as field-value pairs
2. Use clear, descriptive field names
3. Focus on financial figures, dates, and key metrics
4. Keep it simple and straightforward

Return JSON with field-value pairs:
{{
  "Revenue": "value",
  "Growth_Rate": "value",
  "Date": "value"
}}"""

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
        )
        
        content = response.choices[0].message.content
        if content:
            result = json.loads(content)
        else:
            result = {"error": "No content received from OpenAI"}
            
        return {
            "page": table_data.get("page", 1),
            "structured_table": result,
            "original_rows": table_data.get("rows", [])
        }
    except Exception as e:
        print(f"Error processing table: {e}")
        return {
            "page": table_data.get("page", 1),
            "structured_table": {"error": str(e)},
            "original_rows": table_data.get("rows", [])
        }

async def process_key_value_data(key_value_pairs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process key-value pairs with GPT-4o-mini asynchronously"""
    prompt = f"""You are a data extraction specialist. Below are key-value pairs extracted from a document.

Extract and organize this information into clear field-value pairs. Focus on extracting actual data values like company names, dates, amounts, percentages, and other factual information.

Key-Value pairs:
{json.dumps(key_value_pairs, indent=2)}

Return a simple JSON object where each key is a descriptive field name and each value is the actual extracted data. Do not create nested structures or arrays. Provide the response as valid JSON format."""

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
        )
        
        # Track usage and cost
        if hasattr(response, 'usage') and response.usage:
            call_cost = cost_tracker.add_usage(
                response.usage.prompt_tokens,
                response.usage.completion_tokens
            )
            print(f"Key-value processing cost: ${call_cost:.6f}")
        
        content = response.choices[0].message.content
        if content:
            result = json.loads(content)
        else:
            result = {"error": "No content received from OpenAI"}
            
        return {
            "structured_key_values": result,
            "original_pairs": key_value_pairs
        }
    except Exception as e:
        print(f"Error processing key-value pairs: {e}")
        return {
            "structured_key_values": {"error": str(e)},
            "original_pairs": key_value_pairs
        }

async def process_text_chunk(text_chunk: List[str]) -> Dict[str, Any]:
    """Process a text chunk with GPT-4o-mini asynchronously and tabulate the content"""
    text_content = '\n'.join(text_chunk)
    
    prompt = f"""You are a financial document analyst. Extract and tabulate ALL meaningful data from this text segment.

Create a comprehensive table structure that captures the key information in a tabulated format.

Text:
{text_content}

Requirements:
1. Extract ALL meaningful data points and organize them into a table structure
2. Create appropriate column headers based on the content type
3. Structure data into logical rows and columns
4. Include financial metrics, dates, percentages, company info, etc.
5. If the text contains narrative information, extract key facts and tabulate them
6. IGNORE superscript numbers and footnote reference markers (¹²³ or (1)(2)(3) or [1][2][3])
7. Extract clean data values without footnote symbols

Return JSON with BOTH table structure AND individual facts:
{{
  "table_headers": ["Metric", "Value", "Period", "Context"],
  "table_rows": [
    ["Revenue", "$115.5M", "Q4 2023", "33% growth"],
    ["MAU", "65.8M", "Q4 2023", "Global users"],
    ["Market Share", "12%", "2023", "Primary market"]
  ],
  "extracted_facts": {{
    "Company_Name": "Life360",
    "Q4_Revenue": "$115.5 million",
    "MAU_Growth": "33%",
    "Market_Position": "Leading family safety platform"
  }}
}}

Extract comprehensive data - do not limit to just a few items. Return the response as valid JSON format."""

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
        )
        
        # Track usage and cost
        if hasattr(response, 'usage') and response.usage:
            call_cost = cost_tracker.add_usage(
                response.usage.prompt_tokens,
                response.usage.completion_tokens
            )
            print(f"Text chunk processing cost: ${call_cost:.6f}")
        
        content = response.choices[0].message.content
        if content:
            result = json.loads(content)
        else:
            result = {"error": "No content received from OpenAI"}
            
        return {
            "table_headers": result.get("table_headers", []),
            "table_rows": result.get("table_rows", []),
            "extracted_facts": result.get("extracted_facts", {}),
            "original_text": text_chunk
        }
    except Exception as e:
        print(f"Error processing text chunk: {e}")
        return {
            "extracted_facts": {"error": str(e)},
            "original_text": text_chunk
        }

async def match_commentary_to_data(row_data: str, text_chunks: List[str]) -> Dict[str, Any]:
    """Match document text commentary to table row data"""
    text_content = '\n'.join(text_chunks)
    
    prompt = f"""You are analyzing document text to find commentary that directly explains or relates to a specific data point.

DATA POINT: {row_data}

DOCUMENT TEXT TO ANALYZE:
{text_content}

STRICT REQUIREMENTS:
1. Only return commentary if the text DIRECTLY mentions, explains, or provides context about this specific data point
2. The commentary must contain clear references to the field name, value, or related concepts
3. Ignore text that talks about general topics not related to this specific data
4. Do not return commentary that starts mid-sentence or lacks context
5. If no relevant text is found, return null

Return JSON:
{{"commentary": "relevant explanation that directly relates to the data point", "relevant": true}}
OR  
{{"commentary": null, "relevant": false}}

Be very strict - only return commentary that clearly and directly relates to the specific data point provided."""

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
        )
        
        content = response.choices[0].message.content
        if content:
            result = json.loads(content)
            return result
        else:
            return {"commentary": None, "relevant": False}
            
    except Exception as e:
        print(f"Error matching commentary: {e}")
        return {"commentary": None, "relevant": False}

async def process_structured_data_with_llm_async(structured_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process all sections of structured data with asynchronous LLM calls"""
    
    document_text = structured_data.get('document_text', [])
    tables = structured_data.get('tables', [])
    key_values = structured_data.get('key_values', [])
    
    results = {
        "processed_tables": [],
        "processed_key_values": {},
        "processed_document_text": [],
        "enhanced_data_with_commentary": [],
        "general_commentary": "",
        "summary": {
            "total_tables": len(tables),
            "total_key_values": len(key_values),
            "total_text_lines": len(document_text),
            "text_chunks_processed": 0,
            "commentary_matches": 0
        }
    }
    
    # Create tasks for asynchronous processing
    tasks = []
    
    # Process tables asynchronously
    if tables:
        print(f"Processing {len(tables)} tables asynchronously...")
        table_tasks = [process_table_data(table) for table in tables]
        tasks.extend(table_tasks)
    
    # Process key-value pairs
    if key_values:
        print(f"Processing {len(key_values)} key-value pairs asynchronously...")
        kv_task = process_key_value_data(key_values)
        tasks.append(kv_task)
    
    # Process document text in chunks
    text_tasks = []
    if document_text:
        text_chunks = split_text_section(document_text, max_lines=20)
        print(f"Processing document text in {len(text_chunks)} chunks asynchronously...")
        text_tasks = [process_text_chunk(chunk) for chunk in text_chunks]
        tasks.extend(text_tasks)
        results["summary"]["text_chunks_processed"] = len(text_chunks)
    
    # Execute all tasks concurrently
    if tasks:
        print(f"Executing {len(tasks)} LLM processing tasks concurrently...")
        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Organize results
        task_index = 0
        
        # Process table results
        if tables:
            for i in range(len(tables)):
                result = completed_tasks[task_index]
                if isinstance(result, Exception):
                    print(f"Table processing error: {result}")
                    result = {"error": str(result), "page": tables[i].get("page", 1)}
                results["processed_tables"].append(result)
                task_index += 1
        
        # Process key-value result
        if key_values:
            result = completed_tasks[task_index]
            if isinstance(result, Exception):
                print(f"Key-value processing error: {result}")
                result = {"error": str(result)}
            results["processed_key_values"] = result
            task_index += 1
        
        # Process text chunk results
        if text_tasks:
            for i in range(len(text_tasks)):
                result = completed_tasks[task_index]
                if isinstance(result, Exception):
                    print(f"Text chunk processing error: {result}")
                    result = {"error": str(result)}
                results["processed_document_text"].append(result)
                task_index += 1
    
    # Phase 2: Enhanced data processing with commentary matching
    print("Starting commentary matching phase...")
    await process_commentary_matching(results, document_text)
    
    return results

async def process_commentary_matching(results: Dict[str, Any], document_text: List[str]) -> None:
    """Process commentary matching for all extracted data"""
    enhanced_data = []
    used_text_indices = set()
    
    # Collect all structured data points
    all_data_points = []
    
    # From tables
    for table_result in results.get("processed_tables", []):
        if "structured_table" in table_result and not table_result["structured_table"].get("error"):
            structured_table = table_result["structured_table"]
            page = table_result.get("page", "N/A")
            
            for field, value in structured_table.items():
                if field != "error" and value:
                    all_data_points.append({
                        "source": "Table",
                        "type": "Table Data",
                        "field": field,
                        "value": str(value),
                        "page": page,
                        "raw_data": f"{field}: {value}"
                    })
    
    # From key-values
    if "processed_key_values" in results and results["processed_key_values"]:
        kv_data = results["processed_key_values"].get("structured_key_values", {})
        if kv_data and not kv_data.get("error"):
            for field, value in kv_data.items():
                if field != "error" and value:
                    all_data_points.append({
                        "source": "Key-Value Pairs",
                        "type": "Structured Data",
                        "field": field,
                        "value": str(value),
                        "page": "N/A",
                        "raw_data": f"{field}: {value}"
                    })
    
    # From document text facts
    for chunk_idx, chunk in enumerate(results.get("processed_document_text", [])):
        if "extracted_facts" in chunk and not chunk["extracted_facts"].get("error"):
            facts = chunk["extracted_facts"]
            for field, value in facts.items():
                if field != "error" and value:
                    all_data_points.append({
                        "source": f"Text Chunk {chunk_idx+1}",
                        "type": "Financial Data",
                        "field": field,
                        "value": str(value),
                        "page": "N/A",
                        "raw_data": f"{field}: {value}"
                    })
    
    # Process commentary matching for each data point
    text_chunks = split_text_section(document_text, max_lines=10)
    
    # Process each data point for commentary matching
    for data_point in all_data_points:
        best_commentary = ""
        found_match = False
        
        # Try to find commentary for this data point in each text chunk
        for chunk_idx, text_chunk in enumerate(text_chunks):
            if chunk_idx not in used_text_indices:
                try:
                    commentary_result = await match_commentary_to_data(data_point["raw_data"], text_chunk)
                    
                    if isinstance(commentary_result, dict) and commentary_result.get("relevant"):
                        used_text_indices.add(chunk_idx)
                        best_commentary = commentary_result.get("commentary", "")
                        found_match = True
                        results["summary"]["commentary_matches"] += 1
                        break  # Found a match, stop looking
                except Exception as e:
                    print(f"Error matching commentary for {data_point['field']}: {e}")
                    continue
        
        # Add the data point with or without commentary
        enhanced_data_point = {
            **data_point,
            "commentary": best_commentary,
            "has_commentary": found_match
        }
        enhanced_data.append(enhanced_data_point)
    
    # Create general commentary from unmatched text
    unmatched_text_chunks = []
    for i, chunk in enumerate(text_chunks):
        if i not in used_text_indices:
            unmatched_text_chunks.extend(chunk)
    
    if unmatched_text_chunks:
        general_commentary = '\n'.join(unmatched_text_chunks[:50])  # Limit length
        results["general_commentary"] = general_commentary
    
    # Remove duplicates and clean data
    seen_data = set()
    clean_enhanced_data = []
    for item in enhanced_data:
        key = f"{item['field']}_{item['value']}"
        if key not in seen_data:
            seen_data.add(key)
            clean_enhanced_data.append(item)
    
    results["enhanced_data_with_commentary"] = clean_enhanced_data

def process_structured_data_with_llm(structured_data: Dict[str, Any]) -> Dict[str, Any]:
    """Synchronous wrapper for asynchronous processing"""
    return asyncio.run(process_structured_data_with_llm_async(structured_data))