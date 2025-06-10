import json
import os
from openai import OpenAI
from typing import Dict, Any, List
import asyncio
import aiohttp
import concurrent.futures

# the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
# do not change this unless explicitly requested by the user
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def split_text_section(text_lines, max_lines=20):
    """Split text lines into manageable chunks"""
    chunks = []
    for i in range(0, len(text_lines), max_lines):
        chunk = text_lines[i:i + max_lines]
        chunks.append(chunk)
    return chunks

async def process_table_data(table_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process table data with GPT-4o asynchronously"""
    prompt = f"""You are a data analyst. The following table data has been extracted from a document.

Analyze this table and extract the actual data values. Create a clean, structured representation where each meaningful piece of information is clearly identified with appropriate field names.

Table data:
{json.dumps(table_data, indent=2)}

Return the result as a simple JSON object with field-value pairs. Extract actual numbers, dates, percentages, and text values. Do not return nested arrays or complex structures."""

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: openai_client.chat.completions.create(
                model="gpt-4o",
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
    """Process key-value pairs with GPT-4o asynchronously"""
    prompt = f"""You are a data extraction specialist. Below are key-value pairs extracted from a document.

Extract and organize this information into clear field-value pairs. Focus on extracting actual data values like company names, dates, amounts, percentages, and other factual information.

Key-Value pairs:
{json.dumps(key_value_pairs, indent=2)}

Return a simple JSON object where each key is a descriptive field name and each value is the actual extracted data. Do not create nested structures or arrays."""

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: openai_client.chat.completions.create(
                model="gpt-4o",
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
    """Process a text chunk with GPT-4o asynchronously"""
    text_content = '\n'.join(text_chunk)
    
    prompt = f"""You are a financial document analyst. Extract specific measurable data from this text segment.

Focus on extracting concrete facts such as:
- Company names and dates
- Financial figures (revenue, profit, growth rates)
- User metrics (MAU, DAU, subscriber counts)
- Percentages and ratios
- Market data and statistics

Text:
{text_content}

Return a simple JSON object with descriptive field names and actual values. Example:
{{"Company_Name": "Life360", "Q4_Revenue": "$115.5 million", "MAU_Growth": "33%"}}

Do not create nested objects or arrays."""

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: openai_client.chat.completions.create(
                model="gpt-4o",
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
            "extracted_facts": result,
            "original_text": text_chunk
        }
    except Exception as e:
        print(f"Error processing text chunk: {e}")
        return {
            "extracted_facts": {"error": str(e)},
            "original_text": text_chunk
        }

async def process_structured_data_with_llm_async(structured_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process all sections of structured data with asynchronous LLM calls"""
    
    document_text = structured_data.get('document_text', [])
    tables = structured_data.get('tables', [])
    key_values = structured_data.get('key_values', [])
    
    results = {
        "processed_tables": [],
        "processed_key_values": {},
        "processed_document_text": [],
        "summary": {
            "total_tables": len(tables),
            "total_key_values": len(key_values),
            "total_text_lines": len(document_text),
            "text_chunks_processed": 0
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
    
    return results

def process_structured_data_with_llm(structured_data: Dict[str, Any]) -> Dict[str, Any]:
    """Synchronous wrapper for asynchronous processing"""
    return asyncio.run(process_structured_data_with_llm_async(structured_data))