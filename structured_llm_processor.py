import json
import os
from openai import OpenAI
from typing import Dict, Any, List
import asyncio
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

def process_table_data(table_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process table data with GPT-4o"""
    prompt = f"""You are a data analyst. The following table data has been extracted from a document.

Your task is to convert this into structured JSON format. Use the headers if present, and infer meaningful field names if not. Ensure the result is consistent and each row is a dictionary of field-value pairs.

Table data:
{json.dumps(table_data, indent=2)}

Return only valid JSON."""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
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

def process_key_value_data(key_value_pairs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process key-value pairs with GPT-4o"""
    prompt = f"""You are a data structuring assistant. Below is a list of key-value pairs extracted from a document.

Your job is to tabulate this data into a clean JSON format. Normalize similar keys if needed, but do not lose any data.

Key-Value pairs:
{json.dumps(key_value_pairs, indent=2)}

Return only valid JSON."""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
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

def process_text_chunk(text_chunk: List[str]) -> Dict[str, Any]:
    """Process a text chunk with GPT-4o"""
    text_content = '\n'.join(text_chunk)
    
    prompt = f"""You are a financial document interpreter. The following is a segment from a business report.

Extract all measurable facts, statistics, and financial KPIs as structured JSON. Each entry should have a 'field' and a 'value'. Do not guess or infer anything beyond what's stated.

Text:
{text_content}

Return only valid JSON."""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
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

def process_structured_data_with_llm(structured_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process all sections of structured data with separate LLM calls"""
    
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
    
    # Process tables asynchronously
    print(f"Processing {len(tables)} tables...")
    for table in tables:
        processed_table = process_table_data(table)
        results["processed_tables"].append(processed_table)
    
    # Process key-value pairs
    if key_values:
        print(f"Processing {len(key_values)} key-value pairs...")
        results["processed_key_values"] = process_key_value_data(key_values)
    
    # Process document text in chunks
    if document_text:
        text_chunks = split_text_section(document_text, max_lines=20)
        print(f"Processing document text in {len(text_chunks)} chunks...")
        
        for chunk in text_chunks:
            processed_chunk = process_text_chunk(chunk)
            results["processed_document_text"].append(processed_chunk)
        
        results["summary"]["text_chunks_processed"] = len(text_chunks)
    
    return results