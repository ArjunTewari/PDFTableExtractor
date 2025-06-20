"""
Gemini Pro 1.5 processor for PDF text analysis
"""
import os
import json
import asyncio
from typing import Dict, Any, List
import google.generativeai as genai

# Configure Gemini
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))

async def process_table_data_gemini(table_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process table data with Gemini Pro 1.5"""
    try:
        model = genai.GenerativeModel('gemini-1.5-pro')
        
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

        response = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: model.generate_content(prompt)
        )
        
        # Extract JSON from response
        content = response.text
        # Try to extract JSON from the response
        if "```json" in content:
            json_start = content.find("```json") + 7
            json_end = content.find("```", json_start)
            content = content[json_start:json_end]
        elif "{" in content and "}" in content:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            content = content[json_start:json_end]
        
        result = json.loads(content)
        return result
        
    except Exception as e:
        print(f"Error processing table data with Gemini: {e}")
        return {"error": f"Failed to process table: {str(e)}"}

async def process_key_value_data_gemini(key_value_pairs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process key-value pairs with Gemini Pro 1.5"""
    try:
        model = genai.GenerativeModel('gemini-1.5-pro')
        
        prompt = f"""Process these key-value pairs into structured data.

Key-value pairs:
{json.dumps(key_value_pairs, indent=2)}

Extract meaningful field-value pairs from this data. Focus on financial information, dates, and important metrics.

Return a simple JSON object where each key is a descriptive field name and each value is the actual extracted data."""

        response = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: model.generate_content(prompt)
        )
        
        content = response.text
        if "```json" in content:
            json_start = content.find("```json") + 7
            json_end = content.find("```", json_start)
            content = content[json_start:json_end]
        elif "{" in content and "}" in content:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            content = content[json_start:json_end]
        
        result = json.loads(content)
        return {
            "structured_key_values": result,
            "original_pairs": key_value_pairs
        }
        
    except Exception as e:
        print(f"Error processing key-value pairs with Gemini: {e}")
        return {
            "structured_key_values": {"error": str(e)},
            "original_pairs": key_value_pairs
        }

async def process_text_chunk_gemini(text_chunk: List[str]) -> Dict[str, Any]:
    """Process a text chunk with Gemini Pro 1.5"""
    try:
        model = genai.GenerativeModel('gemini-1.5-pro')
        text_content = '\n'.join(text_chunk)
        
        prompt = f"""Extract key financial facts from this text as simple field-value pairs.

Text to analyze:
{text_content}

Instructions:
1. Extract important financial data, metrics, dates
2. Use clear field names
3. Keep values concise and accurate

Return JSON with facts:
{{
  "Revenue": "value",
  "Growth_Rate": "value",
  "Date": "value"
}}"""

        response = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: model.generate_content(prompt)
        )
        
        content = response.text
        if "```json" in content:
            json_start = content.find("```json") + 7
            json_end = content.find("```", json_start)
            content = content[json_start:json_end]
        elif "{" in content and "}" in content:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            content = content[json_start:json_end]
        
        result = json.loads(content)
        return {"extracted_facts": result}
        
    except Exception as e:
        print(f"Error processing text chunk with Gemini: {e}")
        return {"extracted_facts": {"error": str(e)}}

async def enhance_commentary_gemini(commentary: str, field: str, value: str) -> str:
    """Enhance commentary with more insightful analysis using Gemini"""
    try:
        model = genai.GenerativeModel('gemini-1.5-pro')
        
        prompt = f"""Improve this commentary by making it more insightful and less generic. Add potential implications, comparisons to previous quarters if available, and mention if the number is unusually high or low.

Field: {field}
Value: {value}
Original Commentary: {commentary}

Provide enhanced commentary (2-3 sentences max) that adds business context and insights."""

        response = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: model.generate_content(prompt)
        )
        
        enhanced = response.text.strip()
        return enhanced if enhanced else commentary
        
    except Exception as e:
        print(f"Error enhancing commentary with Gemini: {e}")
        return commentary