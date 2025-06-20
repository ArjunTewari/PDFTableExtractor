"""
Simple Gemini processor for synchronous operations
"""
import os
import json
import google.generativeai as genai

# Configure Gemini
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))

def process_with_gemini_sync(prompt: str) -> dict:
    """Process text with Gemini Pro 1.5 synchronously with fallback"""
    try:
        model = genai.GenerativeModel('gemini-1.5-pro')
        response = model.generate_content(prompt)
        
        content = response.text
        if "```json" in content:
            json_start = content.find("```json") + 7
            json_end = content.find("```", json_start)
            content = content[json_start:json_end]
        elif "{" in content and "}" in content:
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            content = content[json_start:json_end]
        
        return json.loads(content)
    except Exception as e:
        error_msg = str(e)
        print(f"Gemini processing error: {error_msg}")
        
        # Check if quota exceeded
        if "quota" in error_msg.lower() or "429" in error_msg:
            print("Gemini quota exceeded - API key needs billing setup or has reached free tier limits")
            return {"error": "Gemini quota exceeded - please check your billing plan"}
        
        return {"error": str(e)}

def enhance_commentary_sync(commentary: str, field: str, value: str) -> str:
    """Enhance commentary synchronously"""
    try:
        prompt = f"""Improve this commentary by making it more insightful and less generic. Add potential implications, comparisons to previous quarters if available, and mention if the number is unusually high or low.

Field: {field}
Value: {value}
Original Commentary: {commentary}

Provide enhanced commentary (2-3 sentences max) that adds business context and insights."""

        model = genai.GenerativeModel('gemini-1.5-pro')
        response = model.generate_content(prompt)
        enhanced = response.text.strip()
        return enhanced if enhanced else commentary
    except Exception as e:
        print(f"Commentary enhancement error: {e}")
        return commentary