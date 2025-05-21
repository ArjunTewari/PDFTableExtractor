import os
import json
from openai import OpenAI

def process_text_with_llm(text):
    """
    Process the extracted text with an LLM to identify structured information.
    
    Args:
        text (str): The text extracted from the PDF
        
    Returns:
        list: A list of dictionaries containing structured data
    """
    # Get API key from environment
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OpenAI API key not found in environment variables")
    
    try:
        # Create OpenAI client
        client = OpenAI(api_key=api_key)
        
        # Build the prompt
        prompt = f"""
        I have extracted text from a PDF document. I need you to analyze the text 
        and extract structured information from it in a tabulated format.
        
        Please identify any relevant fields or categories of information in the text, and 
        organize them into a structured JSON format that can be easily displayed in a table.
        
        The JSON should be an array of objects, where each object represents a row in the table, 
        and each key represents a column. All objects should have the same set of keys.
        
        Here is the extracted text:
        {text}
        
        Based on this text, provide a structured JSON array with the appropriate keys and values.
        Return only the JSON array with no additional text.
        """
        
        # Send the prompt to the model
        # The newest OpenAI model is "gpt-4o" which was released May 13, 2024.
        # do not change this unless explicitly requested by the user
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        
        # Extract JSON from the response
        response_content = response.choices[0].message.content
        
        # Clean up the response to ensure it's valid JSON
        response_content = response_content.strip()
        if response_content.startswith("```json"):
            response_content = response_content.replace("```json", "", 1)
        if response_content.endswith("```"):
            response_content = response_content[:-3]
        response_content = response_content.strip()
        
        # Parse the JSON
        structured_data = json.loads(response_content)
        
        # Ensure we have the data in the expected format
        if "data" in structured_data:
            data = structured_data["data"]
        elif isinstance(structured_data, list):
            data = structured_data
        else:
            # Try to use whatever we got as a fallback
            data = structured_data
            
        # Ensure the result is a list
        if not isinstance(data, list):
            raise ValueError("Expected a JSON array response, but got a different format")
        
        return data
    
    except Exception as e:
        raise Exception(f"Error processing text with LLM: {str(e)}")