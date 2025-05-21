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
        
        Return a JSON object with a 'data' property containing an array of objects.
        Each object in the array represents a row in the table, and each key in the object represents a column.
        All objects should have the same set of keys.
        
        Here is the extracted text:
        {text}
        
        Return a JSON object with this structure:
        {{
          "data": [
            {{ "column1": "value1", "column2": "value2", ... }},
            {{ "column1": "value3", "column2": "value4", ... }},
            ...
          ]
        }}
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
        if response and response.choices and len(response.choices) > 0:
            response_content = response.choices[0].message.content
            
            # Parse the JSON
            if response_content:
                structured_data = json.loads(response_content)
                
                # Ensure we have the data in the expected format
                if "data" in structured_data and isinstance(structured_data["data"], list):
                    return structured_data["data"]
                elif isinstance(structured_data, list):
                    return structured_data
                else:
                    # If we got a JSON object but not in the expected format, try to extract data
                    for key, value in structured_data.items():
                        if isinstance(value, list) and len(value) > 0:
                            return value
                    
                    # If we couldn't find a suitable list, wrap the whole object in a list
                    return [structured_data]
            else:
                raise ValueError("Empty response from OpenAI API")
        else:
            raise ValueError("Invalid response from OpenAI API")
    
    except Exception as e:
        raise Exception(f"Error processing text with LLM: {str(e)}")