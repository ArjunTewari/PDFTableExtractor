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
        
        # Build the prompt using the specific document analysis instructions
        system_prompt = """
        You are a smart document analysis assistant.

        You will receive raw text extracted from a PDF file. Analyze the text carefully to:
        1. Detect and extract important entities and data points.
        2. Automatically identify the correct category or label based on context.
        3. Return the extracted information in a tabular format.

        Guidelines:
        - Use common NER categories like Person, Organization, Location, Date, Email, Designation, Document Type, Product Name, etc.
        - If you find a data point that doesn't fit standard categories, create a new one based on its context (e.g., "Company Website", "Patent ID", "Case Number").
        - Provide the full sentence or a meaningful snippet from the original text as context for each entry.
        - Avoid duplicate rows. Group similar entries if possible.
        
        Your output must be a valid JSON object with this exact structure:
        {
          "data": [
            { 
              "Category": "category_name", 
              "Extracted Text": "extracted_entity_or_data_point", 
              "Context / Sentence from Document": "original_context_sentence" 
            },
            ...more rows...
          ]
        }
        """
        
        user_prompt = f"""
        Here is the extracted text from a PDF document:

        {text}

        Analyze this text and extract information according to the guidelines.
        """
        
        # Send the prompt to the model
        # The newest OpenAI model is "gpt-4o" which was released May 13, 2024.
        # do not change this unless explicitly requested by the user
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
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