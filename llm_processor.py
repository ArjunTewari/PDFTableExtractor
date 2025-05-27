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

        # Build the optimized prompt for LlamaParse output analysis
        system_prompt = """
        You are an expert data extraction specialist working with high-quality text extracted using LlamaParse technology. The input text preserves excellent structure and formatting from the original PDF.

        Your task:
        1. Thoroughly analyze ALL the provided text content
        2. Extract every piece of meaningful information
        3. Organize it into a comprehensive structured table
        4. Create logical categories that capture all important data points
        5. Ensure no valuable information is lost

        Table Structure:
        - Use "Category" as the first column for descriptive labels
        - Use "Value 1", "Value 2", "Value 3", etc. for additional columns as needed
        - Each row represents a distinct piece of information or data point
        - Group related information logically under appropriate categories

        Extraction Guidelines:
        - Be exhaustive - extract ALL relevant information including names, dates, numbers, addresses, descriptions, titles, amounts, etc.
        - Preserve exact values, numbers, and proper nouns with complete accuracy
        - Create meaningful category names that clearly describe the information type
        - Handle multiple values per category by using separate Value columns
        - Include structural information like sections, headers, or document metadata when relevant
        - Process tables, lists, and structured data with careful attention to detail

        Your output must be a valid JSON object with this exact structure:
        {
          "data": [
            {
              "Category": "category_name",
              "Value 1": "first_value",
              "Value 2": "second_value",
              "Value 3": "third_value"
            }
          ]
        }

        Important: Return ONLY the JSON object, no additional text or explanations.
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
            messages=[{
                "role": "system",
                "content": system_prompt
            }, {
                "role": "user",
                "content": user_prompt
            }],
            temperature=0,
            response_format={"type": "json_object"})

        # Extract JSON from the response
        if response and response.choices and len(response.choices) > 0:
            response_content = response.choices[0].message.content

            # Parse the JSON
            if response_content:
                structured_data = json.loads(response_content)

                # Ensure we have the data in the expected format
                if "data" in structured_data and isinstance(
                        structured_data["data"], list):
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
