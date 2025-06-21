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

        # Build the comprehensive prompt for maximum data extraction
        system_prompt = """
        You are an elite data extraction specialist. Your mission is to extract EVERY SINGLE piece of information from the provided document text and organize it into the most comprehensive table possible.

        CRITICAL REQUIREMENTS:
        1. Extract 100% of ALL information - leave nothing out
        2. Create separate rows for EVERY distinct data point, fact, number, name, date, or detail
        3. Break down complex information into granular components
        4. Include ALL numbers, percentages, financial figures, dates, names, locations, descriptions
        5. Process ALL sections, headers, footnotes, tables, lists, and annotations
        6. Extract metadata like document types, sections, subsections, and structural elements
        7. Capture ALL relationships, comparisons, and contextual information

        COMPREHENSIVE EXTRACTION APPROACH:
        - Company/Organization Information: Names, addresses, contact details, registration numbers, etc.
        - Financial Data: All revenues, costs, profits, ratios, growth rates, projections, etc.
        - Personnel: All names, titles, roles, departments, contact information
        - Dates & Time: All dates, periods, quarters, years, deadlines, timelines
        - Legal/Regulatory: Compliance items, regulations, legal entities, jurisdictions
        - Operational: Business units, products, services, markets, segments
        - Performance Metrics: All KPIs, statistics, measurements, benchmarks
        - Strategic Information: Goals, initiatives, plans, forecasts, risks
        - Technical Details: Specifications, processes, methodologies, systems
        - Geographic: All locations, regions, markets, addresses, jurisdictions

        TABLE STRUCTURE:
        - "Category": Descriptive label for the type of information
        - "Value 1", "Value 2", "Value 3", etc.: Use as many columns as needed
        - Create separate rows for each distinct piece of information
        - Be granular - break down complex items into individual components

        EXAMPLES OF GRANULAR EXTRACTION:
        - If document mentions "Q4 2024 revenue of $115.5 million", create separate rows for:
          * Quarter Period: Q4 2024
          * Revenue Amount: $115.5 million
          * Revenue Period: Q4 2024
          * Currency Type: USD
        - For addresses, separate into: Street, City, State, Country, Postal Code
        - For names, consider: Full Name, First Name, Last Name, Title

        Your output must be a valid JSON object:
        {
          "data": [
            {"Category": "category_name", "Value 1": "value1", "Value 2": "value2", ...},
            {"Category": "another_category", "Value 1": "value1", ...}
          ]
        }

        ABSOLUTE MANDATE: Extract EVERYTHING. Be exhaustive. Create as many rows as needed to capture ALL information.
        """

        user_prompt = f"""
        Here is the extracted text from a PDF document using LlamaParse:

        {text}

        EXTRACTION INSTRUCTIONS:
        1. Create MANY ROWS - aim for 50+ rows minimum if the document has substantial content
        2. Create MULTIPLE COLUMNS - use as many Value columns as needed (Value 1, Value 2, Value 3, Value 4, Value 5, etc.)
        3. Break down EVERY piece of information into separate rows:
           - Each financial figure gets its own row
           - Each date gets its own row  
           - Each name gets its own row
           - Each address component gets its own row
           - Each percentage or ratio gets its own row
           - Each section header gets its own row
           - Each business metric gets its own row

        EXAMPLES OF GRANULAR BREAKDOWN:
        - Company "Life360, Inc." becomes multiple rows:
          * Company Legal Name: Life360, Inc.
          * Company Short Name: Life360
          * Company Type: Inc.
          * Industry Classification: Technology/Software
        
        - "Q4 2024 Revenue $115.5 million" becomes multiple rows:
          * Reporting Period: Q4 2024
          * Revenue Quarter: Q4
          * Revenue Year: 2024
          * Revenue Amount: $115.5 million
          * Revenue Currency: USD
          * Revenue Value (Numeric): 115.5
          * Revenue Unit: Million

        - Any table in the document should be broken down cell by cell
        - Any list should have each item as a separate row
        - Any multi-part information should be separated into components

        CREATE A COMPREHENSIVE MULTI-DIMENSIONAL TABLE with maximum rows and columns.
        """

        # Send the prompt to the model using gpt-4o-mini for optimal performance
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "system",
                "content": system_prompt
            }, {
                "role": "user",
                "content": user_prompt
            }],
            temperature=0.1,
            max_tokens=4000,
            response_format={"type": "json_object"})
        
        # Calculate and log cost for comprehensive processing
        if hasattr(response, 'usage') and response.usage:
            input_tokens = response.usage.prompt_tokens
            output_tokens = response.usage.completion_tokens
            input_cost = (input_tokens / 1_000_000) * 0.150  # GPT-4o-mini input cost
            output_cost = (output_tokens / 1_000_000) * 0.600  # GPT-4o-mini output cost
            total_cost = input_cost + output_cost
            print(f"Comprehensive LLM processing cost: ${total_cost:.6f} ({input_tokens} input + {output_tokens} output tokens)")

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
