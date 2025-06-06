import os
import json
from typing import Dict, List, Any
from openai import OpenAI
from storage_manager import storage_manager

class AgenticProcessor:
    def __init__(self):
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    def analyze_text_data(self, text: str) -> Dict[str, Any]:
        """Analyze extracted text to identify all data points"""
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "system",
                "content": """You are a data analysis specialist. Analyze the text and identify:
                1. All distinct data points (numbers, dates, names, amounts, etc.)
                2. Categories and classifications
                3. Relationships between data points
                4. Structural information (headers, sections, etc.)
                
                Return detailed analysis in JSON format."""
            }, {
                "role": "user", 
                "content": f"Analyze this text comprehensively: {text}"
            }],
            response_format={"type": "json_object"},
            max_tokens=2000
        )
        
        try:
            return json.loads(response.choices[0].message.content or "{}")
        except json.JSONDecodeError:
            return {"data_points": [], "categories": [], "relationships": []}

    def compare_and_verify(self, original_text: str, tabulated_data: List[Dict]) -> Dict[str, Any]:
        """Compare original text with tabulated data to find gaps"""
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "system",
                "content": """You are a quality assurance specialist. Compare the original text with the tabulated data and calculate an accurate coverage percentage.

                COVERAGE CALCULATION RULES:
                1. Count total distinct data points in original text
                2. Count how many were successfully extracted to table
                3. Calculate percentage: (extracted_points / total_points) * 100
                4. Consider accuracy of extracted values
                5. Provide specific missing information
                
                Return JSON with: coverage_score (number 0-100), missing_information (array), analysis_details (string)"""
            }, {
                "role": "user",
                "content": f"Original text: {original_text}\n\nTabulated data: {json.dumps(tabulated_data)}\n\nPerform comprehensive verification."
            }],
            response_format={"type": "json_object"},
            max_tokens=2000
        )
        
        try:
            return json.loads(response.choices[0].message.content or "{}")
        except json.JSONDecodeError:
            return {"missing_information": [], "mismatches": [], "coverage_score": 0}

    def optimize_table_format(self, table_data: List[Dict], original_text: str) -> Dict[str, Any]:
        """Final formatting agent that optimizes table structure and removes redundancy"""
        try:
            # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
            # do not change this unless explicitly requested by the user
            prompt = f"""
            You are a table formatting specialist. Analyze this extracted data and optimize it for maximum clarity and usability.

            OPTIMIZATION GOALS:
            1. Preserve ALL information including redundant text (DO NOT remove any data)
            2. Create logical column groupings and hierarchies
            3. Ensure each row represents a distinct data entity
            4. Use clear, descriptive column headers
            5. Optimize for readability in spreadsheet applications

            CURRENT TABLE DATA:
            {json.dumps(table_data, indent=2)}

            ORIGINAL TEXT CONTEXT:
            {original_text[:2000]}...

            Return a JSON object with this structure:
            {{
                "optimized_data": [list of optimized dictionary objects],
                "optimization_summary": "Brief description of changes made",
                "column_count": number,
                "row_count": number,
                "improvements": ["list of specific improvements made"]
            }}

            RULES:
            - Preserve ALL data points including redundant information
            - Create meaningful column names
            - Group related information logically
            - NEVER remove any entries, even if they appear duplicate
            - Ensure data is properly structured for CSV/Excel export
            - Keep all text content exactly as extracted
            """

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content or "{}")
            return result

        except Exception as e:
            print(f"Table optimization error: {e}")
            return {
                "optimized_data": table_data,
                "optimization_summary": "No optimization applied due to error",
                "column_count": len(table_data[0].keys()) if table_data else 0,
                "row_count": len(table_data),
                "improvements": []
            }

    def create_enhanced_tabulation(self, text: str, analysis: Dict, verification: Dict, previous_table: List[Dict] = None) -> Dict[str, Any]:
        """Create enhanced tabulation based on analysis and verification feedback"""
        system_prompt = """
        You are an expert tabulation specialist. Create a comprehensive table that captures ALL information.
        
        CRITICAL REQUIREMENTS:
        1. Extract EVERY piece of information as separate rows
        2. Use multiple Value columns (Value 1, Value 2, etc.) as needed
        3. Aim for maximum granularity - break down complex information
        4. Address all gaps identified in the verification feedback
        5. Include at least 30-50 rows for comprehensive documents
        
        Return JSON with this structure:
        {
          "data": [
            {"Category": "category_name", "Value 1": "value1", "Value 2": "value2", ...}
          ],
          "coverage_analysis": {
            "total_data_points": number,
            "categorized_points": number,
            "coverage_percentage": percentage,
            "improvements_made": ["list of improvements"]
          }
        }
        """
        
        user_prompt = f"""
        Text to tabulate: {text}
        
        Analysis findings: {json.dumps(analysis)}
        
        Verification feedback: {json.dumps(verification)}
        
        Previous table (if any): {json.dumps(previous_table or [])}
        
        Create the most comprehensive table possible, addressing all identified gaps and ensuring maximum information capture.
        """
        
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "system",
                "content": system_prompt
            }, {
                "role": "user",
                "content": user_prompt
            }],
            response_format={"type": "json_object"},
            max_tokens=4000
        )
        
        try:
            return json.loads(response.choices[0].message.content or "{}")
        except json.JSONDecodeError:
            return {"data": [], "coverage_analysis": {"coverage_percentage": 0}}

    def process_with_iterations(self, extracted_text: str, max_iterations: int = 3) -> Dict[str, Any]:
        """
        Process text through multiple iterations with cross-verification
        """
        iteration_results = []
        current_table = []
        
        for iteration in range(max_iterations):
            print(f"Processing iteration {iteration + 1}/{max_iterations}")
            
            # Step 1: Analyze the text
            analysis = self.analyze_text_data(extracted_text)
            
            # Step 2: Create/improve tabulation
            tabulation_result = self.create_enhanced_tabulation(
                extracted_text, 
                analysis, 
                iteration_results[-1]["verification"] if iteration_results else {}, 
                current_table if current_table else []
            )
            
            current_table = tabulation_result.get("data", [])
            
            # Step 3: Verify against original text
            verification = self.compare_and_verify(extracted_text, current_table)
            
            # Store iteration results
            iteration_data = {
                "iteration": iteration + 1,
                "analysis": analysis,
                "tabulation": tabulation_result,
                "verification": verification,
                "coverage_score": verification.get("coverage_score", 0)
            }
            iteration_results.append(iteration_data)
            
            # Check if we should continue based on verification coverage
            coverage = verification.get("coverage_score", 0)
            print(f"Iteration {iteration + 1} coverage: {coverage}%")
            
            # Only stop if we have very high coverage and multiple iterations
            if coverage >= 95 and iteration > 0:  # Require at least 2 iterations and 95% coverage
                print(f"High coverage achieved ({coverage}%), stopping iterations.")
                break
        
        return {
            "final_tabulation": current_table,
            "iteration_history": iteration_results,
            "total_iterations": len(iteration_results),
            "final_coverage": iteration_results[-1]["coverage_score"] if iteration_results else 0
        }

def process_text_with_agents(text: str) -> Dict[str, Any]:
    """
    Main function to process text using agentic approach
    """
    processor = AgenticProcessor()
    return processor.process_with_iterations(text)