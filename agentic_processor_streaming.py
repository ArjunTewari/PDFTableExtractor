import os
import json
import time
from typing import Dict, List, Any, Generator
from openai import OpenAI
from storage_manager import storage_manager

class StreamingAgenticProcessor:
    def __init__(self):
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    def analyze_text_data_streaming(self, text: str) -> Generator[Dict, None, None]:
        """Analyze extracted text to identify all data points with streaming updates"""
        yield {"type": "iteration_start", "step": "analysis", "message": "Starting text analysis..."}
        
        try:
            # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
            # do not change this unless explicitly requested by the user
            prompt = f"""
            Analyze this extracted text and identify ALL data points that should be tabulated.
            Focus on finding structured information, values, categories, dates, and relationships.
            
            Text to analyze:
            {text[:3000]}...
            
            Return a JSON object with:
            {{
                "data_points_found": number,
                "categories": ["list of data categories found"],
                "key_insights": ["important insights about the data structure"],
                "complexity_score": 1-10,
                "recommended_columns": ["suggested column names for tabulation"]
            }}
            """

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content or "{}")
            yield {"type": "step_complete", "step": "analysis", "result": result}
            return result
            
        except Exception as e:
            error_result = {"error": str(e), "data_points_found": 0}
            yield {"type": "step_error", "step": "analysis", "error": str(e)}
            return error_result

    def create_tabulation_streaming(self, text: str, analysis: Dict, previous_table: List[Dict] = None) -> Generator[Dict, None, None]:
        """Create enhanced tabulation with streaming updates"""
        yield {"type": "step_start", "step": "tabulation", "message": "Creating comprehensive table..."}
        
        try:
            system_prompt = """
            Create a comprehensive table that captures ALL information from the text.
            Use the analysis insights to structure the data optimally.
            """

            user_prompt = f"""
            Based on this analysis: {json.dumps(analysis, indent=2)}
            
            Create a comprehensive table from this text:
            {text[:3000]}...
            
            Return JSON with:
            {{
                "data": [array of objects representing table rows],
                "coverage_analysis": {{
                    "coverage_percentage": number (0-100),
                    "captured_categories": ["list"],
                    "missing_elements": ["any gaps identified"]
                }},
                "table_structure": {{
                    "total_rows": number,
                    "total_columns": number,
                    "column_types": {{"column_name": "data_type"}}
                }}
            }}
            """

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            
            result = json.loads(response.choices[0].message.content or "{}")
            yield {"type": "step_complete", "step": "tabulation", "result": result}
            return result
            
        except Exception as e:
            error_result = {"data": [], "coverage_analysis": {"coverage_percentage": 0}}
            yield {"type": "step_error", "step": "tabulation", "error": str(e)}
            return error_result

    def verify_against_text_streaming(self, original_text: str, tabulated_data: List[Dict]) -> Generator[Dict, None, None]:
        """Compare original text with tabulated data with streaming updates"""
        yield {"type": "step_start", "step": "verification", "message": "Verifying data completeness..."}
        
        try:
            prompt = f"""
            Compare the original text with the tabulated data to find any missing information.
            
            Original text snippet:
            {original_text[:2000]}...
            
            Tabulated data:
            {json.dumps(tabulated_data[:5], indent=2)}...
            
            Return JSON with:
            {{
                "coverage_score": number (0-100),
                "missing_information": ["list of data points not captured"],
                "verification_notes": ["observations about data quality"],
                "completeness_assessment": "detailed assessment"
            }}
            """

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content or "{}")
            yield {"type": "step_complete", "step": "verification", "result": result}
            return result
            
        except Exception as e:
            error_result = {"coverage_score": 0, "missing_information": [], "verification_notes": []}
            yield {"type": "step_error", "step": "verification", "error": str(e)}
            return error_result

    def optimize_table_streaming(self, table_data: List[Dict], original_text: str) -> Generator[Dict, None, None]:
        """Final formatting optimization with streaming updates"""
        yield {"type": "step_start", "step": "optimization", "message": "Optimizing table structure..."}
        
        try:
            prompt = f"""
            Optimize this table structure while preserving ALL data:
            
            Current table: {json.dumps(table_data[:3], indent=2)}...
            
            Return JSON with:
            {{
                "optimized_data": [optimized table structure],
                "optimization_summary": "description of improvements",
                "improvements": ["list of specific changes made"]
            }}
            
            RULES: Preserve ALL information, only improve organization.
            """

            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            
            result = json.loads(response.choices[0].message.content or "{}")
            yield {"type": "step_complete", "step": "optimization", "result": result}
            return result
            
        except Exception as e:
            error_result = {"optimized_data": table_data, "optimization_summary": "No optimization applied"}
            yield {"type": "step_error", "step": "optimization", "error": str(e)}
            return error_result

def process_with_streaming(text: str, max_iterations: int = 3) -> Generator[Dict, None, None]:
    """Process text with real-time streaming updates showing crew work"""
    processor = StreamingAgenticProcessor()
    
    yield {"type": "processing_start", "message": "Initializing crew agents..."}
    
    iteration_results = []
    current_table = []
    
    for iteration in range(max_iterations):
        yield {"type": "iteration_start", "iteration": iteration + 1, "total": max_iterations}
        
        # Step 1: Analysis Agent
        analysis = None
        for update in processor.analyze_text_data_streaming(text):
            yield update
            if update["type"] == "step_complete":
                analysis = update["result"]
        
        if not analysis:
            continue
            
        # Step 2: Tabulation Agent  
        tabulation_result = None
        for update in processor.create_tabulation_streaming(text, analysis, current_table):
            yield update
            if update["type"] == "step_complete":
                tabulation_result = update["result"]
        
        if not tabulation_result:
            continue
            
        current_table = tabulation_result.get("data", [])
        
        # Step 3: Verification Agent
        verification = None
        for update in processor.verify_against_text_streaming(text, current_table):
            yield update
            if update["type"] == "step_complete":
                verification = update["result"]
        
        # Store iteration results
        iteration_data = {
            "iteration": iteration + 1,
            "analysis": analysis,
            "tabulation": tabulation_result,
            "verification": verification,
            "coverage_score": verification.get("coverage_score", 0) if verification else 0
        }
        iteration_results.append(iteration_data)
        
        yield {"type": "iteration_complete", "iteration": iteration + 1, "data": iteration_data}
        
        # Check if coverage is sufficient based on verification results
        coverage = verification.get("coverage_score", 0) if verification else 0
        yield {"type": "iteration_coverage", "iteration": iteration + 1, "coverage": coverage}
        
        # Only stop if we have very high coverage and multiple iterations
        if coverage >= 95 and iteration > 0:  # Require at least 2 iterations and 95% coverage
            yield {"type": "coverage_achieved", "coverage": coverage}
            break
    
    # Final results without optimization to preserve all data
    final_result = {
        "final_tabulation": current_table,
        "iteration_history": iteration_results,
        "total_iterations": len(iteration_results),
        "final_coverage": iteration_results[-1]["coverage_score"] if iteration_results else 0
    }
    
    yield {"type": "processing_complete", "result": final_result}