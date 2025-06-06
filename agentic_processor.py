import os
import json
from typing import Dict, List, Any, Tuple
from crewai import Agent, Task, Crew, Process
from crewai.tools import BaseTool
from openai import OpenAI

class TextAnalysisTool(BaseTool):
    name: str = "text_analysis_tool"
    description: str = "Analyzes extracted text to identify all data points and their characteristics"
    
    def _run(self, text: str, *args, **kwargs) -> str:
        """Analyze text and return structured analysis"""
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "system",
                "content": "You are a text analysis expert. Analyze the given text and identify all distinct data points, their types, relationships, and context. Return a detailed analysis in JSON format."
            }, {
                "role": "user", 
                "content": f"Analyze this text and identify all data points: {text}"
            }],
            response_format={"type": "json_object"},
            max_tokens=2000
        )
        
        return response.choices[0].message.content or "{}"

class ComparisonTool(BaseTool):
    name: str = "comparison_tool"
    description: str = "Compares extracted text with tabulated data to find discrepancies"
    
    def _run(self, original_text: str, tabulated_data: str, *args, **kwargs) -> str:
        """Compare original text with tabulated version"""
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{
                "role": "system",
                "content": "You are a data verification expert. Compare the original text with tabulated data and identify missing information, mismatches, or incorrectly categorized data. Return findings in JSON format."
            }, {
                "role": "user",
                "content": f"Original text: {original_text}\n\nTabulated data: {tabulated_data}\n\nFind discrepancies and missing information."
            }],
            response_format={"type": "json_object"},
            max_tokens=2000
        )
        
        return response.choices[0].message.content or "{}"

class TabulationTool(BaseTool):
    name: str = "tabulation_tool"
    description: str = "Creates comprehensive tabulation based on analysis and feedback"
    
    def _run(self, text: str, analysis: str, feedback: str = "") -> str:
        """Create tabulation based on text, analysis, and feedback"""
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        
        system_prompt = """
        You are an expert data tabulation specialist. Create a comprehensive table that captures ALL information from the text.
        Use the analysis and feedback to ensure nothing is missed.
        
        Return JSON with this structure:
        {
          "data": [
            {"Category": "category_name", "Value 1": "value1", "Value 2": "value2", ...}
          ],
          "coverage_analysis": {
            "total_data_points": number,
            "categorized_points": number,
            "coverage_percentage": percentage
          }
        }
        """
        
        user_prompt = f"""
        Text to tabulate: {text}
        
        Analysis: {analysis}
        
        Feedback from previous iteration: {feedback}
        
        Create a comprehensive table ensuring all data points are captured.
        """
        
        response = client.chat.completions.create(
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
        
        return response.choices[0].message.content

class AgenticProcessor:
    def __init__(self):
        self.text_tool = TextAnalysisTool()
        self.comparison_tool = ComparisonTool()
        self.tabulation_tool = TabulationTool()
        
        # Define agents
        self.analyst_agent = Agent(
            role='Data Analysis Specialist',
            goal='Thoroughly analyze extracted text to identify all data points and their characteristics',
            backstory='You are an expert at breaking down complex documents and identifying every piece of meaningful information.',
            tools=[self.text_tool],
            verbose=True,
            allow_delegation=False
        )
        
        self.tabulator_agent = Agent(
            role='Tabulation Expert',
            goal='Create comprehensive tables that capture all identified data points',
            backstory='You specialize in organizing data into structured tables with maximum completeness and accuracy.',
            tools=[self.tabulation_tool],
            verbose=True,
            allow_delegation=False
        )
        
        self.verifier_agent = Agent(
            role='Quality Assurance Specialist',
            goal='Verify that all data from original text is properly captured in tabulated format',
            backstory='You are meticulous about ensuring no information is lost during tabulation and all data is accurately represented.',
            tools=[self.comparison_tool],
            verbose=True,
            allow_delegation=False
        )
        
        self.coordinator_agent = Agent(
            role='Process Coordinator',
            goal='Orchestrate the iterative improvement process and ensure convergence to complete data capture',
            backstory='You coordinate between agents to ensure the tabulation process is thorough and complete.',
            verbose=True,
            allow_delegation=True
        )

    def process_with_iterations(self, extracted_text: str, max_iterations: int = 3) -> Dict[str, Any]:
        """
        Process text through multiple iterations with agent collaboration
        """
        iteration_results = []
        current_tabulation = None
        
        for iteration in range(max_iterations):
            print(f"\n=== Iteration {iteration + 1} ===")
            
            # Create tasks for this iteration
            analysis_task = Task(
                description=f'Analyze the extracted text and identify all data points: {extracted_text}',
                expected_output='Detailed analysis of all data points in JSON format',
                agent=self.analyst_agent
            )
            
            tabulation_task = Task(
                description=f'Create comprehensive tabulation based on analysis. Previous tabulation: {current_tabulation or "None"}',
                expected_output='Complete tabulation in JSON format with coverage analysis',
                agent=self.tabulator_agent,
                context=[analysis_task]
            )
            
            verification_task = Task(
                description=f'Compare original text with tabulated data and identify discrepancies',
                expected_output='Verification report with identified gaps and improvements',
                agent=self.verifier_agent,
                context=[analysis_task, tabulation_task]
            )
            
            coordination_task = Task(
                description='Coordinate the results and determine if another iteration is needed',
                expected_output='Coordination summary and iteration decision',
                agent=self.coordinator_agent,
                context=[analysis_task, tabulation_task, verification_task]
            )
            
            # Create and run crew
            crew = Crew(
                agents=[self.analyst_agent, self.tabulator_agent, self.verifier_agent, self.coordinator_agent],
                tasks=[analysis_task, tabulation_task, verification_task, coordination_task],
                process=Process.sequential,
                verbose=True
            )
            
            try:
                result = crew.kickoff()
                
                # Extract tabulation from result
                tabulation_result = tabulation_task.output
                if isinstance(tabulation_result, str):
                    try:
                        tabulation_data = json.loads(tabulation_result)
                        current_tabulation = tabulation_data
                    except json.JSONDecodeError:
                        current_tabulation = {"data": [], "coverage_analysis": {"coverage_percentage": 0}}
                
                iteration_results.append({
                    "iteration": iteration + 1,
                    "analysis": analysis_task.output,
                    "tabulation": current_tabulation,
                    "verification": verification_task.output,
                    "coordination": coordination_task.output
                })
                
                # Check if we should continue
                if current_tabulation and "coverage_analysis" in current_tabulation:
                    coverage = current_tabulation["coverage_analysis"].get("coverage_percentage", 0)
                    if coverage >= 95:  # Stop if 95% coverage achieved
                        print(f"High coverage achieved ({coverage}%), stopping iterations.")
                        break
                        
            except Exception as e:
                print(f"Error in iteration {iteration + 1}: {str(e)}")
                break
        
        return {
            "final_tabulation": current_tabulation,
            "iteration_history": iteration_results,
            "total_iterations": len(iteration_results)
        }

def process_text_with_agents(text: str) -> Dict[str, Any]:
    """
    Main function to process text using agentic approach
    """
    processor = AgenticProcessor()
    return processor.process_with_iterations(text)