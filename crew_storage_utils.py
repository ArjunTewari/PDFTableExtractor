import json
import requests
from typing import Dict, List, Optional, Any
from storage_manager import storage_manager

class CrewStorageUtils:
    """
    Utilities for crew agents to access and collaborate on stored text data
    """
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        self.base_url = base_url
    
    def get_stored_text(self, text_id: str) -> Optional[Dict]:
        """
        Retrieve stored text by ID for crew processing
        
        Args:
            text_id (str): The storage ID for the text
            
        Returns:
            Dict: Text data with metadata or None if not found
        """
        try:
            response = requests.get(f"{self.base_url}/storage/text/{text_id}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Error retrieving text {text_id}: {e}")
            return storage_manager.retrieve_text(text_id)  # Fallback to direct access
    
    def list_available_texts(self) -> List[Dict]:
        """
        List all available texts for crew processing
        
        Returns:
            List[Dict]: List of available text metadata
        """
        try:
            response = requests.get(f"{self.base_url}/storage/texts")
            if response.status_code == 200:
                return response.json().get('texts', [])
            return []
        except Exception as e:
            print(f"Error listing texts: {e}")
            return storage_manager.list_stored_texts()  # Fallback to direct access
    
    def get_processing_history(self, text_id: str) -> List[Dict]:
        """
        Get processing history for a text to understand previous crew work
        
        Args:
            text_id (str): The text ID
            
        Returns:
            List[Dict]: Processing history
        """
        try:
            response = requests.get(f"{self.base_url}/storage/history/{text_id}")
            if response.status_code == 200:
                return response.json().get('history', [])
            return []
        except Exception as e:
            print(f"Error retrieving history for {text_id}: {e}")
            return storage_manager.get_processing_history(text_id)  # Fallback to direct access
    
    def analyze_text_for_crew(self, text_id: str) -> Dict[str, Any]:
        """
        Analyze stored text and provide crew-relevant insights
        
        Args:
            text_id (str): The text ID to analyze
            
        Returns:
            Dict: Analysis results for crew collaboration
        """
        text_data = self.get_stored_text(text_id)
        if not text_data:
            return {"error": "Text not found"}
        
        text_content = text_data.get('extracted_text', '')
        history = self.get_processing_history(text_id)
        
        analysis = {
            "text_id": text_id,
            "text_metadata": {
                "length": len(text_content),
                "word_count": len(text_content.split()) if text_content else 0,
                "original_filename": text_data.get('original_filename'),
                "extraction_timestamp": text_data.get('extraction_timestamp')
            },
            "processing_history": {
                "total_attempts": len(history),
                "previous_results": [h.get('processing_results', {}) for h in history],
                "last_processing": history[-1] if history else None
            },
            "content_preview": text_content[:500] + "..." if len(text_content) > 500 else text_content,
            "crew_recommendations": self._generate_crew_recommendations(text_content, history)
        }
        
        return analysis
    
    def _generate_crew_recommendations(self, text: str, history: List[Dict]) -> List[str]:
        """
        Generate recommendations for crew processing based on text and history
        
        Args:
            text (str): The text content
            history (List[Dict]): Processing history
            
        Returns:
            List[str]: Recommendations for crew agents
        """
        recommendations = []
        
        # Analyze text characteristics
        if len(text) > 10000:
            recommendations.append("Large document - consider chunked processing approach")
        
        if any(keyword in text.lower() for keyword in ['table', 'figure', 'chart', 'data']):
            recommendations.append("Contains structured data - focus on tabulation accuracy")
        
        if any(keyword in text.lower() for keyword in ['financial', 'report', 'statement']):
            recommendations.append("Financial document - ensure numerical accuracy")
        
        # Analyze processing history
        if len(history) > 1:
            recommendations.append("Multiple processing attempts detected - review previous failures")
        
        if history and any('optimization' in str(h) for h in history):
            recommendations.append("Previous optimization applied - build upon existing structure")
        
        if not recommendations:
            recommendations.append("Standard processing recommended - no special considerations detected")
        
        return recommendations
    
    def get_text_segments(self, text_id: str, segment_size: int = 1000) -> List[Dict]:
        """
        Break stored text into segments for crew parallel processing
        
        Args:
            text_id (str): The text ID
            segment_size (int): Size of each segment
            
        Returns:
            List[Dict]: Text segments with metadata
        """
        text_data = self.get_stored_text(text_id)
        if not text_data:
            return []
        
        text_content = text_data.get('extracted_text', '')
        segments = []
        
        for i in range(0, len(text_content), segment_size):
            segment = {
                "segment_id": f"{text_id}_seg_{i//segment_size + 1}",
                "text_id": text_id,
                "segment_number": i//segment_size + 1,
                "start_position": i,
                "end_position": min(i + segment_size, len(text_content)),
                "content": text_content[i:i + segment_size],
                "overlap_previous": text_content[max(0, i-50):i] if i > 0 else "",
                "overlap_next": text_content[i + segment_size:i + segment_size + 50] if i + segment_size < len(text_content) else ""
            }
            segments.append(segment)
        
        return segments
    
    def merge_crew_results(self, text_id: str, crew_results: List[Dict]) -> Dict[str, Any]:
        """
        Merge results from multiple crew agents working on the same text
        
        Args:
            text_id (str): The text ID
            crew_results (List[Dict]): Results from different crew agents
            
        Returns:
            Dict: Merged and consolidated results
        """
        merged_result = {
            "text_id": text_id,
            "crew_processing": {
                "total_agents": len(crew_results),
                "processing_timestamp": crew_results[0].get('timestamp') if crew_results else None,
                "agent_contributions": []
            },
            "consolidated_data": [],
            "confidence_scores": {},
            "conflicts": []
        }
        
        # Merge tabulated data from all agents
        all_data = []
        for result in crew_results:
            agent_data = result.get('final_tabulation', [])
            if agent_data:
                all_data.extend(agent_data)
                merged_result["crew_processing"]["agent_contributions"].append({
                    "agent_id": result.get('agent_id', 'unknown'),
                    "data_points": len(agent_data),
                    "coverage": result.get('final_coverage', 0)
                })
        
        # Remove duplicates and consolidate
        unique_data = []
        seen_combinations = set()
        
        for item in all_data:
            item_key = tuple(sorted(item.items()))
            if item_key not in seen_combinations:
                unique_data.append(item)
                seen_combinations.add(item_key)
        
        merged_result["consolidated_data"] = unique_data
        merged_result["total_unique_entries"] = len(unique_data)
        
        return merged_result

# Global crew utils instance
crew_utils = CrewStorageUtils()