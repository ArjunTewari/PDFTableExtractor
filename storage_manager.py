import json
import os
from datetime import datetime
from typing import Dict, List, Optional
import hashlib

class TextStorageManager:
    """
    Manages storage and retrieval of extracted text and processing results
    """
    
    def __init__(self, storage_dir: str = "storage"):
        self.storage_dir = storage_dir
        self.ensure_storage_directory()
    
    def ensure_storage_directory(self):
        """Create storage directory if it doesn't exist"""
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
    
    def generate_text_id(self, text: str) -> str:
        """Generate unique ID for text content"""
        text_hash = hashlib.md5(text.encode()).hexdigest()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{timestamp}_{text_hash[:8]}"
    
    def store_extracted_text(self, text: str, filename: str = None) -> str:
        """
        Store extracted text and return storage ID
        
        Args:
            text (str): The extracted text content
            filename (str): Optional original filename
            
        Returns:
            str: Storage ID for the text
        """
        text_id = self.generate_text_id(text)
        
        storage_data = {
            "text_id": text_id,
            "extracted_text": text,
            "original_filename": filename,
            "extraction_timestamp": datetime.now().isoformat(),
            "text_length": len(text),
            "word_count": len(text.split()) if text else 0
        }
        
        storage_path = os.path.join(self.storage_dir, f"{text_id}.json")
        
        with open(storage_path, 'w', encoding='utf-8') as f:
            json.dump(storage_data, f, indent=2, ensure_ascii=False)
        
        return text_id
    
    def retrieve_text(self, text_id: str) -> Optional[Dict]:
        """
        Retrieve stored text by ID
        
        Args:
            text_id (str): The storage ID
            
        Returns:
            Dict: Stored text data or None if not found
        """
        storage_path = os.path.join(self.storage_dir, f"{text_id}.json")
        
        if not os.path.exists(storage_path):
            return None
        
        try:
            with open(storage_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error retrieving text {text_id}: {e}")
            return None
    
    def store_processing_results(self, text_id: str, results: Dict) -> str:
        """
        Store processing results linked to extracted text
        
        Args:
            text_id (str): The original text ID
            results (Dict): Processing results from crew
            
        Returns:
            str: Results storage ID
        """
        results_id = f"{text_id}_results_{datetime.now().strftime('%H%M%S')}"
        
        results_data = {
            "results_id": results_id,
            "text_id": text_id,
            "processing_results": results,
            "processing_timestamp": datetime.now().isoformat(),
            "processing_mode": results.get("metadata", {}).get("processing_mode", "unknown")
        }
        
        results_path = os.path.join(self.storage_dir, f"{results_id}.json")
        
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2, ensure_ascii=False)
        
        return results_id
    
    def get_processing_history(self, text_id: str) -> List[Dict]:
        """
        Get all processing results for a given text ID
        
        Args:
            text_id (str): The text ID
            
        Returns:
            List[Dict]: List of processing results
        """
        history = []
        
        for filename in os.listdir(self.storage_dir):
            if filename.startswith(f"{text_id}_results_") and filename.endswith('.json'):
                filepath = os.path.join(self.storage_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        history.append(json.load(f))
                except Exception as e:
                    print(f"Error reading results file {filename}: {e}")
        
        # Sort by processing timestamp
        history.sort(key=lambda x: x.get("processing_timestamp", ""))
        return history
    
    def list_stored_texts(self) -> List[Dict]:
        """
        List all stored texts with metadata
        
        Returns:
            List[Dict]: List of text metadata
        """
        texts = []
        
        for filename in os.listdir(self.storage_dir):
            if filename.endswith('.json') and not '_results_' in filename:
                filepath = os.path.join(self.storage_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        texts.append({
                            "text_id": data.get("text_id"),
                            "original_filename": data.get("original_filename"),
                            "extraction_timestamp": data.get("extraction_timestamp"),
                            "text_length": data.get("text_length"),
                            "word_count": data.get("word_count")
                        })
                except Exception as e:
                    print(f"Error reading text file {filename}: {e}")
        
        # Sort by extraction timestamp (newest first)
        texts.sort(key=lambda x: x.get("extraction_timestamp", ""), reverse=True)
        return texts
    
    def cleanup_old_files(self, days_old: int = 7):
        """
        Clean up files older than specified days
        
        Args:
            days_old (int): Number of days after which to delete files
        """
        cutoff_time = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
        
        for filename in os.listdir(self.storage_dir):
            filepath = os.path.join(self.storage_dir, filename)
            if os.path.getctime(filepath) < cutoff_time:
                try:
                    os.remove(filepath)
                    print(f"Cleaned up old file: {filename}")
                except Exception as e:
                    print(f"Error cleaning up {filename}: {e}")

# Global storage manager instance
storage_manager = TextStorageManager()