"""
Deduplication utilities using semantic similarity
"""
import difflib
from typing import List, Dict, Any
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

def is_similar(text1: str, text2: str, threshold: float = 0.8) -> bool:
    """Check if two texts are similar using sequence matching"""
    return difflib.SequenceMatcher(None, text1.lower(), text2.lower()).ratio() > threshold

def deduplicate_by_semantic_similarity(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate data using semantic similarity as shown in the image
    """
    if not data:
        return data
    
    seen_data = set()
    deduplicated = []
    
    for item in data:
        # Create a key based on field and value for exact duplicates
        field = item.get('field', '')
        value = str(item.get('value', ''))
        key = f"{field}_{value}"
        
        if key in seen_data:
            continue
            
        # Check for semantic similarity with existing items
        is_duplicate = False
        for existing_item in deduplicated:
            existing_field = existing_item.get('field', '')
            existing_value = str(existing_item.get('value', ''))
            
            # Check field similarity
            if is_similar(field, existing_field, 0.85):
                # Check value similarity
                if is_similar(value, existing_value, 0.9):
                    is_duplicate = True
                    break
        
        if not is_duplicate:
            seen_data.add(key)
            deduplicated.append(item)
    
    return deduplicated

def deduplicate_commentary(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicate commentary while preserving unique data points
    """
    if not data:
        return data
    
    seen_commentary = set()
    deduplicated = []
    
    for item in data:
        commentary = item.get('commentary', '').strip()
        
        # Keep items without commentary
        if not commentary or commentary == '-':
            deduplicated.append(item)
            continue
        
        # Check if we've seen similar commentary before
        is_duplicate_commentary = False
        for seen in seen_commentary:
            if is_similar(commentary, seen, 0.85):
                is_duplicate_commentary = True
                break
        
        if not is_duplicate_commentary:
            seen_commentary.add(commentary)
            deduplicated.append(item)
    
    return deduplicated

def advanced_deduplication(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Advanced deduplication using TF-IDF and cosine similarity
    """
    if len(data) <= 1:
        return data
    
    # First pass: exact field-value deduplication
    data = deduplicate_by_semantic_similarity(data)
    
    # Second pass: commentary deduplication
    data = deduplicate_commentary(data)
    
    # Third pass: advanced semantic deduplication using TF-IDF
    if len(data) > 1:
        texts = []
        for item in data:
            field = item.get('field', '')
            value = str(item.get('value', ''))
            text = f"{field} {value}".lower()
            texts.append(text)
        
        try:
            # Use TF-IDF vectorizer
            vectorizer = TfidfVectorizer(ngram_range=(1, 2), stop_words='english')
            tfidf_matrix = vectorizer.fit_transform(texts)
            
            # Calculate cosine similarity
            similarity_matrix = cosine_similarity(tfidf_matrix)
            
            # Mark duplicates based on high similarity
            to_remove = set()
            for i in range(len(similarity_matrix)):
                for j in range(i + 1, len(similarity_matrix)):
                    if similarity_matrix[i][j] > 0.85:  # High similarity threshold
                        to_remove.add(j)  # Remove the later item
            
            # Keep only non-duplicate items
            data = [item for idx, item in enumerate(data) if idx not in to_remove]
            
        except Exception as e:
            print(f"TF-IDF deduplication failed: {e}, using basic deduplication")
    
    return data