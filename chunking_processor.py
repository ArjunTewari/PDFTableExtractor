import json
import os
from typing import Dict, Any, List, Tuple
from schema_validator import SchemaValidator

class ChunkingProcessor:
    def __init__(self, schema_path: str = "schema.json"):
        """Initialize the chunking processor with schema validation"""
        self.schema_validator = SchemaValidator(schema_path)
        self.max_tokens_per_chunk = 400
    
    def group_raw_outputs_into_batches(self, textract_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Group raw Textract outputs into three batches: Tables, Key-Values, and Narrative
        
        Args:
            textract_result: Raw Textract processing result
            
        Returns:
            Dict containing grouped batches
        """
        batches = {
            "tables": [],
            "key_values": [],
            "narrative_chunks": [],
            "metadata": {
                "total_pages": textract_result.get("total_pages", 0),
                "job_id": textract_result.get("job_id", ""),
                "processing_time": textract_result.get("processing_time", 0)
            }
        }
        
        # Extract tables from each page
        for page_data in textract_result.get("pages", []):
            page_num = page_data.get("page_number", 1)
            
            # Process tables - extract each Block of type TABLE + its Relationships -> CELL
            if page_data.get("extracted_tables"):
                for table_idx, table in enumerate(page_data["extracted_tables"]):
                    table_batch = {
                        "page": page_num,
                        "table_id": f"page_{page_num}_table_{table_idx + 1}",
                        "block_type": "TABLE",
                        "raw_data": table,
                        "relationships": self._extract_table_relationships(table),
                        "cells": self._extract_table_cells(table)
                    }
                    batches["tables"].append(table_batch)
            
            # Process key-values - Block type KEY_VALUE_SET
            if page_data.get("extracted_key_values"):
                for kv_idx, kv_pair in enumerate(page_data["extracted_key_values"]):
                    kv_batch = {
                        "page": page_num,
                        "kv_id": f"page_{page_num}_kv_{kv_idx + 1}",
                        "block_type": "KEY_VALUE_SET",
                        "raw_data": kv_pair
                    }
                    batches["key_values"].append(kv_batch)
            
            # Process narrative text - lines from DetectDocumentText (chunk ~400 tokens each)
            if page_data.get("extracted_text"):
                text_chunks = self._chunk_narrative_text(page_data["extracted_text"], page_num)
                batches["narrative_chunks"].extend(text_chunks)
        
        return batches
    
    def _extract_table_relationships(self, table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract table relationships for CELL mapping"""
        relationships = []
        
        if "rows" in table_data:
            for row_idx, row in enumerate(table_data["rows"]):
                if isinstance(row, list):
                    for col_idx, cell in enumerate(row):
                        relationships.append({
                            "type": "CELL",
                            "row": row_idx,
                            "column": col_idx,
                            "value": str(cell) if cell else ""
                        })
                elif isinstance(row, dict):
                    for col_name, cell_value in row.items():
                        relationships.append({
                            "type": "CELL",
                            "row": row_idx,
                            "column": col_name,
                            "value": str(cell_value) if cell_value else ""
                        })
        
        return relationships
    
    def _extract_table_cells(self, table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract individual cells from table data"""
        cells = []
        
        if "rows" in table_data:
            for row_idx, row in enumerate(table_data["rows"]):
                if isinstance(row, list):
                    for col_idx, cell_value in enumerate(row):
                        cells.append({
                            "row_index": row_idx,
                            "column_index": col_idx,
                            "value": str(cell_value) if cell_value else "",
                            "confidence": 1.0  # Default confidence
                        })
                elif isinstance(row, dict):
                    for col_idx, (col_name, cell_value) in enumerate(row.items()):
                        cells.append({
                            "row_index": row_idx,
                            "column_index": col_idx,
                            "column_name": col_name,
                            "value": str(cell_value) if cell_value else "",
                            "confidence": 1.0
                        })
        
        return cells
    
    def _chunk_narrative_text(self, text_lines: List[str], page_num: int) -> List[Dict[str, Any]]:
        """Chunk narrative text into ~400 token chunks"""
        chunks = []
        current_chunk = []
        current_tokens = 0
        chunk_id = 1
        
        for line in text_lines:
            # Estimate tokens (rough approximation: 1 token ≈ 4 characters)
            line_tokens = len(line) // 4
            
            if current_tokens + line_tokens > self.max_tokens_per_chunk and current_chunk:
                # Save current chunk
                chunks.append({
                    "page": page_num,
                    "chunk_id": f"page_{page_num}_chunk_{chunk_id}",
                    "block_type": "NARRATIVE",
                    "text_lines": current_chunk.copy(),
                    "token_count": current_tokens,
                    "line_count": len(current_chunk)
                })
                
                # Start new chunk
                current_chunk = [line]
                current_tokens = line_tokens
                chunk_id += 1
            else:
                current_chunk.append(line)
                current_tokens += line_tokens
        
        # Add remaining chunk if any
        if current_chunk:
            chunks.append({
                "page": page_num,
                "chunk_id": f"page_{page_num}_chunk_{chunk_id}",
                "block_type": "NARRATIVE",
                "text_lines": current_chunk,
                "token_count": current_tokens,
                "line_count": len(current_chunk)
            })
        
        return chunks
    
    def bundle_into_payload(self, batches: Dict[str, Any]) -> Dict[str, Any]:
        """Bundle each batch into a single Python object as shown in the image"""
        
        # Load schema
        with open("schema.json", "r") as f:
            schema = json.load(f)
        
        payload = {
            "schema": schema,
            "raw": {
                "tables": batches["tables"],
                "kvs": batches["key_values"], 
                "text_chunks": batches["narrative_chunks"]
            },
            "metadata": batches["metadata"],
            "processing_stats": {
                "total_tables": len(batches["tables"]),
                "total_key_values": len(batches["key_values"]),
                "total_text_chunks": len(batches["narrative_chunks"]),
                "total_pages": batches["metadata"]["total_pages"]
            }
        }
        
        return payload
    
    def process_complete_chunking_workflow(self, textract_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Complete chunking workflow: group outputs -> bundle into payload
        
        Args:
            textract_result: Raw Textract processing result
            
        Returns:
            Complete payload ready for LLM processing
        """
        print("Starting chunking and batch preparation...")
        
        # Step 1: Group raw outputs into batches
        print("Grouping raw outputs into batches...")
        batches = self.group_raw_outputs_into_batches(textract_result)
        
        print(f"Created {len(batches['tables'])} table batches")
        print(f"Created {len(batches['key_values'])} key-value batches") 
        print(f"Created {len(batches['narrative_chunks'])} narrative chunks")
        
        # Step 2: Bundle into single payload object
        print("Bundling batches into payload...")
        payload = self.bundle_into_payload(batches)
        
        print("Chunking and batch preparation completed!")
        return payload
    
    def save_payload(self, payload: Dict[str, Any], output_path: str) -> bool:
        """Save the payload to a JSON file"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(payload, f, indent=2)
            
            print(f"Payload saved to: {output_path}")
            return True
            
        except Exception as e:
            print(f"Error saving payload: {e}")
            return False
    
    def validate_payload_structure(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the payload structure meets expected format"""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "structure_check": {
                "has_schema": "schema" in payload,
                "has_raw_data": "raw" in payload,
                "has_tables": "tables" in payload.get("raw", {}),
                "has_kvs": "kvs" in payload.get("raw", {}),
                "has_text_chunks": "text_chunks" in payload.get("raw", {}),
                "has_metadata": "metadata" in payload
            }
        }
        
        # Check required structure
        required_keys = ["schema", "raw", "metadata"]
        for key in required_keys:
            if key not in payload:
                validation_result["valid"] = False
                validation_result["errors"].append(f"Missing required key: {key}")
        
        # Check raw data structure
        if "raw" in payload:
            raw_required_keys = ["tables", "kvs", "text_chunks"]
            for key in raw_required_keys:
                if key not in payload["raw"]:
                    validation_result["valid"] = False
                    validation_result["errors"].append(f"Missing required raw data key: {key}")
        
        # Validate schema if present
        if "schema" in payload:
            try:
                # Basic schema validation
                schema = payload["schema"]
                if not isinstance(schema, dict) or "type" not in schema:
                    validation_result["warnings"].append("Schema format may be invalid")
            except Exception as e:
                validation_result["warnings"].append(f"Schema validation warning: {e}")
        
        return validation_result


# Utility functions for testing
def test_chunking_processor():
    """Test the chunking processor with sample data"""
    print("Testing Chunking Processor")
    print("=" * 40)
    
    # Sample Textract result
    sample_textract_result = {
        "job_id": "test-job-123",
        "total_pages": 2,
        "processing_time": 15.5,
        "pages": [
            {
                "page_number": 1,
                "extracted_tables": [
                    {
                        "rows": [
                            ["Company", "Revenue", "Growth"],
                            ["Life360", "$115.5M", "35%"],
                            ["Competitor A", "$85M", "20%"]
                        ]
                    }
                ],
                "extracted_key_values": [
                    {"key": "Total Users", "value": "66.9 million"},
                    {"key": "MAU Growth", "value": "33%"}
                ],
                "extracted_text": [
                    "Life360 is a leading family safety company.",
                    "The company reported strong Q4 results.",
                    "Revenue growth was driven by subscription increases.",
                    "Mobile app usage reached new highs."
                ]
            },
            {
                "page_number": 2,
                "extracted_tables": [],
                "extracted_key_values": [
                    {"key": "Market Cap", "value": "$2.1B"}
                ],
                "extracted_text": [
                    "Future outlook remains positive.",
                    "International expansion planned for 2024."
                ]
            }
        ]
    }
    
    processor = ChunkingProcessor()
    
    # Test complete workflow
    payload = processor.process_complete_chunking_workflow(sample_textract_result)
    
    # Validate payload
    validation = processor.validate_payload_structure(payload)
    print(f"Payload validation: {validation['valid']}")
    print(f"Structure check: {validation['structure_check']}")
    
    # Save payload
    success = processor.save_payload(payload, "output/test_payload.json")
    print(f"Save successful: {success}")
    
    return payload


if __name__ == "__main__":
    test_chunking_processor()