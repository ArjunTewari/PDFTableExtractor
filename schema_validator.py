import json
import jsonschema
from typing import Dict, Any, List, Union
from jsonschema import validate, ValidationError, Draft7Validator

class SchemaValidator:
    def __init__(self, schema_path: str = "schema.json"):
        """Initialize the validator with the canonical schema"""
        self.schema_path = schema_path
        self.schema = self._load_schema()
        self.validator = Draft7Validator(self.schema)
    
    def _load_schema(self) -> Dict[str, Any]:
        """Load the JSON schema from file"""
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in schema file: {e}")
    
    def validate_data(self, data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate data against the canonical schema
        
        Args:
            data: Data to validate (list of objects or single object)
            
        Returns:
            Dict with validation results
        """
        result = {
            "valid": False,
            "errors": [],
            "warnings": [],
            "summary": {
                "total_items": 0,
                "valid_items": 0,
                "invalid_items": 0,
                "missing_optional_fields": 0
            }
        }
        
        try:
            # Ensure data is a list
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                result["errors"].append("Data must be a list of objects or a single object")
                return result
            
            result["summary"]["total_items"] = len(data)
            
            # Validate against schema
            validate(instance=data, schema=self.schema)
            result["valid"] = True
            result["summary"]["valid_items"] = len(data)
            
            # Check for optional fields and provide warnings
            optional_fields = ["unit", "context", "row_id"]
            for i, item in enumerate(data):
                for field in optional_fields:
                    if field not in item or not item[field]:
                        result["warnings"].append(f"Item {i}: Missing optional field '{field}'")
                        result["summary"]["missing_optional_fields"] += 1
            
        except ValidationError as e:
            result["valid"] = False
            result["errors"].append(f"Schema validation failed: {e.message}")
            result["summary"]["invalid_items"] = len(data)
            
            # Get detailed errors for each item
            for i, item in enumerate(data):
                item_errors = list(self.validator.iter_errors([item]))
                if item_errors:
                    for error in item_errors:
                        result["errors"].append(f"Item {i}: {error.message}")
        
        except Exception as e:
            result["errors"].append(f"Validation error: {str(e)}")
        
        return result
    
    def transform_to_canonical(self, extracted_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform extracted data to canonical schema format
        
        Args:
            extracted_data: Raw extracted data from PDF processing
            
        Returns:
            List of objects conforming to canonical schema
        """
        canonical_data = []
        row_counter = 1
        
        for item in extracted_data:
            canonical_item = {
                "page": item.get("page", 1),
                "section": item.get("source", "Unknown"),
                "row_id": row_counter,
                "column": item.get("field", ""),
                "value": str(item.get("value", "")),
                "unit": self._extract_unit(str(item.get("value", ""))),
                "context": item.get("commentary", "")
            }
            
            canonical_data.append(canonical_item)
            row_counter += 1
        
        return canonical_data
    
    def _extract_unit(self, value: str) -> str:
        """Extract unit from value string (e.g., '$100M' -> 'USD', '25%' -> 'percent')"""
        value = value.strip()
        
        # Common units mapping
        unit_patterns = {
            '$': 'USD',
            '%': 'percent',
            'M': 'millions',
            'B': 'billions',
            'K': 'thousands',
            '€': 'EUR',
            '£': 'GBP'
        }
        
        for symbol, unit in unit_patterns.items():
            if symbol in value:
                return unit
        
        return ""
    
    def save_canonical_data(self, canonical_data: List[Dict[str, Any]], output_path: str) -> bool:
        """
        Save canonical data to JSON file after validation
        
        Args:
            canonical_data: Data in canonical format
            output_path: Path to save the validated data
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            # Validate before saving
            validation_result = self.validate_data(canonical_data)
            
            if not validation_result["valid"]:
                print(f"Data validation failed: {validation_result['errors']}")
                return False
            
            # Save to file
            with open(output_path, 'w') as f:
                json.dump(canonical_data, f, indent=2)
            
            print(f"Canonical data saved to {output_path}")
            print(f"Validation summary: {validation_result['summary']}")
            
            if validation_result["warnings"]:
                print(f"Warnings: {len(validation_result['warnings'])}")
            
            return True
            
        except Exception as e:
            print(f"Error saving canonical data: {e}")
            return False

def validate_json_file(file_path: str, schema_path: str = "schema.json") -> Dict[str, Any]:
    """
    Utility function to validate a JSON file against the canonical schema
    
    Args:
        file_path: Path to JSON file to validate
        schema_path: Path to schema file
        
    Returns:
        Dict with validation results
    """
    try:
        validator = SchemaValidator(schema_path)
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        return validator.validate_data(data)
    
    except Exception as e:
        return {
            "valid": False,
            "errors": [f"Error loading file: {str(e)}"],
            "warnings": [],
            "summary": {"total_items": 0, "valid_items": 0, "invalid_items": 0}
        }

# Example usage and testing
if __name__ == "__main__":
    # Test with sample data
    sample_data = [
        {
            "page": 1,
            "section": "Table 1",
            "row_id": 1,
            "column": "Company",
            "value": "Life360",
            "unit": "",
            "context": "Leading family safety company"
        },
        {
            "page": 1,
            "section": "Table 1",
            "row_id": 2,
            "column": "Revenue",
            "value": "$115.5M",
            "unit": "USD",
            "context": "Q4 2023 revenue growth of 35%"
        }
    ]
    
    validator = SchemaValidator()
    result = validator.validate_data(sample_data)
    
    print("Validation Result:")
    print(f"Valid: {result['valid']}")
    print(f"Errors: {result['errors']}")
    print(f"Warnings: {result['warnings']}")
    print(f"Summary: {result['summary']}")