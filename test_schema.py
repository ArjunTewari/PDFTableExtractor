#!/usr/bin/env python3
"""
Test script for schema validation functionality
"""

import json
from schema_validator import SchemaValidator, validate_json_file

def test_schema_validation():
    """Test the schema validation with sample data"""
    print("Testing Schema Validation System")
    print("=" * 40)
    
    # Initialize validator
    validator = SchemaValidator()
    
    # Test data - valid format
    valid_data = [
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
        },
        {
            "page": 2,
            "section": "Key-Value Pairs",
            "row_id": 3,
            "column": "Monthly Active Users",
            "value": "66.9 million",
            "unit": "users",
            "context": "33% growth year-over-year"
        }
    ]
    
    # Test invalid data - missing required fields
    invalid_data = [
        {
            "page": 1,
            "section": "Table 1",
            # Missing required "column" and "value" fields
            "unit": "",
            "context": "Test data"
        }
    ]
    
    print("1. Testing valid data...")
    result = validator.validate_data(valid_data)
    print(f"   Valid: {result['valid']}")
    print(f"   Errors: {len(result['errors'])}")
    print(f"   Warnings: {len(result['warnings'])}")
    print(f"   Summary: {result['summary']}")
    
    print("\n2. Testing invalid data...")
    result = validator.validate_data(invalid_data)
    print(f"   Valid: {result['valid']}")
    print(f"   Errors: {len(result['errors'])}")
    print(f"   First error: {result['errors'][0] if result['errors'] else 'None'}")
    
    print("\n3. Testing data transformation...")
    # Sample extracted data in the old format
    extracted_data = [
        {
            "source": "Table 1",
            "type": "Table Data",
            "field": "Company",
            "value": "Life360",
            "page": 1,
            "commentary": "Leading family safety company",
            "has_commentary": True
        },
        {
            "source": "Key-Value Pairs",
            "type": "Structured Data", 
            "field": "Revenue",
            "value": "$115.5M",
            "page": 1,
            "commentary": "Q4 2023 revenue growth of 35%",
            "has_commentary": True
        }
    ]
    
    canonical_data = validator.transform_to_canonical(extracted_data)
    print(f"   Transformed {len(extracted_data)} items to canonical format")
    print(f"   Sample canonical item: {canonical_data[0] if canonical_data else 'None'}")
    
    # Validate transformed data
    validation_result = validator.validate_data(canonical_data)
    print(f"   Canonical data valid: {validation_result['valid']}")
    
    print("\n4. Testing file save...")
    success = validator.save_canonical_data(canonical_data, "test_output.json")
    print(f"   File save successful: {success}")
    
    if success:
        print("\n5. Testing file validation...")
        file_result = validate_json_file("test_output.json")
        print(f"   File validation result: {file_result['valid']}")
        print(f"   File items count: {file_result['summary']['total_items']}")

def test_unit_extraction():
    """Test the unit extraction functionality"""
    print("\n" + "=" * 40)
    print("Testing Unit Extraction")
    print("=" * 40)
    
    validator = SchemaValidator()
    
    test_values = [
        "$115.5M",
        "25%", 
        "€50B",
        "£100K",
        "66.9 million users",
        "350 employees",
        "No units here"
    ]
    
    for value in test_values:
        unit = validator._extract_unit(value)
        print(f"   '{value}' -> unit: '{unit}'")

if __name__ == "__main__":
    test_schema_validation()
    test_unit_extraction()
    print("\nSchema validation testing completed!")