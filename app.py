from flask import Flask, render_template, request, jsonify, send_file
import os
import tempfile
import base64
import json
import time
from io import BytesIO

from textract_processor import extract_text_from_pdf, extract_text_from_pdf_bytes, extract_structured_data_from_pdf_bytes, TextractProcessor
from llm_processor import process_text_with_llm
from structured_llm_processor import process_structured_data_with_llm
from export_utils import export_to_pdf
from schema_validator import SchemaValidator
from chunking_processor import ChunkingProcessor
from llm_structured_extractor import LLMStructuredExtractor, extract_structured_data_from_payload
from merge_normalize_qa import MergeNormalizeQA, merge_normalize_qa_from_extraction

app = Flask(__name__)

# Ensure templates directory exists
os.makedirs('templates', exist_ok=True)
os.makedirs('static', exist_ok=True)

# Initialize processors
schema_validator = SchemaValidator()
chunking_processor = ChunkingProcessor()
llm_extractor = LLMStructuredExtractor()
merge_qa_processor = MergeNormalizeQA()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/extract', methods=['POST'])
def extract():
    if 'pdf' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['pdf']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    try:
        # Extract structured data from PDF using enhanced page-by-page Textract
        pdf_bytes = file.read()
        processor = TextractProcessor()
        structured_data = processor.extract_text_from_pdf_bytes_pagewise(pdf_bytes)
        
        # Return the enhanced JSON format with page-by-page results
        return jsonify(structured_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/extract_with_chunking', methods=['POST'])
def extract_with_chunking():
    """Enhanced extraction with chunking and batch preparation"""
    if 'pdf' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['pdf']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    try:
        # Phase 1: Extract structured data from PDF using enhanced page-by-page Textract
        pdf_bytes = file.read()
        processor = TextractProcessor()
        textract_result = processor.extract_text_from_pdf_bytes_pagewise(pdf_bytes)
        
        # Phase 2: Chunking & Batch Preparation
        payload = chunking_processor.process_complete_chunking_workflow(textract_result)
        
        # Validate payload structure
        validation = chunking_processor.validate_payload_structure(payload)
        
        # Save payload for audit
        job_id = textract_result.get('job_id', 'unknown')
        output_path = f'output/payload_{job_id}.json'
        os.makedirs('output', exist_ok=True)
        chunking_processor.save_payload(payload, output_path)
        
        return jsonify({
            'success': True,
            'textract_result': textract_result,
            'chunked_payload': payload,
            'validation': validation,
            'payload_saved': output_path,
            'processing_stats': {
                'textract_time': textract_result.get('processing_time', 0),
                'total_pages': textract_result.get('total_pages', 0),
                'chunking_stats': payload.get('processing_stats', {})
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/complete_extraction_pipeline', methods=['POST'])
def complete_extraction_pipeline():
    """Complete end-to-end extraction pipeline: Phases 1-4"""
    if 'pdf' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['pdf']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    try:
        import asyncio
        
        # Phase 1: Page-by-page Textract Processing
        print("Starting Phase 1: Page-by-page Textract Processing...")
        pdf_bytes = file.read()
        processor = TextractProcessor()
        textract_result = processor.extract_text_from_pdf_bytes_pagewise(pdf_bytes)
        
        # Phase 2: Chunking & Batch Preparation
        print("Starting Phase 2: Chunking & Batch Preparation...")
        payload = chunking_processor.process_complete_chunking_workflow(textract_result)
        
        # Phase 3: LLM Structured Extraction
        print("Starting Phase 3: LLM Structured Extraction...")
        extraction_results = asyncio.run(llm_extractor.process_structured_extraction(payload))
        
        # Phase 4: Merge, Normalize & QA
        print("Starting Phase 4: Merge, Normalize & QA...")
        qa_results = asyncio.run(merge_qa_processor.process_merge_normalize_qa(extraction_results))
        
        # Phase 5: Schema Validation & Canonical Transformation
        print("Starting Phase 5: Schema Validation & Canonical Transformation...")
        canonical_data = schema_validator.transform_to_canonical(
            qa_results.get('final_data', [])
        )
        validation_result = schema_validator.validate_data(canonical_data)
        
        # Save all results for audit
        job_id = textract_result.get('job_id', 'unknown')
        os.makedirs('output', exist_ok=True)
        
        # Save payload
        payload_path = f'output/payload_{job_id}.json'
        chunking_processor.save_payload(payload, payload_path)
        
        # Save extraction results
        extraction_path = f'output/extraction_{job_id}.json'
        llm_extractor.save_extraction_results(extraction_results, extraction_path)
        
        # Save QA results
        qa_path = f'output/qa_results_{job_id}.json'
        merge_qa_processor.save_results(qa_results, qa_path)
        
        # Save canonical data
        canonical_path = f'output/canonical_{job_id}.json'
        schema_validator.save_canonical_data(canonical_data, canonical_path)
        
        # Compile comprehensive results
        complete_results = {
            'success': True,
            'job_id': job_id,
            'phases_completed': 5,
            'phase_1_textract': {
                'total_pages': textract_result.get('total_pages', 0),
                'processing_time': textract_result.get('processing_time', 0),
                'errors': textract_result.get('errors', [])
            },
            'phase_2_chunking': {
                'total_tables': len(payload.get('raw', {}).get('tables', [])),
                'total_kvs': len(payload.get('raw', {}).get('kvs', [])),
                'total_text_chunks': len(payload.get('raw', {}).get('text_chunks', [])),
                'payload_validation': chunking_processor.validate_payload_structure(payload)
            },
            'phase_3_extraction': {
                'tables_extracted': len(extraction_results.get('tables_extracted', [])),
                'keyvalues_extracted': len(extraction_results.get('keyvalues_extracted', [])),
                'narrative_extracted': len(extraction_results.get('narrative_extracted', [])),
                'total_extracted_items': extraction_results.get('total_extracted_items', 0),
                'processing_stats': extraction_results.get('processing_stats', {})
            },
            'phase_4_merge_qa': {
                'original_items': qa_results.get('processing_stats', {}).get('original_items', 0),
                'final_items': qa_results.get('processing_stats', {}).get('final_items', 0),
                'duplicates_removed': qa_results.get('processing_stats', {}).get('duplicates_removed', 0),
                'qa_success_rate': qa_results.get('qa_results', {}).get('summary', {}).get('success_rate', 0),
                'qa_failures': qa_results.get('qa_results', {}).get('summary', {}).get('total_failed', 0)
            },
            'phase_5_validation': {
                'canonical_items': len(canonical_data),
                'validation_valid': validation_result.get('valid', False),
                'validation_errors': len(validation_result.get('errors', [])),
                'validation_warnings': len(validation_result.get('warnings', []))
            },
            'saved_files': {
                'payload': payload_path,
                'extraction_results': extraction_path,
                'qa_results': qa_path,
                'canonical_data': canonical_path
            },
            'final_data': qa_results.get('final_data', []),
            'canonical_data': canonical_data,
            'validation_result': validation_result,
            'qa_results': qa_results.get('qa_results', {})
        }
        
        print(f"Complete extraction pipeline finished: {len(canonical_data)} items extracted and validated")
        return jsonify(complete_results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/extract_legacy', methods=['POST'])
def extract_legacy():
    """Legacy extraction endpoint using the original method"""
    if 'pdf' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['pdf']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    try:
        # Extract structured data from PDF using original Textract method
        pdf_bytes = file.read()
        structured_data = extract_structured_data_from_pdf_bytes(pdf_bytes)
        
        # Return the original JSON format
        return jsonify(structured_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/process', methods=['POST'])
def process():
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    try:
        import pandas as pd
        
        # Process the structured JSON data with separate LLM calls and commentary matching
        result = process_structured_data_with_llm(data)
        
        # Use the enhanced data with commentary if available
        if 'enhanced_data_with_commentary' in result and result['enhanced_data_with_commentary']:
            clean_data = result['enhanced_data_with_commentary']
            
            # Add general commentary as a separate row if it exists
            if result.get('general_commentary'):
                clean_data.append({
                    'source': 'Document Text',
                    'type': 'General Commentary',
                    'field': 'Unmatched Commentary',
                    'value': result['general_commentary'][:500] + '...' if len(result['general_commentary']) > 500 else result['general_commentary'],
                    'page': 'N/A',
                    'commentary': '',
                    'has_commentary': False
                })
        else:
            # Fallback to original processing if enhanced data is not available
            df_data = []
            
            # Process tables
            if 'processed_tables' in result and result['processed_tables']:
                for i, table in enumerate(result['processed_tables']):
                    if table.get('structured_table') and not table['structured_table'].get('error'):
                        table_data = table['structured_table']
                        page = table.get('page', 'N/A')
                        
                        # Handle different table structures
                        if isinstance(table_data, dict):
                            for key, value in table_data.items():
                                if key != 'error':
                                    df_data.append({
                                        'source': f'Table {i+1}',
                                        'type': 'Table Data',
                                        'field': key,
                                        'value': str(value) if value else '',
                                        'page': page,
                                        'commentary': '',
                                        'has_commentary': False
                                    })
            
            # Process key-value pairs
            if 'processed_key_values' in result and result['processed_key_values']:
                kv_data = result['processed_key_values'].get('structured_key_values', {})
                if kv_data and not kv_data.get('error'):
                    for key, value in kv_data.items():
                        if key != 'error':
                            df_data.append({
                                'source': 'Key-Value Pairs',
                                'type': 'Structured Data',
                                'field': key,
                                'value': str(value) if value else '',
                                'page': 'N/A',
                                'commentary': '',
                                'has_commentary': False
                            })
            
            # Process document text facts
            if 'processed_document_text' in result and result['processed_document_text']:
                for chunk_idx, chunk in enumerate(result['processed_document_text']):
                    if 'extracted_facts' in chunk and not chunk['extracted_facts'].get('error'):
                        facts = chunk['extracted_facts']
                        for key, value in facts.items():
                            if key != 'error':
                                df_data.append({
                                    'source': f'Text Chunk {chunk_idx+1}',
                                    'type': 'Financial Data',
                                    'field': key,
                                    'value': str(value) if value else '',
                                    'page': 'N/A',
                                    'commentary': '',
                                    'has_commentary': False
                                })
            
            # Clean data without DataFrame
            if df_data:
                clean_data = []
                for item in df_data:
                    if item['value'] and str(item['value']).strip() and str(item['value']) != 'nan':
                        clean_data.append(item)
            else:
                clean_data = []
        
        # Transform to canonical schema format
        canonical_data = schema_validator.transform_to_canonical(clean_data)
        
        # Validate canonical data
        validation_result = schema_validator.validate_data(canonical_data)
        
        # Return enhanced response with canonical data and validation
        response = {
            **result,
            'dataframe_data': clean_data,
            'canonical_data': canonical_data,
            'validation_result': validation_result,
            'total_rows': len(clean_data),
            'canonical_rows': len(canonical_data)
        }
        
        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/extract_structured', methods=['POST'])
def extract_structured():
    """Extract structured data from PDF using Amazon Textract"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename or not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files are supported'}), 400
    
    try:
        # Extract structured data using Textract
        pdf_bytes = file.read()
        structured_data = extract_structured_data_from_pdf_bytes(pdf_bytes)
        
        return jsonify({
            'success': True,
            'structured_data': structured_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/validate_schema', methods=['POST'])
def validate_schema():
    """Validate data against canonical schema"""
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    try:
        validation_result = schema_validator.validate_data(data)
        return jsonify(validation_result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/transform_to_canonical', methods=['POST'])
def transform_to_canonical():
    """Transform extracted data to canonical schema format"""
    data = request.json
    if not data or 'extracted_data' not in data:
        return jsonify({'error': 'No extracted_data provided'}), 400
    
    try:
        canonical_data = schema_validator.transform_to_canonical(data['extracted_data'])
        validation_result = schema_validator.validate_data(canonical_data)
        
        return jsonify({
            'canonical_data': canonical_data,
            'validation_result': validation_result,
            'total_items': len(canonical_data)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/chunk_and_batch', methods=['POST'])
def chunk_and_batch():
    """Phase 3: Chunking & Batch Preparation"""
    data = request.json
    if not data or 'textract_result' not in data:
        return jsonify({'error': 'No textract_result provided'}), 400
    
    try:
        # Process complete chunking workflow
        payload = chunking_processor.process_complete_chunking_workflow(data['textract_result'])
        
        # Validate payload structure
        validation = chunking_processor.validate_payload_structure(payload)
        
        # Save payload if requested
        if data.get('save_payload', False):
            filename = data.get('filename', f'payload_{int(time.time())}.json')
            output_path = f'output/{filename}'
            os.makedirs('output', exist_ok=True)
            chunking_processor.save_payload(payload, output_path)
        
        return jsonify({
            'success': True,
            'payload': payload,
            'validation': validation,
            'stats': payload.get('processing_stats', {})
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/llm_structured_extraction', methods=['POST'])
def llm_structured_extraction():
    """Phase 4: LLM - Structured Extraction"""
    data = request.json
    if not data or 'payload' not in data:
        return jsonify({'error': 'No payload provided'}), 400
    
    try:
        import asyncio
        
        # Process structured extraction
        extraction_results = asyncio.run(llm_extractor.process_structured_extraction(data['payload']))
        
        # Save extraction results if requested
        if data.get('save_results', False):
            job_id = data['payload'].get('metadata', {}).get('job_id', 'unknown')
            filename = data.get('filename', f'extraction_{job_id}_{int(time.time())}.json')
            output_path = f'output/{filename}'
            os.makedirs('output', exist_ok=True)
            llm_extractor.save_extraction_results(extraction_results, output_path)
        
        # Transform to canonical format and validate
        canonical_data = schema_validator.transform_to_canonical(
            extraction_results.get('combined_extracted_data', [])
        )
        validation_result = schema_validator.validate_data(canonical_data)
        
        return jsonify({
            'success': True,
            'extraction_results': extraction_results,
            'canonical_data': canonical_data,
            'validation_result': validation_result,
            'processing_stats': extraction_results.get('processing_stats', {}),
            'total_extracted_items': extraction_results.get('total_extracted_items', 0)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/merge_normalize_qa', methods=['POST'])
def merge_normalize_qa():
    """Phase 5: Merge, Normalize & QA"""
    data = request.json
    if not data or 'extraction_results' not in data:
        return jsonify({'error': 'No extraction_results provided'}), 400
    
    try:
        import asyncio
        
        # Process merge, normalize and QA
        qa_results = asyncio.run(merge_qa_processor.process_merge_normalize_qa(data['extraction_results']))
        
        # Save results if requested
        if data.get('save_results', False):
            job_id = qa_results.get('metadata', {}).get('job_id', 'unknown')
            filename = data.get('filename', f'merged_qa_{job_id}_{int(time.time())}.json')
            output_path = f'output/{filename}'
            os.makedirs('output', exist_ok=True)
            merge_qa_processor.save_results(qa_results, output_path)
        
        return jsonify({
            'success': True,
            'qa_results': qa_results,
            'processing_stats': qa_results.get('processing_stats', {}),
            'qa_summary': qa_results.get('qa_results', {}).get('summary', {}),
            'final_data_count': len(qa_results.get('final_data', []))
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/save_canonical', methods=['POST'])
def save_canonical():
    """Save canonical data to file after validation"""
    data = request.json
    if not data or 'canonical_data' not in data:
        return jsonify({'error': 'No canonical_data provided'}), 400
    
    try:
        filename = data.get('filename', f'canonical_data_{int(time.time())}.json')
        output_path = f'output/{filename}'
        os.makedirs('output', exist_ok=True)
        
        success = schema_validator.save_canonical_data(data['canonical_data'], output_path)
        
        if success:
            return jsonify({
                'success': True,
                'filename': filename,
                'path': output_path
            })
        else:
            return jsonify({'error': 'Failed to save canonical data'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export/pdf', methods=['POST'])
def export_pdf():
    data = request.json
    if not data or 'data' not in data:
        return jsonify({'error': 'No data provided'}), 400
    
    try:
        import pandas as pd
        df = pd.DataFrame(data['data'])
        pdf_bytes = export_to_pdf(df)
        
        return jsonify({
            'pdf': base64.b64encode(pdf_bytes).decode('utf-8')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)