from flask import Flask, render_template, request, jsonify, send_file
import os
import tempfile
import base64
import json
from io import BytesIO

from textract_processor import extract_text_from_pdf, extract_text_from_pdf_bytes, extract_structured_data_from_pdf_bytes
from llm_processor import process_text_with_llm
from agentic_processor_simple import process_text_with_agents
from structured_llm_processor import process_structured_data_with_llm
from export_utils import export_to_pdf

app = Flask(__name__)

# Ensure templates directory exists
os.makedirs('templates', exist_ok=True)
os.makedirs('static', exist_ok=True)

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
        # Extract structured data from PDF using Amazon Textract
        pdf_bytes = file.read()
        structured_data = extract_structured_data_from_pdf_bytes(pdf_bytes)
        
        # Return the new JSON format
        return jsonify(structured_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/process', methods=['POST'])
def process():
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    try:
        # Process the structured JSON data with separate LLM calls
        result = process_structured_data_with_llm(data)
        
        # Add debug logging to understand the structure
        print("=== DEBUG: AI Processing Result Structure ===")
        print(f"Result keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
        
        if 'processed_tables' in result:
            print(f"Tables count: {len(result['processed_tables'])}")
            for i, table in enumerate(result['processed_tables']):
                print(f"Table {i}: {type(table.get('structured_table', 'N/A'))}")
                if isinstance(table.get('structured_table'), dict):
                    print(f"Table {i} keys: {list(table['structured_table'].keys())}")
        
        if 'processed_key_values' in result:
            kv_data = result['processed_key_values']
            print(f"Key-values type: {type(kv_data)}")
            if isinstance(kv_data, dict) and 'structured_key_values' in kv_data:
                print(f"Structured KV type: {type(kv_data['structured_key_values'])}")
        
        if 'processed_document_text' in result:
            print(f"Document text chunks: {len(result['processed_document_text'])}")
            for i, chunk in enumerate(result['processed_document_text']):
                if 'extracted_facts' in chunk:
                    print(f"Chunk {i} facts type: {type(chunk['extracted_facts'])}")
        
        print("=== END DEBUG ===")
        
        return jsonify(result)
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