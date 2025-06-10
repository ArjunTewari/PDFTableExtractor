from flask import Flask, render_template, request, jsonify, send_file
import os
import tempfile
import base64
import json
from io import BytesIO

from textract_processor import extract_text_from_pdf, extract_text_from_pdf_bytes, extract_structured_data_from_pdf_bytes
from llm_processor import process_text_with_llm
from agentic_processor_simple import process_text_with_agents
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
        # Extract text from PDF
        pdf_bytes = file.read()
        extracted_text = extract_text_from_pdf_bytes(pdf_bytes)
        
        return jsonify({'text': extracted_text})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/process', methods=['POST'])
def process():
    data = request.json
    if not data or 'text' not in data:
        return jsonify({'error': 'No text provided'}), 400
    
    try:
        # Check processing mode (standard or agentic)
        processing_mode = data.get('mode', 'agentic')  # Default to agentic for enhanced processing
        
        if processing_mode == 'agentic':
            # Use agentic processing with iterative cross-verification
            result = process_text_with_agents(data['text'])
            structured_data = result.get('final_tabulation', [])
            
            return jsonify({
                'data': structured_data,
                'iteration_history': result.get('iteration_history', []),
                'metadata': {
                    'processing_mode': 'agentic',
                    'total_iterations': result.get('total_iterations', 0),
                    'final_coverage': result.get('final_coverage', 0),
                    'optimization': result.get('optimization', {})
                }
            })
        else:
            # Use standard processing
            structured_data = process_text_with_llm(data['text'])
            return jsonify({
                'data': structured_data,
                'metadata': {
                    'processing_mode': 'standard'
                }
            })
            
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
    
    if not file.filename.lower().endswith('.pdf'):
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