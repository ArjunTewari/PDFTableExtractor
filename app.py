from flask import Flask, render_template, request, jsonify, send_file, Response
import os
import tempfile
import base64
import json
from io import BytesIO

from pdf_processor import extract_text_from_pdf, extract_text_from_pdf_bytes
from llm_processor import process_text_with_llm
from agentic_processor_simple import process_text_with_agents
from export_utils import export_to_pdf
from storage_manager import storage_manager

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
        
        # Store extracted text for crew access
        text_id = storage_manager.store_extracted_text(extracted_text, file.filename or "unknown.pdf")
        
        return jsonify({
            'text': extracted_text,
            'text_id': text_id
        })
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
            text_content = data['text']
            text_id = data.get('text_id')
            
            result = process_text_with_agents(text_content)
            structured_data = result.get('final_tabulation', [])
            
            # Store processing results if we have a text_id
            if text_id:
                storage_manager.store_processing_results(text_id, result)
            
            return jsonify({
                'data': structured_data,
                'iteration_history': result.get('iteration_history', []),
                'metadata': {
                    'processing_mode': 'agentic',
                    'total_iterations': result.get('total_iterations', 0),
                    'final_coverage': result.get('final_coverage', 0),
                    'optimization': result.get('optimization', {}),
                    'text_id': text_id
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

@app.route('/storage/texts', methods=['GET'])
def list_stored_texts():
    """API endpoint for crew to list all stored texts"""
    try:
        texts = storage_manager.list_stored_texts()
        return jsonify({'texts': texts})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/storage/text/<text_id>', methods=['GET'])
def get_stored_text(text_id):
    """API endpoint for crew to retrieve specific text by ID"""
    try:
        text_data = storage_manager.retrieve_text(text_id)
        if not text_data:
            return jsonify({'error': 'Text not found'}), 404
        return jsonify(text_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/storage/history/<text_id>', methods=['GET'])
def get_processing_history(text_id):
    """API endpoint for crew to get processing history for a text"""
    try:
        history = storage_manager.get_processing_history(text_id)
        return jsonify({'history': history})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/storage/cleanup', methods=['POST'])
def cleanup_storage():
    """API endpoint to cleanup old stored files"""
    try:
        days = request.json.get('days', 7) if request.json else 7
        storage_manager.cleanup_old_files(days)
        return jsonify({'message': f'Cleaned up files older than {days} days'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/process_stream', methods=['POST'])
def process_stream():
    """Stream crew processing results in real-time"""
    data = request.json
    if not data or 'text' not in data:
        return jsonify({'error': 'No text provided'}), 400
    
    def generate_stream():
        try:
            text_content = data['text']
            text_id = data.get('text_id')
            
            # Import the streaming processor
            from agentic_processor_streaming import process_with_streaming
            
            # Process with streaming updates
            for update in process_with_streaming(text_content):
                yield f"data: {json.dumps(update)}\n\n"
                
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return Response(generate_stream(), mimetype='text/event-stream',
                   headers={'Cache-Control': 'no-cache',
                           'Access-Control-Allow-Origin': '*'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)