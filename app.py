from flask import Flask, render_template, request, jsonify, send_file, Response
import os
import tempfile
import base64
import json
from io import BytesIO

from textract_processor import extract_text_from_pdf, extract_text_from_pdf_bytes, extract_structured_data_from_pdf_bytes
from llm_processor import process_text_with_llm
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

def find_relevant_document_text(row_data, document_text):
    """Find relevant text from document that mentions this data point"""
    field = row_data.get('field', '').lower()
    value = str(row_data.get('value', '')).lower()
    
    # Clean field and value for better matching
    field_words = field.replace('_', ' ').split()
    value_clean = value.replace('$', '').replace('%', '').replace(',', '')
    
    best_match = ""
    best_score = 0
    
    # Look for the best matching text segment
    for i, line in enumerate(document_text):
        line_lower = line.lower()
        score = 0
        
        # Score based on field word matches
        for word in field_words:
            if len(word) > 2 and word in line_lower:
                score += 2
        
        # Score based on value matches
        if value_clean and value_clean in line_lower:
            score += 5
        elif value and value in line_lower:
            score += 3
        
        # If we found a good match, include surrounding context
        if score > best_score:
            best_score = score
            # Get context around the matching line
            start_idx = max(0, i - 1)
            end_idx = min(len(document_text), i + 2)
            context_lines = document_text[start_idx:end_idx]
            
            # Join and clean up the context
            context = ' '.join(context_lines).strip()
            # Truncate at sentence boundary if possible
            if len(context) > 400:
                sentences = context.split('. ')
                truncated = '. '.join(sentences[:2])
                if len(truncated) < 400:
                    best_match = truncated + '.'
                else:
                    best_match = context[:400] + '...'
            else:
                best_match = context
    
    return best_match if best_score > 1 else ''

def get_unmatched_document_text(df_data, document_text):
    """Get document text that doesn't match any extracted data"""
    used_indices = set()
    
    # Mark lines that were used for commentary (with context)
    for row in df_data:
        if row.get('commentary'):
            commentary_sample = row['commentary'][:100].lower()
            for i, line in enumerate(document_text):
                if commentary_sample in line.lower():
                    # Mark this line and surrounding context as used
                    for j in range(max(0, i-1), min(len(document_text), i+2)):
                        used_indices.add(j)
    
    # Collect unused lines in meaningful paragraphs
    unmatched_paragraphs = []
    current_paragraph = []
    
    for i, line in enumerate(document_text):
        if i not in used_indices and len(line.strip()) > 15:
            current_paragraph.append(line.strip())
        else:
            # End of paragraph - save if substantial
            if current_paragraph:
                paragraph_text = ' '.join(current_paragraph)
                if len(paragraph_text) > 50:  # Only keep substantial paragraphs
                    unmatched_paragraphs.append(paragraph_text)
                current_paragraph = []
    
    # Don't forget the last paragraph
    if current_paragraph:
        paragraph_text = ' '.join(current_paragraph)
        if len(paragraph_text) > 50:
            unmatched_paragraphs.append(paragraph_text)
    
    # Limit and truncate paragraphs for readability
    final_chunks = []
    for paragraph in unmatched_paragraphs[:3]:  # Limit to 3 substantial chunks
        if len(paragraph) > 600:
            # Truncate at sentence boundary
            sentences = paragraph.split('. ')
            truncated = '. '.join(sentences[:3])
            if len(truncated) < 600:
                final_chunks.append(truncated + '.')
            else:
                final_chunks.append(paragraph[:600] + '...')
        else:
            final_chunks.append(paragraph)
    
    return final_chunks

@app.route('/process_stream', methods=['POST'])
def process_stream():
    """Streaming endpoint for progressive data processing"""
    data = request.json
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    def generate():
        try:
            import pandas as pd
            
            # Process the structured JSON data 
            result = process_structured_data_with_llm(data)
            
            # Initialize data collection
            df_data = []
            
            # Process tables - restore simple format and add commentary
            if 'processed_tables' in result and result['processed_tables']:
                for i, table in enumerate(result['processed_tables']):
                    if table.get('structured_table') and not table['structured_table'].get('error'):
                        table_data = table['structured_table']
                        page = table.get('page', 'N/A')
                        
                        # Stream each table data row as it's processed
                        for key, value in table_data.items():
                            if key != 'error' and value:
                                row_data = {
                                    'source': f'Table {i+1}',
                                    'type': 'Table Data',
                                    'field': key,
                                    'value': str(value),
                                    'page': page,
                                    'commentary': ''  # Will be filled from document text only
                                }
                                df_data.append(row_data)
                                # Stream this row immediately
                                yield f"data: {json.dumps({'type': 'row', 'data': row_data})}\n\n"
            
            # Process key-value pairs
            if 'processed_key_values' in result and result['processed_key_values']:
                kv_data = result['processed_key_values'].get('structured_key_values', {})
                if kv_data and not kv_data.get('error'):
                    for key, value in kv_data.items():
                        if key != 'error' and value:
                            row_data = {
                                'source': 'Key-Value Pairs',
                                'type': 'Structured Data',
                                'field': key,
                                'value': str(value),
                                'page': 'N/A',
                                'commentary': ''  # Will be filled from document text only
                            }
                            df_data.append(row_data)
                            # Stream this row immediately
                            yield f"data: {json.dumps({'type': 'row', 'data': row_data})}\n\n"
            
            # Process labeled paragraphs if available
            if 'processed_labeled_text' in result and result['processed_labeled_text']:
                for chunk_idx, chunk in enumerate(result['processed_labeled_text']):
                    if 'extracted_facts' in chunk and not chunk['extracted_facts'].get('error'):
                        facts = chunk['extracted_facts']
                        for key, value in facts.items():
                            if key != 'error' and value:
                                row_data = {
                                    'source': f'Labeled Text {chunk_idx+1}',
                                    'type': 'Metric Data',
                                    'field': key,
                                    'value': str(value),
                                    'page': 'N/A',
                                    'commentary': ''  # Will be filled from document text only
                                }
                                df_data.append(row_data)
                                # Stream this row immediately
                                yield f"data: {json.dumps({'type': 'row', 'data': row_data})}\n\n"
            
            # Process document text facts (fallback)
            elif 'processed_document_text' in result and result['processed_document_text']:
                for chunk_idx, chunk in enumerate(result['processed_document_text']):
                    if 'extracted_facts' in chunk and not chunk['extracted_facts'].get('error'):
                        facts = chunk['extracted_facts']
                        for key, value in facts.items():
                            if key != 'error' and value:
                                row_data = {
                                    'source': f'Text Chunk {chunk_idx+1}',
                                    'type': 'Financial Data',
                                    'field': key,
                                    'value': str(value),
                                    'page': 'N/A',
                                    'commentary': ''  # Will be filled from document text only
                                }
                                df_data.append(row_data)
                                # Stream this row immediately
                                yield f"data: {json.dumps({'type': 'row', 'data': row_data})}\n\n"
            
            # Now add commentary from document text only (no AI-generated comments)
            document_text = data.get('document_text', [])
            if document_text and df_data:
                for row in df_data:
                    # Find relevant text from document that mentions this data point
                    relevant_text = find_relevant_document_text(row, document_text)
                    if relevant_text:
                        row['commentary'] = relevant_text
                        # Stream updated row with commentary
                        yield f"data: {json.dumps({'type': 'row_update', 'data': row})}\n\n"
            
            # Add general unmatched document text as separate entries
            if document_text:
                unmatched_text = get_unmatched_document_text(df_data, document_text)
                if unmatched_text:
                    for idx, text_chunk in enumerate(unmatched_text):
                        row_data = {
                            'source': 'Document Text',
                            'type': 'General Commentary',
                            'field': f'Text Segment {idx+1}',
                            'value': text_chunk[:500] + '...' if len(text_chunk) > 500 else text_chunk,
                            'page': 'N/A',
                            'commentary': 'Unmatched document content'
                        }
                        df_data.append(row_data)
                        yield f"data: {json.dumps({'type': 'row', 'data': row_data})}\n\n"
            
            # Send completion signal
            yield f"data: {json.dumps({'type': 'complete', 'total_rows': len(df_data)})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'status': 'error', 'error': str(e)})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Cache-Control',
        'X-Accel-Buffering': 'no'
    })
    
    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Cache-Control',
        'X-Accel-Buffering': 'no'
    })

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
            
            # Process document text with tabulation
            if 'processed_document_text' in result and result['processed_document_text']:
                for chunk_idx, chunk in enumerate(result['processed_document_text']):
                    # Handle tabulated document text structure
                    if 'table_headers' in chunk and 'table_rows' in chunk:
                        headers = chunk['table_headers']
                        rows = chunk['table_rows']
                        
                        # Add document text table structure
                        df_data.append({
                            'source': f'Document Text {chunk_idx+1}',
                            'type': 'Document Table',
                            'field': 'Headers',
                            'value': ' | '.join(headers),
                            'page': 'N/A',
                            'commentary': 'Tabulated document content',
                            'has_commentary': True,
                            'is_table_header': True,
                            'table_id': f'doc_{chunk_idx}',
                            'headers': headers,
                            'rows': rows
                        })
                        
                        # Add individual data points from document table
                        for row_idx, row in enumerate(rows):
                            for col_idx, cell_value in enumerate(row):
                                if col_idx < len(headers) and cell_value:
                                    df_data.append({
                                        'source': f'Document Text {chunk_idx+1}',
                                        'type': 'Document Data',
                                        'field': f'{headers[col_idx]}_Row_{row_idx+1}',
                                        'value': str(cell_value),
                                        'page': 'N/A',
                                        'commentary': '',
                                        'has_commentary': False,
                                        'table_id': f'doc_{chunk_idx}'
                                    })
                    
                    # Also handle extracted facts if available
                    if 'extracted_facts' in chunk and not chunk['extracted_facts'].get('error'):
                        facts = chunk['extracted_facts']
                        for key, value in facts.items():
                            if key != 'error' and value:
                                df_data.append({
                                    'source': f'Text Chunk {chunk_idx+1}',
                                    'type': 'Financial Data',
                                    'field': key,
                                    'value': str(value),
                                    'page': 'N/A',
                                    'commentary': '',
                                    'has_commentary': False
                                })
            
            # Clean and prepare data
            if df_data:
                # Filter out empty values directly from the list
                clean_data = [
                    item for item in df_data 
                    if item.get('value', '').strip() and item.get('value') != 'nan'
                ]
            else:
                clean_data = []
        
        # Return both original result and clean DataFrame data
        response = {
            **result,
            'dataframe_data': clean_data,
            'total_rows': len(clean_data)
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