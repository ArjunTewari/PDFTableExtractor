import boto3
import json
import os
from typing import Dict, Any, List
from io import BytesIO
from PIL import Image
import base64
import fitz  # PyMuPDF
import tempfile

class TextractProcessor:
    def __init__(self):
        """Initialize AWS Textract client with credentials from environment"""
        self.textract_client = boto3.client(
            'textract',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
    
    def extract_text_from_pdf_bytes(self, pdf_bytes: bytes) -> str:
        """
        Extract text from PDF bytes using Amazon Textract.
        Converts PDF pages to images first, then processes with Textract.
        
        Args:
            pdf_bytes (bytes): PDF file as bytes
            
        Returns:
            str: Extracted text from the PDF
        """
        try:
            print("Using Amazon Textract for text extraction from PDF bytes")
            
            # Convert PDF to images using PyMuPDF
            pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
            page_count = len(pdf_document)
            all_extracted_text = []
            
            for page_num in range(page_count):
                print(f"Processing page {page_num + 1}/{page_count}")
                
                # Get page
                page = pdf_document.load_page(page_num)
                
                # Convert page to image
                mat = fitz.Matrix(2.0, 2.0)  # Scale factor for better quality
                pix = page.get_pixmap(matrix=mat)
                img_data = pix.tobytes("png")
                
                # Use Textract to analyze the page image
                response = self.textract_client.analyze_document(
                    Document={'Bytes': img_data},
                    FeatureTypes=['TABLES', 'FORMS']
                )
                
                # Extract text from the response
                page_text = self._extract_text_from_response(response)
                if page_text.strip():
                    all_extracted_text.append(f"=== PAGE {page_num + 1} ===\n{page_text}")
            
            pdf_document.close()
            
            # Combine all pages
            final_text = '\n\n'.join(all_extracted_text)
            print(f"Extracted {len(final_text)} characters from {page_count} pages using Textract")
            return final_text
            
        except Exception as e:
            print(f"Textract extraction failed: {e}")
            raise Exception(f"Failed to extract text using Amazon Textract: {str(e)}")
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract text from a PDF file using Amazon Textract.
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            str: Extracted text from the PDF
        """
        try:
            with open(pdf_path, 'rb') as pdf_file:
                pdf_bytes = pdf_file.read()
            
            return self.extract_text_from_pdf_bytes(pdf_bytes)
            
        except Exception as e:
            print(f"Error reading PDF file: {e}")
            raise Exception(f"Failed to read PDF file: {str(e)}")
    
    def _extract_text_from_response(self, response: Dict[str, Any]) -> str:
        """
        Extract and organize text from Textract response.
        
        Args:
            response: Textract analyze_document response
            
        Returns:
            str: Organized extracted text
        """
        blocks = response.get('Blocks', [])
        
        # Separate different types of content
        lines = []
        tables = []
        key_value_pairs = []
        
        # Process blocks
        for block in blocks:
            if block['BlockType'] == 'LINE':
                lines.append({
                    'text': block['Text'],
                    'confidence': block['Confidence'],
                    'geometry': block['Geometry']
                })
            elif block['BlockType'] == 'TABLE':
                table_text = self._extract_table_text(block, blocks)
                if table_text:
                    tables.append(table_text)
            elif block['BlockType'] == 'KEY_VALUE_SET':
                kv_text = self._extract_key_value_text(block, blocks)
                if kv_text:
                    key_value_pairs.append(kv_text)
        
        # Sort lines by their position (top to bottom, left to right)
        lines.sort(key=lambda x: (
            x['geometry']['BoundingBox']['Top'],
            x['geometry']['BoundingBox']['Left']
        ))
        
        # Combine all text
        result_parts = []
        
        # Add line text
        if lines:
            result_parts.append("=== DOCUMENT TEXT ===")
            for line in lines:
                result_parts.append(line['text'])
        
        # Add table data
        if tables:
            result_parts.append("\n=== TABLES ===")
            for table in tables:
                result_parts.append(table)
        
        # Add key-value pairs
        if key_value_pairs:
            result_parts.append("\n=== KEY-VALUE PAIRS ===")
            for kv in key_value_pairs:
                result_parts.append(kv)
        
        return '\n'.join(result_parts)
    
    def _extract_table_text(self, table_block: Dict[str, Any], all_blocks: List[Dict[str, Any]]) -> str:
        """Extract text from table blocks"""
        if 'Relationships' not in table_block:
            return ""
        
        table_text = []
        
        for relationship in table_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for cell_id in relationship['Ids']:
                    cell_block = next((b for b in all_blocks if b['Id'] == cell_id), None)
                    if cell_block and cell_block['BlockType'] == 'CELL':
                        cell_text = self._get_cell_text(cell_block, all_blocks)
                        if cell_text:
                            row = cell_block.get('RowIndex', 0)
                            col = cell_block.get('ColumnIndex', 0)
                            table_text.append(f"[Row {row}, Col {col}]: {cell_text}")
        
        return '\n'.join(table_text) if table_text else ""
    
    def _get_cell_text(self, cell_block: Dict[str, Any], all_blocks: List[Dict[str, Any]]) -> str:
        """Extract text from a table cell"""
        if 'Relationships' not in cell_block:
            return ""
        
        cell_text = []
        for relationship in cell_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for word_id in relationship['Ids']:
                    word_block = next((b for b in all_blocks if b['Id'] == word_id), None)
                    if word_block and word_block['BlockType'] == 'WORD':
                        cell_text.append(word_block['Text'])
        
        return ' '.join(cell_text)
    
    def _extract_key_value_text(self, kv_block: Dict[str, Any], all_blocks: List[Dict[str, Any]]) -> str:
        """Extract text from key-value pairs"""
        if kv_block.get('EntityTypes') and 'KEY' in kv_block['EntityTypes']:
            key_text = self._get_text_from_block(kv_block, all_blocks)
            
            # Find corresponding value
            value_text = ""
            if 'Relationships' in kv_block:
                for relationship in kv_block['Relationships']:
                    if relationship['Type'] == 'VALUE':
                        for value_id in relationship['Ids']:
                            value_block = next((b for b in all_blocks if b['Id'] == value_id), None)
                            if value_block:
                                value_text = self._get_text_from_block(value_block, all_blocks)
                                break
            
            if key_text and value_text:
                return f"{key_text}: {value_text}"
            elif key_text:
                return key_text
        
        return ""
    
    def _get_text_from_block(self, block: Dict[str, Any], all_blocks: List[Dict[str, Any]]) -> str:
        """Get text content from a block"""
        if 'Text' in block:
            return block['Text']
        
        if 'Relationships' not in block:
            return ""
        
        text_parts = []
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = next((b for b in all_blocks if b['Id'] == child_id), None)
                    if child_block and 'Text' in child_block:
                        text_parts.append(child_block['Text'])
        
        return ' '.join(text_parts)

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """
    Main function to extract text from PDF bytes using Amazon Textract.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        str: Extracted text from the PDF
    """
    processor = TextractProcessor()
    return processor.extract_text_from_pdf_bytes(pdf_bytes)

def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Main function to extract text from PDF file using Amazon Textract.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        str: Extracted text from the PDF
    """
    processor = TextractProcessor()
    return processor.extract_text_from_pdf(pdf_path)