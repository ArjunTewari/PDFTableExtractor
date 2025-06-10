import boto3
import json
import os
import time
from typing import Dict, Any, List, Optional
from io import BytesIO
import uuid
import fitz  # PyMuPDF
from PIL import Image

class TextractProcessor:
    def __init__(self):
        """Initialize AWS Textract and S3 clients with credentials from environment"""
        self.textract_client = boto3.client(
            'textract',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        self.s3_bucket = 'textract-temp-bucket-' + str(uuid.uuid4())[:8]
    
    def extract_text_from_pdf_bytes(self, pdf_bytes: bytes) -> Dict[str, Any]:
        """
        Extract structured data from PDF bytes using Amazon Textract asynchronous processing.
        
        Args:
            pdf_bytes (bytes): PDF file as bytes
            
        Returns:
            Dict[str, Any]: Structured JSON with tables, key-values, and raw text
        """
        start_time = time.time()
        
        try:
            print("Using Amazon Textract asynchronous processing for PDF bytes")
            
            # Determine processing method based on file size
            file_size_mb = len(pdf_bytes) / (1024 * 1024)
            
            if file_size_mb > 5:  # Use asynchronous processing for larger files
                return self._process_async(pdf_bytes, start_time)
            else:  # Use synchronous processing for smaller files
                return self._process_sync(pdf_bytes, start_time)
                
        except Exception as e:
            print(f"Textract extraction failed: {e}")
            raise Exception(f"Failed to extract text using Amazon Textract: {str(e)}")
    
    def _process_sync(self, pdf_bytes: bytes, start_time: float) -> Dict[str, Any]:
        """Process PDF synchronously by converting to images and using analyze_document"""
        # Convert PDF to images using PyMuPDF
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
        all_blocks = []
        page_count = len(pdf_document)
        
        for page_num in range(page_count):
            print(f"Processing page {page_num + 1}/{page_count}")
            
            # Get page
            page = pdf_document.load_page(page_num)
            
            # Convert page to image using PyMuPDF
            mat = fitz.Matrix(2.0, 2.0)  # Scale factor for better quality
            pix = page.get_pixmap(matrix=mat)
            img_data = pix.tobytes("png")
            
            # Use Textract to analyze the page image
            response = self.textract_client.analyze_document(
                Document={'Bytes': img_data},
                FeatureTypes=['TABLES', 'FORMS']
            )
            
            # Add page number to blocks
            for block in response.get('Blocks', []):
                block['Page'] = page_num + 1
            
            all_blocks.extend(response.get('Blocks', []))
        
        pdf_document.close()
        
        # Create combined response
        combined_response = {'Blocks': all_blocks}
        return self._parse_textract_response(combined_response, start_time, "bytes")
    
    def _process_async(self, pdf_bytes: bytes, start_time: float) -> Dict[str, Any]:
        """Process PDF asynchronously using start_document_analysis"""
        # Upload to S3 temporarily
        s3_key = f"temp-pdf-{uuid.uuid4()}.pdf"
        
        try:
            # Create bucket if it doesn't exist
            try:
                self.s3_client.create_bucket(Bucket=self.s3_bucket)
            except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
                pass
            except Exception:
                pass  # Bucket might already exist
            
            # Upload PDF to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=pdf_bytes
            )
            
            # Start asynchronous analysis
            response = self.textract_client.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': self.s3_bucket,
                        'Name': s3_key
                    }
                },
                FeatureTypes=['TABLES', 'FORMS']
            )
            
            job_id = response['JobId']
            print(f"Started Textract job: {job_id}")
            
            # Wait for completion
            while True:
                job_status = self.textract_client.get_document_analysis(JobId=job_id)
                status = job_status['JobStatus']
                
                if status == 'SUCCEEDED':
                    break
                elif status == 'FAILED':
                    raise Exception("Textract job failed")
                elif status in ['IN_PROGRESS', 'PARTIAL_SUCCESS']:
                    print("Processing...")
                    time.sleep(2)
                else:
                    raise Exception(f"Unknown status: {status}")
            
            # Get all results (handle pagination)
            all_blocks = []
            next_token = None
            
            while True:
                if next_token:
                    response = self.textract_client.get_document_analysis(
                        JobId=job_id,
                        NextToken=next_token
                    )
                else:
                    response = self.textract_client.get_document_analysis(JobId=job_id)
                
                all_blocks.extend(response.get('Blocks', []))
                next_token = response.get('NextToken')
                
                if not next_token:
                    break
            
            # Clean up S3 object
            try:
                self.s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
            except Exception:
                pass
            
            # Create response structure similar to analyze_document
            full_response = {'Blocks': all_blocks}
            return self._parse_textract_response(full_response, start_time, "S3")
            
        except Exception as e:
            # Clean up S3 object on error
            try:
                self.s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
            except Exception:
                pass
            raise e
    
    def _parse_textract_response(self, response: Dict[str, Any], start_time: float, source: str) -> Dict[str, Any]:
        """Parse Textract response into structured JSON format"""
        blocks = response.get('Blocks', [])
        
        # Initialize result structure
        result = {
            "metadata": {
                "pages": 0,
                "source": source,
                "processing_time": f"{time.time() - start_time:.1f}s"
            },
            "tables": [],
            "key_values": [],
            "raw_text": ""
        }
        
        # Create block lookup
        block_map = {block['Id']: block for block in blocks}
        
        # Extract pages count
        page_blocks = [b for b in blocks if b['BlockType'] == 'PAGE']
        result["metadata"]["pages"] = len(page_blocks)
        
        # Extract raw text (LINE blocks)
        lines = []
        for block in blocks:
            if block['BlockType'] == 'LINE':
                page_num = block.get('Page', 1)
                lines.append({
                    'text': block['Text'],
                    'page': page_num,
                    'geometry': block['Geometry']
                })
        
        # Sort lines by page and position
        lines.sort(key=lambda x: (x['page'], x['geometry']['BoundingBox']['Top']))
        result["raw_text"] = '\n'.join([line['text'] for line in lines])
        
        # Extract tables
        table_blocks = [b for b in blocks if b['BlockType'] == 'TABLE']
        for table_block in table_blocks:
            table_data = self._extract_table_structure(table_block, block_map)
            if table_data:
                result["tables"].append(table_data)
        
        # Extract key-value pairs
        key_value_blocks = [b for b in blocks if b['BlockType'] == 'KEY_VALUE_SET']
        for kv_block in key_value_blocks:
            kv_data = self._extract_key_value_pair(kv_block, block_map)
            if kv_data:
                result["key_values"].append(kv_data)
        
        return result
    
    def _extract_table_structure(self, table_block: Dict[str, Any], block_map: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract table as nested list structure"""
        if 'Relationships' not in table_block:
            return None
        
        # Get table cells
        cells = []
        for relationship in table_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for cell_id in relationship['Ids']:
                    cell_block = block_map.get(cell_id)
                    if cell_block and cell_block['BlockType'] == 'CELL':
                        cell_text = self._get_cell_text(cell_block, block_map)
                        cells.append({
                            'text': cell_text,
                            'row': cell_block.get('RowIndex', 1),
                            'col': cell_block.get('ColumnIndex', 1)
                        })
        
        if not cells:
            return None
        
        # Build table matrix
        max_row = max(cell['row'] for cell in cells)
        max_col = max(cell['col'] for cell in cells)
        
        table_matrix = [['' for _ in range(max_col)] for _ in range(max_row)]
        
        for cell in cells:
            row_idx = cell['row'] - 1  # Convert to 0-based index
            col_idx = cell['col'] - 1
            table_matrix[row_idx][col_idx] = cell['text']
        
        return {
            "page": table_block.get('Page', 1),
            "table": table_matrix
        }
    
    def _get_cell_text(self, cell_block: Dict[str, Any], block_map: Dict[str, Any]) -> str:
        """Extract text from a table cell"""
        if 'Relationships' not in cell_block:
            return ""
        
        cell_text = []
        for relationship in cell_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = block_map.get(child_id)
                    if child_block and child_block['BlockType'] == 'WORD':
                        cell_text.append(child_block['Text'])
        
        return ' '.join(cell_text)
    
    def _extract_key_value_pair(self, kv_block: Dict[str, Any], block_map: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract key-value pairs"""
        if not kv_block.get('EntityTypes') or 'KEY' not in kv_block['EntityTypes']:
            return None
        
        key_text = self._get_text_from_block(kv_block, block_map)
        
        # Find corresponding value
        value_text = ""
        if 'Relationships' in kv_block:
            for relationship in kv_block['Relationships']:
                if relationship['Type'] == 'VALUE':
                    for value_id in relationship['Ids']:
                        value_block = block_map.get(value_id)
                        if value_block:
                            value_text = self._get_text_from_block(value_block, block_map)
                            break
        
        if key_text:
            return {
                "page": kv_block.get('Page', 1),
                "key": key_text,
                "value": value_text
            }
        
        return None
    
    def _get_text_from_block(self, block: Dict[str, Any], block_map: Dict[str, Any]) -> str:
        """Get text content from a block"""
        if 'Text' in block:
            return block['Text']
        
        if 'Relationships' not in block:
            return ""
        
        text_parts = []
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = block_map.get(child_id)
                    if child_block and 'Text' in child_block:
                        text_parts.append(child_block['Text'])
        
        return ' '.join(text_parts)

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """
    Main function to extract raw text from PDF bytes using Amazon Textract.
    Returns raw_text for backward compatibility with existing code.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        str: Raw extracted text from the PDF
    """
    processor = TextractProcessor()
    result = processor.extract_text_from_pdf_bytes(pdf_bytes)
    return result.get("raw_text", "")

def extract_structured_data_from_pdf_bytes(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Main function to extract structured data from PDF bytes using Amazon Textract.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        Dict[str, Any]: Structured JSON with tables, key-values, and raw text
    """
    processor = TextractProcessor()
    return processor.extract_text_from_pdf_bytes(pdf_bytes)

def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Main function to extract raw text from PDF file using Amazon Textract.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        str: Raw extracted text from the PDF
    """
    with open(pdf_path, 'rb') as pdf_file:
        pdf_bytes = pdf_file.read()
    
    return extract_text_from_pdf_bytes(pdf_bytes)