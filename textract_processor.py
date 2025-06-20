import boto3
import time
import uuid
from typing import Dict, Any, List, Optional

class TextractProcessor:
    def __init__(self):
        """Initialize AWS Textract and S3 clients with credentials from environment"""
        self.textract_client = boto3.client('textract')
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'textract-bucket-lk'

    def extract_text_from_pdf_bytes(self, pdf_bytes: bytes) -> Dict[str, Any]:
        """
        Extract structured data from PDF bytes using Amazon Textract with S3 storage.
        
        Args:
            pdf_bytes (bytes): PDF file as bytes
            
        Returns:
            Dict[str, Any]: Structured JSON with document_text, tables, and key_values
        """
        start_time = time.time()
        
        try:
            print("Using Amazon Textract with S3 storage for PDF processing")
            
            # Upload PDF to S3
            file_key = f"textract-input/{uuid.uuid4()}.pdf"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=pdf_bytes,
                ContentType='application/pdf'
            )
            
            # Start document analysis
            response = self.textract_client.start_document_analysis(
                DocumentLocation={
                    'S3Object': {
                        'Bucket': self.bucket_name,
                        'Name': file_key
                    }
                },
                FeatureTypes=['TABLES', 'FORMS']
            )
            
            job_id = response['JobId']
            print(f"Started Textract job: {job_id}")
            
            # Wait for job completion
            while True:
                result = self.textract_client.get_document_analysis(JobId=job_id)
                status = result['JobStatus']
                print(f"Job status: {status}")
                
                if status in ['SUCCEEDED', 'FAILED']:
                    break
                
                time.sleep(5)
            
            if status == 'FAILED':
                raise Exception("Textract job failed")
            
            # Fetch full results (handle pagination)
            pages = []
            next_token = None
            
            while True:
                if next_token:
                    response = self.textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
                else:
                    response = self.textract_client.get_document_analysis(JobId=job_id)
                
                pages.extend(response['Blocks'])
                
                next_token = response.get('NextToken')
                if not next_token:
                    break
            
            print(f"Total blocks extracted: {len(pages)}")
            
            # Parse the results
            structured_data = self._parse_textract_blocks(pages, start_time)
            
            # Clean up S3 file
            try:
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=file_key)
            except Exception as e:
                print(f"Warning: Could not delete S3 file: {e}")
            
            return structured_data
            
        except Exception as e:
            print(f"Textract extraction failed: {e}")
            raise Exception(f"Failed to extract text using Amazon Textract: {str(e)}")

    def _parse_textract_blocks(self, blocks: List[Dict[str, Any]], start_time: float) -> Dict[str, Any]:
        """Parse Textract blocks with paragraph merging and metadata"""
        
        block_map = {block['Id']: block for block in blocks}
        
        # Collect line blocks with metadata
        line_blocks = []
        tables = []
        key_values = []
        
        # Process blocks
        for block in blocks:
            if block['BlockType'] == 'LINE':
                text = block.get('Text', '').strip()
                if text:
                    line_blocks.append({
                        'text': text,
                        'page': block.get('Page', 1),
                        'confidence': block.get('Confidence', 0),
                        'geometry': block.get('Geometry', {}),
                        'id': block.get('Id', '')
                    })
            
            elif block['BlockType'] == 'TABLE':
                table_data = self._extract_table_structure(block, block_map)
                if table_data:
                    # Add metadata to table
                    table_data['page'] = block.get('Page', 1)
                    table_data['confidence'] = block.get('Confidence', 0)
                    tables.append(table_data)
            
            elif block['BlockType'] == 'KEY_VALUE_SET':
                kv_pair = self._extract_key_value_pair(block, block_map)
                if kv_pair:
                    # Add metadata to key-value pair
                    kv_pair['page'] = block.get('Page', 1)
                    kv_pair['confidence'] = block.get('Confidence', 0)
                    key_values.append(kv_pair)
        
        # Merge lines into paragraphs
        document_text = self._merge_lines_into_paragraphs(line_blocks)
        
        processing_time = f"{time.time() - start_time:.1f}s"
        print(f"Textract processing completed in {processing_time}")
        
        return {
            "document_text": document_text,
            "tables": tables,
            "key_values": key_values,
            "metadata": {
                "total_blocks": len(blocks),
                "total_lines": len(line_blocks),
                "total_paragraphs": len(document_text),
                "processing_time": processing_time,
                "extraction_timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
            }
        }

    def _merge_lines_into_paragraphs(self, line_blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge adjacent lines into coherent paragraphs with metadata"""
        if not line_blocks:
            return []
        
        # Sort by page, then by vertical position
        line_blocks.sort(key=lambda x: (
            x['page'],
            x['geometry'].get('BoundingBox', {}).get('Top', 0)
        ))
        
        paragraphs = []
        current_paragraph = {
            'text': '',
            'page': line_blocks[0]['page'],
            'confidence_scores': [],
            'line_count': 0
        }
        
        for i, line in enumerate(line_blocks):
            should_start_new = False
            
            if i > 0:
                prev_line = line_blocks[i-1]
                
                # Start new paragraph if different page
                if line['page'] != prev_line['page']:
                    should_start_new = True
                else:
                    # Calculate vertical gap between lines
                    prev_bbox = prev_line['geometry'].get('BoundingBox', {})
                    curr_bbox = line['geometry'].get('BoundingBox', {})
                    
                    if prev_bbox and curr_bbox:
                        prev_bottom = prev_bbox.get('Top', 0) + prev_bbox.get('Height', 0)
                        current_top = curr_bbox.get('Top', 0)
                        vertical_gap = current_top - prev_bottom
                        
                        # Large gap indicates paragraph break
                        if vertical_gap > 0.02:
                            should_start_new = True
                    
                    # Short line followed by longer line might be header/paragraph break
                    if len(prev_line['text']) < 40 and len(line['text']) > 60:
                        should_start_new = True
            
            if should_start_new and current_paragraph['text']:
                # Finalize current paragraph
                avg_confidence = sum(current_paragraph['confidence_scores']) / len(current_paragraph['confidence_scores']) if current_paragraph['confidence_scores'] else 0
                paragraphs.append({
                    'text': current_paragraph['text'],
                    'page': current_paragraph['page'],
                    'line_count': current_paragraph['line_count'],
                    'confidence': round(avg_confidence, 2)
                })
                
                # Start new paragraph
                current_paragraph = {
                    'text': '',
                    'page': line['page'],
                    'confidence_scores': [],
                    'line_count': 0
                }
            
            # Add line to current paragraph
            if current_paragraph['text']:
                current_paragraph['text'] += ' ' + line['text']
            else:
                current_paragraph['text'] = line['text']
            
            current_paragraph['confidence_scores'].append(line['confidence'])
            current_paragraph['line_count'] += 1
        
        # Add final paragraph
        if current_paragraph['text']:
            avg_confidence = sum(current_paragraph['confidence_scores']) / len(current_paragraph['confidence_scores']) if current_paragraph['confidence_scores'] else 0
            paragraphs.append({
                'text': current_paragraph['text'],
                'page': current_paragraph['page'],
                'line_count': current_paragraph['line_count'],
                'confidence': round(avg_confidence, 2)
            })
        
        return paragraphs

    def _extract_table_structure(self, table_block: Dict[str, Any], block_map: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract table as rows format"""
        if 'Relationships' not in table_block:
            return None
        
        # Get page number
        page_num = table_block.get('Page', 1)
        
        # Find all cells in the table
        cells = []
        for relationship in table_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    if child_id in block_map and block_map[child_id]['BlockType'] == 'CELL':
                        cells.append(block_map[child_id])
        
        if not cells:
            return None
        
        # Organize cells by row and column
        table_structure = {}
        max_row = 0
        max_col = 0
        
        for cell in cells:
            row_index = cell.get('RowIndex', 1) - 1  # Convert to 0-based
            col_index = cell.get('ColumnIndex', 1) - 1  # Convert to 0-based
            
            max_row = max(max_row, row_index)
            max_col = max(max_col, col_index)
            
            cell_text = self._get_cell_text(cell, block_map)
            
            if row_index not in table_structure:
                table_structure[row_index] = {}
            table_structure[row_index][col_index] = cell_text
        
        # Convert to list of lists (rows)
        rows = []
        for row_idx in range(max_row + 1):
            row = []
            for col_idx in range(max_col + 1):
                cell_value = table_structure.get(row_idx, {}).get(col_idx, "")
                row.append(cell_value)
            rows.append(row)
        
        return {
            "page": page_num,
            "rows": rows
        }

    def _get_cell_text(self, cell_block: Dict[str, Any], block_map: Dict[str, Any]) -> str:
        """Extract text from a table cell"""
        if 'Relationships' not in cell_block:
            return ""
        
        text_parts = []
        for relationship in cell_block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    if child_id in block_map:
                        child_block = block_map[child_id]
                        if child_block['BlockType'] == 'WORD':
                            text_parts.append(child_block.get('Text', ''))
        
        return ' '.join(text_parts)

    def _extract_key_value_pair(self, kv_block: Dict[str, Any], block_map: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract key-value pairs"""
        if kv_block.get('EntityTypes') and 'KEY' in kv_block['EntityTypes']:
            page_num = kv_block.get('Page', 1)
            
            key_text = self._get_text_from_block(kv_block, block_map)
            value_text = ""
            
            # Find the corresponding VALUE
            if 'Relationships' in kv_block:
                for relationship in kv_block['Relationships']:
                    if relationship['Type'] == 'VALUE':
                        for value_id in relationship['Ids']:
                            if value_id in block_map:
                                value_block = block_map[value_id]
                                value_text = self._get_text_from_block(value_block, block_map)
                                break
            
            if key_text:
                return {
                    "key": key_text,
                    "value": value_text,
                    "page": page_num
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
                    if child_id in block_map:
                        child_block = block_map[child_id]
                        if child_block['BlockType'] in ['WORD', 'LINE']:
                            text_parts.append(child_block.get('Text', ''))
        
        return ' '.join(text_parts)


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """
    Main function to extract raw text from PDF bytes using Amazon Textract.
    Returns document_text joined for backward compatibility.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        str: Raw extracted text from the PDF
    """
    processor = TextractProcessor()
    result = processor.extract_text_from_pdf_bytes(pdf_bytes)
    return '\n'.join(result.get('document_text', []))


def extract_structured_data_from_pdf_bytes(pdf_bytes: bytes) -> Dict[str, Any]:
    """
    Main function to extract structured data from PDF bytes using Amazon Textract.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        Dict[str, Any]: Structured JSON with document_text, tables, and key_values
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
    with open(pdf_path, 'rb') as file:
        pdf_bytes = file.read()
    return extract_text_from_pdf_bytes(pdf_bytes)