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
        
        # Count block types for debugging
        block_types = {}
        for block in blocks:
            block_type = block.get('BlockType', 'UNKNOWN')
            block_types[block_type] = block_types.get(block_type, 0) + 1
        
        print(f"Block type counts: {block_types}")
        
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
                print(f"Processing TABLE block on page {block.get('Page', 1)}")
                table_data = self._extract_table_structure(block, block_map)
                if table_data:
                    # Add metadata to table
                    table_data['page'] = block.get('Page', 1)
                    table_data['confidence'] = block.get('Confidence', 0)
                    tables.append(table_data)
                    print(f"Successfully extracted table with {len(table_data.get('rows', []))} rows")
                else:
                    print("Failed to extract table structure")
            
            elif block['BlockType'] == 'KEY_VALUE_SET':
                print(f"Processing KEY_VALUE_SET block on page {block.get('Page', 1)}")
                kv_pair = self._extract_key_value_pair(block, block_map)
                if kv_pair:
                    # Add metadata to key-value pair
                    kv_pair['page'] = block.get('Page', 1)
                    kv_pair['confidence'] = block.get('Confidence', 0)
                    key_values.append(kv_pair)
                    print(f"Successfully extracted key-value: {kv_pair.get('key', '')} = {kv_pair.get('value', '')}")
                else:
                    print("Failed to extract key-value pair")
        
        # Merge lines into paragraphs
        document_text = self._merge_lines_into_paragraphs(line_blocks)
        
        processing_time = f"{time.time() - start_time:.1f}s"
        print(f"Textract processing completed in {processing_time}")
        print(f"Extracted: {len(document_text)} paragraphs, {len(tables)} tables, {len(key_values)} key-value pairs")
        
        return {
            "document_text": document_text,
            "tables": tables,
            "key_values": key_values,
            "metadata": {
                "total_blocks": len(blocks),
                "total_lines": len(line_blocks),
                "total_paragraphs": len(document_text),
                "total_tables": len(tables),
                "total_key_values": len(key_values),
                "processing_time": processing_time,
                "extraction_timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "block_types": block_types
            }
        }

    def _contains_numeric_financial_data(self, text: str) -> bool:
        """Check if text contains numeric or financial data that should not be merged"""
        import re
        
        # Patterns for numeric/financial data
        patterns = [
            r'\d+',              # Any numbers
            r'\$',               # Currency symbols
            r'%',                # Percentages
            r'Q\d',              # Quarter references (Q1, Q2, etc.)
            r'FY\d{2,4}',        # Fiscal year references (FY24, FY2024, etc.)
            r'\d{1,2}/\d{1,2}/\d{2,4}',  # Dates (MM/DD/YYYY)
            r'\d{4}-\d{2}-\d{2}',        # ISO dates (YYYY-MM-DD)
            r'[A-Z]{3}\s+\d{4}',         # Month year (JAN 2024)
            r'\d+[KMB]',                 # Numbers with K/M/B suffixes
            r'\d+\.\d+',                 # Decimal numbers
            r':\s*\d+',                  # Colon followed by numbers (ratios, times)
            r'revenue|profit|loss|income|expense|cost|price|amount|total|sum',  # Financial keywords
            r'million|billion|thousand|USD|EUR|GBP',  # Financial magnitudes and currencies
        ]
        
        # Check if any pattern matches (case insensitive)
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        return False

    def _split_paragraph_at_structured_data(self, text: str, page: int, confidence: float) -> List[Dict[str, Any]]:
        """Split paragraph text at structured data points, creating standalone rows"""
        import re
        
        # Enhanced patterns for structured data that should be standalone
        structured_patterns = [
            r'[A-Z]{2,}:\s*[\d\$%]+[^.]*',  # Labels with data (MAU: 79.6 million)
            r'\$[\d,]+\s*(?:million|billion|thousand)',  # Currency amounts
            r'\d+%\s*(?:growth|increase|decrease|change)',  # Percentage changes
            r'Q[1-4]\s+\d{4}:\s*[\d\$%]+',  # Quarterly data
            r'FY\d{2,4}:\s*[\d\$%]+',  # Fiscal year data
            r'(?:Revenue|Profit|Loss|Income|EBITDA|MAU|ARPU):\s*[\d\$%,]+',  # Key metrics
            r'\d+\.\d+\s*(?:million|billion|thousand)',  # Decimal numbers with magnitudes
            r'[\d,]+\s+(?:users|subscribers|customers|members)',  # User counts
        ]
        
        # Find all structured data matches
        matches = []
        for pattern in structured_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                matches.append((match.start(), match.end(), match.group()))
        
        # Sort matches by position
        matches.sort(key=lambda x: x[0])
        
        # If no structured data found, return as single paragraph
        if not matches:
            return [{
                'text': text,
                'page': page,
                'confidence': confidence,
                'line_count': 1,
                'contains_financial_data': self._contains_numeric_financial_data(text)
            }]
        
        # Split text at structured data points
        segments = []
        last_end = 0
        
        for start, end, matched_text in matches:
            # Add text before the match as a paragraph (if any)
            if start > last_end:
                pre_text = text[last_end:start].strip()
                if pre_text:
                    segments.append({
                        'text': pre_text,
                        'page': page,
                        'confidence': confidence,
                        'line_count': 1,
                        'contains_financial_data': self._contains_numeric_financial_data(pre_text)
                    })
            
            # Add the structured data as standalone row
            segments.append({
                'text': matched_text.strip(),
                'page': page,
                'confidence': confidence,
                'line_count': 1,
                'contains_financial_data': True  # Always true for structured data
            })
            
            last_end = end
        
        # Add remaining text after last match (if any)
        if last_end < len(text):
            post_text = text[last_end:].strip()
            if post_text:
                segments.append({
                    'text': post_text,
                    'page': page,
                    'confidence': confidence,
                    'line_count': 1,
                    'contains_financial_data': self._contains_numeric_financial_data(post_text)
                })
        
        return segments

    def _merge_lines_into_paragraphs(self, line_blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge adjacent lines into paragraphs, keeping structured data as standalone rows"""
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
            'line_count': 0,
            'contains_financial_data': False
        }
        
        for i, line in enumerate(line_blocks):
            line_text = line['text']
            has_numeric_data = self._contains_numeric_financial_data(line_text)
            
            should_start_new = False
            
            if i > 0:
                prev_line = line_blocks[i-1]
                prev_has_numeric = self._contains_numeric_financial_data(prev_line['text'])
                
                # Start new paragraph if different page
                if line['page'] != prev_line['page']:
                    should_start_new = True
                # Start new paragraph if current or previous line has numeric data
                elif has_numeric_data or prev_has_numeric:
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
                # Process the current paragraph for structured data splitting
                avg_confidence = sum(current_paragraph['confidence_scores']) / len(current_paragraph['confidence_scores']) if current_paragraph['confidence_scores'] else 0
                
                # Split paragraph at structured data points
                split_segments = self._split_paragraph_at_structured_data(
                    current_paragraph['text'],
                    current_paragraph['page'],
                    round(avg_confidence, 2)
                )
                paragraphs.extend(split_segments)
                
                # Start new paragraph
                current_paragraph = {
                    'text': '',
                    'page': line['page'],
                    'confidence_scores': [],
                    'line_count': 0,
                    'contains_financial_data': False
                }
            
            # Add line to current paragraph
            if current_paragraph['text']:
                current_paragraph['text'] += ' ' + line_text
            else:
                current_paragraph['text'] = line_text
            
            current_paragraph['confidence_scores'].append(line['confidence'])
            current_paragraph['line_count'] += 1
            
            # Mark if this paragraph contains financial data
            if has_numeric_data:
                current_paragraph['contains_financial_data'] = True
        
        # Add final paragraph with structured data splitting
        if current_paragraph['text']:
            avg_confidence = sum(current_paragraph['confidence_scores']) / len(current_paragraph['confidence_scores']) if current_paragraph['confidence_scores'] else 0
            
            split_segments = self._split_paragraph_at_structured_data(
                current_paragraph['text'],
                current_paragraph['page'],
                round(avg_confidence, 2)
            )
            paragraphs.extend(split_segments)
        
        financial_count = sum(1 for p in paragraphs if p['contains_financial_data'])
        text_count = len(paragraphs) - financial_count
        print(f"Paragraph processing complete: {financial_count} structured data rows, {text_count} text paragraphs")
        
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