import pdfplumber
import io

def extract_text_from_pdf(pdf_path):
    """
    Extract text from a PDF file using pdfplumber for better layout preservation.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        str: Extracted text from the PDF
    """
    text = ""
    try:
        # Open the PDF file with pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            # Iterate through each page and extract text
            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text()
                if page_text:
                    text += f"\n--- Page {page_num + 1} ---\n"
                    text += page_text + "\n"
                
                # Also try to extract table data if present
                tables = page.extract_tables()
                if tables:
                    text += "\n--- Tables ---\n"
                    for table in tables:
                        for row in table:
                            if row:
                                # Join non-empty cells
                                row_text = " | ".join([cell for cell in row if cell])
                                text += row_text + "\n"
                    text += "\n"
            
    except Exception as e:
        raise Exception(f"Error extracting text from PDF: {str(e)}")
    
    return text.strip()

def extract_text_from_pdf_bytes(pdf_bytes):
    """
    Extract text from PDF bytes using pdfplumber for better layout preservation.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        str: Extracted text from the PDF
    """
    text = ""
    try:
        # Create a BytesIO object
        file_stream = io.BytesIO(pdf_bytes)
        
        # Open the PDF with pdfplumber
        with pdfplumber.open(file_stream) as pdf:
            # Iterate through each page and extract text
            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text()
                if page_text:
                    text += f"\n--- Page {page_num + 1} ---\n"
                    text += page_text + "\n"
                
                # Also try to extract table data if present
                tables = page.extract_tables()
                if tables:
                    text += "\n--- Tables ---\n"
                    for table in tables:
                        for row in table:
                            if row:
                                # Join non-empty cells
                                row_text = " | ".join([cell for cell in row if cell])
                                text += row_text + "\n"
                    text += "\n"
        
    except Exception as e:
        raise Exception(f"Error extracting text from PDF bytes: {str(e)}")
    
    return text.strip()
