import PyPDF2
import io

def extract_text_from_pdf(pdf_path):
    """
    Extract text from a PDF file.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        str: Extracted text from the PDF
    """
    text = ""
    try:
        # Open the PDF file
        with open(pdf_path, 'rb') as file:
            # Create a PDF reader object
            pdf_reader = PyPDF2.PdfReader(file)
            
            # Iterate through each page and extract text
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                text += page.extract_text()
                
            # Add page breaks if needed
            text = text.replace('\n\n', ' ').replace('\n', ' ')
            
    except Exception as e:
        raise Exception(f"Error extracting text from PDF: {str(e)}")
    
    return text

def extract_text_from_pdf_bytes(pdf_bytes):
    """
    Extract text from PDF bytes.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        str: Extracted text from the PDF
    """
    text = ""
    try:
        # Create a BytesIO object
        file_stream = io.BytesIO(pdf_bytes)
        
        # Create a PDF reader object
        pdf_reader = PyPDF2.PdfReader(file_stream)
        
        # Iterate through each page and extract text
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
            
        # Add page breaks if needed
        text = text.replace('\n\n', ' ').replace('\n', ' ')
        
    except Exception as e:
        raise Exception(f"Error extracting text from PDF bytes: {str(e)}")
    
    return text
