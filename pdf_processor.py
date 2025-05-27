import os
import tempfile
from llama_cloud_services import LlamaParse

def extract_text_from_pdf(pdf_path):
    """
    Extract text from a PDF file using LlamaParse for superior accuracy.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        str: Extracted text from the PDF
    """
    try:
        # Initialize LlamaParse with API key
        parser = LlamaParse(
            api_key="llx-CuuMEEIRSedt2PvGFWWam1ym70sMLMh4ACOC6soERwzL59HW",
            num_workers=4,
            verbose=True,
            language="en"
        )
        
        print(f"Using LlamaParse for text extraction from {pdf_path}")
        # Parse the PDF file
        result = parser.parse(pdf_path)
        
        # Extract text from result
        if result:
            extracted_text = ""
            # Handle both single document and list of documents
            if isinstance(result, list):
                for doc in result:
                    if hasattr(doc, 'text'):
                        extracted_text += doc.text + "\n"
                    elif hasattr(doc, 'get_content'):
                        extracted_text += doc.get_content() + "\n"
                    else:
                        extracted_text += str(doc) + "\n"
            else:
                # Single document
                if hasattr(result, 'text'):
                    extracted_text = result.text
                elif hasattr(result, 'get_content'):
                    extracted_text = result.get_content()
                else:
                    extracted_text = str(result)
            
            if extracted_text.strip():
                return extracted_text.strip()
            else:
                raise Exception("No text content found in parsed result")
        else:
            raise Exception("No result returned from LlamaParse")
            
    except Exception as e:
        raise Exception(f"Error extracting text from PDF with LlamaParse: {str(e)}")

def extract_text_from_pdf_bytes(pdf_bytes):
    """
    Extract text from PDF bytes using LlamaParse for superior accuracy.
    
    Args:
        pdf_bytes (bytes): PDF file as bytes
        
    Returns:
        str: Extracted text from the PDF
    """
    try:
        # Create a temporary file to store the PDF bytes
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
            temp_file.write(pdf_bytes)
            temp_file_path = temp_file.name
        
        try:
            # Initialize LlamaParse with API key
            parser = LlamaParse(
                api_key="llx-CuuMEEIRSedt2PvGFWWam1ym70sMLMh4ACOC6soERwzL59HW",
                num_workers=4,
                verbose=True,
                language="en"
            )
            
            print(f"Using LlamaParse for text extraction from PDF bytes")
            # Parse the temporary PDF file
            result = parser.parse(temp_file_path)
            
            # Extract text from result
            if result:
                extracted_text = ""
                # Handle both single document and list of documents
                if isinstance(result, list):
                    for doc in result:
                        if hasattr(doc, 'text'):
                            extracted_text += doc.text + "\n"
                        elif hasattr(doc, 'get_content'):
                            extracted_text += doc.get_content() + "\n"
                        else:
                            extracted_text += str(doc) + "\n"
                else:
                    # Single document
                    if hasattr(result, 'text'):
                        extracted_text = result.text
                    elif hasattr(result, 'get_content'):
                        extracted_text = result.get_content()
                    else:
                        extracted_text = str(result)
                
                if extracted_text.strip():
                    return extracted_text.strip()
                else:
                    raise Exception("No text content found in parsed result")
            else:
                raise Exception("No result returned from LlamaParse")
                
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)
            
    except Exception as e:
        raise Exception(f"Error extracting text from PDF bytes with LlamaParse: {str(e)}")
