import pdfplumber
import io
import base64
import os
from openai import OpenAI

# Initialize OpenAI client
openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def convert_pdf_page_to_image_base64(page):
    """
    Convert a PDF page to base64 image for OpenAI vision analysis.
    
    Args:
        page: pdfplumber page object
        
    Returns:
        str: Base64 encoded image
    """
    try:
        # Convert page to image
        img = page.to_image(resolution=300)
        
        # Convert PIL image to base64
        from io import BytesIO
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        img_base64 = base64.b64encode(buffered.getvalue()).decode()
        
        return img_base64
    except Exception as e:
        print(f"Error converting page to image: {str(e)}")
        return None

def extract_text_with_gpt4o(page_image_base64):
    """
    Use GPT-4o vision to extract text from a PDF page image.
    
    Args:
        page_image_base64 (str): Base64 encoded image of the PDF page
        
    Returns:
        str: Extracted text from the image
    """
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",  # the newest OpenAI model is "gpt-4o" which was released May 13, 2024. do not change this unless explicitly requested by the user
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Extract all text from this PDF page image. Preserve the layout, formatting, and structure as much as possible. Include tables, headers, and any structured data. Maintain the original order and hierarchy of information."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{page_image_base64}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=4000
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error with GPT-4o extraction: {str(e)}")
        return None

def extract_text_from_pdf(pdf_path):
    """
    Extract text from a PDF file using both pdfplumber and GPT-4o for enhanced accuracy.
    
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
                text += f"\n--- Page {page_num + 1} ---\n"
                
                # First try pdfplumber extraction
                page_text = page.extract_text()
                
                # If pdfplumber extraction is poor or empty, use GPT-4o
                if not page_text or len(page_text.strip()) < 50:
                    print(f"Using GPT-4o for page {page_num + 1} due to poor pdfplumber extraction")
                    page_image = convert_pdf_page_to_image_base64(page)
                    if page_image:
                        gpt_text = extract_text_with_gpt4o(page_image)
                        if gpt_text:
                            text += gpt_text + "\n"
                        else:
                            text += page_text + "\n" if page_text else ""
                    else:
                        text += page_text + "\n" if page_text else ""
                else:
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
    Extract text from PDF bytes using both pdfplumber and GPT-4o for enhanced accuracy.
    
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
                text += f"\n--- Page {page_num + 1} ---\n"
                
                # First try pdfplumber extraction
                page_text = page.extract_text()
                
                # If pdfplumber extraction is poor or empty, use GPT-4o
                if not page_text or len(page_text.strip()) < 50:
                    print(f"Using GPT-4o for page {page_num + 1} due to poor pdfplumber extraction")
                    page_image = convert_pdf_page_to_image_base64(page)
                    if page_image:
                        gpt_text = extract_text_with_gpt4o(page_image)
                        if gpt_text:
                            text += gpt_text + "\n"
                        else:
                            text += page_text + "\n" if page_text else ""
                    else:
                        text += page_text + "\n" if page_text else ""
                else:
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
