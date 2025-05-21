import json
import base64
import pandas as pd
from io import BytesIO
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

def export_to_pdf(df):
    """
    Export a DataFrame to PDF format.
    
    Args:
        df (pandas.DataFrame): DataFrame to export
        
    Returns:
        bytes: PDF content as bytes
    """
    buffer = BytesIO()
    
    # Create the PDF document
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    
    # Get the style for paragraphs
    styles = getSampleStyleSheet()
    title_style = styles['Heading1']
    
    # Add a title
    elements.append(Paragraph("Extracted Information", title_style))
    
    # Convert DataFrame to list for table
    data = [df.columns.tolist()] + df.values.tolist()
    
    # Create the table
    table = Table(data)
    
    # Add style to the table
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('BOX', (0, 0), (-1, -1), 1, colors.black),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ])
    table.setStyle(style)
    
    # Add the table to the elements
    elements.append(table)
    
    # Build the PDF
    doc.build(elements)
    
    # Get the PDF content
    pdf_content = buffer.getvalue()
    buffer.close()
    
    return pdf_content

def create_download_link(content, filename, mime_type):
    """
    Create an HTML download link for the content.
    
    Args:
        content: The content to download
        filename (str): The name of the file
        mime_type (str): The MIME type of the file
        
    Returns:
        str: HTML download link
    """
    if mime_type == 'application/pdf':
        # For PDF, content is already base64 encoded
        href = f'data:{mime_type};base64,{content}'
    else:
        # For other types, encode the content
        b64 = base64.b64encode(content.encode()).decode()
        href = f'data:{mime_type};base64,{b64}'
    
    # Create the download link
    download_link = f'<a href="{href}" download="{filename}">Download {filename}</a>'
    
    return download_link
