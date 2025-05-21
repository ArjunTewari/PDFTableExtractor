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
    
    # Create the PDF document with landscape orientation for better table display
    doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=30, rightMargin=30, topMargin=30, bottomMargin=30)
    elements = []
    
    # Get the style for paragraphs
    styles = getSampleStyleSheet()
    title_style = styles['Heading1']
    
    # Add a title
    elements.append(Paragraph("Extracted Information", title_style))
    
    # Handle the new table format where column names might vary
    # First, get all unique column names across all rows
    all_columns = set()
    for _, row in df.iterrows():
        all_columns.update(row.keys())
    
    # Ensure "Category" is the first column
    column_order = ["Category"]
    value_columns = sorted([col for col in all_columns if col != "Category" and col.startswith("Value")])
    column_order.extend(value_columns)
    
    # Create a new DataFrame with the ordered columns
    ordered_df = pd.DataFrame(columns=column_order)
    for idx, row in df.iterrows():
        ordered_row = {}
        for col in column_order:
            ordered_row[col] = row.get(col, "") if col in row else ""
        ordered_df.loc[idx] = ordered_row
    
    # Convert DataFrame to list for table
    data = [ordered_df.columns.tolist()] + ordered_df.values.tolist()
    
    # Create the table with column widths
    col_widths = [120]  # Width for Category column
    col_widths.extend([180] * (len(column_order) - 1))  # Width for Value columns
    table = Table(data, colWidths=col_widths)
    
    # Add style to the table
    style = TableStyle([
        # Header row
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        
        # Category column
        ('BACKGROUND', (0, 1), (0, -1), colors.lightgrey),
        ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        
        # Data cells
        ('BACKGROUND', (1, 1), (-1, -1), colors.beige),
        
        # Grid and borders
        ('BOX', (0, 0), (-1, -1), 1, colors.black),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        
        # Text alignment
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),  # Left align category names
        ('ALIGN', (1, 1), (-1, -1), 'LEFT'),  # Left align values
        
        # Padding
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
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
