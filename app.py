import streamlit as st
import pandas as pd
import os
import tempfile
import base64
from io import BytesIO

from pdf_processor import extract_text_from_pdf
from llm_processor import process_text_with_llm
from export_utils import export_to_pdf, create_download_link

# Set page config
st.set_page_config(
    page_title="PDF Text Extractor and Tabulator",
    page_icon="📄",
    layout="wide"
)

# App title and description
st.title("PDF Text Extractor and Tabulator")
st.markdown("""
This application extracts text from PDF files, processes it using an LLM to identify structured information,
and displays the results in a tabulated format. You can download the results in various formats.
""")

# Initialize session state if not already done
if 'extracted_data' not in st.session_state:
    st.session_state.extracted_data = None
if 'structured_data' not in st.session_state:
    st.session_state.structured_data = None
if 'df' not in st.session_state:
    st.session_state.df = None
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

# File uploader for PDF
uploaded_file = st.file_uploader("Upload a PDF file", type=['pdf'])

# Process PDF once uploaded
if uploaded_file is not None:
    with st.spinner("Extracting text from PDF..."):
        # Save the uploaded file to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name
        
        # Extract text from the PDF
        extracted_text = extract_text_from_pdf(tmp_file_path)
        
        # Clean up the temporary file
        os.unlink(tmp_file_path)
        
        # Store the extracted text in session state
        st.session_state.extracted_text = extracted_text
        
        # Display the extracted text
        with st.expander("Show Extracted Text"):
            st.text_area("Extracted Text", extracted_text, height=200)
    
    # Process button
    if st.button("Process with LLM"):
        with st.spinner("Processing text with LLM to extract structured data..."):
            # Process the text with LLM
            structured_data = process_text_with_llm(st.session_state.extracted_text)
            
            # Store the structured data in session state
            st.session_state.structured_data = structured_data
            
            # Convert to DataFrame
            df = pd.DataFrame(structured_data)
            st.session_state.df = df
            st.session_state.processing_complete = True
            
            # Force a rerun to show the results
            st.rerun()

# Display results if processing is complete
if st.session_state.processing_complete and st.session_state.df is not None:
    st.subheader("Extracted Information")
    
    # Display the data in a table
    st.dataframe(st.session_state.df)
    
    # Export options
    st.subheader("Export Options")
    
    col1, col2 = st.columns(2)
    
    # JSON export
    with col1:
        if st.button("Export as JSON"):
            json_data = st.session_state.df.to_json(orient="records")
            download_link = create_download_link(json_data, "data.json", "text/json")
            st.markdown(download_link, unsafe_allow_html=True)
    
    # PDF export
    with col2:
        if st.button("Export as PDF"):
            pdf_bytes = export_to_pdf(st.session_state.df)
            b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
            download_link = create_download_link(b64_pdf, "data.pdf", "application/pdf")
            st.markdown(download_link, unsafe_allow_html=True)

# Add footer
st.markdown("---")
st.caption("PDF Text Extractor and Tabulator - Powered by LangChain and OpenAI")
