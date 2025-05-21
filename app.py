import streamlit as st
import pandas as pd
import os
import tempfile
import base64
from io import BytesIO

from pdf_processor import extract_text_from_pdf
from llm_processor import process_text_with_llm
from export_utils import export_to_pdf, create_download_link

# Set page config with optimized settings
st.set_page_config(
    page_title="PDF Text Extractor and Tabulator",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed"  # Collapse sidebar by default for more space
)

# Apply custom CSS for better performance
st.markdown("""
<style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stButton button {
        background-color: #2563eb;
        color: white;
        border-radius: 6px;
        padding: 0.5rem 1rem;
        font-weight: bold;
    }
    .stButton button:hover {
        background-color: #1d4ed8;
    }
    .stSpinner > div > div {
        border-color: #2563eb transparent transparent transparent;
    }
</style>
""", unsafe_allow_html=True)

# App title and description in the main container
st.container().markdown("""
# PDF Text Extractor and Tabulator
This app extracts text from PDFs, processes it with AI to identify structured information, 
and displays results in a table format. Download the results in your preferred format.
""")

# Session state management
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = {}
    
def init_file_state(file_id):
    if file_id not in st.session_state.processed_files:
        st.session_state.processed_files[file_id] = {
            'extracted_text': None,
            'structured_data': None,
            'df': None,
            'processing_complete': False
        }
    return st.session_state.processed_files[file_id]

# File upload section
with st.container():
    uploaded_file = st.file_uploader("📄 Upload a PDF file", type=['pdf'], key="pdf_uploader")

# Process PDF section
if uploaded_file is not None:
    # Get a unique ID for this file
    file_id = uploaded_file.name
    file_state = init_file_state(file_id)
    
    # Only extract text if not already done for this file
    if file_state['extracted_text'] is None:
        with st.spinner("⏳ Extracting text from PDF..."):
            # Process with optimized memory handling
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_file_path = tmp_file.name
            
            # Extract text efficiently
            extracted_text = extract_text_from_pdf(tmp_file_path)
            
            # Clean up immediately
            os.unlink(tmp_file_path)
            
            # Update state
            file_state['extracted_text'] = extracted_text
    
    # Display extracted text in a collapsible section
    with st.expander("📝 View Extracted Text"):
        st.text_area("", file_state['extracted_text'], height=150)
    
    # Process with LLM section
    process_col, status_col = st.columns([3, 1])
    
    with process_col:
        process_button = st.button("🔍 Process with AI", 
                                use_container_width=True,
                                disabled=file_state['processing_complete'])
        
    with status_col:
        if file_state['processing_complete']:
            st.success("✅ Processing complete")
            
    # Process on button click    
    if process_button and not file_state['processing_complete']:
        with st.spinner("🧠 Analyzing text and extracting structured data..."):
            try:
                # Process text with LLM
                structured_data = process_text_with_llm(file_state['extracted_text'])
                
                # Convert to DataFrame efficiently
                df = pd.DataFrame(structured_data)
                
                # Update state
                file_state['structured_data'] = structured_data
                file_state['df'] = df
                file_state['processing_complete'] = True
            except Exception as e:
                st.error(f"Error during processing: {str(e)}")

# Results section
if uploaded_file is not None:
    file_id = uploaded_file.name
    file_state = st.session_state.processed_files[file_id]
    
    if file_state['processing_complete'] and file_state['df'] is not None:
        st.divider()
        st.subheader("📊 Extracted Information")
        
        # Display data in an optimized table
        st.dataframe(
            file_state['df'], 
            use_container_width=True,
            hide_index=True
        )
        
        # Export options section
        st.subheader("💾 Export Options")
        
        col1, col2, col3 = st.columns(3)
        
        # JSON export
        with col1:
            if st.button("Export as JSON", key="json_export"):
                json_data = file_state['df'].to_json(orient="records")
                download_link = create_download_link(json_data, "data.json", "text/json")
                st.markdown(download_link, unsafe_allow_html=True)
        
        # PDF export
        with col2:
            if st.button("Export as PDF", key="pdf_export"):
                pdf_bytes = export_to_pdf(file_state['df'])
                b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
                download_link = create_download_link(b64_pdf, "data.pdf", "application/pdf")
                st.markdown(download_link, unsafe_allow_html=True)
        
        # CSV export
        with col3:
            if st.button("Export as CSV", key="csv_export"):
                csv = file_state['df'].to_csv(index=False)
                download_link = create_download_link(csv, "data.csv", "text/csv")
                st.markdown(download_link, unsafe_allow_html=True)

# Add footer with minimal rendering
st.divider()
st.caption("PDF Text Extractor and Tabulator • Powered by OpenAI • v1.0")
