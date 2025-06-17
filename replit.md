# PDF Text Extractor and Tabulator

## Overview

This Flask-based web application leverages advanced cloud technologies to extract, analyze, and visualize structured information from PDF documents using intelligent asynchronous processing. The application now features enhanced page-by-page Textract processing with commentary matching functionality.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

The application follows an enhanced cloud-native architecture:

1. **Frontend**: Flask web interface with responsive HTML/CSS/JavaScript
2. **Backend Processing**: Python modules for advanced PDF processing and AI-powered analysis
3. **Cloud Services**: Amazon Textract for document analysis with S3 storage
4. **AI Processing**: OpenAI GPT-4o for intelligent data structuring and commentary matching
5. **Data Flow**: PDF upload → Page-by-page Textract extraction → LLM processing with commentary → Enhanced tabulation → Export options

The architecture prioritizes comprehensive data extraction, intelligent analysis, and structured presentation with commentary context.

## Key Components

### 1. Flask Web Interface (`app.py`)
The main entry point that provides:
- PDF file upload functionality via responsive web interface
- Enhanced page-by-page Textract processing endpoint
- Legacy processing endpoint for backwards compatibility
- Display of extracted and processed data with commentary
- Export options for the processed data

### 2. Enhanced Textract Processing (`textract_processor.py`)
Advanced PDF processing using Amazon Textract with page-by-page analysis:
- `extract_text_from_pdf_bytes_pagewise`: Page-by-page Textract processing with S3 storage
- `analyze_page_with_textract`: Synchronous page analysis with TABLES and FORMS features
- `extract_raw_text_from_page`: OCR fallback using DetectDocumentText
- Raw JSON storage in S3 for audit and reprocessing capabilities

### 3. Structured LLM Processing (`structured_llm_processor.py`)
Enhanced OpenAI GPT-4o processing with commentary matching:
- Asynchronous processing of tables, key-values, and document text
- Commentary matching functionality that relates document text to extracted data
- Comprehensive data extraction with ALL table values preserved
- General commentary collection for unmatched text segments

### 4. Export Utilities (`export_utils.py`)
Provides functionality to:
- Export processed data to PDF format using ReportLab
- Create downloadable links for the exported data
- Handle enhanced data structures with commentary fields

## Data Flow

1. **Input**: User uploads a PDF document through the Streamlit interface
2. **Processing**:
   - PDF text is extracted using PyPDF2
   - Extracted text is sent to OpenAI's GPT-4o model
   - The LLM analyzes the text and structures it into tabular format
3. **Output**:
   - Structured data is displayed as a table in the interface
   - User can download the data in various formats

## External Dependencies

### Core Libraries
- `streamlit`: Web application framework
- `pandas`: Data manipulation and analysis
- `PyPDF2`: PDF processing
- `langchain`: LLM interaction framework
- `openai`: API for GPT-4o access
- `reportlab`: PDF generation for exports

### External Services
- **OpenAI API**: Requires an API key set as an environment variable (`OPENAI_API_KEY`)

## Deployment Strategy

The application is configured for deployment on Replit with:
- Python 3.11 runtime
- Streamlit server running on port 5000
- Autoscaling deployment target
- Custom workflow configuration for the run button
- Headless server mode with external accessibility

The deployment uses Nix packaging to ensure all dependencies are properly managed and the application runs consistently in the Replit environment.

## Development Notes

1. The application uses GPT-4o (released May 2024) as the default language model
2. The PDF processing is basic text extraction without OCR capabilities
3. The export functionality currently supports PDF format
4. All processed data is session-based and not persisted between sessions