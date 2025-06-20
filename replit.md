# PDF Text Extractor and Tabulator

## Overview

This application extracts text from PDF files, processes the extracted content using an OpenAI language model to identify structured information, and displays the results in a tabulated format. Users can then download the results in various formats.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

The application follows a simple, streamlined architecture:

1. **Frontend**: Streamlit web interface for user interactions
2. **Backend Processing**: Python modules for PDF processing and LLM interaction 
3. **Data Flow**: PDF upload → Text extraction → LLM processing → Tabulation → Export options

The architecture prioritizes simplicity and straightforward user interaction, with each component handling a specific responsibility in the data processing pipeline.

## Key Components

### 1. Streamlit Interface (`app.py`)
The main entry point that provides:
- PDF file upload functionality
- Display of extracted and processed data
- Export options for the processed data

### 2. PDF Processing (`pdf_processor.py`)
Handles the extraction of text from PDF files using PyPDF2:
- `extract_text_from_pdf`: Processes PDF files from a file path
- `extract_text_from_pdf_bytes`: Processes PDF content provided as bytes

### 3. LLM Processing (`llm_processor.py`)
Leverages OpenAI's GPT-4o model to:
- Analyze extracted text from PDFs
- Convert unstructured text into structured, tabular JSON data
- Uses LangChain for prompt construction and model interaction

### 4. Export Utilities (`export_utils.py`)
Provides functionality to:
- Export processed data to PDF format using ReportLab
- Create downloadable links for the exported data

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

## Recent Changes

### June 2025 - Enhanced Text Processing and Commentary System
- **Paragraph Grouping**: Implemented intelligent paragraph grouping using punctuation and spacing heuristics in Textract processing
- **Narrative Classification**: Added automatic identification of commentary-rich paragraphs containing metrics and action verbs
- **Reduced Key-Value Redundancy**: Implemented filtering to prevent duplicate key-value pairs from being processed
- **Context-Aware Commentary Matching**: Enhanced commentary matching with scoring system and surrounding context inclusion
- **Streaming Results Display**: Real-time streaming of extracted data rows with progressive commentary updates
- **Simple Table Format**: Restored clean field-value pair format with document-sourced commentary only

### Textract Processing Improvements
- **Smart Paragraph Assembly**: Lines are now grouped into coherent paragraphs before processing, reducing fragmentation
- **Commentary Detection**: Paragraphs containing financial metrics and action verbs are automatically flagged as potential commentary
- **Duplicate Prevention**: Key-value extraction now filters out redundant entries to improve data quality
- **Enhanced Text Structure**: Document text is organized into meaningful paragraphs rather than individual lines

## Development Notes

1. The application uses GPT-4o (released May 2024) as the default language model
2. Uses Amazon Textract for advanced PDF text and table extraction
3. Export functionality supports JSON, CSV, and PDF formats
4. All processed data is session-based and not persisted between sessions
5. Commentary matching uses intelligent text analysis to relate document content to extracted data
6. Multi-column table preservation ensures no data loss during processing