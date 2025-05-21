# PDF Text Extractor and Tabulator

A web application that extracts text from PDF files, processes it using OpenAI's GPT-4o model to identify structured information, and displays the results in a tabulated format with export options.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [File Structure](#file-structure)
- [How It Works](#how-it-works)
- [Setup and Installation](#setup-and-installation)
- [Technologies Used](#technologies-used)
- [API Integration](#api-integration)
- [Export Options](#export-options)
- [Customization](#customization)

## Overview

This application provides a streamlined way to extract structured information from PDF documents. It uses advanced AI to analyze text content and organize it into a tabular format that can be easily viewed and exported. The system automatically identifies entities and their categories, making it particularly useful for processing documents with consistent information patterns.

## Features

- **PDF Text Extraction**: Upload and extract text from PDF documents
- **AI-Powered Analysis**: Process text with OpenAI's GPT-4o model to identify structured information
- **Interactive UI**: Clean, modern interface with drag-and-drop file upload
- **Real-time Processing**: See extracted text immediately after upload
- **Tabulated Results**: View structured data in a clean, organized table
- **Multiple Export Options**: Download results as JSON, CSV, or PDF
- **Responsive Design**: Works on desktop and mobile devices

## Architecture

The application follows a client-server architecture with:

1. **Frontend**: HTML, CSS, and JavaScript for user interface
2. **Backend**: Flask Python server to handle requests
3. **Processing Layer**: PDF processing and AI analysis components
4. **Export Layer**: Conversion utilities for different output formats

## File Structure

```
├── app.py                  # Main Flask application
├── llm_processor.py        # AI text processing with OpenAI
├── pdf_processor.py        # PDF text extraction with PyPDF2
├── export_utils.py         # Export functionality for various formats
├── templates/
│   └── index.html          # Main HTML template
└── static/
    ├── style.css           # CSS styling
    └── script.js           # Frontend JavaScript
```

## How It Works

1. **PDF Upload**:
   - User uploads a PDF through the web interface
   - The file is sent to the server for processing

2. **Text Extraction**:
   - The `pdf_processor.py` module extracts raw text from the PDF 
   - Uses PyPDF2 to access and read PDF content
   - Extracted text is displayed to the user

3. **AI Analysis**:
   - User initiates processing with the "Process with AI" button
   - The `llm_processor.py` module sends the text to OpenAI's GPT-4o model
   - Custom prompting guides the AI to identify structured information
   - The AI returns data categorized into predefined columns

4. **Result Display**:
   - Processed data is displayed in a table format
   - Information is organized by category and extracted text

5. **Data Export**:
   - User selects preferred export format (JSON, CSV, PDF)
   - The `export_utils.py` module handles conversion to the selected format
   - Generated file is downloaded to the user's device

## Setup and Installation

1. **Prerequisites**:
   - Python 3.11 or higher
   - OpenAI API key

2. **Installation**:
   ```bash
   # Clone the repository
   git clone [repository-url]
   cd pdf-text-extractor

   # Install dependencies
   pip install flask openai pandas pypdf2 reportlab
   ```

3. **Environment Variables**:
   - Set your OpenAI API key:
   ```
   export OPENAI_API_KEY="your-api-key"
   ```

4. **Running the Application**:
   ```bash
   python app.py
   ```
   - Access the application at http://localhost:5000

## Technologies Used

- **Flask**: Lightweight web framework for the backend
- **OpenAI API**: GPT-4o model for text analysis
- **PyPDF2**: PDF parsing and text extraction
- **Pandas**: Data manipulation for structured information
- **ReportLab**: PDF generation for exports
- **Bootstrap**: Frontend styling framework
- **JavaScript**: Frontend interactivity and API calls

## API Integration

The application integrates with OpenAI's API to process the extracted text. The `llm_processor.py` file handles this integration with the following approach:

1. Creates an OpenAI client instance with the API key
2. Constructs a specialized prompt for document analysis
3. Sends the prompt along with the extracted text to the GPT-4o model
4. Processes the JSON response to extract structured data
5. Returns the data in a format ready for display and export

## Export Options

The application provides three export formats:

1. **JSON**: Raw structured data in a machine-readable format
2. **CSV**: Tabular data suitable for spreadsheet applications
3. **PDF**: Formatted document with the extracted information in a table

The export functionality is handled in `export_utils.py` and the frontend JavaScript.

## Customization

The AI prompt in `llm_processor.py` can be customized to extract different types of information or to format the output differently. The current prompt instructs the model to extract:

- Category: Entity type (Person, Organization, Location, etc.)
- Extracted Text: The identified data points or entities

You can customize this prompt to focus on specific types of information relevant to your documents.