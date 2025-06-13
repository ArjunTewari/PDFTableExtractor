// Main script for PDF Text Extractor and Tabulator

document.addEventListener('DOMContentLoaded', function() {
    // Elements
    const dropArea = document.getElementById('drop-area');
    const fileInput = document.getElementById('file-input');
    const browseBtn = document.getElementById('browse-btn');
    const fileInfo = document.getElementById('file-info');
    const uploadProgress = document.getElementById('upload-progress');
    const progressBar = uploadProgress.querySelector('.progress-bar');
    const extractedTextSection = document.getElementById('extracted-text-section');
    const extractedTextContent = document.getElementById('extracted-text-content');
    const processBtn = document.getElementById('process-btn');
    const resultsSection = document.getElementById('results-section');
    const tableHeader = document.getElementById('table-header');
    const tableBody = document.getElementById('table-body');
    const exportJsonBtn = document.getElementById('export-json-btn');
    const exportCsvBtn = document.getElementById('export-csv-btn');
    const exportPdfBtn = document.getElementById('export-pdf-btn');
    const loadingOverlay = document.getElementById('loading-overlay');
    const loadingMessage = document.getElementById('loading-message');
    const errorMessage = document.getElementById('error-message');

    // Store data
    let extractedText = '';
    let processedData = [];
    let currentStructuredData = null;

    // Initialize drag and drop events
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover'].forEach(eventName => {
        dropArea.addEventListener(eventName, highlight, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropArea.addEventListener(eventName, unhighlight, false);
    });

    function highlight() {
        dropArea.classList.add('dragover');
    }

    function unhighlight() {
        dropArea.classList.remove('dragover');
    }

    // Handle file drop
    dropArea.addEventListener('drop', handleDrop, false);

    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        
        if (files.length) {
            handleFiles(files);
        }
    }

    // Handle file selection via browse button
    browseBtn.addEventListener('click', () => {
        fileInput.click();
    });

    fileInput.addEventListener('change', () => {
        if (fileInput.files.length) {
            handleFiles(fileInput.files);
        }
    });

    function handleFiles(files) {
        const file = files[0];
        
        if (!file.type.includes('pdf')) {
            showError('Please upload a PDF file');
            return;
        }
        
        showFileInfo(file);
        uploadFile(file);
    }

    function showFileInfo(file) {
        fileInfo.textContent = `File: ${file.name} (${formatFileSize(file.size)})`;
        fileInfo.classList.remove('d-none');
    }

    function formatFileSize(bytes) {
        if (bytes < 1024) return bytes + ' bytes';
        else if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
        else return (bytes / 1048576).toFixed(1) + ' MB';
    }

    function uploadFile(file) {
        showLoading('Extracting text from PDF...');
        
        // Set up form data
        const formData = new FormData();
        formData.append('pdf', file);

        // Show progress
        uploadProgress.classList.remove('d-none');
        progressBar.style.width = '0%';
        
        // Simulate progress (since fetch doesn't provide upload progress easily)
        let progress = 0;
        const progressInterval = setInterval(() => {
            progress += 5;
            if (progress <= 90) {
                progressBar.style.width = progress + '%';
            }
            
            if (progress > 90) {
                clearInterval(progressInterval);
            }
        }, 100);

        // Send file to server
        fetch('/extract', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            clearInterval(progressInterval);
            progressBar.style.width = '100%';
            
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to extract text');
                });
            }
            return response.json();
        })
        .then(data => {
            hideLoading();
            
            if (data.document_text) {
                extractedText = data.document_text.join('\n');
                currentStructuredData = data;
                showExtractedText(extractedText, data);
            } else {
                throw new Error('No text was extracted from the PDF');
            }
        })
        .catch(error => {
            hideLoading();
            showError(error.message);
        });
    }

    function showExtractedText(text, structuredData = null) {
        // Clear any existing structured info
        const existingStructuredInfo = extractedTextSection.querySelector('.structured-data-info');
        if (existingStructuredInfo) {
            existingStructuredInfo.remove();
        }
        
        // Show processing info and skip to AI processing
        if (structuredData) {
            const documentText = structuredData.document_text || [];
            const tables = structuredData.tables || [];
            const keyValues = structuredData.key_values || [];
            
            const structuredInfo = document.createElement('div');
            structuredInfo.className = 'alert alert-success mb-3 structured-data-info';
            structuredInfo.innerHTML = `
                <h6 class="mb-2">📊 Document Processed Successfully</h6>
                <div class="row small">
                    <div class="col-md-3">Text Lines: ${documentText.length}</div>
                    <div class="col-md-3">Tables: ${tables.length}</div>
                    <div class="col-md-3">Key-Values: ${keyValues.length}</div>
                    <div class="col-md-3">Processing: Complete</div>
                </div>
                <div class="text-center mt-3">
                    <button id="process-ai-btn" class="btn btn-primary btn-lg">
                        <i class="bi bi-gear"></i> Process with AI
                    </button>
                    <button class="btn btn-outline-secondary btn-sm ms-2" onclick="showJsonModal()">
                        View Raw JSON
                    </button>
                </div>
            `;
            
            // Replace extracted text section content
            extractedTextSection.innerHTML = '';
            extractedTextSection.appendChild(structuredInfo);
            extractedTextSection.classList.remove('d-none');
            
            // Add event listener for AI processing
            document.getElementById('process-ai-btn').addEventListener('click', processText);
        }
        
        window.scrollTo({
            top: extractedTextSection.offsetTop - 20,
            behavior: 'smooth'
        });
    }

    // Process extracted text
    processBtn.addEventListener('click', processText);

    function processText() {
        if (!currentStructuredData) {
            showError('No structured data to process');
            return;
        }

        showLoading('Processing structured data with AI...');

        fetch('/process', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentStructuredData)
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to process text');
                });
            }
            return response.json();
        })
        .then(data => {
            hideLoading();
            
            // Display structured processing results
            displayStructuredResults(data);
        })
        .catch(error => {
            hideLoading();
            showError(error.message);
        });
    }

    function displayStructuredResults(data) {
        const resultsSection = document.getElementById('results-section');
        
        // Clear previous results
        resultsSection.innerHTML = '';
        
        let html = `
            <div class="processing-summary alert alert-success mb-3">
                <h5>AI Processing Complete</h5>
                <div class="row small">
                    <div class="col-md-3">Tables: ${data.processed_tables?.length || 0}</div>
                    <div class="col-md-3">Key-Values: ${data.processed_key_values ? 'Processed' : 'None'}</div>
                    <div class="col-md-3">Text Chunks: ${data.processed_document_text?.length || 0}</div>
                    <div class="col-md-3">Total Rows: ${data.total_rows || 0}</div>
                </div>
            </div>
        `;
        
        let allCsvData = [];
        let hasData = false;
        
        // Display Tables
        if (data.processed_tables && data.processed_tables.length > 0) {
            html += '<h5>📊 Extracted Tables</h5>';
            data.processed_tables.forEach((table, index) => {
                if (table.structured_table && !table.structured_table.error) {
                    hasData = true;
                    html += `
                        <div class="card mb-3">
                            <div class="card-header bg-primary text-white">
                                <h6 class="mb-0">Table ${index + 1} (Page ${table.page})</h6>
                            </div>
                            <div class="card-body">
                                <div class="table-responsive">
                                    <table class="table table-striped table-sm">
                                        <thead class="table-light">
                                            <tr><th>Field</th><th>Value</th></tr>
                                        </thead>
                                        <tbody>
                    `;
                    
                    Object.entries(table.structured_table).forEach(([key, value]) => {
                        if (key !== 'error') {
                            html += `<tr><td><strong>${key}</strong></td><td>${value}</td></tr>`;
                            allCsvData.push({
                                source: `Table ${index + 1}`,
                                type: 'Table Data',
                                field: key,
                                value: String(value),
                                page: table.page
                            });
                        }
                    });
                    
                    html += `
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    `;
                }
            });
        }
        
        // Display Key-Value Pairs
        if (data.processed_key_values && data.processed_key_values.structured_key_values && !data.processed_key_values.structured_key_values.error) {
            hasData = true;
            html += `
                <h5>🔑 Key-Value Pairs</h5>
                <div class="card mb-3">
                    <div class="card-header bg-info text-white">
                        <h6 class="mb-0">Document Metadata</h6>
                    </div>
                    <div class="card-body">
                        <div class="table-responsive">
                            <table class="table table-striped table-sm">
                                <thead class="table-light">
                                    <tr><th>Field</th><th>Value</th></tr>
                                </thead>
                                <tbody>
            `;
            
            Object.entries(data.processed_key_values.structured_key_values).forEach(([key, value]) => {
                if (key !== 'error') {
                    html += `<tr><td><strong>${key}</strong></td><td>${value}</td></tr>`;
                    allCsvData.push({
                        source: 'Key-Value Pairs',
                        type: 'Structured Data',
                        field: key,
                        value: String(value),
                        page: 'N/A'
                    });
                }
            });
            
            html += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            `;
        }
        
        // Display Financial Data
        if (data.processed_document_text && data.processed_document_text.length > 0) {
            const validChunks = data.processed_document_text.filter(chunk => 
                chunk.extracted_facts && !chunk.extracted_facts.error && 
                Object.keys(chunk.extracted_facts).length > 0
            );
            
            if (validChunks.length > 0) {
                hasData = true;
                html += '<h5>💰 Financial & Business Data</h5>';
                
                validChunks.forEach((chunk, index) => {
                    html += `
                        <div class="card mb-3">
                            <div class="card-header bg-success text-white">
                                <h6 class="mb-0">Text Segment ${index + 1}</h6>
                            </div>
                            <div class="card-body">
                                <div class="table-responsive">
                                    <table class="table table-striped table-sm">
                                        <thead class="table-light">
                                            <tr><th>Metric</th><th>Value</th></tr>
                                        </thead>
                                        <tbody>
                    `;
                    
                    Object.entries(chunk.extracted_facts).forEach(([key, value]) => {
                        if (key !== 'error') {
                            html += `<tr><td><strong>${key}</strong></td><td>${value}</td></tr>`;
                            allCsvData.push({
                                source: `Text Segment ${index + 1}`,
                                type: 'Financial Data',
                                field: key,
                                value: String(value),
                                page: 'N/A'
                            });
                        }
                    });
                    
                    html += `
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        </div>
                    `;
                });
            }
        }
        
        if (!hasData) {
            html += `
                <div class="alert alert-warning">
                    <h6>No structured data found</h6>
                    <p>The AI processing did not extract meaningful data from the document.</p>
                </div>
            `;
        }
        
        // Add export buttons
        html += `
            <div class="text-center mt-4">
                <button id="export-csv-btn" class="btn btn-success me-2" ${!hasData ? 'disabled' : ''}>
                    <i class="bi bi-file-earmark-spreadsheet"></i> Export CSV (${allCsvData.length} rows)
                </button>
                <button id="export-json-btn" class="btn btn-outline-primary me-2">
                    <i class="bi bi-file-earmark-code"></i> Export JSON
                </button>
                <button id="export-excel-btn" class="btn btn-outline-success" ${!hasData ? 'disabled' : ''}>
                    <i class="bi bi-file-earmark-excel"></i> Export Excel
                </button>
            </div>
        `;
        
        resultsSection.innerHTML = html;
        resultsSection.classList.remove('d-none');
        
        // Add export event listeners
        if (hasData) {
            document.getElementById('export-csv-btn').addEventListener('click', () => {
                exportToCSV(allCsvData);
            });
            
            document.getElementById('export-excel-btn').addEventListener('click', () => {
                exportToExcel(allCsvData);
            });
        }
        
        document.getElementById('export-json-btn').addEventListener('click', () => {
            const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'structured_data.json';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        });
        
        window.scrollTo({
            top: resultsSection.offsetTop - 20,
            behavior: 'smooth'
        });
    }
    
    function createUnifiedTable(data) {
        let unifiedData = [];
        
        // Helper function to extract values from nested objects
        function extractValues(obj, source, type, page = null) {
            if (!obj || typeof obj !== 'object') return [];
            
            let results = [];
            
            // Handle different data structures
            if (Array.isArray(obj)) {
                obj.forEach((item, index) => {
                    if (typeof item === 'object' && item !== null) {
                        // Recursively extract from array items
                        const subResults = extractValues(item, source, type, page);
                        results = results.concat(subResults);
                    } else if (item !== null && item !== undefined && item !== '') {
                        results.push({
                            source: source,
                            type: type,
                            field: `item_${index}`,
                            value: String(item),
                            page: page
                        });
                    }
                });
            } else {
                // Handle object with nested properties
                function flattenObject(obj, prefix = '') {
                    Object.entries(obj).forEach(([key, value]) => {
                        if (value !== null && value !== undefined && value !== '') {
                            const fieldName = prefix ? `${prefix}.${key}` : key;
                            
                            if (Array.isArray(value)) {
                                // Handle arrays by extracting each element
                                value.forEach((arrayItem, arrayIndex) => {
                                    if (typeof arrayItem === 'object' && arrayItem !== null) {
                                        // Recursively flatten array objects
                                        const subResults = extractValues(arrayItem, source, type, page);
                                        results = results.concat(subResults.map(r => ({
                                            ...r,
                                            field: `${fieldName}[${arrayIndex}].${r.field}`
                                        })));
                                    } else if (arrayItem !== null && arrayItem !== undefined && arrayItem !== '') {
                                        results.push({
                                            source: source,
                                            type: type,
                                            field: `${fieldName}[${arrayIndex}]`,
                                            value: String(arrayItem),
                                            page: page
                                        });
                                    }
                                });
                            } else if (typeof value === 'object' && value !== null) {
                                // Recursively flatten nested objects
                                flattenObject(value, fieldName);
                            } else {
                                results.push({
                                    source: source,
                                    type: type,
                                    field: fieldName,
                                    value: String(value),
                                    page: page
                                });
                            }
                        }
                    });
                }
                
                flattenObject(obj);
            }
            
            return results;
        }
        
        // Process tables
        if (data.processed_tables && data.processed_tables.length > 0) {
            data.processed_tables.forEach((table, index) => {
                if (table.structured_table && !table.structured_table.error) {
                    const tableResults = extractValues(
                        table.structured_table, 
                        `Table ${index + 1}`, 
                        'Table Data', 
                        table.page
                    );
                    unifiedData = unifiedData.concat(tableResults);
                }
            });
        }
        
        // Process key-value pairs
        if (data.processed_key_values && data.processed_key_values.structured_key_values && !data.processed_key_values.structured_key_values.error) {
            const kvResults = extractValues(
                data.processed_key_values.structured_key_values,
                'Key-Value Pairs',
                'Structured Data'
            );
            unifiedData = unifiedData.concat(kvResults);
        }
        
        // Process extracted facts from document text
        if (data.processed_document_text && data.processed_document_text.length > 0) {
            data.processed_document_text.forEach((chunk, chunkIndex) => {
                if (chunk.extracted_facts && !chunk.extracted_facts.error) {
                    const factsResults = extractValues(
                        chunk.extracted_facts,
                        `Text Chunk ${chunkIndex + 1}`,
                        'Financial Data'
                    );
                    unifiedData = unifiedData.concat(factsResults);
                }
            });
        }
        
        return unifiedData;
    }
    
    function convertTableToHTML(tableData, tableIndex, page) {
        let html = `
            <div class="table-responsive mb-4">
                <table class="table table-striped table-hover">
                    <thead class="table-dark">
        `;
        
        let csvData = [];
        let headers = [];
        
        // Helper function to safely convert values to string
        function safeStringify(value) {
            if (value === null || value === undefined) return '';
            if (typeof value === 'object') {
                return JSON.stringify(value);
            }
            return String(value);
        }
        
        // Handle different table structures
        if (Array.isArray(tableData)) {
            // Array of objects
            if (tableData.length > 0 && typeof tableData[0] === 'object') {
                headers = Object.keys(tableData[0]);
                html += '<tr>';
                headers.forEach(header => {
                    html += `<th>${header}</th>`;
                });
                html += '</tr></thead><tbody>';
                
                tableData.forEach(row => {
                    html += '<tr>';
                    let csvRow = { source: `Table ${tableIndex} (Page ${page})`, type: 'Table Data' };
                    headers.forEach(header => {
                        const value = safeStringify(row[header]);
                        html += `<td>${value}</td>`;
                        csvRow[header] = value;
                    });
                    html += '</tr>';
                    csvData.push(csvRow);
                });
            }
        } else if (typeof tableData === 'object' && tableData !== null) {
            // Object with key-value pairs
            headers = ['Field', 'Value'];
            html += '<tr><th>Field</th><th>Value</th></tr></thead><tbody>';
            
            // Flatten nested objects for better display
            function flattenObject(obj, prefix = '') {
                let flattened = {};
                for (let key in obj) {
                    if (obj.hasOwnProperty(key)) {
                        const fullKey = prefix ? `${prefix}.${key}` : key;
                        const value = obj[key];
                        
                        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                            // Recursively flatten nested objects
                            Object.assign(flattened, flattenObject(value, fullKey));
                        } else {
                            flattened[fullKey] = safeStringify(value);
                        }
                    }
                }
                return flattened;
            }
            
            const flattenedData = flattenObject(tableData);
            
            Object.entries(flattenedData).forEach(([key, value]) => {
                html += `<tr><td><strong>${key}</strong></td><td>${value}</td></tr>`;
                csvData.push({
                    source: `Table ${tableIndex} (Page ${page})`,
                    type: 'Table Data',
                    field: key,
                    value: value
                });
            });
        }
        
        html += '</tbody></table></div>';
        
        return { html, csvData };
    }
    
    function convertKeyValuesToHTML(kvData) {
        let html = `
            <div class="table-responsive mb-4">
                <table class="table table-striped">
                    <thead class="table-dark">
                        <tr><th>Field</th><th>Value</th></tr>
                    </thead>
                    <tbody>
        `;
        
        let csvData = [];
        
        // Helper function to safely convert values to string
        function safeStringify(value) {
            if (value === null || value === undefined) return '';
            if (typeof value === 'object') {
                return JSON.stringify(value);
            }
            return String(value);
        }
        
        // Flatten nested objects for better display
        function flattenObject(obj, prefix = '') {
            let flattened = {};
            for (let key in obj) {
                if (obj.hasOwnProperty(key)) {
                    const fullKey = prefix ? `${prefix}.${key}` : key;
                    const value = obj[key];
                    
                    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                        Object.assign(flattened, flattenObject(value, fullKey));
                    } else {
                        flattened[fullKey] = safeStringify(value);
                    }
                }
            }
            return flattened;
        }
        
        const flattenedData = flattenObject(kvData);
        
        Object.entries(flattenedData).forEach(([key, value]) => {
            html += `<tr><td><strong>${key}</strong></td><td>${value}</td></tr>`;
            csvData.push({
                source: 'Key-Value Pairs',
                type: 'Structured Data',
                field: key,
                value: value
            });
        });
        
        html += '</tbody></table></div>';
        
        return { html, csvData };
    }
    
    function convertFactsToHTML(factsArray) {
        let html = `
            <div class="table-responsive mb-4">
                <table class="table table-striped">
                    <thead class="table-dark">
                        <tr><th>Metric</th><th>Value</th><th>Source</th></tr>
                    </thead>
                    <tbody>
        `;
        
        let csvData = [];
        
        // Helper function to safely convert values to string
        function safeStringify(value) {
            if (value === null || value === undefined) return '';
            if (typeof value === 'object') {
                return JSON.stringify(value);
            }
            return String(value);
        }
        
        // Flatten nested objects for better display
        function flattenObject(obj, prefix = '') {
            let flattened = {};
            for (let key in obj) {
                if (obj.hasOwnProperty(key)) {
                    const fullKey = prefix ? `${prefix}.${key}` : key;
                    const value = obj[key];
                    
                    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                        Object.assign(flattened, flattenObject(value, fullKey));
                    } else {
                        flattened[fullKey] = safeStringify(value);
                    }
                }
            }
            return flattened;
        }
        
        factsArray.forEach((chunk, chunkIndex) => {
            if (chunk.extracted_facts && !chunk.extracted_facts.error) {
                const flattenedFacts = flattenObject(chunk.extracted_facts);
                
                Object.entries(flattenedFacts).forEach(([key, value]) => {
                    if (key && value && value !== '') {
                        html += `<tr><td><strong>${key}</strong></td><td>${value}</td><td>Text Chunk ${chunkIndex + 1}</td></tr>`;
                        csvData.push({
                            source: `Text Chunk ${chunkIndex + 1}`,
                            type: 'Financial Data',
                            field: key,
                            value: value
                        });
                    }
                });
            }
        });
        
        html += '</tbody></table></div>';
        
        return { html, csvData };
    }
    
    function exportToCSV(data) {
        if (!data || data.length === 0) {
            alert('No data to export');
            return;
        }
        
        // Get all unique keys for CSV headers
        const allKeys = new Set();
        data.forEach(row => {
            Object.keys(row).forEach(key => allKeys.add(key));
        });
        
        const headers = Array.from(allKeys);
        let csv = headers.join(',') + '\n';
        
        data.forEach(row => {
            const values = headers.map(header => {
                const value = row[header] || '';
                // Escape commas and quotes in CSV
                return `"${String(value).replace(/"/g, '""')}"`;
            });
            csv += values.join(',') + '\n';
        });
        
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'extracted_data.csv';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }
    
    function exportToExcel(data) {
        // For now, export as CSV with .xlsx extension
        // In a real implementation, you'd use a library like SheetJS
        exportToCSV(data);
        alert('Excel export completed as CSV format. For true Excel format, please use the CSV file with Excel.');
    }

    function showProcessingSummary(metadata) {
        if (!metadata) return;
        
        const summaryHtml = `
            <div class="processing-summary alert alert-success mb-3">
                <h5>Processing Complete</h5>
                <div class="summary-stats">
                    <small class="text-muted">
                        Extracted and analyzed using ${metadata.processing_mode === 'agentic' ? 'advanced AI processing' : 'standard processing'}
                        ${metadata.optimization && metadata.optimization.optimization_applied ? ' with table optimization applied' : ''}
                    </small>
                </div>
            </div>
        `;
        
        const resultsSection = document.querySelector('.results-section');
        if (resultsSection) {
            const existingSummary = resultsSection.querySelector('.processing-summary');
            if (existingSummary) {
                existingSummary.remove();
            }
            resultsSection.insertAdjacentHTML('afterbegin', summaryHtml);
        }
    }

    function displayResults(data) {
        if (!data.length) {
            showError('No structured data could be extracted');
            return;
        }

        // Clear previous results
        tableHeader.innerHTML = '';
        tableBody.innerHTML = '';

        // Check if any row has commentary
        const hasCommentary = data.some(row => row.commentary && row.commentary.trim());

        // Create table header with conditional commentary column
        const headers = ['source', 'type', 'field', 'value', 'page'];
        if (hasCommentary) {
            headers.push('commentary');
        }
        
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header.charAt(0).toUpperCase() + header.slice(1);
            tableHeader.appendChild(th);
        });

        // Create table rows
        data.forEach(row => {
            const tr = document.createElement('tr');
            
            // Add source column with badge
            const sourceTd = document.createElement('td');
            sourceTd.innerHTML = `<span class="badge bg-secondary">${row.source || ''}</span>`;
            tr.appendChild(sourceTd);
            
            // Add type column with badge
            const typeTd = document.createElement('td');
            typeTd.innerHTML = `<span class="badge bg-info">${row.type || ''}</span>`;
            tr.appendChild(typeTd);
            
            // Add field column with bold text
            const fieldTd = document.createElement('td');
            fieldTd.innerHTML = `<strong>${row.field || ''}</strong>`;
            tr.appendChild(fieldTd);
            
            // Add value column
            const valueTd = document.createElement('td');
            valueTd.textContent = row.value || '';
            tr.appendChild(valueTd);
            
            // Add page column
            const pageTd = document.createElement('td');
            pageTd.textContent = row.page || '';
            tr.appendChild(pageTd);
            
            // Add commentary column if needed
            if (hasCommentary) {
                const commentaryTd = document.createElement('td');
                commentaryTd.className = 'commentary-cell';
                if (row.commentary && row.commentary.trim()) {
                    commentaryTd.innerHTML = `<span class="text-muted small">${row.commentary}</span>`;
                    tr.classList.add('has-commentary');
                } else {
                    commentaryTd.innerHTML = '<span class="text-muted">-</span>';
                }
                tr.appendChild(commentaryTd);
            }
            
            tableBody.appendChild(tr);
        });

        // Show results section
        resultsSection.classList.remove('d-none');
        window.scrollTo({
            top: resultsSection.offsetTop - 20,
            behavior: 'smooth'
        });
    }

    // Export functions
    exportJsonBtn.addEventListener('click', exportJson);
    exportCsvBtn.addEventListener('click', exportCsv);
    exportPdfBtn.addEventListener('click', exportPdf);

    function exportJson() {
        if (!processedData.length) {
            showError('No data to export');
            return;
        }

        const jsonString = JSON.stringify(processedData, null, 2);
        downloadFile(jsonString, 'extracted_data.json', 'application/json');
    }

    function exportCsv() {
        if (!processedData.length) {
            showError('No data to export');
            return;
        }

        const headers = Object.keys(processedData[0]);
        
        // Create CSV content
        let csvContent = headers.join(',') + '\n';
        
        processedData.forEach(row => {
            const values = headers.map(header => {
                const value = row[header] || '';
                // Escape commas and quotes in values
                return `"${value.toString().replace(/"/g, '""')}"`;
            });
            csvContent += values.join(',') + '\n';
        });
        
        downloadFile(csvContent, 'extracted_data.csv', 'text/csv');
    }

    function exportPdf() {
        if (!processedData.length) {
            showError('No data to export');
            return;
        }

        showLoading('Generating PDF...');

        fetch('/export/pdf', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ data: processedData })
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to generate PDF');
                });
            }
            return response.json();
        })
        .then(data => {
            hideLoading();
            
            if (data.pdf) {
                // Create and click download link
                const link = document.createElement('a');
                link.href = `data:application/pdf;base64,${data.pdf}`;
                link.download = 'extracted_data.pdf';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            } else {
                throw new Error('No PDF data received from server');
            }
        })
        .catch(error => {
            hideLoading();
            showError(error.message);
        });
    }

    function downloadFile(content, fileName, contentType) {
        const blob = new Blob([content], { type: contentType });
        const url = URL.createObjectURL(blob);
        
        const link = document.createElement('a');
        link.href = url;
        link.download = fileName;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        setTimeout(() => {
            URL.revokeObjectURL(url);
        }, 100);
    }

    // UI helpers
    function showLoading(message) {
        loadingMessage.textContent = message || 'Processing...';
        loadingOverlay.classList.remove('d-none');
        loadingOverlay.classList.add('active');
    }

    function hideLoading() {
        loadingOverlay.classList.remove('active');
        loadingOverlay.classList.add('d-none');
    }

    function showError(message) {
        errorMessage.textContent = message;
        errorMessage.classList.remove('d-none');
        
        setTimeout(() => {
            errorMessage.classList.add('d-none');
        }, 5000);
    }

    // Global function to show JSON modal
    window.showJsonModal = function() {
        if (!currentStructuredData) {
            alert('No structured data available');
            return;
        }

        // Create modal HTML
        const modalHtml = `
            <div class="modal fade" id="jsonModal" tabindex="-1" aria-labelledby="jsonModalLabel" aria-hidden="true">
                <div class="modal-dialog modal-xl">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title" id="jsonModalLabel">Complete Amazon Textract JSON Structure</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <div class="mb-3">
                                <button class="btn btn-outline-secondary btn-sm" onclick="copyJsonToClipboard()">Copy JSON</button>
                                <button class="btn btn-outline-primary btn-sm" onclick="downloadJson()">Download JSON</button>
                            </div>
                            <pre class="bg-light p-3 rounded" style="max-height: 500px; overflow-y: auto; font-size: 12px;"><code id="jsonContent">${JSON.stringify(currentStructuredData, null, 2)}</code></pre>
                        </div>
                    </div>
                </div>
            </div>
        `;

        // Remove existing modal if any
        const existingModal = document.getElementById('jsonModal');
        if (existingModal) {
            existingModal.remove();
        }

        // Add modal to body
        document.body.insertAdjacentHTML('beforeend', modalHtml);

        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('jsonModal'));
        modal.show();
    };

    window.copyJsonToClipboard = function() {
        const jsonContent = JSON.stringify(currentStructuredData, null, 2);
        navigator.clipboard.writeText(jsonContent).then(() => {
            alert('JSON copied to clipboard!');
        }).catch(() => {
            alert('Failed to copy JSON to clipboard');
        });
    };

    window.downloadJson = function() {
        const jsonContent = JSON.stringify(currentStructuredData, null, 2);
        const blob = new Blob([jsonContent], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'textract_output.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    };
});