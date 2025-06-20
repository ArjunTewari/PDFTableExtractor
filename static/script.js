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

        showLoading('Starting AI processing...');

        // Try streaming first, fallback to regular processing
        processWithStreaming();
    }

    function processWithStreaming() {
        fetch('/process_stream', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentStructuredData)
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Streaming not available');
            }
            
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            
            function readStream() {
                return reader.read().then(({ done, value }) => {
                    if (done) {
                        hideLoading();
                        return;
                    }
                    
                    buffer += decoder.decode(value, { stream: true });
                    const lines = buffer.split('\n');
                    
                    // Keep the last incomplete line in buffer
                    buffer = lines.pop() || '';
                    
                    lines.forEach(line => {
                        if (line.trim().startsWith('data: ')) {
                            try {
                                const jsonStr = line.slice(6).trim();
                                if (jsonStr && jsonStr !== '{}') {
                                    const data = JSON.parse(jsonStr);
                                    handleStreamingData(data);
                                }
                            } catch (e) {
                                console.error('Error parsing streaming data:', e, 'Line:', line);
                            }
                        }
                    });
                    
                    return readStream();
                });
            }
            
            return readStream();
        })
        .catch(error => {
            console.log('Streaming failed, using regular processing');
            processRegular();
        });
    }

    let streamedRows = [];
    let tableInitialized = false;

    let cleanedRows = [];
    
    function handleStreamingData(data) {
        if (data.type === 'row') {
            // Add new row to streaming results
            streamedRows.push(data.data);
            displayStreamingRow(data.data);
        } else if (data.type === 'row_update') {
            // Update existing row with commentary
            updateStreamingRow(data.data);
        } else if (data.type === 'cleaned_row') {
            // Add cleaned row
            cleanedRows.push(data.data);
            displayCleanedRow(data.data);
        } else if (data.type === 'cleanup_complete') {
            console.log('Data cleanup complete. Original:', data.original_rows, 'Cleaned:', data.cleaned_rows);
            showCleanupSummary(data.original_rows, data.cleaned_rows);
        } else if (data.type === 'complete') {
            hideLoading();
            console.log('Streaming complete. Total rows:', data.total_rows);
            processedData = cleanedRows.length > 0 ? cleanedRows : streamedRows;
            finalizeStreamingDisplay();
        } else if (data.status === 'error') {
            hideLoading();
            showError('Processing failed: ' + data.error);
        }
    }

    function displayStreamingRow(rowData) {
        if (!tableInitialized) {
            initializeStreamingTable();
            tableInitialized = true;
        }
        
        const tableBody = document.getElementById('streaming-table-body');
        const tr = document.createElement('tr');
        tr.id = `row-${streamedRows.length - 1}`;
        
        tr.innerHTML = `
            <td><span class="badge bg-secondary">${rowData.source}</span></td>
            <td><span class="badge bg-info">${rowData.type}</span></td>
            <td><strong>${rowData.field}</strong></td>
            <td>${rowData.value}</td>
            <td>${rowData.page}</td>
            <td class="commentary-cell">${rowData.commentary || '<span class="text-muted">-</span>'}</td>
        `;
        
        if (rowData.commentary) {
            tr.classList.add('has-commentary');
        }
        
        tableBody.appendChild(tr);
    }

    function updateStreamingRow(rowData) {
        const rowIndex = streamedRows.findIndex(row => 
            row.field === rowData.field && row.source === rowData.source
        );
        
        if (rowIndex >= 0) {
            streamedRows[rowIndex] = rowData;
            const tr = document.getElementById(`row-${rowIndex}`);
            if (tr) {
                const commentaryCell = tr.querySelector('.commentary-cell');
                if (commentaryCell) {
                    commentaryCell.innerHTML = rowData.commentary ? 
                        `<span class="text-muted small">${rowData.commentary}</span>` : 
                        '<span class="text-muted">-</span>';
                }
                if (rowData.commentary) {
                    tr.classList.add('has-commentary');
                }
            }
        }
    }

    function initializeStreamingTable() {
        hideLoading();
        resultsSection.innerHTML = `
            <div class="row">
                <div class="col-12">
                    <h4>Extracted Data with Commentary</h4>
                    <div class="table-responsive">
                        <table class="table table-striped table-hover">
                            <thead class="table-dark">
                                <tr>
                                    <th>Source</th>
                                    <th>Type</th>
                                    <th>Field</th>
                                    <th>Value</th>
                                    <th>Page</th>
                                    <th>Commentary</th>
                                </tr>
                            </thead>
                            <tbody id="streaming-table-body"></tbody>
                        </table>
                    </div>
                </div>
            </div>
            <div id="cleaned-section" style="display: none;">
                <hr>
                <div class="row">
                    <div class="col-12">
                        <h4>Cleaned & Interpreted Data</h4>
                        <div class="alert alert-info">
                            <small>Redundancies removed, similar columns combined, commentary summarized</small>
                        </div>
                        <div class="table-responsive">
                            <table class="table table-striped table-hover">
                                <thead class="table-success">
                                    <tr>
                                        <th>Source</th>
                                        <th>Type</th>
                                        <th>Field</th>
                                        <th>Value</th>
                                        <th>Page</th>
                                        <th>Commentary</th>
                                    </tr>
                                </thead>
                                <tbody id="cleaned-table-body"></tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
            <div class="mt-3">
                <button class="btn btn-outline-primary" id="export-json-btn">Export as JSON</button>
                <button class="btn btn-outline-success" id="export-csv-btn">Export as CSV</button>
                <button class="btn btn-outline-danger" id="export-pdf-btn">Export as PDF</button>
            </div>
        `;
        
        resultsSection.classList.remove('d-none');
    }

    function displayCleanedRow(rowData) {
        // Show cleaned section if not visible
        const cleanedSection = document.getElementById('cleaned-section');
        if (cleanedSection) {
            cleanedSection.style.display = 'block';
        }
        
        const cleanedTableBody = document.getElementById('cleaned-table-body');
        if (!cleanedTableBody) return;
        
        const tr = document.createElement('tr');
        tr.className = 'table-success';
        
        tr.innerHTML = `
            <td><span class="badge bg-success">${rowData.source}</span></td>
            <td><span class="badge bg-primary">${rowData.type}</span></td>
            <td><strong>${rowData.field}</strong></td>
            <td>${rowData.value}</td>
            <td>${rowData.page}</td>
            <td class="commentary-cell">${rowData.commentary || '<span class="text-muted">-</span>'}</td>
        `;
        
        cleanedTableBody.appendChild(tr);
    }

    function showCleanupSummary(originalRows, cleanedRows) {
        const reductionPercentage = Math.round(((originalRows - cleanedRows) / originalRows) * 100);
        
        const summaryDiv = document.createElement('div');
        summaryDiv.className = 'alert alert-success mt-2';
        summaryDiv.innerHTML = `
            <h6>Data Cleanup Complete</h6>
            <small>Reduced from ${originalRows} to ${cleanedRows} rows (${reductionPercentage}% reduction)</small>
        `;
        
        const cleanedSection = document.getElementById('cleaned-section');
        if (cleanedSection) {
            cleanedSection.appendChild(summaryDiv);
        }
    }

    function finalizeStreamingDisplay() {
        // Re-attach export event listeners
        document.getElementById('export-json-btn').addEventListener('click', exportJson);
        document.getElementById('export-csv-btn').addEventListener('click', exportCsv);
        document.getElementById('export-pdf-btn').addEventListener('click', exportPdf);
        
        // Add summary
        const summaryDiv = document.createElement('div');
        summaryDiv.className = 'alert alert-success mt-3';
        const dataSource = cleanedRows.length > 0 ? 'cleaned and interpreted' : 'original extracted';
        const totalRows = cleanedRows.length > 0 ? cleanedRows.length : streamedRows.length;
        
        summaryDiv.innerHTML = `
            <h6>Processing Complete</h6>
            <small>Total ${dataSource} rows: ${totalRows} | Commentary from document text only</small>
        `;
        resultsSection.appendChild(summaryDiv);
    }

    function displayProcessingSummary(data) {
        resultsSection.innerHTML = `
            <div class="alert alert-info">
                <h5>Processing Complete</h5>
                <p>The document was analyzed successfully but no structured data could be extracted in the expected format.</p>
                <div class="mt-3">
                    <h6>Processing Details:</h6>
                    <ul>
                        <li>Tables processed: ${data.summary?.total_tables || 0}</li>
                        <li>Key-value pairs: ${data.summary?.total_key_values || 0}</li>
                        <li>Text chunks: ${data.summary?.text_chunks_processed || 0}</li>
                        <li>Commentary matches: ${data.summary?.commentary_matches || 0}</li>
                    </ul>
                </div>
                ${data.processed_tables ? `
                    <div class="mt-3">
                        <h6>Raw Processing Results:</h6>
                        <pre class="bg-light p-3 small">${JSON.stringify(data.processed_tables, null, 2)}</pre>
                    </div>
                ` : ''}
            </div>
        `;
        resultsSection.classList.remove('d-none');
    }

    function processRegular() {
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
            
            processedData = data.dataframe_data || [];
            displayResultsWithTables(processedData);
            
            if (data.summary) {
                showProcessingSummary(data.summary);
            }
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
        displayResultsWithTables(data);
    }

    function displayResultsWithTables(data) {
        if (!data.length) {
            showError('No structured data could be extracted');
            return;
        }

        // Clear previous results
        resultsSection.innerHTML = `
            <h4>Extracted Data with Commentary</h4>
            <div id="tables-container"></div>
            <div id="data-table-container">
                <h5>All Data Points</h5>
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead id="data-table-header" class="table-dark"></thead>
                        <tbody id="data-table-body"></tbody>
                    </table>
                </div>
            </div>
            <div class="mt-3">
                <button class="btn btn-outline-primary" id="export-json-btn">Export as JSON</button>
                <button class="btn btn-outline-success" id="export-csv-btn">Export as CSV</button>
                <button class="btn btn-outline-danger" id="export-pdf-btn">Export as PDF</button>
            </div>
        `;

        const tablesContainer = document.getElementById('tables-container');
        const dataTableHeader = document.getElementById('data-table-header');
        const dataTableBody = document.getElementById('data-table-body');

        // Reconstruct and display actual tables
        const tableStructures = data.filter(row => row.is_table_header && row.headers && row.rows);
        
        tableStructures.forEach((tableHeader, index) => {
            const tableDiv = document.createElement('div');
            tableDiv.className = 'card mb-4';
            
            const headers = tableHeader.headers;
            const rows = tableHeader.rows;
            
            // Determine table type for styling
            const isDocumentTable = tableHeader.source && tableHeader.source.includes('Document Text');
            const headerClass = isDocumentTable ? 'bg-success' : 'bg-primary';
            const tableTitle = isDocumentTable ? 
                `Document Content Table ${index + 1}` : 
                `Extracted Table ${index + 1} (Page ${tableHeader.page})`;
            
            let tableHtml = `
                <div class="card-header ${headerClass} text-white">
                    <h6 class="mb-0">${tableTitle}</h6>
                    <small>Columns: ${headers.length} | Rows: ${rows.length}</small>
                </div>
                <div class="card-body">
                    <div class="table-responsive">
                        <table class="table table-striped table-hover table-sm">
                            <thead class="table-light">
                                <tr>`;
            
            // Add headers
            headers.forEach(header => {
                tableHtml += `<th style="min-width: 120px;">${header}</th>`;
            });
            
            tableHtml += `</tr></thead><tbody>`;
            
            // Add rows
            rows.forEach(row => {
                tableHtml += '<tr>';
                for (let i = 0; i < headers.length; i++) {
                    const cellValue = i < row.length ? (row[i] || '-') : '-';
                    tableHtml += `<td>${cellValue}</td>`;
                }
                tableHtml += '</tr>';
            });
            
            tableHtml += `
                            </tbody>
                        </table>
                    </div>
                    <small class="text-muted">
                        ${isDocumentTable ? 
                            'Document content organized into structured table format' : 
                            'Original table reconstructed from extracted data points'}
                    </small>
                </div>
            `;
            
            tableDiv.innerHTML = tableHtml;
            tablesContainer.appendChild(tableDiv);
        });

        // Check if any row has commentary
        const hasCommentary = data.some(row => row.commentary && row.commentary.trim());

        // Create data table header with conditional commentary column
        const headers = ['Source', 'Type', 'Field', 'Value', 'Page'];
        if (hasCommentary) {
            headers.push('Commentary');
        }
        
        dataTableHeader.innerHTML = `
            <tr>
                ${headers.map(header => `<th>${header}</th>`).join('')}
            </tr>
        `;

        // Create data table rows
        data.forEach(row => {
            // Skip table header rows in the detailed view
            if (row.is_table_header) return;
            
            const tr = document.createElement('tr');
            
            // Add source column with badge
            const sourceTd = document.createElement('td');
            sourceTd.innerHTML = `<span class="badge bg-secondary">${row.source || ''}</span>`;
            tr.appendChild(sourceTd);
            
            // Add type column with badge
            const typeTd = document.createElement('td');
            const badgeClass = row.type === 'General Commentary' ? 'bg-warning' : 
                              row.type === 'Table Data' ? 'bg-success' : 'bg-info';
            typeTd.innerHTML = `<span class="badge ${badgeClass}">${row.type || ''}</span>`;
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
            
            dataTableBody.appendChild(tr);
        });

        // Re-attach export event listeners
        document.getElementById('export-json-btn').addEventListener('click', exportJson);
        document.getElementById('export-csv-btn').addEventListener('click', exportCsv);
        document.getElementById('export-pdf-btn').addEventListener('click', exportPdf);

        // Store processed data globally
        processedData = data;

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