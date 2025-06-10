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
            
            if (data.text) {
                extractedText = data.text;
                showExtractedText(data.text, data.structured_data);
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
        // Update text content
        extractedTextContent.textContent = text;
        
        // Add structured data display if available
        if (structuredData) {
            const metadata = structuredData.metadata || {};
            const tables = structuredData.tables || [];
            const keyValues = structuredData.key_values || [];
            
            const structuredInfo = document.createElement('div');
            structuredInfo.className = 'alert alert-info mb-3';
            structuredInfo.innerHTML = `
                <h6 class="mb-2">📊 Amazon Textract Analysis</h6>
                <div class="row small">
                    <div class="col-md-3">Pages: ${metadata.pages || 0}</div>
                    <div class="col-md-3">Tables: ${tables.length}</div>
                    <div class="col-md-3">Key-Values: ${keyValues.length}</div>
                    <div class="col-md-3">Time: ${metadata.processing_time || 'N/A'}</div>
                </div>
                ${tables.length > 0 ? `
                    <div class="mt-2">
                        <strong>Tables Detected:</strong>
                        ${tables.map((table, i) => 
                            `<span class="badge bg-secondary me-1">Page ${table.page} (${table.table.length}×${table.table[0]?.length || 0})</span>`
                        ).join('')}
                    </div>
                ` : ''}
                ${keyValues.length > 0 ? `
                    <div class="mt-2">
                        <strong>Key-Value Pairs Found:</strong> 
                        <span class="badge bg-info">${keyValues.length} pairs</span>
                    </div>
                ` : ''}
            `;
            
            extractedTextSection.insertBefore(structuredInfo, extractedTextContent.parentElement);
        }
        
        extractedTextSection.classList.remove('d-none');
        window.scrollTo({
            top: extractedTextSection.offsetTop - 20,
            behavior: 'smooth'
        });
    }

    // Process extracted text
    processBtn.addEventListener('click', processText);

    function processText() {
        if (!extractedText.trim()) {
            showError('No text to process');
            return;
        }

        showLoading('Processing text with AI...');

        fetch('/process', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 
                text: extractedText,
                mode: 'agentic'
            })
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
            
            if (data.data && Array.isArray(data.data)) {
                processedData = data.data;
                displayResults(data.data);
                
                // Visualization removed for simpler interface
                
                // Show processing summary
                if (data.metadata) {
                    showProcessingSummary(data.metadata);
                }
            } else {
                throw new Error('Invalid data format received from server');
            }
        })
        .catch(error => {
            hideLoading();
            showError(error.message);
        });
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

        // Create table header
        const headers = Object.keys(data[0]);
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header;
            tableHeader.appendChild(th);
        });

        // Create table rows
        data.forEach(row => {
            const tr = document.createElement('tr');
            
            headers.forEach(header => {
                const td = document.createElement('td');
                td.textContent = row[header] || '';
                tr.appendChild(td);
            });
            
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
});