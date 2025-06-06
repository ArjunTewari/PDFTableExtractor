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
    let currentTextId = null;

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
                currentTextId = data.text_id;
                showExtractedText(data.text);
            } else {
                throw new Error('No text was extracted from the PDF');
            }
        })
        .catch(error => {
            hideLoading();
            showError(error.message);
        });
    }

    function showExtractedText(text) {
        extractedTextContent.textContent = text;
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

        showLoading('Initializing crew agents...');
        startRealTimeProcessing();
    }

    function startRealTimeProcessing() {
        const liveProgressContainer = createLiveProgressContainer();
        let iterationHistory = [];
        let currentIteration = 0;
        let finalData = null;

        // Use fetch with streaming for real-time updates
        fetch('/process_stream', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                text: extractedText,
                text_id: currentTextId
            })
        })
        .then(response => {
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            
            function readStream() {
                return reader.read().then(({ done, value }) => {
                    if (done) {
                        hideLoading();
                        return;
                    }
                    
                    const chunk = decoder.decode(value);
                    const lines = chunk.split('\n');
                    
                    lines.forEach(line => {
                        if (line.startsWith('data: ')) {
                            try {
                                const jsonData = line.substring(6);
                                const update = JSON.parse(jsonData);
                                handleStreamingUpdate(update, liveProgressContainer);
                                
                                if (update.type === 'iteration_complete') {
                                    iterationHistory.push(update.data);
                                    updateLiveProgress(update.data, liveProgressContainer);
                                }
                                
                                if (update.type === 'processing_complete') {
                                    finalData = update.result;
                                    hideLoading();
                                    showFinalResults(finalData, iterationHistory);
                                    return;
                                }
                                
                                if (update.type === 'error') {
                                    hideLoading();
                                    showError(update.error);
                                    return;
                                }
                            } catch (e) {
                                console.error('Error parsing streaming data:', e);
                            }
                        }
                    });
                    
                    return readStream();
                });
            }
            
            return readStream();
        })
        .catch(error => {
            console.error('Streaming failed:', error);
            hideLoading();
            // Fallback to regular processing
            fallbackToRegularProcessing();
        });
    }

    function createLiveProgressContainer() {
        const progressHtml = `
            <div class="live-progress-container mb-4" id="live-progress">
                <h5>🔄 Crew Agents Working Live</h5>
                <div class="crew-status">
                    <div class="agent-status" id="analysis-agent">
                        <span class="agent-name">Analysis Agent</span>
                        <span class="status-indicator waiting">⏳ Waiting</span>
                    </div>
                    <div class="agent-status" id="tabulation-agent">
                        <span class="agent-name">Tabulation Agent</span>
                        <span class="status-indicator waiting">⏳ Waiting</span>
                    </div>
                    <div class="agent-status" id="verification-agent">
                        <span class="agent-name">Verification Agent</span>
                        <span class="status-indicator waiting">⏳ Waiting</span>
                    </div>
                </div>
                <div class="iteration-live-feed" id="iteration-feed"></div>
            </div>
        `;
        
        const resultsSection = document.querySelector('.results-section') || document.body;
        resultsSection.insertAdjacentHTML('afterbegin', progressHtml);
        return document.getElementById('live-progress');
    }

    function handleStreamingUpdate(update, container) {
        const feedElement = container.querySelector('#iteration-feed');
        const timestamp = new Date().toLocaleTimeString();
        
        let message = '';
        let agentId = '';
        
        switch(update.type) {
            case 'iteration_start':
                message = `Starting iteration ${update.iteration}/${update.total}`;
                break;
            case 'step_start':
                agentId = update.step;
                message = update.message || `${update.step} agent starting...`;
                updateAgentStatus(agentId, 'working', '🔄 Working');
                break;
            case 'step_complete':
                agentId = update.step;
                message = `${update.step} agent completed`;
                updateAgentStatus(agentId, 'complete', '✅ Complete');
                break;
            case 'step_error':
                agentId = update.step;
                message = `${update.step} agent error: ${update.error}`;
                updateAgentStatus(agentId, 'error', '❌ Error');
                break;
            case 'coverage_achieved':
                message = `High coverage achieved (${update.coverage}%)`;
                break;
        }
        
        if (message) {
            const logEntry = document.createElement('div');
            logEntry.className = 'feed-entry';
            logEntry.innerHTML = `<span class="timestamp">${timestamp}</span> ${message}`;
            feedElement.appendChild(logEntry);
            feedElement.scrollTop = feedElement.scrollHeight;
        }
    }

    function updateAgentStatus(agentId, status, statusText) {
        const agentElement = document.getElementById(`${agentId}-agent`);
        if (agentElement) {
            const indicator = agentElement.querySelector('.status-indicator');
            indicator.className = `status-indicator ${status}`;
            indicator.textContent = statusText;
        }
    }

    function updateLiveProgress(iterationData, container) {
        const feedElement = container.querySelector('#iteration-feed');
        
        // Add iteration summary
        const summary = document.createElement('div');
        summary.className = 'iteration-summary';
        summary.innerHTML = `
            <div class="iteration-header">
                <strong>Iteration ${iterationData.iteration} Summary</strong>
                <span class="coverage-badge">Coverage: ${iterationData.coverage_score}%</span>
            </div>
            <div class="iteration-details">
                <small>Data points: ${iterationData.tabulation?.data?.length || 0} | 
                Gaps found: ${iterationData.verification?.missing_information?.length || 0}</small>
            </div>
        `;
        feedElement.appendChild(summary);
        feedElement.scrollTop = feedElement.scrollHeight;
        
        // Reset agent statuses for next iteration
        setTimeout(() => {
            ['analysis', 'tabulation', 'verification'].forEach(agent => {
                updateAgentStatus(agent, 'waiting', '⏳ Waiting');
            });
        }, 1000);
    }

    function showFinalResults(finalData, iterationHistory) {
        if (finalData && finalData.final_tabulation) {
            processedData = finalData.final_tabulation;
            
            // Show final iteration progress
            if (iterationHistory.length > 0) {
                showIterationProgress(iterationHistory);
            }
            
            displayResults(finalData.final_tabulation);
            
            // Initialize visualization
            if (iterationHistory.length > 0 && window.dataVisualization) {
                window.dataVisualization.initialize(
                    extractedText,
                    finalData.final_tabulation,
                    iterationHistory
                );
            }
            
            // Show processing summary
            showProcessingSummary({
                processing_mode: 'agentic_live',
                total_iterations: finalData.total_iterations,
                final_coverage: finalData.final_coverage
            });
        }
    }

    function fallbackToRegularProcessing() {
        fetch('/process', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 
                text: extractedText,
                text_id: currentTextId,
                mode: 'agentic'
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.data && Array.isArray(data.data)) {
                processedData = data.data;
                displayResults(data.data);
                if (data.metadata) {
                    showProcessingSummary(data.metadata);
                }
            }
        })
        .catch(error => {
            showError('Processing failed: ' + error.message);
        });
    }

    function showProcessingSummary(metadata) {
        if (!metadata) return;
        
        let optimizationInfo = '';
        if (metadata.optimization && metadata.optimization.optimization_applied) {
            const opt = metadata.optimization;
            optimizationInfo = `
                <div class="mt-2">
                    <small class="text-muted">
                        <strong>Table Optimization:</strong> ${opt.summary || 'Applied formatting improvements'}
                        ${opt.final_structure ? ` | ${opt.final_structure.rows} rows, ${opt.final_structure.columns} columns` : ''}
                    </small>
                </div>
            `;
        }
        
        const summaryHtml = `
            <div class="processing-summary alert alert-info mb-3">
                <h5>AI Processing Complete</h5>
                <div class="summary-stats d-flex gap-3">
                    <span class="badge bg-primary">Mode: ${metadata.processing_mode}</span>
                    ${metadata.total_iterations ? `<span class="badge bg-success">Iterations: ${metadata.total_iterations}</span>` : ''}
                    ${metadata.final_coverage ? `<span class="badge bg-warning">Coverage: ${metadata.final_coverage}%</span>` : ''}
                    ${metadata.optimization && metadata.optimization.optimization_applied ? `<span class="badge bg-info">Optimized</span>` : ''}
                </div>
                ${optimizationInfo}
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

    function showIterationProgress(iterationHistory) {
        const progressHtml = `
            <div class="iteration-progress mb-4" id="iteration-progress">
                <h5>🤖 Crew Processing Progress</h5>
                <div class="iteration-timeline">
                    ${iterationHistory.map((iteration, index) => `
                        <div class="iteration-item" data-iteration="${index + 1}">
                            <div class="iteration-header">
                                <strong>Iteration ${iteration.iteration}</strong>
                                <span class="badge bg-${iteration.coverage_score >= 90 ? 'success' : iteration.coverage_score >= 70 ? 'warning' : 'secondary'}">
                                    Coverage: ${iteration.coverage_score}%
                                </span>
                            </div>
                            <div class="iteration-details">
                                <div class="agent-work">
                                    <small><strong>Analysis Agent:</strong> ${iteration.analysis?.data_points_found || 0} data points identified</small><br>
                                    <small><strong>Tabulation Agent:</strong> ${iteration.tabulation?.data?.length || 0} entries created</small><br>
                                    <small><strong>Verification Agent:</strong> ${iteration.verification?.missing_information?.length || 0} gaps found</small>
                                </div>
                                ${iteration.tabulation?.data ? `
                                    <div class="iteration-table mt-2">
                                        <small>Sample data from this iteration:</small>
                                        <div class="table-preview">
                                            ${generateTablePreview(iteration.tabulation.data.slice(0, 3))}
                                        </div>
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
        
        const resultsSection = document.querySelector('.results-section') || document.body;
        const existingProgress = resultsSection.querySelector('.iteration-progress');
        if (existingProgress) {
            existingProgress.remove();
        }
        resultsSection.insertAdjacentHTML('afterbegin', progressHtml);
        
        // Add animation to show iterations progressively
        animateIterationProgress(iterationHistory.length);
    }

    function generateTablePreview(data) {
        if (!data || data.length === 0) return '<em>No data available</em>';
        
        const headers = Object.keys(data[0]);
        return `
            <table class="table table-sm table-bordered">
                <thead>
                    <tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>
                </thead>
                <tbody>
                    ${data.map(row => `
                        <tr>${headers.map(h => `<td>${String(row[h] || '').substring(0, 50)}${String(row[h] || '').length > 50 ? '...' : ''}</td>`).join('')}</tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    }

    function animateIterationProgress(totalIterations) {
        const items = document.querySelectorAll('.iteration-item');
        items.forEach((item, index) => {
            item.style.opacity = '0';
            item.style.transform = 'translateX(-20px)';
            
            setTimeout(() => {
                item.style.transition = 'all 0.5s ease';
                item.style.opacity = '1';
                item.style.transform = 'translateX(0)';
            }, index * 300);
        });
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

    // UI helpers - removed loading overlay to prevent blocking streaming visualization
    function showLoading(message) {
        console.log('Processing:', message);
    }

    function hideLoading() {
        console.log('Processing complete');
    }

    function showError(message) {
        errorMessage.textContent = message;
        errorMessage.classList.remove('d-none');
        
        setTimeout(() => {
            errorMessage.classList.add('d-none');
        }, 5000);
    }
});