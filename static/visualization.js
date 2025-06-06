class DataVisualization {
    constructor() {
        this.extractedText = '';
        this.tabulatedData = [];
        this.matchedSegments = new Set();
        this.iterationHistory = [];
        this.currentIteration = 0;
    }

    initialize(extractedText, tabulatedData, iterationHistory = []) {
        this.extractedText = extractedText;
        this.tabulatedData = tabulatedData;
        this.iterationHistory = iterationHistory;
        this.currentIteration = iterationHistory.length;
        this.createVisualizationPanel();
        this.updateVisualization();
    }

    createVisualizationPanel() {
        // Remove existing panel if it exists
        const existingPanel = document.getElementById('visualization-panel');
        if (existingPanel) {
            existingPanel.remove();
        }

        // Create main visualization panel
        const panel = document.createElement('div');
        panel.id = 'visualization-panel';
        panel.className = 'visualization-panel';
        panel.innerHTML = `
            <div class="viz-header">
                <h3>🔍 Data Extraction Visualization</h3>
                <div class="iteration-controls">
                    <span>Iteration: ${this.currentIteration}</span>
                    <button id="prev-iteration" class="iteration-btn">‹ Previous</button>
                    <button id="next-iteration" class="iteration-btn">Next ›</button>
                </div>
            </div>
            
            <div class="viz-content">
                <div class="viz-section">
                    <h4>📄 Original Text Analysis</h4>
                    <div id="text-analysis" class="text-container"></div>
                </div>
                
                <div class="viz-section">
                    <h4>📊 Coverage Statistics</h4>
                    <div id="coverage-stats" class="stats-container"></div>
                </div>
                
                <div class="viz-section">
                    <h4>🎯 Data Mapping</h4>
                    <div id="data-mapping" class="mapping-container"></div>
                </div>
                
                <div class="viz-section">
                    <h4>⚠️ Gaps & Improvements</h4>
                    <div id="gaps-analysis" class="gaps-container"></div>
                </div>
            </div>
        `;

        // Add CSS styles
        this.addVisualizationStyles();
        
        // Insert panel after the results section
        const resultsSection = document.querySelector('.results-section');
        if (resultsSection) {
            resultsSection.parentNode.insertBefore(panel, resultsSection.nextSibling);
        } else {
            document.body.appendChild(panel);
        }

        // Add event listeners
        this.addEventListeners();
    }

    addVisualizationStyles() {
        if (document.getElementById('visualization-styles')) return;

        const styles = document.createElement('style');
        styles.id = 'visualization-styles';
        styles.textContent = `
            .visualization-panel {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border-radius: 15px;
                padding: 25px;
                margin: 30px 0;
                box-shadow: 0 15px 35px rgba(0,0,0,0.1);
                color: white;
            }
            
            .viz-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 25px;
                padding-bottom: 15px;
                border-bottom: 2px solid rgba(255,255,255,0.2);
            }
            
            .viz-header h3 {
                margin: 0;
                font-size: 1.5em;
                font-weight: bold;
            }
            
            .iteration-controls {
                display: flex;
                align-items: center;
                gap: 10px;
            }
            
            .iteration-btn {
                background: rgba(255,255,255,0.2);
                border: 1px solid rgba(255,255,255,0.3);
                color: white;
                padding: 8px 15px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 14px;
                transition: all 0.3s ease;
            }
            
            .iteration-btn:hover {
                background: rgba(255,255,255,0.3);
                transform: translateY(-2px);
            }
            
            .iteration-btn:disabled {
                opacity: 0.5;
                cursor: not-allowed;
                transform: none;
            }
            
            .viz-content {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
            }
            
            .viz-section {
                background: rgba(255,255,255,0.1);
                border-radius: 10px;
                padding: 20px;
                backdrop-filter: blur(10px);
            }
            
            .viz-section h4 {
                margin: 0 0 15px 0;
                font-size: 1.1em;
                color: #fff;
            }
            
            .text-container {
                max-height: 300px;
                overflow-y: auto;
                background: rgba(0,0,0,0.2);
                padding: 15px;
                border-radius: 8px;
                font-family: 'Courier New', monospace;
                font-size: 12px;
                line-height: 1.4;
            }
            
            .highlighted-text {
                background: rgba(46, 204, 113, 0.3);
                border-left: 3px solid #2ecc71;
                padding: 2px 4px;
                margin: 1px 0;
                border-radius: 3px;
            }
            
            .unmatched-text {
                background: rgba(231, 76, 60, 0.3);
                border-left: 3px solid #e74c3c;
                padding: 2px 4px;
                margin: 1px 0;
                border-radius: 3px;
            }
            
            .stats-container {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 15px;
            }
            
            .stat-item {
                background: rgba(0,0,0,0.2);
                padding: 15px;
                border-radius: 8px;
                text-align: center;
            }
            
            .stat-value {
                font-size: 2em;
                font-weight: bold;
                color: #2ecc71;
            }
            
            .stat-label {
                font-size: 0.9em;
                opacity: 0.8;
                margin-top: 5px;
            }
            
            .coverage-bar {
                width: 100%;
                height: 10px;
                background: rgba(0,0,0,0.3);
                border-radius: 5px;
                overflow: hidden;
                margin: 10px 0;
            }
            
            .coverage-fill {
                height: 100%;
                background: linear-gradient(90deg, #e74c3c, #f39c12, #2ecc71);
                transition: width 0.5s ease;
                border-radius: 5px;
            }
            
            .mapping-container {
                max-height: 300px;
                overflow-y: auto;
            }
            
            .mapping-item {
                background: rgba(0,0,0,0.2);
                padding: 10px;
                margin: 8px 0;
                border-radius: 6px;
                border-left: 4px solid #3498db;
            }
            
            .mapping-source {
                font-size: 0.85em;
                opacity: 0.8;
                margin-bottom: 5px;
            }
            
            .mapping-target {
                font-weight: bold;
            }
            
            .gaps-container {
                max-height: 300px;
                overflow-y: auto;
            }
            
            .gap-item {
                background: rgba(231, 76, 60, 0.2);
                border: 1px solid rgba(231, 76, 60, 0.4);
                padding: 12px;
                margin: 8px 0;
                border-radius: 6px;
            }
            
            .gap-type {
                font-weight: bold;
                color: #e74c3c;
                font-size: 0.9em;
            }
            
            .gap-description {
                margin-top: 5px;
                font-size: 0.85em;
            }
            
            .iteration-summary {
                background: rgba(52, 152, 219, 0.2);
                border: 1px solid rgba(52, 152, 219, 0.4);
                padding: 15px;
                border-radius: 8px;
                margin-bottom: 20px;
            }
            
            .pulse-animation {
                animation: pulse 2s infinite;
            }
            
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.7; }
                100% { opacity: 1; }
            }
        `;
        
        document.head.appendChild(styles);
    }

    addEventListeners() {
        const prevBtn = document.getElementById('prev-iteration');
        const nextBtn = document.getElementById('next-iteration');
        
        if (prevBtn) {
            prevBtn.addEventListener('click', () => this.showPreviousIteration());
        }
        
        if (nextBtn) {
            nextBtn.addEventListener('click', () => this.showNextIteration());
        }
        
        this.updateIterationButtons();
    }

    updateIterationButtons() {
        const prevBtn = document.getElementById('prev-iteration');
        const nextBtn = document.getElementById('next-iteration');
        
        if (prevBtn) prevBtn.disabled = this.currentIteration <= 1;
        if (nextBtn) nextBtn.disabled = this.currentIteration >= this.iterationHistory.length;
    }

    showPreviousIteration() {
        if (this.currentIteration > 1) {
            this.currentIteration--;
            this.updateVisualization();
            this.updateIterationButtons();
        }
    }

    showNextIteration() {
        if (this.currentIteration < this.iterationHistory.length) {
            this.currentIteration++;
            this.updateVisualization();
            this.updateIterationButtons();
        }
    }

    updateVisualization() {
        this.updateTextAnalysis();
        this.updateCoverageStats();
        this.updateDataMapping();
        this.updateGapsAnalysis();
    }

    updateTextAnalysis() {
        const container = document.getElementById('text-analysis');
        if (!container) return;

        // Analyze text segments and highlight them
        const segments = this.extractTextSegments();
        const highlightedText = this.highlightTextSegments(segments);
        
        container.innerHTML = highlightedText;
    }

    extractTextSegments() {
        // Split text into meaningful segments (sentences, phrases, data points)
        const sentences = this.extractedText.split(/[.!?]+/).filter(s => s.trim().length > 0);
        const dataPoints = this.extractedText.match(/\$[\d,]+\.?\d*|\d+%|\d{4}-\d{2}-\d{2}|\b\d+\.\d+\b/g) || [];
        
        return {
            sentences: sentences.map(s => s.trim()),
            dataPoints: dataPoints
        };
    }

    highlightTextSegments(segments) {
        let highlightedText = this.extractedText;
        
        // Highlight matched data points
        segments.dataPoints.forEach(point => {
            const isMatched = this.isDataPointMatched(point);
            const className = isMatched ? 'highlighted-text' : 'unmatched-text';
            highlightedText = highlightedText.replace(
                new RegExp(this.escapeRegex(point), 'g'),
                `<span class="${className}">${point}</span>`
            );
        });

        return highlightedText;
    }

    isDataPointMatched(dataPoint) {
        // Check if data point appears in tabulated data
        const tableText = JSON.stringify(this.tabulatedData).toLowerCase();
        return tableText.includes(dataPoint.toLowerCase().replace(/[$,]/g, ''));
    }

    updateCoverageStats() {
        const container = document.getElementById('coverage-stats');
        if (!container) return;

        const currentIteration = this.getCurrentIterationData();
        const coverage = currentIteration?.tabulation?.coverage_analysis || {
            total_data_points: 0,
            categorized_points: 0,
            coverage_percentage: 0
        };

        container.innerHTML = `
            <div class="stat-item">
                <div class="stat-value">${coverage.total_data_points}</div>
                <div class="stat-label">Data Points Found</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">${coverage.categorized_points}</div>
                <div class="stat-label">Points Categorized</div>
            </div>
            <div class="coverage-bar">
                <div class="coverage-fill" style="width: ${coverage.coverage_percentage}%"></div>
            </div>
            <div style="text-align: center; margin-top: 10px;">
                <strong>${coverage.coverage_percentage}% Coverage</strong>
            </div>
        `;
    }

    updateDataMapping() {
        const container = document.getElementById('data-mapping');
        if (!container) return;

        const mappings = this.generateDataMappings();
        
        container.innerHTML = mappings.map(mapping => `
            <div class="mapping-item">
                <div class="mapping-source">Source: "${mapping.source}"</div>
                <div class="mapping-target">→ ${mapping.category}: ${mapping.value}</div>
            </div>
        `).join('');
    }

    generateDataMappings() {
        const mappings = [];
        
        this.tabulatedData.forEach(row => {
            const category = row.Category || 'Unknown';
            const values = Object.keys(row)
                .filter(key => key.startsWith('Value'))
                .map(key => row[key])
                .filter(value => value && value.trim());
            
            values.forEach(value => {
                // Try to find this value in the original text
                const sourceMatch = this.findSourceInText(value);
                mappings.push({
                    source: sourceMatch || 'Auto-detected',
                    category: category,
                    value: value
                });
            });
        });
        
        return mappings.slice(0, 10); // Show first 10 mappings
    }

    findSourceInText(value) {
        // Find the context around a value in the original text
        const valueRegex = new RegExp(this.escapeRegex(value), 'i');
        const match = this.extractedText.match(valueRegex);
        
        if (match) {
            const index = this.extractedText.indexOf(match[0]);
            const start = Math.max(0, index - 50);
            const end = Math.min(this.extractedText.length, index + match[0].length + 50);
            return this.extractedText.substring(start, end).trim();
        }
        
        return null;
    }

    updateGapsAnalysis() {
        const container = document.getElementById('gaps-analysis');
        if (!container) return;

        const currentIteration = this.getCurrentIterationData();
        const gaps = this.analyzeGaps(currentIteration);

        container.innerHTML = gaps.map(gap => `
            <div class="gap-item">
                <div class="gap-type">${gap.type}</div>
                <div class="gap-description">${gap.description}</div>
            </div>
        `).join('');
    }

    analyzeGaps(iterationData) {
        const gaps = [];
        
        if (iterationData?.verification) {
            try {
                const verification = typeof iterationData.verification === 'string' 
                    ? JSON.parse(iterationData.verification) 
                    : iterationData.verification;
                
                if (verification.missing_information) {
                    verification.missing_information.forEach(item => {
                        gaps.push({
                            type: 'Missing Data',
                            description: item
                        });
                    });
                }
                
                if (verification.mismatches) {
                    verification.mismatches.forEach(item => {
                        gaps.push({
                            type: 'Data Mismatch',
                            description: item
                        });
                    });
                }
            } catch (e) {
                gaps.push({
                    type: 'Analysis',
                    description: 'Verification data processing needed'
                });
            }
        }
        
        if (gaps.length === 0) {
            gaps.push({
                type: 'Status',
                description: 'No significant gaps identified in current iteration'
            });
        }
        
        return gaps;
    }

    getCurrentIterationData() {
        if (this.iterationHistory.length === 0) return null;
        return this.iterationHistory[this.currentIteration - 1] || this.iterationHistory[this.iterationHistory.length - 1];
    }

    escapeRegex(string) {
        return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    showIterationProgress(iteration, total) {
        // Add visual feedback for iteration progress
        const header = document.querySelector('.viz-header h3');
        if (header) {
            header.innerHTML = `🔍 Data Extraction Visualization <span class="pulse-animation">(Processing ${iteration}/${total})</span>`;
        }
    }

    hideIterationProgress() {
        const header = document.querySelector('.viz-header h3');
        if (header) {
            header.innerHTML = '🔍 Data Extraction Visualization';
        }
    }
}

// Global instance
window.dataVisualization = new DataVisualization();