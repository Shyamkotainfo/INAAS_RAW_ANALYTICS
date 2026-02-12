// Global variables to store chart instances
let chartInstances = {};
let currentChartType = 'bar';
let multipleCharts = [];

// Function to generate chart from JSON data
async function generateChartFromJSON(jsonData, chartType = 'bar') {
    try {
        const response = await fetch('/api/generate-charts-from-json', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                jsonData: jsonData,
                chartType: chartType
            })
        });

        if (!response.ok) {
            throw new Error('Failed to generate chart from JSON');
        }

        const result = await response.json();
        
        if (result.success) {
            // Show the chart section
            const generatedChartSection = document.getElementById('generatedChartSection');
            const chartSelectionSection = document.getElementById('chartSelectionSection');
            
            if (generatedChartSection) generatedChartSection.style.display = 'block';
            if (chartSelectionSection) chartSelectionSection.style.display = 'block';
            
            // Create the chart with the received data
            createChartFromConfig(result.chart);
            
            // Scroll to the chart
            generatedChartSection.scrollIntoView({ behavior: 'smooth' });
        } else {
            throw new Error(result.error || 'Failed to generate chart');
        }
        
    } catch (error) {
        console.error('Error generating chart from JSON:', error);
        alert('Error generating chart from JSON: ' + error.message);
    }
}

// Function to generate new chart from backend
async function generateNewChart(prompt) {
    try {
        const response = await fetch('/api/generate-charts', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ prompt: prompt })
        });

        if (!response.ok) {
            throw new Error('Failed to generate chart');
        }

        const result = await response.json();
        
        if (result.success) {
            // Add new chart to the list
            addChartToList(result.chart, result.chartNumber);
            
            // Scroll to the new chart
            const chartsContainer = document.getElementById('generatedChartsContainer');
            chartsContainer.scrollIntoView({ behavior: 'smooth' });
        } else {
            throw new Error(result.error || 'Failed to generate chart');
        }
        
    } catch (error) {
        console.error('Error generating new chart:', error);
        alert('Error generating chart: ' + error.message);
    }
}

// Function to add chart to the list
function addChartToList(chartConfig, chartNumber) {
    const chartsList = document.getElementById('chartsList');
    
    // Create chart container
    const chartSection = document.createElement('div');
    chartSection.className = 'generated-chart-section';
    chartSection.id = `chart-${chartNumber}`;
    
    // Create chart title
    const chartTitle = document.createElement('h3');
    chartTitle.textContent = chartConfig.title;
    chartTitle.style.color = '#2c5aa0';
    chartTitle.style.marginBottom = '15px';
    chartTitle.style.fontSize = '16px';
    
    // Create chart selection section for this chart
    const chartSelectionSection = document.createElement('div');
    chartSelectionSection.className = 'chart-selection-section';
    chartSelectionSection.id = `chartSelectionSection-${chartNumber}`;
    chartSelectionSection.style.display = 'block';
    chartSelectionSection.style.position = 'static';
    chartSelectionSection.style.marginBottom = '10px';
    chartSelectionSection.style.maxWidth = '250px';
    
    chartSelectionSection.innerHTML = `
        <h3>Select Chart Type:</h3>
        <div class="chart-selection-controls">
            <select class="chart-type-select" onchange="changeChartTypeForChart(${chartNumber}, this.value)">
                <option value="bar">Bar Chart</option>
                <option value="line">Line Chart</option>
                <option value="pie">Pie Chart</option>
                <option value="doughnut">Doughnut Chart</option>
                <option value="radar">Radar Chart</option>
                <option value="polarArea">Polar Area Chart</option>
                <option value="bubble">Bubble Chart</option>
                <option value="scatter">Scatter Chart</option>
            </select>
        </div>
    `;
    
    // Create three-column container
    const threeColumnContainer = document.createElement('div');
    threeColumnContainer.className = 'three-column-container';
    
    // Create summary box (left side)
    const summaryBox = document.createElement('div');
    summaryBox.className = 'summary-box';
    summaryBox.id = `summary-${chartNumber}`;
    
    // Get summary based on chart number
    const summary = getSummaryForChart(chartNumber);
    summaryBox.innerHTML = `
        <h4>Summary ${chartNumber}</h4>
        <div class="summary-content">
            ${summary}
        </div>
    `;
    
    // Create chart container (middle)
    const chartContainer = document.createElement('div');
    chartContainer.className = 'chart-container-middle';
    
    // Create canvas
    const canvas = document.createElement('canvas');
    canvas.id = `chartCanvas-${chartNumber}`;
    canvas.style.width = '100%';
    canvas.style.height = '250px';
    canvas.style.maxHeight = '250px';
    
    chartContainer.appendChild(canvas);
    
    // Create action buttons container (right side - red box)
    const actionButtonsContainer = document.createElement('div');
    actionButtonsContainer.className = 'action-buttons-container';
    actionButtonsContainer.id = `actionButtons-${chartNumber}`;
    
    actionButtonsContainer.innerHTML = `
        <div class="action-button-panel">
            <div class="action-button-item">
                <button class="chart-type-btn" onclick="toggleChartTypeDropdown(${chartNumber})" title="Select Chart Type">
                    📈
                </button>
                <div class="chart-type-dropdown" id="chartTypeDropdown-${chartNumber}" style="display: none;">
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'bar')">Bar Chart</div>
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'line')">Line Chart</div>
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'pie')">Pie Chart</div>
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'doughnut')">Doughnut Chart</div>
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'radar')">Radar Chart</div>
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'polarArea')">Polar Area Chart</div>
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'bubble')">Bubble Chart</div>
                    <div class="chart-type-option" onclick="changeChartTypeForChart(${chartNumber}, 'scatter')">Scatter Chart</div>
                </div>
            </div>
            
            <button class="action-icon-btn view-data-btn" onclick="showFlyingDataPanel(${chartNumber}, ${JSON.stringify(chartConfig).replace(/"/g, '&quot;')})" title="View Data Table">
                📊
            </button>
            
            <button class="action-icon-btn refresh-btn" onclick="refreshChart(${chartNumber})" title="Refresh Chart">
                🔄
            </button>
            
            <button class="action-icon-btn download-pdf-btn" onclick="downloadChartAsPDF('chart-${chartNumber}', '${chartConfig.title}')" title="Download PDF">
                📄
            </button>
        </div>
    `;
    
    // Create chart selection checkbox for PDF download
    const chartCheckbox = document.createElement('div');
    chartCheckbox.className = 'chart-checkbox-container';
    chartCheckbox.innerHTML = `
        <input type="checkbox" id="chart-checkbox-${chartNumber}" class="chart-checkbox" checked>
        <label for="chart-checkbox-${chartNumber}" class="chart-checkbox-label">Chart ${chartNumber}</label>
    `;
    
    // Append elements to three-column container
    threeColumnContainer.appendChild(summaryBox);
    threeColumnContainer.appendChild(chartContainer);
    threeColumnContainer.appendChild(actionButtonsContainer);
    
    // Add checkbox to the left sidebar
    addChartCheckboxToSidebar(chartNumber, chartConfig.title);
    
    chartSection.appendChild(chartTitle); // Title remains at the top of the overall section
    chartSection.appendChild(threeColumnContainer);
    
    // Add dividing line if not the first chart
    if (chartNumber > 1) {
        const divider = document.createElement('div');
        divider.className = 'chart-divider';
        chartsList.appendChild(divider);
    }
    
    // Add chart to list
    chartsList.appendChild(chartSection);
    
    // Create the chart
    createChartFromConfig(chartConfig, chartNumber);
}

// Function to get summary for each chart
function getSummaryForChart(chartNumber) {
    const summaries = {
        1: `
            <p><strong>Highest spender:</strong> Diana ($300)</p>
            <p><strong>Lowest spender:</strong> Eve ($120)</p>
            <p><strong>Total spending:</strong> $1,035</p>
            <p><strong>Average spending per customer:</strong> $207</p>
        `,
        2: `
            <p><strong>Highest spender:</strong> John ($400)</p>
            <p><strong>Lowest spender:</strong> Tom ($100)</p>
            <p><strong>Total spending:</strong> $1,280</p>
            <p><strong>Average spending:</strong> $256</p>
        `,
        3: `
            <p><strong>Highest spender:</strong> Emma ($350)</p>
            <p><strong>Lowest spender:</strong> Sophia ($130)</p>
            <p><strong>Total spending:</strong> $1,250</p>
            <p><strong>Average spending:</strong> $250</p>
        `,
        4: `
            <p><strong>Highest spender:</strong> Raj ($500)</p>
            <p><strong>Lowest spender:</strong> Neha ($190)</p>
            <p><strong>Total spending:</strong> $1,600</p>
            <p><strong>Average spending:</strong> $320</p>
        `,
        5: `
            <p><strong>Highest spender:</strong> Liam ($600)</p>
            <p><strong>Lowest spender:</strong> James ($220)</p>
            <p><strong>Total spending:</strong> $1,910</p>
            <p><strong>Average spending:</strong> $382</p>
        `
    };
    
    return summaries[chartNumber] || summaries[1]; // Default to summary 1 if not found
}

// Function to show chart loading animation
function showChartLoading() {
    const chartsContainer = document.getElementById('generatedChartsContainer');
    const loadingDiv = document.createElement('div');
    loadingDiv.id = 'chartLoading';
    loadingDiv.className = 'chart-loading';
    loadingDiv.innerHTML = `
        <div class="loading-spinner-chart"></div>
        <p>Generating beautiful chart...</p>
    `;
    chartsContainer.appendChild(loadingDiv);
}

// Function to hide chart loading animation
function hideChartLoading() {
    const loadingDiv = document.getElementById('chartLoading');
    if (loadingDiv) {
        loadingDiv.remove();
    }
}

// Function to toggle data table view
function toggleDataTable(chartNumber, chartConfig) {
    const chartContainer = document.getElementById(`chart-${chartNumber}`);
    const existingTable = chartContainer.querySelector('.data-table');
    const viewDataBtn = chartContainer.querySelector('.view-data-btn');
    
    if (existingTable) {
        // Remove existing table
        existingTable.remove();
        viewDataBtn.textContent = 'View Data Table';
        viewDataBtn.classList.remove('active');
    } else {
        // Create and show data table
        const dataTable = createDataTable(chartConfig);
        dataTable.className = 'data-table';
        
        // Insert table after the chart canvas
        const canvas = document.getElementById(`chartCanvas-${chartNumber}`);
        canvas.parentNode.insertBefore(dataTable, canvas.nextSibling);
        
        viewDataBtn.textContent = 'Hide Data Table';
        viewDataBtn.classList.add('active');
    }
}

// Function to create data table
function createDataTable(chartConfig) {
    const table = document.createElement('div');
    table.className = 'data-table-container';
    
    // Extract data from chart config
    const labels = chartConfig.data.labels || [];
    const datasets = chartConfig.data.datasets || [];
    
    if (datasets.length === 0) {
        table.innerHTML = '<p>No data available</p>';
        return table;
    }
    
    const data = datasets[0].data || [];
    
    let tableHTML = `
        <div class="table-header">
            <h4>Data Table</h4>
        </div>
        <table class="data-table-content">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Spending ($)</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    // Add data rows
    for (let i = 0; i < labels.length; i++) {
        const name = labels[i];
        const value = data[i];
        tableHTML += `
            <tr>
                <td>${name}</td>
                <td>$${value}</td>
            </tr>
        `;
    }
    
    tableHTML += `
            </tbody>
        </table>
    `;
    
    table.innerHTML = tableHTML;
    return table;
}

// Function to create chart from configuration
function createChartFromConfig(chartConfig, chartNumber = 'mainChart') {
    const canvasId = chartNumber === 'mainChart' ? 'mainChartCanvas' : `chartCanvas-${chartNumber}`;
    const canvas = document.getElementById(canvasId);
    
    if (!canvas) {
        console.error('Canvas element not found:', canvasId);
        return;
    }
    
    // Destroy existing chart if it exists
    if (chartInstances[chartNumber]) {
        chartInstances[chartNumber].destroy();
    }
    
    // Create new chart
    const ctx = canvas.getContext('2d');
    const chartType = chartConfig.chart_type.toLowerCase();
    
    const chartOptions = {
        type: chartType,
        data: chartConfig.data,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
                duration: 2000,
                easing: 'easeInOutQuart',
                onProgress: function(animation) {
                    const chart = animation.chart;
                    const ctx = chart.ctx;
                    const dataset = chart.data.datasets[0];
                    const meta = chart.getDatasetMeta(0);
                    
                    if (meta.data) {
                        meta.data.forEach((bar, index) => {
                            const data = dataset.data[index];
                            const value = data;
                            const maxValue = Math.max(...dataset.data);
                            const percentage = value / maxValue;
                            
                            // Add glow effect
                            ctx.shadowColor = chartConfig.data.datasets[0].backgroundColor[index];
                            ctx.shadowBlur = 10;
                            ctx.shadowOffsetX = 0;
                            ctx.shadowOffsetY = 0;
                        });
                    }
                },
                onComplete: function(animation) {
                    const chart = animation.chart;
                    const ctx = chart.ctx;
                    ctx.shadowBlur = 0;
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'bottom',
                    labels: {
                        boxWidth: 12,
                        padding: 8,
                        font: {
                            size: 11
                        },
                        usePointStyle: true,
                        pointStyle: 'circle'
                    }
                },
                title: {
                    display: true,
                    text: chartConfig.title,
                    font: {
                        size: 14,
                        weight: 'bold'
                    },
                    color: '#2c5aa0'
                },
                tooltip: {
                    backgroundColor: 'rgba(44, 90, 160, 0.9)',
                    titleColor: '#ffffff',
                    bodyColor: '#ffffff',
                    borderColor: '#2c5aa0',
                    borderWidth: 1,
                    cornerRadius: 8,
                    displayColors: true,
                    animation: {
                        duration: 300
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: chartConfig.ylabel,
                        font: {
                            size: 11,
                            weight: 'bold'
                        },
                        color: '#2c5aa0'
                    },
                    ticks: {
                        font: {
                            size: 10
                        },
                        color: '#666'
                    },
                    grid: {
                        color: 'rgba(44, 90, 160, 0.1)',
                        lineWidth: 1
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: chartConfig.xlabel,
                        font: {
                            size: 11,
                            weight: 'bold'
                        },
                        color: '#2c5aa0'
                    },
                    ticks: {
                        font: {
                            size: 10
                        },
                        color: '#666'
                    },
                    grid: {
                        color: 'rgba(44, 90, 160, 0.1)',
                        lineWidth: 1
                    }
                }
            },
            elements: {
                bar: {
                    borderRadius: 6,
                    borderSkipped: false
                },
                point: {
                    radius: 6,
                    hoverRadius: 8,
                    borderWidth: 2
                },
                line: {
                    tension: 0.4,
                    borderWidth: 3
                }
            },
            interaction: {
                intersect: false,
                mode: 'index'
            }
        }
    };
    
    chartInstances[chartNumber] = new Chart(ctx, chartOptions);
}

// Hardcoded JSON data for guaranteed output
const hardcodedChartData = {
  "data": [
    {
      "name": "Diana",
      "email": "diana@domain.com",
      "total_spending": 300.0
    },
    {
      "name": "Charlie",
      "email": "charlie@x.com",
      "total_spending": 260.0
    },
    {
      "name": "Alice",
      "email": "alice@example.com",
      "total_spending": 205.0
    },
    {
      "name": "Bob",
      "email": "bob@example.com",
      "total_spending": 150.0
    }
  ],
  "title": "List all users with their total spending",
  "xlabel": "name",
  "ylabel": "email",
  "label": "email Data",
  "explanation": "This dataset supports multiple chart types including bar, line, and pie for numeric comparisons, and table for general reference."
};

// Speech recognition setup
let recognition = null;
let isListening = false;

// Initialize speech recognition
function initSpeechRecognition() {
    try {
        if (typeof webkitSpeechRecognition !== 'undefined') {
            recognition = new webkitSpeechRecognition();
        } else if (typeof SpeechRecognition !== 'undefined') {
            recognition = new SpeechRecognition();
        } else {
            console.log('Speech recognition not supported');
            hideSpeechButtons();
            return;
        }

        recognition.continuous = false;
        recognition.interimResults = false;
        recognition.lang = 'en-US';
        recognition.maxAlternatives = 1;

        recognition.onstart = function() {
            isListening = true;
            updateSpeechUI(true);
        };

        recognition.onresult = function(event) {
            if (event.results && event.results.length > 0) {
                const transcript = event.results[0][0].transcript;
                const promptInput = document.getElementById('promptInput');
                if (promptInput) {
                    promptInput.value = transcript;
                    promptInput.dispatchEvent(new Event('input', { bubbles: true }));
                }
            }
        };

        recognition.onend = function() {
            isListening = false;
            updateSpeechUI(false);
        };

        recognition.onerror = function(event) {
            console.error('Speech recognition error:', event.error);
            isListening = false;
            updateSpeechUI(false);
            alert('Speech recognition error. Please try again.');
        };

    } catch (error) {
        console.error('Error initializing speech recognition:', error);
        hideSpeechButtons();
    }
}

function hideSpeechButtons() {
    const speechBtn = document.getElementById('speechBtn');
    if (speechBtn) speechBtn.style.display = 'none';
}

function updateSpeechUI(listening) {
    const speechBtn = document.getElementById('speechBtn');
    const speechStatus = document.getElementById('speechStatus');
    
    if (speechBtn) {
        if (listening) {
            speechBtn.classList.add('listening');
            speechBtn.title = 'Stop listening';
        } else {
            speechBtn.classList.remove('listening');
            speechBtn.title = 'Speech to Text';
        }
    }
    
    if (speechStatus) {
        speechStatus.style.display = listening ? 'flex' : 'none';
    }
}

function startSpeechRecognition() {
    if (!recognition) {
        alert('Speech recognition is not available.');
        return;
    }

    if (isListening) {
        recognition.stop();
        return;
    }

    try {
            recognition.start();
    } catch (error) {
        console.error('Error starting speech recognition:', error);
        alert('Error starting speech recognition. Please try again.');
    }
}

// Chart configuration options
const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            position: 'bottom',
            labels: {
                padding: 20,
                usePointStyle: true,
                font: { size: 12 }
            }
        }
    },
    scales: {
        y: {
            beginAtZero: true,
            grid: { color: '#e0e0e0' }
        },
        x: {
            grid: { color: '#e0e0e0' }
            }
    },
    animation: { duration: 0 }
};

// Color palette for charts
const colorPalette = [
    '#2c5aa0', '#ff9800', '#4CAF50', '#e91e63',
    '#9c27b0', '#00bcd4', '#ff5722', '#795548'
];

// Function to create a single chart
function createSingleChart(chartType = 'bar') {
    try {
        const container = document.getElementById('mainChartContainer');
        const canvas = document.getElementById('mainChartCanvas');
        const title = document.getElementById('chartTitle');
        
        if (!container || !canvas) {
            console.error('Chart container or canvas not found');
            return;
        }
        
        // Update title
        title.textContent = `${chartType.charAt(0).toUpperCase() + chartType.slice(1)} Chart - ${hardcodedChartData.title}`;
        
        // Prepare chart data
        const labels = hardcodedChartData.data.map(item => item.name);
        const values = hardcodedChartData.data.map(item => item.total_spending);
        
        // Destroy existing chart if it exists
        if (chartInstances['mainChart']) {
            chartInstances['mainChart'].destroy();
        }
        
        // Create chart configuration
        const chartConfig = {
            type: chartType,
            data: {
                labels: labels,
                datasets: [{
                    label: hardcodedChartData.label,
                    data: values,
                    backgroundColor: colorPalette.slice(0, labels.length),
                    borderColor: colorPalette.slice(0, labels.length),
                    borderWidth: 2,
                    fill: false
                }]
            },
            options: {
                ...chartOptions,
                plugins: {
                    ...chartOptions.plugins,
                    title: {
                        display: true,
                        text: title.textContent,
                        font: { size: 16, weight: 'bold' }
                    }
                }
            }
        };
        
        // Special configurations for different chart types
        if (chartType === 'pie' || chartType === 'doughnut') {
            chartConfig.options.scales = {};
            chartConfig.data.datasets[0].borderColor = '#ffffff';
            chartConfig.data.datasets[0].borderWidth = 2;
        }
        
        if (chartType === 'radar' || chartType === 'polarArea') {
            chartConfig.options.scales = {};
        }
        
        // Create new chart
        const ctx = canvas.getContext('2d');
        chartInstances['mainChart'] = new Chart(ctx, chartConfig);
        
        console.log(`Single chart created successfully: ${chartType}`);
        currentChartType = chartType;
        
    } catch (error) {
        console.error('Error creating single chart:', error);
    }
}

// Function to create multiple charts
function createMultipleCharts(selectedTypes) {
    try {
        const chartsGrid = document.getElementById('chartsGrid');
        chartsGrid.innerHTML = '';
        multipleCharts = [];
        
        selectedTypes.forEach((chartType, index) => {
            const chartItem = document.createElement('div');
            chartItem.className = 'chart-item';
            chartItem.id = `chart-${index}`;
            
            const chartTitle = document.createElement('h4');
            chartTitle.textContent = `${chartType.charAt(0).toUpperCase() + chartType.slice(1)} Chart`;
            
            const canvas = document.createElement('canvas');
            canvas.id = `canvas-${index}`;
            canvas.style.maxHeight = '200px';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = true;
            checkbox.onchange = () => toggleChartSelection(chartItem);
            
            chartItem.appendChild(checkbox);
            chartItem.appendChild(chartTitle);
            chartItem.appendChild(canvas);
            chartsGrid.appendChild(chartItem);
            
            // Create chart
            const labels = hardcodedChartData.data.map(item => item.name);
            const values = hardcodedChartData.data.map(item => item.total_spending);
            
            const chartConfig = {
                type: chartType,
                data: {
                    labels: labels,
                    datasets: [{
                        label: hardcodedChartData.label,
                        data: values,
                        backgroundColor: colorPalette.slice(0, labels.length),
                        borderColor: colorPalette.slice(0, labels.length),
                        borderWidth: 2,
                        fill: false
                    }]
                },
                options: {
                    ...chartOptions,
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 10,
                                usePointStyle: true,
                                font: { size: 10 }
                            }
                        }
                    }
                }
            };
            
            if (chartType === 'pie' || chartType === 'doughnut') {
                chartConfig.options.scales = {};
            chartConfig.data.datasets[0].borderColor = '#ffffff';
            chartConfig.data.datasets[0].borderWidth = 2;
        }
        
            if (chartType === 'radar' || chartType === 'polarArea') {
                chartConfig.options.scales = {};
            }
            
            const ctx = canvas.getContext('2d');
            chartInstances[`multiple-${index}`] = new Chart(ctx, chartConfig);
            multipleCharts.push({ type: chartType, id: `multiple-${index}`, element: chartItem });
        });
        
        console.log(`Multiple charts created: ${selectedTypes.length} charts`);
        
    } catch (error) {
        console.error('Error creating multiple charts:', error);
    }
}

// Function to toggle chart selection
function toggleChartSelection(chartItem) {
    chartItem.classList.toggle('selected');
}

// Function to get selected charts for PDF download
function getSelectedCharts() {
    const selectedCharts = [];
    multipleCharts.forEach(chart => {
        const checkbox = chart.element.querySelector('input[type="checkbox"]');
        if (checkbox && checkbox.checked) {
            selectedCharts.push(chart);
        }
    });
    return selectedCharts;
}

// Function to generate single chart
async function generateSingleChart() {
    const prompt = document.getElementById('promptInput').value.trim();
    
    if (!prompt) {
        alert('Please enter a prompt to generate chart.');
        return;
    }
    
    const generateBtn = document.getElementById('generateChartsBtn');
    generateBtn.disabled = true;
    generateBtn.textContent = 'Generating...';
    
    try {
        // Show charts container
        document.getElementById('generatedChartsContainer').style.display = 'block';
        
        // Make prompt box compact
        const promptSection = document.querySelector('.prompt-section');
        promptSection.classList.add('compact');
        
        // Show loading animation
        showChartLoading();
        
        // Generate new chart from backend
        await generateNewChart(prompt);
        
        // Hide loading animation
        hideChartLoading();
        
    } catch (error) {
        console.error('Error generating chart:', error);
        alert('Error generating chart. Please try again.');
    } finally {
        generateBtn.disabled = false;
        generateBtn.textContent = 'Generate Chart';
    }
}

// Function to toggle chart type dropdown
function toggleChartTypeDropdown(chartNumber) {
    console.log('Toggle dropdown for chart:', chartNumber); // Debug log
    
    const dropdown = document.getElementById(`chartTypeDropdown-${chartNumber}`);
    
    console.log('Dropdown element:', dropdown); // Debug log
    
    if (dropdown) {
        const currentDisplay = dropdown.style.display;
        console.log('Current display:', currentDisplay); // Debug log
        
        // Close all dropdowns first
        const allDropdowns = document.querySelectorAll('.chart-type-dropdown');
        const allButtons = document.querySelectorAll('.chart-type-btn');
        
        allDropdowns.forEach(d => {
            d.style.display = 'none';
            d.classList.remove('show');
        });
        allButtons.forEach(b => {
            b.classList.remove('active');
        });
        
        // Toggle current dropdown
        if (currentDisplay === 'none' || currentDisplay === '') {
            dropdown.style.display = 'block';
            dropdown.classList.add('show');
            console.log('Showing dropdown'); // Debug log
            
            // Add active state to button
            const button = dropdown.previousElementSibling;
            if (button) {
                button.classList.add('active');
            }
        }
    } else {
        console.log('Dropdown not found!'); // Debug log
    }
}

// Function to show flying data panel
function showFlyingDataPanel(chartNumber, chartConfig) {
    // Remove existing flying panel if any
    const existingPanel = document.querySelector('.flying-data-panel');
    if (existingPanel) {
        existingPanel.remove();
    }
    
    // Create flying data panel
    const flyingPanel = document.createElement('div');
    flyingPanel.className = 'flying-data-panel';
    flyingPanel.id = `flyingDataPanel-${chartNumber}`;
    
    // Extract data from chart config
    const labels = chartConfig.data.labels || [];
    const datasets = chartConfig.data.datasets || [];
    const data = datasets.length > 0 ? datasets[0].data || [] : [];
    
    let panelHTML = `
        <button class="close-btn" onclick="closeFlyingDataPanel(${chartNumber})">×</button>
        <h3>Chart Data - ${chartConfig.title}</h3>
        <div class="data-content">
    `;
    
    if (data.length > 0) {
        panelHTML += '<table style="width: 100%; border-collapse: collapse; margin-top: 10px;">';
        panelHTML += '<thead><tr style="background: #f8f9fa;"><th style="padding: 8px; text-align: left; border-bottom: 2px solid #dee2e6;">Name</th><th style="padding: 8px; text-align: left; border-bottom: 2px solid #dee2e6;">Value</th></tr></thead>';
        panelHTML += '<tbody>';
        
        for (let i = 0; i < labels.length && i < data.length; i++) {
            panelHTML += `<tr style="border-bottom: 1px solid #dee2e6;">`;
            panelHTML += `<td style="padding: 8px;">${labels[i]}</td>`;
            panelHTML += `<td style="padding: 8px; font-weight: bold;">$${data[i]}</td>`;
            panelHTML += `</tr>`;
        }
        
        panelHTML += '</tbody></table>';
    } else {
        panelHTML += '<p style="text-align: center; color: #666;">No data available</p>';
    }
    
    panelHTML += '</div>';
    flyingPanel.innerHTML = panelHTML;
    
    // Add to body and show
    document.body.appendChild(flyingPanel);
    setTimeout(() => {
        flyingPanel.classList.add('show');
    }, 10);
}

// Function to close flying data panel
function closeFlyingDataPanel(chartNumber) {
    const panel = document.getElementById(`flyingDataPanel-${chartNumber}`);
    if (panel) {
        panel.classList.remove('show');
        setTimeout(() => {
            panel.remove();
        }, 300);
    }
}

// Function to refresh chart
function refreshChart(chartNumber) {
    const chartInstance = chartInstances[chartNumber];
    if (chartInstance) {
        // Store the current configuration
        const config = chartInstance.config;
        
        // Update the chart with animation
        chartInstance.update('active');
        
        // Add a subtle animation effect
        const canvas = document.getElementById(`chart-${chartNumber}`);
        if (canvas) {
            canvas.style.opacity = '0.7';
            setTimeout(() => {
                canvas.style.opacity = '1';
            }, 300);
        }
        
        console.log(`Chart ${chartNumber} refreshed successfully`);
    }
}



// Function to add chart checkbox to left sidebar
function addChartCheckboxToSidebar(chartNumber, chartTitle) {
    // Create or get the left sidebar
    let leftSidebar = document.getElementById('leftSidebar');
    if (!leftSidebar) {
        leftSidebar = document.createElement('div');
        leftSidebar.id = 'leftSidebar';
        leftSidebar.className = 'left-sidebar';
        
        // Add header
        const header = document.createElement('div');
        header.className = 'left-sidebar-header';
        header.innerHTML = `
            <h3>Select Charts for PDF</h3>
            <button class="select-all-btn" onclick="toggleSelectAll()" title="Select or unselect all charts">Select All</button>
        `;
        leftSidebar.appendChild(header);
        
        // Add download button
        const downloadBtn = document.createElement('button');
        downloadBtn.className = 'download-selected-btn';
        downloadBtn.textContent = 'Download SELECTED PDF';
        downloadBtn.title = 'Download selected charts as PDF';
        downloadBtn.onclick = downloadSelectedChartsPDF;
        leftSidebar.appendChild(downloadBtn);
        
        // Add charts list container
        const chartsList = document.createElement('div');
        chartsList.className = 'charts-list-container';
        chartsList.id = 'chartsListContainer';
        leftSidebar.appendChild(chartsList);
        
        // Insert sidebar at the beginning of the body
        document.body.insertBefore(leftSidebar, document.body.firstChild);
    }
    
    // Add checkbox for this chart
    const chartsListContainer = document.getElementById('chartsListContainer');
    const chartItem = document.createElement('div');
    chartItem.className = 'chart-item-checkbox';
    chartItem.id = `chart-item-${chartNumber}`;
    chartItem.innerHTML = `
        <input type="checkbox" id="sidebar-chart-checkbox-${chartNumber}" class="sidebar-chart-checkbox" checked>
        <label for="sidebar-chart-checkbox-${chartNumber}" class="sidebar-chart-checkbox-label">
            Chart ${chartNumber}
        </label>
    `;
    chartsListContainer.appendChild(chartItem);
}

// Function to toggle select all
function toggleSelectAll() {
    const checkboxes = document.querySelectorAll('.sidebar-chart-checkbox');
    const selectAllBtn = document.querySelector('.select-all-btn');
    const allChecked = Array.from(checkboxes).every(cb => cb.checked);
    
    checkboxes.forEach(checkbox => {
        checkbox.checked = !allChecked;
    });
    
    selectAllBtn.textContent = allChecked ? 'Select All' : 'Unselect All';
}

// Function to download selected charts as PDF
async function downloadSelectedChartsPDF() {
    const selectedCheckboxes = document.querySelectorAll('.sidebar-chart-checkbox:checked');
    
    if (selectedCheckboxes.length === 0) {
        alert('Please select at least one chart to download.');
        return;
    }
    
    try {
        if (typeof jsPDF === 'undefined') {
            const script = document.createElement('script');
            script.src = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
            script.onload = () => generateSelectedChartsPDF(selectedCheckboxes);
            document.head.appendChild(script);
        } else {
            generateSelectedChartsPDF(selectedCheckboxes);
        }
    } catch (error) {
        console.error('Error downloading selected charts PDF:', error);
        alert('Error generating PDF. Please try again.');
    }
}

// Function to generate PDF for selected charts
function generateSelectedChartsPDF(selectedCheckboxes) {
    try {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();
        
        // Add title
        doc.setFontSize(20);
        doc.text('IAAS - BI BOT Selected Charts Report', 20, 20);
        
        // Add timestamp
        doc.setFontSize(12);
        doc.text(`Generated on: ${new Date().toLocaleString()}`, 20, 30);
        
        // Add prompt
        const prompt = document.getElementById('promptInput').value;
        if (prompt) {
            doc.setFontSize(14);
            doc.text('Prompt:', 20, 45);
            doc.setFontSize(12);
            doc.text(prompt, 20, 55);
        }
        
        let currentPage = 1;
        let yPosition = 70;
        
        selectedCheckboxes.forEach((checkbox, index) => {
            const chartNumber = checkbox.id.replace('sidebar-chart-checkbox-', '');
            const chartInstance = chartInstances[chartNumber];
            
            if (chartInstance) {
                // Add new page if not first chart
                if (index > 0) {
                    doc.addPage();
                    currentPage++;
                    yPosition = 30;
                }
                
                // Add chart title
                doc.setFontSize(16);
                doc.text(`Chart ${chartNumber}`, 20, yPosition);
                yPosition += 15;
                
                try {
                    // Capture chart image
                    const chartImage = chartInstance.toBase64Image();
                    const imgWidth = 170;
                    const imgHeight = 120;
                    
                    doc.addImage(chartImage, 'PNG', 20, yPosition, imgWidth, imgHeight);
                    yPosition += imgHeight + 10;
                    
                    // Add summary if available
                    const summaryBox = document.getElementById(`summary-${chartNumber}`);
                    if (summaryBox) {
        doc.setFontSize(14);
                        doc.text('Summary:', 20, yPosition);
                        yPosition += 8;
                        
                        const summaryText = summaryBox.querySelector('.summary-content').textContent;
                        const lines = doc.splitTextToSize(summaryText, 170);
        doc.setFontSize(10);
                        doc.text(lines, 20, yPosition);
                    }
                    
                } catch (chartError) {
                    console.error(`Error capturing chart ${chartNumber}:`, chartError);
                    doc.setFontSize(12);
                    doc.text(`Error capturing Chart ${chartNumber}`, 20, yPosition);
                }
            }
        });
        
        // Save the PDF
        doc.save('iaas-bi-bot-selected-charts.pdf');
        
        console.log(`PDF downloaded successfully with ${selectedCheckboxes.length} charts`);
    } catch (error) {
        console.error('Error generating PDF:', error);
        alert('Error generating PDF. Please try again.');
    }
}

// Function to change chart type
// Function to change chart type for any specific chart
function changeChartTypeForChart(chartNumber, chartType = null) {
    if (!chartType) {
        // Get the selected value from the dropdown for this specific chart
        const selectElement = document.querySelector(`#chartSelectionSection-${chartNumber} .chart-type-select`);
        chartType = selectElement ? selectElement.value : 'bar';
    }
    
    const canvas = document.getElementById(`chartCanvas-${chartNumber}`);
    
    if (canvas && chartInstances[chartNumber]) {
        // Update the chart type
        chartInstances[chartNumber].config.type = chartType;
        chartInstances[chartNumber].update();
        
        // Close the dropdown after selection
        const dropdown = document.getElementById(`chartTypeDropdown-${chartNumber}`);
        
        if (dropdown) {
            dropdown.style.display = 'none';
            dropdown.classList.remove('show');
        }
        
        // Remove active state from button
        const button = dropdown ? dropdown.previousElementSibling : null;
        if (button) {
            button.classList.remove('active');
        }
    }
}

function changeChartType() {
    const chartType = document.getElementById('chartTypeSelect').value;
    // Get the latest chart and change its type
    const chartsList = document.getElementById('chartsList');
    const lastChart = chartsList.lastElementChild;
    
    if (lastChart && lastChart.classList.contains('generated-chart-section')) {
        const chartNumber = lastChart.id.replace('chart-', '');
        changeChartTypeForChart(chartNumber, chartType);
    }
}

// Function to generate multiple charts
function generateMultipleCharts() {
    const checkboxes = document.querySelectorAll('.chart-checkboxes input[type="checkbox"]:checked');
    const selectedTypes = Array.from(checkboxes).map(cb => cb.value);
    
    if (selectedTypes.length === 0) {
        alert('Please select at least one chart type.');
        return;
    }
    
    createMultipleCharts(selectedTypes);
    document.getElementById('multipleChartsDisplay').style.display = 'block';
    document.getElementById('multipleChartsDisplay').scrollIntoView({ behavior: 'smooth' });
}

// Function to download single chart as PDF
async function downloadChartAsPDF(chartId, chartTitle) {
    try {
        if (typeof jsPDF === 'undefined') {
            const script = document.createElement('script');
            script.src = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
            script.onload = () => downloadSingleChartPDF(chartId, chartTitle);
            document.head.appendChild(script);
        } else {
            downloadSingleChartPDF(chartId, chartTitle);
        }
    } catch (error) {
        console.error('Error downloading chart PDF:', error);
        alert('Error generating PDF. Please try again.');
    }
}

function downloadSingleChartPDF(chartId, chartTitle) {
    try {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();
        
        // PAGE 1: CHART
        doc.setFontSize(20);
        doc.text('IAAS - BI BOT Chart Report', 20, 20);
        
        doc.setFontSize(12);
        doc.text(`Generated on: ${new Date().toLocaleString()}`, 20, 30);
        
        doc.setFontSize(16);
        doc.text(chartTitle, 20, 45);
        
        // Add prompt
        const prompt = document.getElementById('promptInput').value;
        if (prompt) {
            doc.setFontSize(14);
            doc.text('Prompt:', 20, 60);
            doc.setFontSize(12);
            doc.text(prompt, 20, 70);
        }
        
        // Capture chart image for Page 1
        const chartCanvas = document.querySelector(`#${chartId} canvas`);
        if (chartCanvas) {
            try {
                const chartImage = chartCanvas.toDataURL('image/png');
                doc.addImage(chartImage, 'PNG', 15, 85, 180, 120);
            } catch (chartError) {
                console.error('Error capturing chart image:', chartError);
            }
        }
        
        // PAGE 2: SUMMARY
        doc.addPage();
        doc.setFontSize(20);
        doc.text('Summary', 20, 20);
        
        // Get summary content
        const summaryBox = document.querySelector(`#${chartId.replace('chart-', 'summary-')}`);
        if (summaryBox) {
            const summaryContent = summaryBox.querySelector('.summary-content');
            if (summaryContent) {
                doc.setFontSize(12);
                const summaryText = summaryContent.textContent;
                const lines = doc.splitTextToSize(summaryText, 170);
                doc.text(lines, 20, 40);
            }
        }
        
        // PAGE 3: DATA TABLE
        doc.addPage();
        doc.setFontSize(20);
        doc.text('Data Table', 20, 20);
        
        // Create data table
        doc.setFontSize(12);
        let yPosition = 40;
        
        // Table header
        doc.setFontSize(14);
        doc.text('Name', 20, yPosition);
        doc.text('Email', 80, yPosition);
        doc.text('Total Spending', 140, yPosition);
        yPosition += 20;
        
        // Table data
        doc.setFontSize(12);
        const chartNumber = chartId.replace('chart-', '');
        const chartInstance = chartInstances[chartNumber];
        if (chartInstance && chartInstance.config && chartInstance.config.data) {
            const labels = chartInstance.config.data.labels || [];
            const data = chartInstance.config.data.datasets[0]?.data || [];
            
            for (let i = 0; i < labels.length; i++) {
                doc.text(labels[i], 20, yPosition);
                doc.text(`user${i+1}@example.com`, 80, yPosition);
                doc.text(`$${data[i]}`, 140, yPosition);
                yPosition += 15;
            }
        }
        
        // Save the PDF
        doc.save(`iaas-bi-bot-${chartTitle.toLowerCase().replace(/[^a-z0-9]/g, '-')}.pdf`);
        
        console.log(`PDF downloaded successfully for ${chartTitle}`);
    } catch (error) {
        console.error('Error in downloadSingleChartPDF:', error);
        alert('Error generating PDF. Please try again.');
    }
}

// Function to download selected multiple charts as PDF
async function downloadSelectedChartsPDF() {
    try {
        if (typeof jsPDF === 'undefined') {
            const script = document.createElement('script');
            script.src = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
            script.onload = () => downloadMultipleChartsPDF();
            document.head.appendChild(script);
        } else {
            downloadMultipleChartsPDF();
        }
    } catch (error) {
        console.error('Error downloading multiple charts PDF:', error);
        alert('Error generating PDF. Please try again.');
    }
}

function downloadMultipleChartsPDF() {
    try {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();
        
        // Add title
        doc.setFontSize(20);
        doc.text('IAAS - BI BOT Multiple Charts Report', 20, 20);
        
        // Add timestamp
        doc.setFontSize(12);
        doc.text(`Generated on: ${new Date().toLocaleString()}`, 20, 30);
        
        // Add prompt
        const prompt = document.getElementById('promptInput').value;
        if (prompt) {
            doc.setFontSize(14);
            doc.text('Prompt:', 20, 45);
            doc.setFontSize(12);
            doc.text(prompt, 20, 55);
        }
        
        const selectedCharts = getSelectedCharts();
        let currentPage = 1;
        
        selectedCharts.forEach((chart, index) => {
            // Add new page for each chart to give more space
            if (index > 0) {
                doc.addPage();
                currentPage++;
            }
            
            try {
                const chartImage = chartInstances[chart.id].toBase64Image();
                const chartTitle = `${chart.type.charAt(0).toUpperCase() + chart.type.slice(1)} Chart`;
                
                // Chart title
                doc.setFontSize(16);
                doc.text(chartTitle, 20, 30);
                
                // Larger chart size - almost full page
                const imgWidth = 170;
                const imgHeight = 120;
                const x = 20;
                const y = 45;
                
                doc.addImage(chartImage, 'PNG', x, y, imgWidth, imgHeight);
                
                // Add data summary below chart
                doc.setFontSize(14);
                doc.text('Data Summary:', 20, 180);
                doc.setFontSize(10);
                
                let yPosition = 190;
                hardcodedChartData.data.forEach((item, dataIndex) => {
                    const text = `${item.name}: $${item.total_spending}`;
                    doc.text(text, 20, yPosition);
                    yPosition += 8;
                });
                
            } catch (chartError) {
                console.error(`Error capturing chart ${index + 1}:`, chartError);
            }
        });
        
        // Save the PDF
        doc.save('iaas-bi-bot-multiple-charts.pdf');
        
        console.log(`PDF downloaded successfully with ${selectedCharts.length} charts`);
    } catch (error) {
        console.error('Error in downloadMultipleChartsPDF:', error);
        alert('Error generating PDF. Please try again.');
    }
}

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    console.log('Application loading...');
    
    // Initialize speech recognition
    initSpeechRecognition();
    
    // Add event listeners
    document.getElementById('generateChartsBtn').addEventListener('click', generateSingleChart);
    document.getElementById('speechBtn').addEventListener('click', startSpeechRecognition);
    document.getElementById('changeChartBtn').addEventListener('click', changeChartType);
    document.getElementById('generateMultipleBtn').addEventListener('click', generateMultipleCharts);
    document.getElementById('downloadSelectedBtn').addEventListener('click', downloadSelectedChartsPDF);
    
    // Add enter key support for prompt input
    document.getElementById('promptInput').addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault(); // Prevent default to avoid new line
            generateSingleChart();
        }
    });
    
    // Close dropdowns when clicking outside
    document.addEventListener('click', function(event) {
        if (!event.target.closest('.chart-type-btn') && !event.target.closest('.chart-type-dropdown')) {
            const allDropdowns = document.querySelectorAll('.chart-type-dropdown');
            const allButtons = document.querySelectorAll('.chart-type-btn');
            
            allDropdowns.forEach(dropdown => {
                dropdown.style.display = 'none';
                dropdown.classList.remove('show');
            });
            allButtons.forEach(button => {
                button.classList.remove('active');
            });
        }
        
        // Close flying data panel when clicking outside
        if (!event.target.closest('.flying-data-panel') && !event.target.closest('.view-data-btn')) {
            const flyingPanels = document.querySelectorAll('.flying-data-panel');
            flyingPanels.forEach(panel => {
                panel.classList.remove('show');
                setTimeout(() => {
                    panel.remove();
                }, 300);
            });
        }
    });
    
    console.log('Application loaded successfully');
}); 