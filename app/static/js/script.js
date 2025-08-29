document.addEventListener('DOMContentLoaded', function() {
    const uploadForm = document.getElementById('uploadForm');
    const progressSection = document.getElementById('progressSection');
    const progressBar = document.getElementById('progressBar');
    const progressPercent = document.getElementById('progressPercent');
    const statusMessage = document.getElementById('statusMessage');
    
    // Excel upload elements on index page
    const excelForm = document.getElementById('excel-upload-form');
    const excelFileInput = document.getElementById('excelFile');
    const uiProgressBar = document.getElementById('upload-progress-bar');
    const uiProgressInner = document.getElementById('progress-bar-inner');
    const excelResults = document.getElementById('excel-results');
    
    if (uploadForm) {
        uploadForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const fileInput = document.getElementById('fileInput');
            const file = fileInput.files[0];
            
            if (!file) {
                showAlert('Please select a file', 'danger');
                return;
            }
            
            // Validate file type
            if (!file.name.toLowerCase().endsWith('.pdf')) {
                showAlert('Please select a PDF file', 'danger');
                return;
            }
            
            // Show progress section
            progressSection.classList.remove('d-none');
            updateProgress(0, 'Uploading file...');
            
            // Create FormData and send request
            const formData = new FormData();
            formData.append('file', file);
            
            fetch('/api/upload', {
                method: 'POST',
                body: formData
            })
            .then(response => {
                if (!response.ok) {
                    return response.json().then(err => { throw new Error(err.error) });
                }
                return response.json();
            })
            .then(data => {
                if (data.success) {
                    updateProgress(100, 'Upload complete! Redirecting to processing...');
                    // Redirect to processing page
                    setTimeout(() => {
                        window.location.href = `/processing?file=${data.filename}`;
                    }, 1000);
                } else {
                    throw new Error(data.error || 'Upload failed');
                }
            })
            .catch(error => {
                updateProgress(0, `Error: ${error.message}`);
                progressBar.classList.remove('bg-success');
                progressBar.classList.add('bg-danger');
                statusMessage.classList.remove('alert-info');
                statusMessage.classList.add('alert-danger');
                statusMessage.innerHTML = `<i class="fas fa-exclamation-circle me-2"></i>${error.message}`;
            });
        });
    }
    
    if (excelForm) {
        excelForm.addEventListener('submit', function(e) {
            e.preventDefault();
            const file = excelFileInput && excelFileInput.files && excelFileInput.files[0];
            if (!file) {
                showAlert('Please select an Excel file', 'danger');
                return;
            }
            const name = file.name.toLowerCase();
            if (!(name.endsWith('.xlsx') || name.endsWith('.xls'))){
                showAlert('Please upload a valid Excel file (.xlsx/.xls)', 'danger');
                return;
            }

            // Reset UI
            if (excelResults) excelResults.innerHTML = '';
            if (uiProgressBar && uiProgressInner) {
                uiProgressBar.style.display = 'block';
                uiProgressInner.style.width = '0%';
                uiProgressInner.textContent = '0%';
            }

            // Step 1: Upload Excel
            const formData = new FormData();
            formData.append('excelFile', file);

            // Use XMLHttpRequest to track upload progress
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/api/excel/upload', true);
            xhr.upload.onprogress = function(evt) {
                if (evt.lengthComputable && uiProgressInner) {
                    const pct = Math.round((evt.loaded / evt.total) * 100);
                    uiProgressInner.style.width = pct + '%';
                    uiProgressInner.textContent = pct + '%';
                }
            };
            xhr.onreadystatechange = function() {
                if (xhr.readyState === XMLHttpRequest.DONE) {
                    try {
                        const resp = JSON.parse(xhr.responseText || '{}');
                        if (xhr.status >= 200 && xhr.status < 300 && resp.success) {
                            // Show sheet information
                            if (resp.sheet_count !== undefined) {
                                const sheetInfo = document.createElement('div');
                                sheetInfo.className = 'alert alert-info mt-3';
                                
                                let sheetList = '';
                                if (resp.sheet_names && resp.sheet_names.length > 0) {
                                    sheetList = '<ul class="mb-0">' + 
                                        resp.sheet_names.map(sheet => `<li>${sheet}</li>`).join('') + 
                                        '</ul>';
                                }
                                
                                sheetInfo.innerHTML = `
                                    <h5 class="alert-heading">
                                        <i class="fas fa-file-excel me-2"></i>File Uploaded Successfully
                                    </h5>
                                    <p>Found ${resp.sheet_count} sheet(s) in the Excel file:</p>
                                    ${sheetList}
                                    <hr>
                                    <p class="mb-0">Processing file for KYC violations...</p>
                                `;
                                
                                if (excelResults) {
                                    excelResults.innerHTML = '';
                                    excelResults.appendChild(sheetInfo);
                                }
                            }
                            
                            // Step 2: Process Excel on server for graph matching
                            if (uiProgressInner) {
                                uiProgressInner.style.width = '100%';
                                uiProgressInner.textContent = '100%';
                            }
                            
                            setTimeout(() => {
                                if (uiProgressInner) {
                                    uiProgressInner.style.width = '10%';
                                    uiProgressInner.textContent = 'Processing...';
                                }
                                
                                fetch('/api/excel/process', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ filename: resp.filename })
                                })
                                .then(r => r.json())
                                .then(data => {
                                    if (!data.success) throw new Error(data.error || 'Processing failed');
                                    // Render all Excel columns + rulebook details if available
                                    renderExcelMatchedRows(data);
                                    if (uiProgressInner) {
                                        uiProgressInner.style.width = '100%';
                                        uiProgressInner.textContent = 'Done';
                                    }
                                })
                                .catch(err => {
                                    showAlert(err.message || 'Processing failed', 'danger');
                                    if (uiProgressBar) uiProgressBar.style.display = 'none';
                                });
                            }, 1000);
                        } else {
                            showAlert(resp.error || 'Excel upload failed', 'danger');
                            if (uiProgressBar) uiProgressBar.style.display = 'none';
                        }
                    } catch (e) {
                        showAlert('Upload failed', 'danger');
                        if (uiProgressBar) uiProgressBar.style.display = 'none';
                    }
                }
            };
            xhr.onerror = function() {
                showAlert('Upload failed', 'danger');
                if (uiProgressBar) uiProgressBar.style.display = 'none';
            };
            xhr.send(formData);
        });
    }
    
    function updateProgress(percent, message) {
        if (progressBar) {
            progressBar.style.width = `${percent}%`;
            progressBar.textContent = `${percent}%`;
        }
        if (progressPercent) {
            progressPercent.textContent = `${percent}%`;
        }
        if (statusMessage) {
            statusMessage.innerHTML = `<i class="fas fa-info-circle me-2"></i>${message}`;
        }
    }
    
    function showAlert(message, type) {
        // Create alert element
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
        alertDiv.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        // Insert at the beginning of the container
        const container = document.querySelector('.container');
        container.insertBefore(alertDiv, container.firstChild);
        
        // Auto remove after 5 seconds
        setTimeout(() => {
            alertDiv.classList.remove('show');
            setTimeout(() => alertDiv.remove(), 150);
        }, 5000);
    }

    function renderExcelMatchedRows(payload) {
        if (!excelResults) return;
        
        // Handle the new response structure with transactions and KYC data
        if (payload && payload.results && (payload.results.transactions || payload.results.kyc_violations)) {
            const { transactions = [], kyc_violations = [], summary = {} } = payload.results;
            
            // Filter transactions to only show those with KYC violations
            const transactionsWithViolations = transactions.filter(tx => {
                return tx.has_violation || 
                       (tx.violation_details && tx.violation_details.length > 0) ||
                       kyc_violations.some(kyc => 
                           kyc.account_number && 
                           (tx.sender_account === kyc.account_number || 
                            tx.sender === kyc.account_number ||
                            (tx.sender && tx.sender.includes(kyc.customer_name)))
                       );
            });

            // Create tabs for Transactions and KYC Violations
            let html = `
                <ul class="nav nav-tabs" id="excelTabs" role="tablist">
                    <li class="nav-item" role="presentation">
                        <button class="nav-link active" id="transactions-tab" data-bs-toggle="tab" 
                                data-bs-target="#transactions" type="button" role="tab" 
                                aria-controls="transactions" aria-selected="true">
                            Transactions with Violations
                            <span class="badge bg-danger ms-1">${transactionsWithViolations.length}</span>
                        </button>
                    </li>
               
                    <li class="nav-item ms-auto">
                        <div class="d-flex align-items-center h-100 px-3 text-muted">
                            <small>
                                ${summary.violation_transactions || 0} violations in ${summary.total_transactions || 0} transactions | 
                                ${summary.customers_with_violations || 0} customers with violations
                            </small>
                        </div>
                    </li>
                </ul>
                <div class="tab-content p-3 border border-top-0 rounded-bottom" id="excelTabsContent">
                    <div class="tab-pane fade show active" id="transactions" role="tabpanel" aria-labelledby="transactions-tab">
                        ${renderTransactionTable(transactionsWithViolations)}
                    </div>
                    <div class="tab-pane fade" id="kyc" role="tabpanel" aria-labelledby="kyc-tab">
                        ${renderKYCTable(kyc_violations)}
                    </div>
                </div>`;
                
            excelResults.innerHTML = html;
        } else {
            // Fallback for old response format
            const items = Array.isArray(payload) ? payload : [];
            if (items.length === 0) {
                excelResults.innerHTML = '<div class="alert alert-info">No data found.</div>';
                return;
            }
            
            // Try to determine if this is a transaction or KYC record
            const firstItem = items[0];
            if (firstItem.customer_id !== undefined) {
                excelResults.innerHTML = renderKYCTable(items);
            } else {
                excelResults.innerHTML = renderTransactionTable(items);
            }
        }
        
        function renderKYCTable(kycViolations) {
            if (!kycViolations || kycViolations.length === 0) {
                return '<div class="alert alert-info">No KYC violation data available.</div>';
            }
            
            const headers = [
                'Customer Name', 'Account Number', 'Violation Type', 'Rule Invoked', 
                'KYC Status', 'Date', 'Actions'
            ];
            
            const rows = kycViolations.map(violation => {
                return `
                    <tr>
                        <td>${violation.customer_name || '—'}</td>
                        <td>${violation.account_number || '—'}</td>
                        <td>${violation.violation_type || '—'}</td>
                        <td>${violation.rule_invoked || '—'}</td>
                        <td>${violation.kyc_status ? 
                            `<span class="badge ${violation.kyc_status === 'Verified' ? 'bg-success' : 'bg-warning'}">${violation.kyc_status}</span>` : 
                            '—'}
                        </td>
                        <td>${violation.date || '—'}</td>
                        <td>
                            <button class="btn btn-sm btn-outline-primary" 
                                    onclick="showKYCDetails(${JSON.stringify(violation).replace(/"/g, '&quot;')})">
                                View Details
                            </button>
                        </td>
                    </tr>`;
            }).join('');
            
            return `
                <div class="table-responsive">
                    <table class="table table-hover align-middle">
                        <thead class="table-light">
                            <tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>`;
        }
        
        function renderTransactionTable(transactions) {
            if (!transactions || transactions.length === 0) {
                return '<div class="alert alert-info">No transaction data available.</div>';
            }
            
            const headers = [
                'Transaction ID', 'Date', 'Sender', 'Amount',
                'Matched Rules', 'Explanation', 'Status', 'Actions'
            ];
            
            const rows = transactions.map(tx => {
                const aiAnalysis = tx.ai_analysis || {};
                
                // Get matched rules from transaction or AI analysis
                const matchedRules = tx.matched_rules || [];
                const hasViolation = tx.has_violation || matchedRules.length > 0 || 
                                   (aiAnalysis.matched_rules && aiAnalysis.matched_rules[0] !== 'No Violation');
                
                // Determine risk level and status badge
                const riskLevel = tx.risk_level || aiAnalysis.risk_level || 'LOW';
                let riskBadgeClass = 'bg-secondary';
                
                switch(riskLevel.toUpperCase()) {
                    case 'CRITICAL':
                        riskBadgeClass = 'bg-danger';
                        break;
                    case 'HIGH':
                        riskBadgeClass = 'bg-danger';
                        break;
                    case 'MEDIUM':
                        riskBadgeClass = 'bg-warning text-dark';
                        break;
                    case 'LOW':
                        riskBadgeClass = 'bg-info';
                        break;
                    default:
                        riskBadgeClass = 'bg-secondary';
                }
                
                // Format matched rules for display
                const rulesList = matchedRules.length > 0 ? 
                    matchedRules.map(rule => rule.rule_name || 'Rule').join(', ') : 
                    (aiAnalysis.matched_rules && aiAnalysis.matched_rules[0] !== 'No Violation' ? 
                        aiAnalysis.matched_rules.join(', ') : '—');
                
                // Format amount with currency
                const amount = tx.amount ? 
                    (typeof tx.amount === 'string' ? 
                        tx.amount.replace(/[^0-9.-]+/g,"") : 
                        tx.amount
                    ) : 0;
                const formattedAmount = amount ? '₹' + parseFloat(amount).toLocaleString('en-IN') : '—';
                
                // Format explanation text
                const explanation = aiAnalysis.explanation || 
                    (tx.violation_details && tx.violation_details[0]?.reason) || '—';
                
                // Determine status text and class
                let statusText = 'Clean';
                let statusClass = 'bg-success';
                
                if (hasViolation) {
                    statusText = 'Violation';
                    statusClass = 'bg-danger';
                } else if (riskLevel === 'MEDIUM') {
                    statusText = 'Review';
                    statusClass = 'bg-warning text-dark';
                }
                
                return `
                    <tr class="${hasViolation ? 'table-warning' : ''}">
                        <td>${tx.transaction_id || '—'}</td>
                        <td>${tx.date ? new Date(tx.date).toISOString().split('T')[0] : '—'}</td>
                        <td>${tx.sender || '—'}</td>
                        <td class="text-nowrap">${formattedAmount}</td>
                        <td>
                            <span class="violation-badge" 
                                  data-bs-toggle="tooltip" 
                                  title="${rulesList}" 
                                  style="cursor: pointer;">
                                ${rulesList.length > 20 ? rulesList.substring(0, 20) + '...' : rulesList}
                            </span>
                        </td>
                        <td class="small">
                            <span class="d-inline-block text-truncate" style="max-width: 150px;" 
                                  data-bs-toggle="tooltip" 
                                  title="${explanation}">
                                ${explanation}
                            </span>
                        </td>
                        <td><span class="badge ${statusClass}">${statusText}</span></td>
                        <td class="text-nowrap">
                            <button class="btn btn-sm btn-outline-primary view-details" 
                                    onclick="showTransactionDetails(${escapeHtml(JSON.stringify(tx))})">
                                <i class="bi bi-eye"></i> View
                            </button>
                        </td>
                    </tr>
                `;
            }).join('');
            
            return `
                <div class="table-responsive">
                    <table class="table table-hover align-middle">
                        <thead class="table-light">
                            <tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
            `;
        }
        
        function renderKYCTable(kycViolations) {
            if (!kycViolations || kycViolations.length === 0) {
                return '<div class="alert alert-info">No KYC violation data available.</div>';
            }
            
            const headers = [
                'Customer ID', 'Name', 'Account #', 'KYC Status', 
                'Violation Type', 'Legal Provision', 'Actions'
            ];
            
            const rows = kycViolations.map(k => {
                const details = k.violation_details && k.violation_details[0] || {};
                
                return `
                    <tr>
                        <td>${k.customer_id || '—'}</td>
                        <td>${k.customer_name || '—'}</td>
                        <td>${k.account_number || '—'}</td>
                        <td>${renderKYCStatus(k.kyc_status)}</td>
                        <td>${k.violation_type || '—'}</td>
                        <td>${details.legal_provision || '—'}</td>
                        <td>
                            <button class="btn btn-sm btn-outline-primary" 
                                    onclick="showViolationDetails(this)" 
                                    data-violation='${JSON.stringify(k.violation_details || [])}'>
                                <i class="fas fa-search-plus"></i> Details
                            </button>
                        </td>
                    </tr>
                `;
            }).join('');
            
            return `
                <div class="table-responsive">
                    <table class="table table-hover align-middle">
                        <thead class="table-light">
                            <tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
                <!-- Violation Details Modal -->
                <div class="modal fade" id="violationModal" tabindex="-1" aria-hidden="true">
                    <div class="modal-dialog modal-lg">
                            </div>
                        </div>
                    </div>
                </div>
                <script>
                    function showViolationDetails(button) {
                        const violations = JSON.parse(button.getAttribute('data-violation') || '[]');
                        const modal = new bootstrap.Modal(document.getElementById('violationModal'));
                        const content = document.getElementById('violationDetailsContent');
                        
                        if (violations.length === 0) {
                            content.innerHTML = '<p>No violation details available.</p>';
                            modal.show();
                            return;
                        }
                        
                        const html = violations.map(v => {
                            const parts = [];
                            parts.push('<div class="card mb-3"><div class="card-header bg-light"><h6 class="mb-0">' + 
                                (v.violation_type || 'Violation') + '</h6></div><div class="card-body"><dl class="row mb-0">');
                            
                            if (v.legal_provision) {
                                parts.push('<dt class="col-sm-3">Legal Provision</dt><dd class="col-sm-9">' + 
                                    v.legal_provision + '</dd>');
                            }
                            
                            if (v.circular) {
                                parts.push('<dt class="col-sm-3">Circular</dt><dd class="col-sm-9">' + 
                                    v.circular + '</dd>');
                            }
                            
                            if (v.penalty_min || v.penalty_max) {
                                parts.push('<dt class="col-sm-3">Penalty Range</dt><dd class="col-sm-9">' +
                                    '₹' + (v.penalty_min ? v.penalty_min.toLocaleString() : '0') + ' - ' +
                                    '₹' + (v.penalty_max ? v.penalty_max.toLocaleString() : '0') + '</dd>');
                            }
                            
                            if (v.reason) {
                                parts.push('<dt class="col-sm-3">Reason</dt><dd class="col-sm-9">' + 
                                    v.reason + '</dd>');
                            }
                            
                            parts.push('</dl></div></div>');
                            return parts.join('');
                        }).join('');
                        
                        content.innerHTML = html;
                        modal.show();
                    }
                </script>
            `;
        }
        
        function renderViolationDetails(details) {
            if (!details || !details.length) return '';
            
            return `
                <tr class="table-active">
                    <td colspan="9" class="p-0">
                        <div class="accordion accordion-flush" id="violationDetails">
                            <div class="accordion-item">
                                <h2 class="accordion-header">
                                    <button class="accordion-button collapsed py-1 px-3" type="button" 
                                            data-bs-toggle="collapse" data-bs-target="#violationDetailsCollapse" 
                                            aria-expanded="false" aria-controls="violationDetailsCollapse">
                                        <small class="text-muted">View violation details (${details.length})</small>
                                    </button>
                                </h2>
                                <div id="violationDetailsCollapse" class="accordion-collapse collapse" 
                                     data-bs-parent="#violationDetails">
                                    <div class="accordion-body p-2">
                                        ${details.map(d => `
                                            <div class="card mb-2">
                                                <div class="card-body p-2">
                                                    <h6 class="card-title mb-1">${d.violation_type || 'Violation'}</h6>
                                                    ${d.legal_provision ? `
                                                        <p class="mb-1"><small class="text-muted">
                                                            ${d.legal_provision}
                                                        </small></p>
                                                    ` : ''}
                                                    ${d.reason ? `
                                                        <p class="mb-0 small">${d.reason}</p>
                                                    ` : ''}
                                                </div>
                                            </div>
                                        `).join('')}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </td>
                </tr>
            `;
        }
        
        function renderKYCStatus(status) {
            if (!status) return '<span class="badge bg-secondary">Unknown</span>';
            
            const statusLower = status.toLowerCase();
            if (statusLower.includes('fail') || statusLower.includes('reject') || statusLower.includes('suspicious')) {
                return `<span class="badge bg-danger">${status}</span>`;
            } else if (statusLower.includes('pending') || statusLower.includes('review')) {
                return `<span class="badge bg-warning text-dark">${status}</span>`;
            } else if (statusLower.includes('pass') || statusLower.includes('complete') || statusLower.includes('verified')) {
                return `<span class="badge bg-success">${status}</span>`;
            }
            return `<span class="badge bg-secondary">${status}</span>`;
        }
    }

    // Function to show KYC violation details in a modal
    function showKYCDetails(violation) {
        // Create modal if it doesn't exist
        let modal = document.getElementById('kycDetailsModal');
        if (!modal) {
            modal = document.createElement('div');
            modal.className = 'modal fade';
            modal.id = 'kycDetailsModal';
            modal.tabIndex = '-1';
            modal.setAttribute('aria-labelledby', 'kycDetailsModalLabel');
            modal.setAttribute('aria-hidden', 'true');
            document.body.appendChild(modal);
        }
        
        // Format the modal content
        modal.innerHTML = `
            <div class="modal-dialog modal-lg">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="kycDetailsModalLabel">KYC Violation Details</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <div class="row mb-3">
                            <div class="col-md-6">
                                <h6>Customer Information</h6>
                                <dl class="row">
                                    <dt class="col-sm-4">Name</dt>
                                    <dd class="col-sm-8">${violation.customer_name || '—'}</dd>
                                    
                                    <dt class="col-sm-4">Account Number</dt>
                                    <dd class="col-sm-8">${violation.account_number || '—'}</dd>
                                    
                                    <dt class="col-sm-4">KYC Status</dt>
                                    <dd class="col-sm-8">
                                        ${violation.kyc_status ? 
                                            `<span class="badge ${violation.kyc_status === 'Verified' ? 'bg-success' : 'bg-warning'}">
                                                ${violation.kyc_status}
                                            </span>` : 
                                            '—'}
                                    </dd>
                                </dl>
                            </div>
                            <div class="col-md-6">
                                <h6>Violation Details</h6>
                                <dl class="row">
                                    <dt class="col-sm-4">Violation Type</dt>
                                    <dd class="col-sm-8">${violation.violation_type || '—'}</dd>
                                    
                                    <dt class="col-sm-4">Rule Invoked</dt>
                                    <dd class="col-sm-8">${violation.rule_invoked || '—'}</dd>
                                    
                                    <dt class="col-sm-4">Date</dt>
                                    <dd class="col-sm-8">${violation.date || '—'}</dd>
                                </dl>
                            </div>
                        </div>
                        
                        ${violation.description ? `
                            <div class="mb-3">
                                <h6>Description</h6>
                                <p>${violation.description}</p>
                            </div>
                        ` : ''}
                        
                        ${violation.recommendation ? `
                            <div class="alert alert-warning">
                                <h6>Recommendation</h6>
                                <p class="mb-0">${violation.recommendation}</p>
                            </div>
                        ` : ''}
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        `;
        
        // Initialize and show the modal
        const modalInstance = new bootstrap.Modal(modal);
        modalInstance.show();
        
        // Clean up the modal when it's closed
        modal.addEventListener('hidden.bs.modal', function () {
            modal.remove();
        });
    }

    // Helper function to escape HTML special characters
    function escapeHtml(unsafe) {
        if (typeof unsafe === 'string') {
            return unsafe
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#039;');
        }
        return unsafe;
    }

    // Function to show transaction details in a modal
    window.showTransactionDetails = function(transaction) {
        // If transaction is a string, parse it (happens when coming from HTML onclick)
        if (typeof transaction === 'string') {
            try {
                // First try direct JSON parse
                try {
                    transaction = JSON.parse(transaction);
                } catch (e) {
                    // If that fails, try unescaping HTML entities first
                    const decoded = transaction
                        .replace(/&quot;/g, '"')
                        .replace(/&#39;/g, "'")
                        .replace(/&lt;/g, '<')
                        .replace(/&gt;/g, '>')
                        .replace(/&amp;/g, '&');
                    transaction = JSON.parse(decoded);
                }
            } catch (e) {
                console.error('Error parsing transaction data:', e, 'Raw data:', transaction);
                showAlert('Error loading transaction details. Please try again.', 'danger');
                return;
            }
        }
        
        // Get all possible violation details
        const details = {
            ...(transaction.violation_details && transaction.violation_details[0] || {}),
            ...(transaction.ai_analysis || {})
        };
        
        // Extract matched rules from both locations
        const matchedRules = [
            ...(transaction.matched_rules || []),
            ...(transaction.ai_analysis?.matched_rules || [])
        ].filter((rule, index, self) => 
            index === self.findIndex(r => 
                (r.rule_name || r) === (rule.rule_name || rule)
            )
        );
        
        // Get explanation from either violation details or AI analysis
        const explanation = transaction.ai_analysis?.explanation || 
                          (transaction.violation_details?.[0]?.details || 
                           transaction.violation_details?.[0]?.reason || 
                           'No explanation available');
        const modalId = 'transactionDetailsModal' + Math.random().toString(36).substr(2, 9);
        
        // Format amount and balance with currency
        const formatCurrency = (amount) => {
            if (amount === undefined || amount === null) return '—';
            const num = typeof amount === 'string' ? parseFloat(amount.replace(/[^0-9.-]+/g,"")) : amount;
            return '₹' + num.toLocaleString('en-IN', { minimumFractionDigits: 2 });
        };

        // Format date and time
        const formatDateTime = (dateTimeStr) => {
            if (!dateTimeStr) return '—';
            const date = new Date(dateTimeStr);
            return date.toLocaleString('en-IN', {
                year: 'numeric',
                month: 'short',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hour12: true
            });
        };
        
        // Create modal HTML
        const modalHTML = `
        <div class="modal fade" id="${modalId}" tabindex="-1" aria-labelledby="transactionModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-xl">
                <div class="modal-content">
                    <div class="modal-header bg-light">
                        <h5 class="modal-title" id="transactionModalLabel">
                            <i class="bi bi-receipt me-2"></i>Transaction Details
                            ${transaction.has_violation || matchedRules.length > 0 ? 
                                `<span class="badge bg-danger ms-2">
                                    ${matchedRules.length > 0 ? matchedRules.length + ' ' : ''}Violation${matchedRules.length !== 1 ? 's' : ''} Detected
                                </span>` : 
                                '<span class="badge bg-success ms-2">Clean</span>'}
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body">
                        <div class="row">
                            <!-- Basic Information -->
                            <div class="col-md-6 mb-4">
                                <div class="card h-100">
                                    <div class="card-header bg-light">
                                        <h6 class="mb-0"><i class="bi bi-info-circle me-2"></i>Basic Information</h6>
                                    </div>
                                    <div class="card-body">
                                        <dl class="row g-2">
                                            <dt class="col-sm-4">Transaction ID</dt>
                                            <dd class="col-sm-8">${transaction.transaction_id || transaction.Transaction_ID || '—'}</dd>
                                            
                                            <dt class="col-sm-4">Transaction Mode</dt>
                                            <dd class="col-sm-8">${transaction.transaction_mode || transaction.Transaction_Mode || transaction.mode || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Channel</dt>
                                            <dd class="col-sm-8">${transaction.channel || transaction.Channel || transaction.payment_channel || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Reference Number</dt>
                                            <dd class="col-sm-8">${transaction.reference_number || transaction.Reference_Number || transaction.ref_no || transaction.UTR || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Location</dt>
                                            <dd class="col-sm-8">${transaction.location || transaction.Location || transaction.branch_location || transaction.city || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Date</dt>
                                            <dd class="col-sm-8">${transaction.date ? formatDateTime(transaction.date) : (transaction.Date ? formatDateTime(transaction.Date) : 'N/A')}</dd>
                                            
                                            <dt class="col-sm-4">Time</dt>
                                            <dd class="col-sm-8">${transaction.time || transaction.Time || (transaction.date ? new Date(transaction.date).toLocaleTimeString() : (transaction.Date ? new Date(transaction.Date).toLocaleTimeString() : 'N/A'))}</dd>
                                            
                                            <dt class="col-sm-4">Transaction Type</dt>
                                            <dd class="col-sm-8">${transaction.transaction_type || transaction.Transaction_Type || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Transaction Mode</dt>
                                            <dd class="col-sm-8">${transaction.transaction_mode || transaction.Transaction_Mode || transaction.payment_mode || transaction.mode || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Channel</dt>
                                            <dd class="col-sm-8">${transaction.channel || transaction.Channel || transaction.payment_channel || transaction.source || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Reference Number</dt>
                                            <dd class="col-sm-8">${transaction.reference_number || transaction.Reference_Number || transaction.ref_no || transaction.reference || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Location</dt>
                                            <dd class="col-sm-8">${transaction.location || transaction.Location || transaction.branch_location || transaction.city || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Description</dt>
                                            <dd class="col-sm-8">${transaction.description || transaction.Description || '—'}</dd>
                                            
                                            <dt class="col-sm-4">Receiver Name</dt>
                                            <dd class="col-sm-8">${transaction.receiver_name || transaction.Receiver_Name || transaction.beneficiary_name || transaction.recipient_name || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Receiver Account</dt>
                                            <dd class="col-sm-8">${transaction.receiver_account || transaction.Receiver_Account || transaction.receiver || 'N/A'}</dd>
                                        </dl>
                                    </div>
                                </div>
                            </div>
                            
                            <!-- Amount & Balance -->
                            <div class="col-md-6">
                                <div class="card h-100">
                                    <div class="card-header bg-light">
                                        <h6 class="mb-0"><i class="bi bi-cash-stack me-2"></i>Financial Details</h6>
                                    </div>
                                    <div class="card-body">
                                        <dl class="row g-2">
                                            <dt class="col-sm-4">Amount</dt>
                                            <dd class="col-sm-8 fw-bold">${formatCurrency(transaction.amount || transaction.Amount || 0)}</dd>
                                            
                                            <dt class="col-sm-4">Balance After</dt>
                                            <dd class="col-sm-8">${transaction.balance_after !== undefined ? formatCurrency(transaction.balance_after) : 
                                               transaction.Balance_After ? formatCurrency(transaction.Balance_After) :
                                               transaction.available_balance ? formatCurrency(transaction.available_balance) : 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Currency</dt>
                                            <dd class="col-sm-8">${transaction.currency || transaction.Currency || 'INR'}</dd>
                                            
                                            <dt class="col-sm-4">Branch Code</dt>
                                            <dd class="col-sm-8">${transaction.branch_code || transaction.Branch_Code || transaction.branch_id || transaction.branch || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Sender Account</dt>
                                            <dd class="col-sm-8">${transaction.sender_account || transaction.Sender_Account || transaction.sender || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">Sender Name</dt>
                                            <dd class="col-sm-8">${transaction.sender_name || transaction.Sender_Name || transaction.sender || 'N/A'}</dd>
                                            
                                            <dt class="col-sm-4">KYC Status</dt>
                                            <dd class="col-sm-8">
                                                <span class="badge ${(transaction.sender_kyc_status || transaction.Sender_KYC_Status || '').toLowerCase() === 'verified' ? 'bg-success' : 'bg-warning'}">
                                                    ${transaction.sender_kyc_status || transaction.Sender_KYC_Status || 'N/A'}
                                                </span>
                                            </dd>
                                        </dl>
                                    </div>
                                </div>
                            </div>
                        </div>
                        
                        <!-- Violation Details -->
                        ${transaction.has_violation || (transaction.violation_details && transaction.violation_details.length > 0) ? `
                        <div class="card border-danger mb-4">
                            <div class="card-header bg-danger text-white">
                                <h6 class="mb-0"><i class="bi bi-exclamation-triangle me-2"></i>Violation Details</h6>
                            </div>
                            <div class="card-body">
                                <h6>Violating Transaction</h6>
                                <div class="alert alert-warning mb-3">
                                    <strong>Transaction ID:</strong> ${transaction.transaction_id || transaction.Transaction_ID || 'N/A'}<br>
                                    <strong>Description:</strong> ${transaction.description || transaction.Description || 'N/A'}<br>
                                    <strong>Amount:</strong> ${formatCurrency(transaction.amount || transaction.Amount || 0)}<br>
                                    <strong>Date/Time:</strong> ${formatDateTime(transaction.date || transaction.Date || transaction.timestamp)}<br>
                                    <strong>Sender:</strong> ${transaction.sender_name || transaction.Sender_Name || 'N/A'}<nobr> (${transaction.sender_account || transaction.Sender_Account || 'N/A'})</nobr><br>
                                    <strong>Receiver:</strong> ${transaction.receiver_name || transaction.Receiver_Name || 'N/A'}<nobr> (${transaction.receiver_account || transaction.Receiver_Account || 'N/A'})</nobr>
                                </div>

                                <h6>Matched Rules</h6>
                                <div class="mb-3">
                                    ${(transaction.ai_analysis?.matched_rules?.length > 0 || transaction.matched_rules?.length > 0) ? 
                                        (transaction.ai_analysis?.matched_rules || transaction.matched_rules).map(rule => {
                                            const ruleName = typeof rule === 'string' ? rule : (rule.rule_name || rule.name || 'Rule');
                                            return `<span class="badge bg-danger me-1 mb-1">${ruleName}</span>`;
                                        }).join('') : 
                                        '<span class="text-muted">No specific rules matched</span>'}
                                </div>
                                
                                <h6>Violation Details</h6>
                                <div class="alert alert-light border">
                                    ${transaction.ai_analysis?.explanation || 
                                      (transaction.violation_details?.length > 0 ? 
                                        transaction.violation_details.map(v => v.details || v.reason).filter(Boolean).join('<br><br>') : 
                                        'No detailed explanation available.')}
                                </div>
                            <div class="card-body">
                                ${(transaction.violation_details || []).map((violation, index) => `
                                    <div class="mb-3 ${index > 0 ? 'mt-3 pt-3 border-top' : ''}">
                                        <h6>Violation #${index + 1}: ${violation.rule_name || 'Rule Violation'}</h6>
                                        <div class="row">
                                            <div class="col-md-6">
                                                <dl class="row g-2">
                                                    <dt class="col-sm-4">Severity</dt>
                                                    <dd class="col-sm-8">
                                                        <span class="badge ${violation.severity === 'HIGH' ? 'bg-danger' : 
                                                                        violation.severity === 'MEDIUM' ? 'bg-warning text-dark' : 
                                                                        'bg-secondary'}">
                                                            ${violation.severity || 'MEDIUM'}
                                                        </span>
                                                    </dd>
                                                    
                                                    
                                                    <dt class="col-sm-4">Category</dt>
                                                    <dd class="col-sm-8">${violation.category || 'Compliance'}</dd>
                                                </dl>
                                            </div>
                                            <div class="col-md-6">
                                                <dl class="row g-2">
                                                    <dt class="col-sm-4">Legal Provision</dt>
                                                    <dd class="col-sm-8">${violation.legal_provision || '—'}</dd>
                                                    
                                                   
                                                </dl>
                                            </div>
                                        </div>
                                        ${violation.description ? `
                                            <div class="alert alert-light mt-2 mb-0">
                                                <strong>Description:</strong> ${violation.description}
                                            </div>
                                        ` : ''}
                                        ${violation.recommendation ? `
                                            <div class="alert alert-info mt-2 mb-0">
                                                <strong>Recommendation:</strong> ${violation.recommendation}
                                            </div>
                                        ` : ''}
                                    </div>
                                `).join('')}
                            </div>
                        </div>
                        ` : ''}
                        
                        <!-- AI Analysis -->
                        ${transaction.ai_analysis ? `
                        <div class="card ${transaction.has_violation ? 'border-warning' : 'border-success'}">
                            <div class="card-header ${transaction.has_violation ? 'bg-warning text-dark' : 'bg-success text-white'}">
                                <h6 class="mb-0"><i class="bi bi-robot me-2"></i>AI Analysis</h6>
                            </div>
                            <div class="card-body">
                                <div class="row">
                                    <div class="col-md-6">
                                        <h6>Matched Rules</h6>
                                        <div class="mb-3">
                                            ${(transaction.ai_analysis?.matched_rules?.length > 0 || transaction.matched_rules?.length > 0) ? 
                                                (transaction.ai_analysis?.matched_rules || transaction.matched_rules).map(rule => {
                                                    const ruleName = typeof rule === 'string' ? rule : (rule.rule_name || rule.name || 'Rule');
                                                    return `<span class="badge bg-info me-1 mb-1">${ruleName}</span>`;
                                                }).join('') : 
                                                '<span class="text-muted">No specific rules matched</span>'}
                                        </div>
                                        
                                        ${(transaction.ai_analysis?.explanation || transaction.violation_details?.length > 0) && `
                                        <h6>Explanation</h6>
                                        <div class="alert alert-light border">
                                            ${transaction.ai_analysis?.explanation || 
                                              transaction.violation_details?.map(v => v.details || v.reason).filter(Boolean).join(' ') || 
                                              'No detailed explanation available.'}
                                        </div>`}
                                        
                                        ${transaction.ai_analysis?.recommendation && `
                                        <h6>Recommendation</h6>
                                        <div class="alert alert-warning">
                                            <i class="bi bi-lightbulb me-2"></i>
                                            ${transaction.ai_analysis.recommendation}
                                        </div>`}
                                        
                                        <h6>Risk Level</h6>
                                        <div class="mb-3">
                                            <span class="badge ${transaction.ai_analysis.risk_level === 'HIGH' ? 'bg-danger' : 
                                                              transaction.ai_analysis.risk_level === 'MEDIUM' ? 'bg-warning text-dark' : 
                                                              'bg-success'}">
                                                ${transaction.ai_analysis.risk_level || 'LOW'}
                                            </span>
                                        </div>
                                    </div>
                                    <div class="col-md-6">
                                        <h6>Explanation</h6>
                                        <div class="alert alert-light">
                                            ${transaction.ai_analysis.explanation || 'No explanation provided by the AI model.'}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        ` : ''}
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">
                            <i class="bi bi-x-circle me-1"></i> Close
                        </button>
                        ${transaction.has_violation ? `
                        <button type="button" class="btn btn-danger">
                            <i class="bi bi-flag me-1"></i> Report Issue
                        </button>
                        ` : ''}
                    </div>
                </div>
            </div>
        </div>`;
        
        // Add modal to body and show it
        const modalContainer = document.createElement('div');
        modalContainer.innerHTML = modalHTML;
        document.body.appendChild(modalContainer);
        
        const modalElement = document.getElementById(modalId);
        const modal = new bootstrap.Modal(modalElement);
        modal.show();
        
        // Remove modal from DOM when hidden
        modalElement.addEventListener('hidden.bs.modal', function() {
            modal.dispose();
            modalContainer.remove();
        });
    };
});