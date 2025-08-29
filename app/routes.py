
from flask import Blueprint, render_template, request, jsonify, send_file, current_app, flash, redirect, url_for, session, send_from_directory
from werkzeug.utils import secure_filename
import os
import csv
from datetime import datetime
import pandas as pd
import os
import logging
import google.generativeai as genai
import time
import json
import re
from app.utils.extraction import process_rbi_pdf
from app.utils.graph import get_client_from_env
from app.utils.graph import find_violations_by_type, find_violations_by_account
from typing import Dict, List, Any, Optional, Tuple

# Setup logging
LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'app.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()]
)

bp = Blueprint('main', __name__)

@bp.route('/api/database/log', methods=['GET'])
def get_database_log():
    """Endpoint to get database contents for logging"""
    try:
        current_app.logger.info("Attempting to get database log...")
        neo = get_client_from_env()
        
        # Check if the Neo4j client is properly initialized
        if not hasattr(neo, '_driver') or not neo._driver:
            current_app.logger.error("Neo4j driver is not initialized")
            return jsonify({
                'status': 'error',
                'message': 'Database connection is not properly initialized'
            }), 500
            
        current_app.logger.info("Neo4j client initialized successfully")
        
        # Get counts for each node type
        node_query = """
        UNWIND ['Circular', 'Violation', 'PenaltyRange', 'LegalProvision', 'Reason', 'ComplianceRule'] AS label
        CALL {
            WITH label
            MATCH (n)
            WHERE label IN labels(n)
            RETURN count(n) AS count
        }
        RETURN label, count
        """
        
        # Get relationship counts
        rel_query = """
        MATCH ()-[r]->()
        RETURN type(r) AS type, count(*) AS count
        ORDER BY type
        """
        
        current_app.logger.info("Executing node count query...")
        node_result = []
        rel_result = []
        
        # Execute queries using session with explicit database name
        try:
            current_app.logger.info(f"Attempting to connect to database: {neo._driver}")
            
            # First, test a simple query to verify connection
            with neo._driver.session(database="rbi") as session:
                current_app.logger.info("Successfully connected to 'rbi' database")
                
                # Test a simple query
                test_result = session.run("RETURN 'Connection test successful' AS message").single()
                current_app.logger.info(f"Test query result: {test_result['message']}")
                
                # Execute node query with error handling
                try:
                    current_app.logger.info("Running node query...")
                    node_result = session.run(node_query).data()
                    current_app.logger.info(f"Node query returned {len(node_result)} results")
                    
                    # Execute relationship query
                    current_app.logger.info("Running relationship query...")
                    rel_result = session.run(rel_query).data()
                    current_app.logger.info(f"Relationship query returned {len(rel_result)} results")
                    
                except Exception as query_error:
                    current_app.logger.error(f"Query execution error: {str(query_error)}", exc_info=True)
                    return jsonify({
                        'status': 'error',
                        'message': f'Query execution error: {str(query_error)}',
                        'query': node_query if 'node' in str(query_error).lower() else rel_query
                    }), 500
                
        except Exception as e:
            current_app.logger.error(f"Database query error: {str(e)}", exc_info=True)
            return jsonify({
                'status': 'error',
                'message': f'Database query error: {str(e)}'
            }), 500
        
        # Log the results
        log_message = "\n=== DATABASE STATISTICS ===\n"
        log_message += "=== NODES ===\n"
        for item in node_result:
            log_message += f"{item['label']}: {item['count']} nodes\n"
            
        log_message += "\n=== RELATIONSHIPS ===\n"
        for item in rel_result:
            log_message += f"{item['type']}: {item['count']} relationships\n"
            
        current_app.logger.info(log_message)
        
        return jsonify({
            'status': 'success',
            'nodes': node_result,
            'relationships': rel_result
        })
        
    except Exception as e:
        error_msg = f"Error getting database log: {str(e)}"
        current_app.logger.error(error_msg, exc_info=True)
        return jsonify({
            'status': 'error',
            'message': error_msg
        }), 500

@bp.route('/api/graph-data')
def get_graph_data():
    """Endpoint to fetch data for graphs"""
    try:
        neo = get_client_from_env()
        data = {
            'fines_trend': [],
            'violation_types': [],
            'relationship_distribution': {}
        }
        
        if neo.enabled:
            with neo.get_session() as db_session:
                # Get fines trend data (example: fines by month)
                trend_query = """
                MATCH (v:Violation)-[r:PENALTY_IN_RANGE]->(p:PenaltyRange)
                RETURN v.date as date, sum(p.max) as total_fine
                ORDER BY date
                """
                result = db_session.run(trend_query).data()
                data['fines_trend'] = [
                    {'date': item['date'], 'amount': float(item['total_fine'] or 0)}
                    for item in result
                ]
                
                # Get violation types distribution
                violation_query = """
                MATCH (v:Violation)
                RETURN v.type as violation_type, count(*) as count
                ORDER BY count DESC
                LIMIT 10
                """
                result = db_session.run(violation_query).data()
                data['violation_types'] = [
                    {'type': item['violation_type'], 'count': item['count']}
                    for item in result
                ]
                
                # Get relationship distribution
                rel_query = """
                MATCH ()-[r]->()
                RETURN type(r) as rel_type, count(*) as count
                ORDER BY count DESC
                """
                result = db_session.run(rel_query).data()
                data['relationship_distribution'] = {
                    item['rel_type']: item['count']
                    for item in result
                }
                
        return jsonify(data)
        
    except Exception as e:
        current_app.logger.error(f"Error fetching graph data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/recent-fines')
def get_recent_fines():
    """Endpoint to fetch recent fines data"""
    try:
        uploads_dir = os.path.join(current_app.root_path, '..', 'uploads')
        recent_fines = []
        
        # Look for CSV files in uploads directory
        for filename in os.listdir(uploads_dir):
            if filename.startswith('results_') and filename.endswith('.csv'):
                filepath = os.path.join(uploads_dir, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Parse penalty amount from the Penalty Range column
                        penalty_range = row.get('Penalty Range', '')
                        amount = 0
                        if '₹' in penalty_range:
                            try:
                                # Extract the first number in the range
                                amount_str = penalty_range.split('₹')[-1].split('–')[0].strip().replace(',', '')
                                amount = float(amount_str) if amount_str.replace('.', '').isdigit() else 0
                            except (ValueError, IndexError):
                                amount = 0
                        
                        recent_fines.append({
                            'circular': row.get('Circular / Direction', 'N/A'),
                            'violation': row.get('Violation Type', 'N/A'),
                            'penalty_range': penalty_range,
                            'amount': amount,
                            'legal_provision': row.get('Legal Provision Invoked', 'N/A'),
                            'reason': row.get('Reason / Description', 'N/A'),
                            'source_file': filename
                        })
        
        # Sort by amount in descending order and get top 5
        recent_fines = sorted(recent_fines, key=lambda x: x['amount'], reverse=True)[:5]
        return jsonify(recent_fines)
        
    except Exception as e:
        current_app.logger.error(f"Error fetching recent fines: {str(e)}")
        return jsonify({'error': str(e)}), 500

@bp.route('/')
def index():
    if not session.get('logged_in'):
        return redirect(url_for('auth.login', next=request.url))
    
    # Initialize default values
    db_counts = {
        'total_violations': 0,
        'total_circulars': 0,
        'total_penalties': 0,
        'total_legal_provisions': 0,
        'total_reasons': 0,
        'total_compliance_rules': 0,
        'total_has_reason': 0,
        'total_has_violation': 0,
        'total_invokes': 0,
        'total_penalty_in_range': 0
    }
    
    try:
        neo = get_client_from_env()
        
        if neo.enabled:
            with neo.get_session() as db_session:
                # Get all node counts in a single query for better performance
                node_counts_query = """
                MATCH (n)
                WITH 
                    count(CASE WHEN 'Violation' IN labels(n) THEN 1 ELSE null END) as violations,
                    count(CASE WHEN 'Circular' IN labels(n) THEN 1 ELSE null END) as circulars,
                    count(CASE WHEN 'PenaltyRange' IN labels(n) THEN 1 ELSE null END) as penalties,
                    count(CASE WHEN 'LegalProvision' IN labels(n) THEN 1 ELSE null END) as legal_provisions,
                    count(CASE WHEN 'Reason' IN labels(n) THEN 1 ELSE null END) as reasons,
                    count(CASE WHEN 'ComplianceRule' IN labels(n) THEN 1 ELSE null END) as compliance_rules
                RETURN violations, circulars, penalties, legal_provisions, reasons, compliance_rules
                """
                result = db_session.run(node_counts_query).single()
                
                if result:
                    db_counts.update({
                        'total_violations': result.get('violations', 0),
                        'total_circulars': result.get('circulars', 0),
                        'total_penalties': result.get('penalties', 0),
                        'total_legal_provisions': result.get('legal_provisions', 0),
                        'total_reasons': result.get('reasons', 0),
                        'total_compliance_rules': result.get('compliance_rules', 0)
                    })
                
                # Get relationship counts
                rel_counts_query = """
                MATCH ()-[r]->()
                WITH 
                    sum(CASE WHEN type(r) = 'HAS_REASON' THEN 1 ELSE 0 END) as has_reason,
                    sum(CASE WHEN type(r) = 'HAS_VIOLATION' THEN 1 ELSE 0 END) as has_violation,
                    sum(CASE WHEN type(r) = 'INVOKES' THEN 1 ELSE 0 END) as invokes,
                    sum(CASE WHEN type(r) = 'PENALTY_IN_RANGE' THEN 1 ELSE 0 END) as penalty_in_range
                RETURN has_reason, has_violation, invokes, penalty_in_range
                """
                rel_result = db_session.run(rel_counts_query).single()
                
                if rel_result:
                    db_counts.update({
                        'total_has_reason': rel_result.get('has_reason', 0),
                        'total_has_violation': rel_result.get('has_violation', 0),
                        'total_invokes': rel_result.get('invokes', 0),
                        'total_penalty_in_range': rel_result.get('penalty_in_range', 0)
                    })
                
                # Log all counts for debugging
                current_app.logger.info("Database counts: %s", db_counts)
                
                # Store counts in session for quick access
                session['db_counts'] = db_counts
                
                # 6. Count all nodes with label 'ComplianceRule'
                rules_query = """
                MATCH (n)
                WHERE 'ComplianceRule' IN labels(n)
                RETURN count(n) as count
                """
                result = db_session.run(rules_query).single()
                db_counts['total_compliance_rules'] = result['count'] if result and 'count' in result else 0
                current_app.logger.info(f"ComplianceRules count: {db_counts['total_compliance_rules']}")
                
                # Log final counts for debugging
                current_app.logger.info(f"Final database counts: {db_counts}")
                
                # Set default values for other template variables
                recent_violations = []
                top_penalties = []
                total_settlements = 0
                total_fines = 0
                monitored_fines = 0
                non_compliance = 0
                critical_violations = 0
                high_risk_violations = 0
                medium_risk_violations = 0
                low_risk_violations = 0
                recent_fines = []
                trend_labels = []
                trend_values = []
                violation_labels = []
                violation_values = []
        
        # Log all counts for debugging
        current_app.logger.info(f"Final database counts: {db_counts}")
        
        # Set default values for other template variables
        recent_violations = []
        top_penalties = []
        total_settlements = 0
        total_fines = 0  # This is used in the template
        monitored_fines = 0
        non_compliance = 0
        critical_violations = 0
        high_risk_violations = 0
        medium_risk_violations = 0
        low_risk_violations = 0
        recent_fines = []
        trend_labels = []
        trend_values = []
        violation_labels = []
        violation_values = []
        
        # Get total fines from the database
        try:
            if neo._driver:
                with neo._driver.session() as db_session:
                    # Query to get the sum of penalty amounts from Violation nodes
                    fines_query = """
                    MATCH (v:Violation)-[:PENALTY_IN_RANGE]->(p:PenaltyRange)
                    RETURN sum(p.max) as total_fines
                    """
                    result = db_session.run(fines_query).single()
                    if result and 'total_fines' in result and result['total_fines'] is not None:
                        total_fines = int(result['total_fines'])
        except Exception as e:
            current_app.logger.error(f"Error getting total fines: {str(e)}")
        
        # Debug log the counts
        current_app.logger.info(f"Database counts: {db_counts}")
        
        # Return the template with all required variables
        return render_template('index.html',
                           total_violations=db_counts['total_violations'],
                           total_circulars=db_counts['total_circulars'],
                           total_penalties=db_counts['total_penalties'],
                           total_legal_provisions=db_counts['total_legal_provisions'],
                           total_fines=total_fines,
                           recent_violations=recent_violations,
                           top_penalties=top_penalties,
                           total_settlements=total_settlements,
                           monitored_fines=monitored_fines,
                           non_compliance=non_compliance,
                           critical_violations=critical_violations,
                           high_risk_violations=high_risk_violations,
                           medium_risk_violations=medium_risk_violations,
                           low_risk_violations=low_risk_violations,
                           recent_fines=recent_fines,
                           trend_labels=json.dumps(trend_labels or []),
                           trend_values=json.dumps(trend_values or []),
                           violation_labels=json.dumps(violation_labels or []),
                           violation_values=json.dumps(violation_values or []))
    except Exception as e:
        logging.error(f"Error loading dashboard data: {str(e)}")
        # Return empty data in case of error
        return render_template('index.html',
                           total_settlements=0,
                           total_fines=0,
                           monitored_fines=0,
                           non_compliance=0,
                           recent_fines=[],
                           trend_labels=json.dumps([]),
                           trend_values=json.dumps([]),
                           violation_labels=json.dumps([]),
                           violation_values=json.dumps([]))

def get_compliance_rules():
    """Return a list of compliance rules with their details from the database.
    
    If the database is not available or there's an error, returns a default set of rules.
    """
    try:
        # Get the Neo4j client
        neo = get_client_from_env()
        
        # Check if we have a valid Neo4j client
        if not neo or not hasattr(neo, '_driver') or not neo._driver:
            logging.warning("No valid Neo4j connection available, using default compliance rules")
            return get_default_compliance_rules()
            
        # Import the required functions
        from app.utils.graph import get_compliance_rules as get_db_rules, initialize_compliance_rules
        
        try:
            # Initialize default rules if they don't exist
            initialize_compliance_rules(neo)
            
            # Fetch rules from the database
            rules = get_db_rules(neo)
            if rules:
                return rules
                
        except Exception as e:
            logging.error(f"Error initializing/fetching compliance rules: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            
        # Fall back to default rules if anything goes wrong
        return get_default_compliance_rules()
        
    except Exception as e:
        logging.error(f"Error in get_compliance_rules: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return get_default_compliance_rules()


def get_default_compliance_rules():
    """Return an empty list of compliance rules.
    
    This function is kept for backward compatibility but returns an empty list
    to enforce database-driven rule management.
    """
    logging.warning("No compliance rules found in the database. Please add rules to the database.")
    return []

@bp.route('/upload')
def upload():
    if not session.get('logged_in'):
        return redirect(url_for('auth.login', next=request.url))
    
    # Get compliance rules
    compliance_rules = get_compliance_rules()
    
    return render_template('upload.html', compliance_rules=compliance_rules)

@bp.route('/excel-upload')
def excel_upload():
    if not session.get('logged_in'):
        return redirect(url_for('auth.login', next=request.url))
    return render_template('excel_upload.html')

@bp.route('/api/upload', methods=['POST'])
def api_upload():
    if 'file' not in request.files:
        logging.warning('No file uploaded in request')
        return jsonify({'error': 'No file uploaded'}), 400
    file = request.files['file']
    if file.filename=='':
        logging.warning('No file selected for upload')
        return jsonify({'error': 'No file selected'}), 400
    if file and file.filename.lower().endswith('.pdf'):
        filename = secure_filename(file.filename)
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        logging.info(f'File uploaded: {filename} at {filepath}')
        return jsonify({
            'success': True,
            'filename': filename,
            'message': 'File uploaded successfully'
        })
    logging.warning(f'Invalid file type attempted: {file.filename}')
    return jsonify({'error': 'Invalid file type. Please upload a PDF'}), 400

@bp.route('/processing')
def processing():
    filename = request.args.get('file')
    if not filename:
        flash('No file specified for processing', 'danger')
        return redirect(url_for('main.upload'))
    
    return render_template('processing.html', filename=filename)

@bp.route('/api/process', methods=['POST'])
def api_process():
    data = request.get_json()
    filename = data.get('filename')
    logging.info(f'Received process request for filename: {filename}')
    if not filename:
        logging.error('No filename provided in process request')
        return jsonify({'error': 'No filename provided'}), 400
    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
    if not os.path.exists(filepath):
        logging.error(f'File not found for processing: {filepath}')
        return jsonify({'error': 'File not found'}), 404
    try:
        # Process the PDF
        logging.info(f'Starting PDF processing for: {filepath}')
        results = process_rbi_pdf(filepath, current_app.config['GEMINI_API_KEY'])
        # Log the extracted data (first 5 rows for brevity)
        if not results.empty:
            logging.info(f'Extracted data sample: {results.head().to_dict(orient="records")}')
        else:
            logging.info('No fines extracted from the document.')
        # Save results to a temporary file
        results_file = os.path.join(current_app.config['UPLOAD_FOLDER'], f'results_{filename}.csv')
        # Always write header row for CSV
        results.to_csv(results_file, index=False, header=True)
        logging.info(f'Processing complete. Results saved to: {results_file}')
        # Write to Neo4j (no-op if env not set)
        try:
            neo = get_client_from_env()
            if neo.enabled and not results.empty:
                for row in results.to_dict('records'):
                    # Normalize numeric penalty range if parsable
                    pen_min, pen_max = None, None
                    try:
                        # Penalty Range might be like "₹10,000 – ₹100,000" or a single value
                        import re
                        nums = [int(n.replace(',', '')) for n in re.findall(r"\d[\d,]*", str(row.get('Penalty Range','')))]
                        if len(nums) == 1:
                            pen_min = pen_max = nums[0]
                        elif len(nums) >= 2:
                            pen_min, pen_max = nums[0], nums[-1]
                    except Exception:
                        pass
                    neo_record = {
                        'circular': str(row.get('Circular / Direction','')).strip(),
                        'slNo': int(row.get('SL No')) if str(row.get('SL No','')).strip().isdigit() else None,
                        'page': row.get('Page'),
                        'violationType': str(row.get('Violation Type','')),
                        'penMin': pen_min,
                        'penMax': pen_max,
                        'currency': 'INR',
                        'legal': str(row.get('Legal Provision Invoked','')),
                        'reason': str(row.get('Reason / Description','')),
                    }
                    logging.info(f"Writing to Neo4j: {neo_record}")
                    neo.upsert_violation(neo_record)
            if 'neo' in locals():
                neo.close()
        except Exception as neo_err:
            logging.error(f'Neo4j write skipped due to error: {neo_err}')
        # Always return a valid structure
        return jsonify({
            'success': True,
            'results_file': f'results_{filename}.csv',
            'data': results.to_dict('records') if not results.empty else []
        })
    except Exception as e:
        logging.exception(f'Error during PDF processing for {filename}: {e}')
        return jsonify({'error': str(e)}), 500

@bp.route('/results')
def results():
    results_file = request.args.get('file')
    logging.info(f'Results page requested for file: {results_file}')
    if not results_file:
        logging.warning('Missing results file parameter for results page')
        flash('Missing parameters for results', 'danger')
        return redirect(url_for('main.upload'))

    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], results_file)
    if not os.path.exists(filepath):
        logging.error(f'Results file not found: {filepath}')
        flash('Results file not found', 'danger')
        return redirect(url_for('main.upload'))

    try:
        df = pd.read_csv(filepath)
        data = df.to_dict(orient='records')
        logging.info(f'Results data loaded successfully for file: {results_file}')
        return render_template('results.html', data=data, results_file=results_file)
    except Exception as e:
        logging.error(f'Invalid results file format: {e}')
        flash('Invalid results file format', 'danger')
        return redirect(url_for('main.upload'))

@bp.route('/api/download/<filename>')
def api_download(filename):
    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
    logging.info(f'Download requested for file: {filepath}')
    if not os.path.exists(filepath):
        logging.error(f'File not found for download: {filepath}')
        flash('File not found', 'danger')
        return redirect(url_for('main.upload'))
    return send_file(
        filepath,
        as_attachment=True,
        download_name=filename
    )

@bp.route('/api/results', methods=['GET'])
def get_results():
    file = request.args.get('file')
    logging.info(f'API get_results called for file: {file}')
    if not file:
        logging.warning('No file specified in get_results')
        return jsonify({"error": "No file specified"}), 400
    try:
        df = pd.read_csv(os.path.join('uploads', file))
        data = df.to_dict(orient="records")
        logging.info(f'Results loaded and returned for file: {file}')
        return jsonify(data)
    except Exception as e:
        logging.error(f'Invalid data format in get_results: {e}')
        return jsonify({"error": "Invalid data format"}), 400

# ---------------- Excel Upload and Processing ----------------

@bp.route('/api/excel/debug', methods=['GET'])
def debug_excel():
    """Debug endpoint to inspect Excel file structure."""
    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], 'Customer_Violation_and_Transactions.xlsx')
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
        
    try:
        xls = pd.ExcelFile(filepath)
        result = {
            'sheet_names': xls.sheet_names,
            'first_sheet_columns': pd.read_excel(filepath, sheet_name=0, nrows=1).columns.tolist(),
            'first_sheet_first_row': pd.read_excel(filepath, sheet_name=0, nrows=1).iloc[0].to_dict()
        }
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@bp.route('/api/excel/upload', methods=['POST'])
def api_excel_upload():
    if 'excelFile' not in request.files:
        logging.warning('No excelFile uploaded in request')
        return jsonify({'error': 'No file uploaded'}), 400
        
    file = request.files['excelFile']
    if file.filename == '':
        logging.warning('No file selected for Excel upload')
        return jsonify({'error': 'No file selected'}), 400
        
    allowed = (file.filename.lower().endswith('.xlsx') or file.filename.lower().endswith('.xls'))
    if not allowed:
        logging.warning(f'Invalid Excel file type attempted: {file.filename}')
        return jsonify({'error': 'Invalid file type. Please upload an Excel (.xlsx/.xls)'}), 400
        
    filename = secure_filename(file.filename)
    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    logging.info(f'Excel uploaded: {filename} at {filepath}')
    
    try:
        # Read the Excel file to get sheet information
        xls = pd.ExcelFile(filepath)
        sheet_names = xls.sheet_names
        sheet_count = len(sheet_names)
        
        return jsonify({
            'success': True,
            'filename': filename,
            'sheet_count': sheet_count,
            'sheet_names': sheet_names,
            'message': f'File uploaded successfully with {sheet_count} sheets',
            'filepath': filepath
        })
    except Exception as e:
        # If there's an error reading the Excel file, still return success but with a warning
        logging.error(f"Error reading Excel file: {str(e)}")
        return jsonify({
            'success': True,
            'filename': filename,
            'warning': f'File uploaded but could not read sheet information: {str(e)}',
            'filepath': filepath
        })


def _resolve_excel_columns(df: pd.DataFrame, sheet_type='transaction'):
    """Resolve column names for different sheet types.
    
    Args:
        df: DataFrame to resolve columns for
        sheet_type: Type of sheet ('transaction' or 'kyc')
    
    Returns:
        dict: Mapping of standardized column names to actual column names
    """
    # Get all column names in lowercase for case-insensitive matching
    print(f"DEBUG: Processing sheet type: {sheet_type}")
    print(f"DEBUG: Raw input columns: {df.columns.tolist()}")
    
    # Create a normalized mapping of column names
    cols = {str(col).strip().lower(): str(col).strip() for col in df.columns}
    print(f"DEBUG: Normalized column mapping: {cols}")
    
    resolved = {}
    
    if sheet_type == 'transaction':
        # Transaction sheet columns
        resolved['transaction_id'] = next(
            (cols[c] for c in ['transaction id', 'transaction_id', 'txnid', 'transactionid'] 
             if c in cols), None)
        resolved['sender'] = next(
            (cols[c] for c in ['sender name', 'sender', 'from', 'sender_account', 'from_account'] 
             if c in cols), None)
        resolved['receiver'] = next(
            (cols[c] for c in ['receiver name', 'receiver', 'to', 'receiver_account', 'to_account'] 
             if c in cols), None)
        resolved['amount'] = next(
            (cols[c] for c in ['amount', 'txn_amount', 'transaction amount'] 
             if c in cols), None)
        resolved['date'] = next(
            (cols[c] for c in ['date', 'transaction_date', 'txn_date', 'value_date'] 
             if c in cols), None)
    
    elif sheet_type == 'kyc':
        # KYC sheet columns - first try to find exact matches
        resolved['customer_id'] = next(
            (cols[c] for c in ['customer id', 'customer_id', 'cust_id', 'client_id'] 
             if c in cols), None)
        resolved['customer_name'] = next(
            (cols[c] for c in ['customer name', 'customer_name', 'name', 'customer', 'john smith'] 
             if c in cols), None)
        resolved['account_number'] = next(
            (cols[c] for c in ['account no', 'account no.', 'account number', 'account_number', 'account', 'acct_no'] 
             if c in cols), None)
        resolved['violation_type'] = next(
            (cols[c] for c in ['customer violation', 'violation type', 'violation_type', 'violation', 'rule invoked', 'rule_invoked'] 
             if c in cols), None)
        resolved['kyc_status'] = next(
            (cols[c] for c in ['kyc verified', 'kyc status', 'kyc_status', 'status', 'customer_status'] 
             if c in cols), None)
        
        # If account_number not found, try to find a column that might contain account numbers
        if not resolved['account_number']:
            for col_name, actual_name in cols.items():
                if any(term in col_name for term in ['account', 'acct', 'acc no', 'accno']):
                    resolved['account_number'] = actual_name
                    break
    
    # Handle different sheet types
    if sheet_type == 'kyc':
        # For KYC sheet, we're looking for customer violations
        kyc_columns = {
            'transaction_id': ['transaction id', 'transaction_id', 'txnid', 'transactionid', 'transaction id'],
            'account_number': ['account no', 'account no.', 'account number', 'account_number', 'account', 'acct_no'],
            'sender_name': ['sender name', 'sender_name', 'customer name', 'name', 'john smith'],
            'date': ['date', 'transaction date', 'txn_date', 'value date'],
            'kyc_verified': ['kyc verified', 'kyc_verified', 'kyc status', 'kyc_status', 'kyc'],
            'violation_type': ['customer violation', 'violation type', 'violation_type', 'rule invoked', 'rule_invoked', 'violation'],
        }
        
        for field, possible_names in kyc_columns.items():
            for name in possible_names:
                if name in cols:
                    resolved[field] = cols[name]
                    break
                    
    else:  # transaction sheet
        # For transaction sheet, we're looking for transaction details
        tx_columns = {
            'transaction_id': ['transaction id', 'transaction_id', 'txnid', 'transactionid'],
            'date': ['date', 'transaction date', 'txn_date'],
            'sender_account': ['sender account', 'sender_account', 'from account', 'from_account'],
            'sender_name': ['sender name', 'sender_name', 'from name', 'from_name'],
            'receiver_account': ['receiver account', 'receiver_account', 'to account', 'to_account'],
            'receiver_name': ['receiver name', 'receiver_name', 'to name', 'to_name'],
            'amount': ['amount', 'transaction amount', 'amt'],
            'transaction_type': ['transaction type', 'transaction_type', 'type', 'txn_type'],
            'description': ['description', 'desc', 'details', 'transaction details'],
            'balance': ['balance', 'account balance', 'current balance']
        }
        
        for field, possible_names in tx_columns.items():
            for name in possible_names:
                if name in cols:
                    resolved[field] = cols[name]
                    break
    
    print(f"DEBUG: All resolved columns: {resolved}")
    
    return {k: v for k, v in resolved.items() if v is not None}


def _process_transaction_sheet(df, neo, sheet_name=None, kyc_data=None):
    """Process transaction details sheet and match with violation rules.
    
    Args:
        df: DataFrame containing transaction data
        neo: Neo4j client instance
        sheet_name: Name of the sheet being processed
        kyc_data: Dictionary mapping account numbers to KYC violation data
    
    Returns:
        dict: Results containing matched violations and summary
    """
    if kyc_data is None:
        kyc_data = {}
        
    print(f"DEBUG: Processing transaction sheet: {sheet_name}")
    print(f"DEBUG: Raw DataFrame columns: {df.columns.tolist()}")
    
    # Resolve column names
    columns = _resolve_excel_columns(df, sheet_type='transaction')
    print(f"DEBUG: Resolved columns: {columns}")
    
    # Check for required columns
    required_columns = ['transaction_id', 'sender_account', 'amount', 'date']
    missing_columns = [col for col in required_columns if col not in columns]
    if missing_columns:
        error_msg = f"Missing required columns: {', '.join(missing_columns)}. Available columns: {df.columns.tolist()}"
        print(f"DEBUG: {error_msg}")
        return {'error': error_msg}, 400
    
    # Check for monthly deposit limit violations
    try:
        logging.info("Checking for monthly deposit limit violations...")
        monthly_limit_violations = _check_monthly_deposit_limit(df.rename(columns={
            columns['sender_account']: 'sender_account',
            columns['amount']: 'amount',
            columns['date']: 'date'
        }))
        
        # Log the number of violations found
        if monthly_limit_violations:
            logging.info(f"Found monthly deposit limit violations for {len(monthly_limit_violations)} accounts")
        else:
            logging.info("No monthly deposit limit violations found")
            
        # Add monthly limit violations to kyc_data
        for account, violations in monthly_limit_violations.items():
            if account not in kyc_data:
                kyc_data[account] = {
                    'violation_type': 'Monthly Deposit Limit Exceeded',
                    'violation_details': violations,
                    'has_violation': True
                }
                logging.info(f"Added monthly deposit limit violation for account {account}: {len(violations)} violations")
                
    except Exception as e:
        error_msg = f"Error checking monthly deposit limits: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return {'error': error_msg}, 500
    
    results = []
    for idx, row in df.iterrows():
        try:
            # Get account number (use sender account by default)
            account_number = str(row.get(columns.get('sender_account', ''))).strip()
            if not account_number or account_number.lower() == 'nan':
                continue
                
            # Initialize result with basic transaction info
            result = {
                'row_id': idx,
                'sheet': sheet_name or 'transactions',
                'transaction_id': str(row[columns['transaction_id']]).strip() if 'transaction_id' in columns and pd.notna(row[columns['transaction_id']]) else None,
                'sender_account': account_number,
                'receiver': str(row[columns.get('receiver', '')]).strip() if 'receiver' in columns and pd.notna(row[columns.get('receiver', '')]) else None,
                'amount': float(str(row[columns['amount']]).replace(',', '').replace('₹', '').replace('$', '').strip() or '0') if 'amount' in columns and pd.notna(row[columns['amount']]) else 0,
                'date': str(row[columns.get('date', '')]) if 'date' in columns and pd.notna(row[columns.get('date', '')]) else None,
                'violation_details': [],
                'has_violation': False
            }
            
            # Check for monthly deposit limit violation
            if account_number in monthly_limit_violations:
                for violation in monthly_limit_violations[account_number]:
                    result['violation_details'].append({
                        'violation_type': 'Monthly Deposit Limit Exceeded',
                        'legal_provision': 'Internal Risk Management Policy',
                        'circular': 'INTERNAL/RISK/2023/001',
                        'penalty_min': 10000,
                        'penalty_max': 50000,
                        'reason': f"Account exceeded monthly deposit limit of ₹10,000. Deposited ₹{violation['total_deposits']:.2f} in {violation['month']}",
                        'excess_amount': violation['excess_amount']
                    })
                    result['has_violation'] = True
            
            # Check for other KYC violations
            if account_number in kyc_data:
                violation_info = kyc_data[account_number]
                if 'violation_details' in violation_info and isinstance(violation_info['violation_details'], list):
                    for detail in violation_info['violation_details']:
                        if isinstance(detail, dict):
                            result['violation_details'].append({
                                'violation_type': detail.get('violation_type', 'KYC Violation'),
                                'legal_provision': detail.get('legal_provision', 'RBI KYC Master Direction'),
                                'circular': detail.get('circular', 'RBI/2022-23/123'),
                                'penalty_min': detail.get('penalty_min', 5000),
                                'penalty_max': detail.get('penalty_max', 100000),
                                'reason': detail.get('reason', 'KYC/AML violation detected')
                            })
                            result['has_violation'] = True
                            # Get additional details from Neo4j if available
                if neo.enabled and result['has_violation']:
                    try:
                        # Get all unique violation types for this transaction
                        violation_types = list(set([
                            detail.get('violation_type') 
                            for detail in result['violation_details'] 
                            if detail.get('violation_type')
                        ]))
                        
                        for v_type in violation_types:
                            matches = find_violations_by_type(neo, v_type)
                            for match in matches:
                                result['violation_details'].append({
                                    'violation_type': match.get('violationType') or v_type,
                                    'legal_provision': match.get('legalProvision', ''),
                                    'circular': match.get('circular'),
                                    'penalty_min': match.get('penMin'),
                                    'penalty_max': match.get('penMax'),
                                    'reason': match.get('reason'),
                                    'person': {
                                        'name': match.get('personName'),
                                        'id': match.get('personId'),
                                        'email': match.get('personEmail'),
                                        'phone': match.get('personPhone')
                                    } if any(match.get(k) for k in ['personName', 'personId', 'personEmail', 'personPhone']) else None
                                })
                    except Exception as qerr:
                        logging.error(f'Neo4j query error: {qerr}')
                
                results.append(result)
                
        except Exception as e:
            print(f"DEBUG: Error processing row {idx+2}: {str(e)}")
            continue
    
    # Prepare the final result with summary
    summary = {
        'total_transactions': len(df),
        'violations_found': len([r for r in results if r.get('has_violation', False)]),
        'unique_violation_types': list({d['violation_type'] for r in results for d in r.get('violation_details', [])}),
        'total_amount': sum(float(str(r.get('amount', '0')).replace(',', '').replace('₹', '').replace('$', '').strip() or '0') for r in results if r.get('amount') is not None)
    }
    
    return {
        'data': results,
        'summary': summary,
        'status': 'success',
        'message': f'Processed {len(results)} transactions with {summary["violations_found"]} violations found'
    }


def _process_kyc_sheet(df, neo, sheet_name=None):
    """Process KYC violation sheet.
    
    Args:
        df: DataFrame containing KYC violation data
        neo: Neo4j client instance
        sheet_name: Name of the sheet being processed
        
    Returns:
        dict: Contains processed KYC violation data and summary
    """
    print(f"DEBUG: Processing KYC sheet: {sheet_name}")
    print(f"DEBUG: Raw DataFrame columns: {df.columns.tolist()}")
    
    # Resolve column names
    columns = _resolve_excel_columns(df, sheet_type='kyc')
    print(f"DEBUG: Resolved KYC columns: {columns}")
    
    # Check for required columns
    required_columns = ['account_number', 'violation_type']
    missing_columns = [col for col in required_columns if col not in columns]
    
    if missing_columns:
        error_msg = f"Missing required columns in KYC sheet: {', '.join(missing_columns)}. " \
                  f"Available columns: {df.columns.tolist()}"
        print(f"DEBUG: {error_msg}")
        return {'error': error_msg}, 400
    
    results = []
    for idx, row in df.iterrows():
        try:
            account_number = str(row[columns['account_number']]).strip()
            violation_type = str(row[columns['violation_type']]).strip()
            
            if not account_number or not violation_type:
                print(f"DEBUG: Skipping row {idx+2} - missing account number or violation type")
                continue
                
            # Get additional violation details from Neo4j if available
            violation_details = []
            if neo.enabled:
                violations = find_violations_by_account(neo, account_number)
                if violations:
                    violation_details = [{
                        'violation_type': v.get('violationType', violation_type),
                        'description': v.get('description', ''),
                        'legal_provision': v.get('legalProvision', ''),
                        'penalty_range': v.get('penaltyRange', '')
                    } for v in violations]
            
            result = {
                'account_number': account_number,
                'violation_type': violation_type,
                'sender_name': str(row.get(columns.get('sender_name', ''), '')).strip(),
                'date': str(row.get(columns.get('date', ''), '')).strip(),
                'kyc_verified': str(row.get(columns.get('kyc_verified', ''), '')).strip(),
                'sheet_name': sheet_name,
                'row_index': idx + 2,  # +2 for 1-based index and header row
                'violation_details': violation_details,
                'has_violation': bool(violation_details)
            }
            
            # Add any additional columns that might be present
            for col in df.columns:
                if col not in ['account_number', 'violation_type', 'sender_name', 'date', 'kyc_verified']:
                    result[col] = row[col] if pd.notna(row[col]) else ''
            
            results.append(result)
            
        except Exception as e:
            print(f"DEBUG: Error processing row {idx+2}: {str(e)}")
            continue
    
    summary = {
        'total_records': len(df),
        'total_violations': len(results),
        'unique_accounts': len({r['account_number'] for r in results}),
        'violation_types': list({r['violation_type'] for r in results if r.get('violation_type')})
    }
    
    print(f"DEBUG: Processed {len(results)} KYC violations from sheet {sheet_name}")
    return {
        'data': results,
        'summary': summary,
        'status': 'success',
        'message': f'Processed {len(results)} KYC records with {summary["total_violations"]} violations found'
    }

def _analyze_transaction_with_rules(transaction_row: Dict[str, Any], model):
    """
    Analyze a single transaction against compliance rules using Gemini AI.
    
    Args:
        transaction_row: Dictionary containing all transaction fields from Excel row
        model: Gemini model instance for AI analysis
        
    Returns:
        Dictionary with rule analysis results
    """
    # Extract amount and convert to float for comparison
    try:
        amount = float(transaction_row.get('Amount', 0))
    except (ValueError, TypeError):
        amount = 0.0
    
    # Define compliance rules with more detailed triggers and conditions
    compliance_rules = [
        {
            "name": "High Value Transaction",
            "description": "Any single transaction exceeding ₹1000 must be flagged for additional scrutiny.",
            "condition": "isinstance(t.get('amount', 0), (int, float)) and float(t.get('amount', 0)) > 1000",
            "risk_level": "HIGH"
        },
        {
            "name": "Non-compliance with KYC norms",
            "description": "Banks must collect KYC documents, verify customer identity, perform periodic KYC updates.",
            "condition": "t.get('sender_kyc_status') and (kyc_status := str(t.get('sender_kyc_status', '')).strip().lower()) and kyc_status not in ['', 'completed'] and kyc_status in ['expired', 'rejected', 'incomplete', 'pending']",
            "risk_level": "HIGH"
        },
        {
            "name": "Violation of customer protection norms",
            "description": "Banks must follow RBI's Charter of Customer Rights (fair treatment, transparency, grievance redress, etc).",
            "condition": "any(term in description.lower() for term in ['unauthorized', 'disputed', 'unfair', 'complaint'])",
            "risk_level": "HIGH"
        },
        {
            "name": "Non-submission or delay in regulatory returns",
            "description": "Banks must file periodic regulatory returns (NPAs, statutory returns, fraud reports) on time.",
            "condition": "any(term in description.lower() for term in ['overdue', 'late submission', 'penalty', 'compliance charge'])",
            "risk_level": "MEDIUM"
        },
        {
            "name": "Inadequate oversight of outsourced activities",
            "description": "Banks must audit outsourced services, cannot outsource policy formulation or loan sanction.",
            "condition": "any(term in description.lower() for term in ['third-party', 'outsourced', 'vendor'])",
            "risk_level": "MEDIUM"
        },
        {
            "name": "Breach of digital lending norms",
            "description": "Direct loan disbursement, APR disclosures, no automatic credit limit increases without consent.",
            "condition": "any(term in description.lower() for term in ['fintech', 'lending app', 'lsp']) or 'digital_lending' in transaction_mode.lower()",
            "risk_level": "HIGH"
        },
        {
            "name": "Lapses in cybersecurity compliance",
            "description": "Banks must maintain a cybersecurity framework, CISOs, audits; customers must follow safe practices.",
            "condition": "any(term in description.lower() for term in ['suspicious', 'alert', 'fraud', 'cyber', 'hack'])",
            "risk_level": "CRITICAL"
        }
    ]
    
    # Extract transaction details
    # sender_kyc_status = str(transaction_row.get('Sender_KYC_Status', 'Unknown')).strip()
    # description = str(transaction_row.get('Description', '')).lower()
    # transaction_mode = str(transaction_row.get('Transaction_Mode', '')).lower()
    
    # Create comprehensive transaction text from all Excel fields
    transaction_details = f"""
TRANSACTION DETAILS:
• Transaction ID: {transaction_row.get('Transaction_ID', 'N/A')}
• Date: {transaction_row.get('Date', 'N/A')} at {transaction_row.get('Time', 'N/A')}
• Sender: {transaction_row.get('Sender_Name', 'N/A')} (Account: {transaction_row.get('Sender_Account', 'N/A')})
• Sender KYC Status: {sender_kyc_status}
• Receiver: {transaction_row.get('Receiver_Name', 'N/A')} (Account: {transaction_row.get('Receiver_Account', 'N/A')})
• Amount: ₹{amount:,.2f}
• Transaction Type: {transaction_row.get('Transaction_Type', 'N/A')}
• Transaction Mode: {transaction_mode}
• Channel: {transaction_row.get('Channel', 'N/A')}
• Branch: {transaction_row.get('Branch_Code', 'N/A')}
• Location: {transaction_row.get('Location', 'N/A')}
• Description: {transaction_row.get('Description', 'No description')}
• Balance After: ₹{float(transaction_row.get('Balance_After', 0)):,.2f}
• Reference: {transaction_row.get('Reference_Number', 'N/A')}
"""

    # Prepare rule descriptions for AI prompt
    rule_descriptions = [
        f"{i+1}. {rule['name']} → {rule['description']} (Risk: {rule['risk_level']})" 
        for i, rule in enumerate(compliance_rules)
    ]
    
    # Add additional rules that don't need automated checking
    additional_rules = [
        "Misclassification of NPAs → NPAs must be classified correctly after 90+ days of non-payment.",
        "Non-reporting of large exposures → Large exposures (10%+ Tier 1 capital, or ₹1 lakh+ for reporting) must be reported.",
        "Penalties under PMLA → Suspicious transactions must be reported and identity verified.",
        "Non-compliance by Credit Information Companies → CICs must notify customers and secure data.",
        "Non-cooperation with Ombudsman → Banks must cooperate with Ombudsman rulings."
    ]
    
    rule_descriptions.extend([
        f"{i+len(compliance_rules)+1}. {rule}" 
        for i, rule in enumerate(additional_rules)
    ])
    
    # Build the prompt with all rules and transaction details
    prompt = f"""You are a compliance classification assistant for RBI banking regulations.
You will receive one complete financial transaction record at a time.
Your task is to analyze if this transaction violates any of the following RBI compliance rules.

RBI COMPLIANCE RULES (in priority order):
{chr(10).join(rule_descriptions)}

{transaction_details}

ANALYSIS INSTRUCTIONS:
1. Review the transaction details carefully against each compliance rule
2. Pay special attention to:
   - Transaction amount (especially > ₹1000)
   - Sender KYC status
   - Transaction mode and channel
   - Description text for any red flags
   - Any unusual patterns or combinations

3. For each rule that is violated, include:
   - The exact rule name
   - A brief explanation of how the transaction violates this rule
   - The risk level (CRITICAL, HIGH, MEDIUM, or LOW)

4. If no rules are violated, respond with "No Violation"

RESPONSE FORMAT (strict JSON):
{{
  "transaction_id": "{transaction_row.get('Transaction_ID', 'N/A')}",
  "matched_rules": ["Rule Name 1", "Rule Name 2"],
  "explanation": "Detailed explanation of the violations found, including which parts of the transaction triggered which rules.",
  "kyc_status": "{sender_kyc_status}",
  "amount": {amount},
  "risk_level": "HIGHEST_RISK_LEVEL_FOUND"
}}

EXAMPLE RESPONSE FOR VIOLATION:
{{
  "transaction_id": "TXN12345",
  "matched_rules": ["High Value Transaction", "Lapses in cybersecurity compliance"],
  "explanation": "Transaction amount of ₹1,200 exceeds the ₹1,000 threshold for high-value transactions. Additionally, the description contains 'suspicious activity' which triggers cybersecurity concerns.",
  "kyc_status": "Verified",
  "amount": 1200.0,
  "risk_level": "CRITICAL"
}}

EXAMPLE RESPONSE FOR NO VIOLATION:
{{
  "transaction_id": "{transaction_row.get('Transaction_ID', 'N/A')}",
  "matched_rules": ["No Violation"],
  "explanation": "This is a normal customer activity and does not violate any regulatory rules.",
  "kyc_status": "{sender_kyc_status}",
  "amount": {amount},
  "risk_level": "LOW"
}}"""

    try:
        # Generate analysis from Gemini
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        # Log the raw response for debugging
        logging.debug(f"Raw AI response for transaction {transaction_row.get('Transaction_ID', 'UNKNOWN')}:\n{response_text}")
        
        # Extract JSON from response
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            try:
                result = json.loads(json_match.group(0))
                
                # Check if this is a no-violation response
                if result.get('matched_rules') == ["No Violation"] or not any(result.get('matched_rules', [])):
                    return {
                        'transaction_id': result.get('transaction_id', transaction_row.get('Transaction_ID', 'UNKNOWN')),
                        'has_violation': False,
                        'violation_details': [],
                        'ai_analysis': {
                            'matched_rules': ['No Violation'],
                            'explanation': result.get('explanation', 'No compliance violations detected.'),
                            'risk_level': 'LOW'
                        }
                    }
                
                # Process violations
                matched_rules = result.get('matched_rules', [])
                explanation = result.get('explanation', 'No explanation provided')
                risk_level = result.get('risk_level', 'MEDIUM')
                
                # Format the response with detailed violation information
                violation_details = []
                for rule_name in matched_rules:
                    # Find the rule details from our compliance_rules
                    rule_details = next(
                        (r for r in compliance_rules if r['name'] == rule_name),
                        {'description': 'Rule details not available', 'risk_level': risk_level}
                    )
                    
                    violation_details.append({
                        'rule_name': rule_name,
                        'description': rule_details.get('description', ''),
                        'risk_level': rule_details.get('risk_level', risk_level),
                        'details': {
                            'transaction_id': result.get('transaction_id', transaction_row.get('Transaction_ID', 'UNKNOWN')),
                            'status': 'VIOLATION',
                            'severity': rule_details.get('risk_level', risk_level),
                            'explanation': explanation
                        }
                    })
                
                # Determine the highest risk level from all violations
                risk_levels = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2, 'CRITICAL': 3}
                highest_risk = max(
                    [risk_levels.get(v['risk_level'].upper(), 0) for v in violation_details],
                    default=0
                )
                overall_risk = [k for k, v in risk_levels.items() if v == highest_risk][0]
                
                return {
                    'transaction_id': result.get('transaction_id', transaction_row.get('Transaction_ID', 'UNKNOWN')),
                    'has_violation': True,
                    'violation_details': violation_details,
                    'ai_analysis': {
                        'matched_rules': matched_rules,
                        'explanation': explanation,
                        'risk_level': overall_risk
                    }
                }
                
            except json.JSONDecodeError as je:
                logging.error(f"Failed to parse AI response as JSON: {je}\nResponse: {response_text}")
                raise ValueError(f"Invalid JSON response from AI: {je}")
            except Exception as e:
                logging.error(f"Error processing AI response: {e}\nResponse: {response_text}")
                raise
        else:
            logging.error(f"No JSON found in AI response. Full response:\n{response_text}")
            raise ValueError("Could not find valid JSON in AI response")
            
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Error analyzing transaction: {error_msg}")
        
        return {
            'transaction_id': transaction_row.get('Transaction_ID', 'UNKNOWN'),
            'has_violation': False,
            'error': error_msg,
            'ai_analysis': {
                'matched_rules': ['Analysis Error'],
                'explanation': f'Error analyzing transaction: {error_msg}',
                'risk_level': 'UNKNOWN'
            },
            'violation_details': [{
                'rule_name': 'Analysis Error',
                'description': f'Failed to analyze transaction: {error_msg}',
                'risk_level': 'UNKNOWN',
                'details': {
                    'transaction_id': transaction_row.get('Transaction_ID', 'UNKNOWN'),
                    'status': 'ERROR',
                    'severity': 'UNKNOWN',
                    'explanation': f'Error during analysis: {error_msg}'
                }
            }]
        }

def _clean_for_json(obj):
    """
    Recursively clean data for JSON serialization by converting non-serializable types.
    Handles NaN values specifically in 'rule invoked' and other numeric fields.
    
    Args:
        obj: Object to clean
        
    Returns:
        JSON-serializable version of the object
    """
    try:
        # Handle None and numpy.nan
        if obj is None or (hasattr(obj, 'item') and str(obj).lower() in ['nan', 'nat']):
            return None
            
        # Handle basic JSON types
        if isinstance(obj, (str, int, bool)):
            return obj
            
        # Handle float specifically to catch NaN, inf, -inf
        if isinstance(obj, float):
            if pd.isna(obj) or pd.isnull(obj):
                return None
            return obj
            
        # Handle pandas NA/NaT/None
        if str(obj).lower() in ['nat', 'nan', 'none', 'null']:
            return None
            
        # Handle dictionaries - clean each value
        if isinstance(obj, dict):
            cleaned = {}
            for k, v in obj.items():
                # Special handling for 'rule invoked' field
                if str(k).lower() == 'rule invoked' and (pd.isna(v) or v == ''):
                    cleaned[str(k)] = None
                else:
                    cleaned[str(k)] = _clean_for_json(v)
            return cleaned
            
        # Handle lists and tuples
        if isinstance(obj, (list, tuple, set)):
            return [_clean_for_json(item) for item in obj]
            
        # Handle pandas Series and DataFrames
        if hasattr(obj, 'to_dict') and callable(getattr(obj, 'to_dict')):
            return _clean_for_json(obj.to_dict())
            
        # Handle numpy types
        if hasattr(obj, 'item') and callable(getattr(obj, 'item')):
            try:
                return _clean_for_json(obj.item())
            except (ValueError, TypeError):
                return None
                
        # Handle datetime objects
        if hasattr(obj, 'isoformat') and callable(getattr(obj, 'isoformat')):
            return obj.isoformat()
            
        # Convert to string as last resort
        try:
            return str(obj) if obj is not None else None
        except Exception:
            return None
            
    except Exception as e:
        logging.error(f"Error cleaning object for JSON: {str(e)}, type: {type(obj)}, value: {str(obj)[:200] if obj is not None else 'None'}")
        return None

def _check_monthly_deposit_limit(transactions_df, monthly_limit=3000):
    """Check for accounts that exceed the monthly deposit limit.
    
    Args:
        transactions_df: DataFrame containing transaction data with columns:
            - sender_account: Account number
            - amount: Transaction amount
            - date: Transaction date (YYYY-MM-DD format)
        monthly_limit: Maximum allowed deposit amount per month (default: 3000)
        
    Returns:
        Dictionary mapping account numbers to their violation details
    """
    try:
        # Make a copy to avoid modifying the original DataFrame
        df = transactions_df.copy()
        
        # Convert date column to datetime and extract year-month
        df['transaction_date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['transaction_date'])  # Drop rows with invalid dates
        df['year_month'] = df['transaction_date'].dt.to_period('M')
        
        # Group by account and month, then sum the deposits
        monthly_deposits = df.groupby(['sender_account', 'year_month'])['amount'].sum().reset_index()
        
        # Find accounts exceeding the limit
        violations = monthly_deposits[monthly_deposits['amount'] > monthly_limit].copy()
        
        # Add violation details
        violations['excess_amount'] = violations['amount'] - monthly_limit
        violations['violation_type'] = 'Monthly Deposit Limit Exceeded'
        violations['reason'] = f'Total deposits of ₹{violations["amount"].astype(int):,} exceed monthly limit of ₹{monthly_limit:,}'
        violations['legal_provision'] = 'RBI Guidelines on Digital Lending'
        violations['penalty_min'] = 2500  # Example penalty range
        violations['penalty_max'] = 3000
        
        # Log the violations
        if not violations.empty:
            logging.info(f"Found {len(violations)} monthly deposit limit violations")
            for _, row in violations.iterrows():
                logging.info(
                    f"Account {row['sender_account']} exceeded monthly limit in {row['year_month']}: "
                    f"₹{row['amount']:,.2f} (limit: ₹{monthly_limit:,}) - "
                    f"Excess: ₹{row['excess_amount']:,.2f}"
                )
        
        # Convert to the format expected by the frontend
        violations_dict = {}
        for _, row in violations.iterrows():
            account = row['sender_account']
            if account not in violations_dict:
                violations_dict[account] = []
                
            violations_dict[account].append({
                'violation_type': row['violation_type'],
                'month': str(row['year_month']),
                'total_deposits': float(row['amount']),
                'excess_amount': float(row['excess_amount']),
                'reason': row['reason'],
                'legal_provision': row['legal_provision'],
                'penalty_min': row['penalty_min'],
                'penalty_max': row['penalty_max']
            })
            
        return violations_dict
        
    except Exception as e:
        error_msg = f"Error in _check_monthly_deposit_limit: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return {}
    

def _process_excel_file(filepath: str) -> Dict[str, Any]:

    """
    Process Excel file to analyze transactions against compliance rules using AI.
    
    Args:
        filepath: Path to the Excel file
        
    Returns:
        Dictionary containing analyzed transactions with rule violations
    """
    try:
        # Initialize Gemini model
        api_key = current_app.config.get('GEMINI_API_KEY')
        if not api_key:
            return {'error': 'GEMINI_API_KEY not configured'}
        
        logging.info(f"Initializing Gemini model with API key: {api_key[:10]}...")
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Test API connection first
        try:
            test_response = model.generate_content("Hello, test message")
            logging.info("✅ Gemini API connection successful")
        except Exception as e:
            error_msg = str(e)
            logging.error(f"❌ Gemini API connection failed: {error_msg}")
            if "429" in error_msg or "quota" in error_msg.lower():
                return {'error': 'Gemini API quota exceeded. Please check your API limits.'}
            return {'error': f'Gemini API error: {error_msg}'}
        
        # Read the Excel file (single sheet with transactions)
        df = pd.read_excel(filepath)
        logging.info(f"Excel columns: {df.columns.tolist()}")
        
        # Standardize column names
        df.columns = [str(col).strip() for col in df.columns]
        
        # Find key columns
        description_col = next((col for col in df.columns if 'description' in col.lower()), None)
        if not description_col:
            return {'error': 'Description column not found in Excel sheet'}
        
        # Process only first 2 rows
        df = df.head(2)
        total_rows = len(df)
        logging.info(f"Processing first {total_rows} rows (one by one)")
        
        # If there are fewer than 2 rows, log a warning
        if total_rows < 2:
            logging.warning(f"Only {total_rows} rows found in the Excel file")
        
        transactions: List[Dict[str, Any]] = []
        
        # Process each row individually with detailed logging
        for idx, row in df.iterrows():
            try:
                # Log start of processing for this row
                logging.info(f"\n{'='*80}")
                logging.info(f"📋 Processing Row {idx + 1}/{total_rows}")
                logging.info(f"{'='*80}")
                
                # Convert row to dictionary with standardized column names
                transaction_data = {}
                for col in df.columns:
                    # Clean column names by removing special characters and converting to lowercase
                    clean_col = str(col).strip().lower().replace(' ', '_')
                    transaction_data[clean_col] = row[col] if pd.notna(row[col]) else ''
                    logging.info(f"  - {clean_col}: {transaction_data[clean_col]}")
                
                # Get transaction description from the most likely column
                description_cols = [col for col in transaction_data.keys() 
                                 if 'desc' in col or 'note' in col or 'detail' in col or 'particular' in col]
                description_col = description_cols[0] if description_cols else next(iter(transaction_data.keys()))
                transaction_text = str(transaction_data.get(description_col, '')).strip()
                
                if not transaction_text or transaction_text.lower() in ['nan', 'none', '']:
                    logging.warning(f"⚠️  Skipping row {idx + 1}: No description found")
                    continue
                
                logging.info(f"🔍 Analyzing transaction {idx + 1}/{total_rows}: {transaction_text[:100]}...")
                
                # Add index as transaction_id if not present
                if 'transaction_id' not in transaction_data or not transaction_data['transaction_id']:
                    transaction_data['transaction_id'] = f"TXN_{idx + 1}"
                
                # First, apply all rule-based checks
                matched_rules = []
                violation_details = []
                
                # Get the compliance rules from the database
                compliance_rules = get_compliance_rules()
                
                # Define additional compliance rules
                additional_rules = [
                    # High-value transaction rule
                    {
                        'id': 'high_value_transaction',
                        'name': 'High Value Transaction',
                        'description': 'Single transaction exceeds ₹1000',
                        'risk': 'HIGH',
                        'condition': "isinstance(t.get('amount', 0), (int, float)) and float(t.get('amount', 0)) > 1000"
                    },
                    # Suspicious transaction patterns
                    {
                        'id': 'suspicious_transaction_pattern',
                        'name': 'Suspicious Transaction Pattern',
                        'description': 'Transaction matches known suspicious patterns',
                        'risk': 'HIGH',
                        'condition': "any(term in str(t.get('description', '')).lower() for term in ['urgent', 'immediate', 'crypto', 'bitcoin', 'forex', 'gambling'])"
                    },
                    # Non-KYC transaction
                    {
                        'id': 'non_kyc_transaction',
                        'name': 'Non-KYC Transaction',
                        'description': 'Transaction from an account with incomplete or expired KYC',
                        'risk': 'HIGH',
                        'condition': "str(t.get('sender_kyc_status', '')).lower() in ['incomplete', 'expired', 'pending', 'rejected']"
                    },
                    # Unusual transaction time
                    {
                        'id': 'unusual_transaction_time',
                        'name': 'Unusual Transaction Time',
                        'description': 'Transaction occurred during non-business hours',
                        'risk': 'MEDIUM',
                        'condition': "'time' in t and t['time'] and isinstance(t['time'], str) and ':' in t['time'] and int(t['time'].split(':')[0]) not in range(9, 18)"
                    },
                    # High-risk transaction type
                    {
                        'id': 'high_risk_transaction_type',
                        'name': 'High-Risk Transaction Type',
                        'description': 'Transaction type is considered high-risk',
                        'risk': 'HIGH',
                        'condition': "str(t.get('transaction_type', '')).lower() in ['offshore', 'crypto', 'forex', 'gambling']"
                    }
                ]
                
                # Add additional rules to compliance_rules
                compliance_rules.extend(additional_rules)
                
                # Log the rules being used
                logging.debug(f"\n{'='*50}\nActive Compliance Rules:\n" + 
                             '\n'.join([f"- {r['name']} (Risk: {r.get('risk', 'MEDIUM')}): {r['description']}" 
                                      for r in compliance_rules]) + 
                             f"\n{'='*50}")
                
                # First, normalize all transaction data keys to lowercase for consistent access
                normalized_transaction = {str(k).lower(): v for k, v in transaction_data.items()}
                
                # Log the normalized transaction data for debugging
                logging.debug(f"\n{'='*50}\nProcessing transaction {idx + 1} with data:\n{json.dumps(normalized_transaction, indent=2, default=str)}\n{'='*50}")
                
                # Add a debug log for the high-value transaction check
                amount = float(normalized_transaction.get('amount', 0))
                logging.debug(f"Checking high-value transaction: Amount = {amount}, Type = {type(amount).__name__}")
                
                # Compile the condition strings into callable functions
                for rule in compliance_rules:
                    if 'condition' in rule and isinstance(rule['condition'], str):
                        try:
                            # Replace field names in condition to use lowercase
                            condition = rule['condition']
                            # Handle common field name variations
                            condition = condition.replace('Amount', 'amount')
                            condition = condition.replace('Transaction_Type', 'transaction_type')
                            condition = condition.replace('Sender_KYC_Status', 'sender_kyc_status')
                            # Safely evaluate the condition string into a function
                            rule['condition'] = eval(f"lambda t: {condition}")
                        except Exception as e:
                            logging.error(f"Error compiling condition for rule {rule.get('id', 'unknown')}: {str(e)}")
                            # If there's an error, make the condition always return False
                            rule['condition'] = lambda t: False
                
                # Check each rule against the transaction
                for rule in compliance_rules:
                    try:
                        # Skip KYC-related rules if KYC status is empty
                        if 'kyc' in rule.get('name', '').lower() and not normalized_transaction.get('sender_kyc_status'):
                            logging.debug(f"Skipping {rule.get('name')} - KYC status is empty")
                            continue
                            
                        # Log the rule being checked and its condition for debugging
                        rule_name = rule.get('name', 'Unnamed Rule')
                        condition = rule.get('condition', 'No condition')
                        logging.debug(f"\n{'='*30}\nChecking rule: {rule_name}\nCondition: {condition}")
                        
                        # Log the specific values being checked
                        if 'amount' in str(condition):
                            logging.debug(f"Amount check - Value: {normalized_transaction.get('amount', 'N/A')}, Type: {type(normalized_transaction.get('amount')).__name__}")
                        if 'transaction_type' in str(condition):
                            logging.debug(f"Transaction Type: {normalized_transaction.get('transaction_type', 'N/A')}")
                        if 'sender_kyc_status' in str(condition):
                            logging.debug(f"Sender KYC Status: '{normalized_transaction.get('sender_kyc_status', 'N/A')}'")
                        
                        # Apply the condition to the normalized transaction data
                        try:
                            condition_func = rule['condition']
                            if not callable(condition_func):
                                logging.warning(f"Rule '{rule_name}' condition is not callable")
                                continue
                                
                            if condition_func(normalized_transaction):
                                matched_rules.append(rule['name'])
                                risk_level = rule.get('risk_level', rule.get('risk', 'MEDIUM'))
                                
                                # Only include valid violations (non-empty KYC status for KYC rules)
                                if 'kyc' in rule.get('name', '').lower() and not normalized_transaction.get('sender_kyc_status'):
                                    logging.debug(f"Skipping violation addition for {rule['name']} - Empty KYC status")
                                    continue
                                    
                                violation_details.append({
                                    'violation_type': rule['name'],
                                    'legal_provision': 'RBI Master Direction',
                                    'circular': 'RBI/2022-23/123',
                                    'penalty_min': 10000 if risk_level in ['HIGH', 'CRITICAL'] else 5000,
                                    'penalty_max': 100000 if risk_level in ['HIGH', 'CRITICAL'] else 50000,
                                    'reason': f"{rule.get('description', '')} - {risk_level} risk"
                                })
                                
                                # Log the successful rule match
                                logging.info(f"✅ Rule matched for transaction {idx + 1}: {rule_name} (Risk: {risk_level})")
                                if violation_details:  # Only log if we have details to show
                                    logging.debug(f"Violation details: {violation_details[-1]}")
                                    
                        except Exception as e:
                            logging.error(f"Error applying rule {rule.get('name', 'unknown')}: {str(e)}")
                            logging.error(f"Rule details: {rule}")
                            logging.error(f"Transaction data: {normalized_transaction}")
                            continue
                            
                    except Exception as e:
                        logging.error(f"Error processing rule {rule.get('name', 'unknown')}: {str(e)}")
                        continue
                            
                # Process based on whether we found any rule matches
                if matched_rules:
                    # Get the highest risk level from matched rules
                    logging.info(f"⚠️  Rule violations found for transaction {idx + 1}: Found {len(matched_rules)} rule violation(s): {', '.join(matched_rules)}")
                    matched_rule_objects = [r for r in compliance_rules if r['name'] in matched_rules]
                    risk_level = max((rule['risk'] for rule in matched_rule_objects),
                                  key=lambda x: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'].index(x))
                    
                    analysis_result = {
                        'matched_rules': matched_rules,
                        'explanation': f"Found {len(matched_rules)} rule violation(s): {', '.join(matched_rules)}",
                        'risk_level': risk_level,
                        'violation_details': violation_details,
                        'matched_rule_details': [
                            {
                                'rule_id': rule['id'],
                                'rule_name': rule['name'],
                                'description': rule['description'],
                                'risk': rule['risk']
                            } for rule in matched_rule_objects
                        ]
                    }
                    logging.info(f"⚠️  Rule violations found for transaction {idx + 1}: {analysis_result['explanation']}")
                
                # Build transaction record with all fields
                transaction_record = {
                    # Standard fields
                    'transaction_id': str(transaction_data.get('transaction_id', '')).strip() or f"TXN_{idx + 1}",
                    'date': str(transaction_data.get('date', '')).strip() or '',
                    'time': str(transaction_data.get('time', '')).strip() or '',
                    'timestamp': str(transaction_data.get('timestamp', '')).strip() or '',
                    
                    # Sender information
                    'sender': str(transaction_data.get('sender_name', transaction_data.get('sender', ''))).strip(),
                    'sender_name': str(transaction_data.get('sender_name', '')).strip() or '',
                    'sender_account': str(transaction_data.get('sender_account', '')).strip() or '',
                    'sender_kyc_status': str(transaction_data.get('sender_kyc_status', '')).strip() or '',
                    
                    # Receiver information
                    'receiver': str(transaction_data.get('receiver_name', transaction_data.get('receiver', ''))).strip(),
                    'receiver_name': str(transaction_data.get('receiver_name', '')).strip() or '',
                    'receiver_account': str(transaction_data.get('receiver_account', '')).strip() or '',
                    
                    # Transaction details
                    'amount': transaction_data.get('amount', ''),
                    'transaction_type': str(transaction_data.get('transaction_type', '')).strip() or '',
                    'transaction_mode': str(transaction_data.get('transaction_mode', '')).strip() or '',
                    'description': transaction_text,
                    'balance': transaction_data.get('balance', ''),
                    'balance_after': transaction_data.get('balance_after', ''),
                    'currency': str(transaction_data.get('currency', 'INR')).strip(),
                    'channel': str(transaction_data.get('channel', '')).strip() or '',
                    'reference_number': str(transaction_data.get('reference_number', '')).strip() or '',
                    'location': str(transaction_data.get('location', '')).strip() or '',
                    'branch_code': str(transaction_data.get('branch_code', '')).strip() or '',
                    'branch_location': str(transaction_data.get('branch_location', '')).strip() or '',
                    
                    # Violation information
                    'has_violation': bool(matched_rules) or (analysis_result.get('matched_rules', ['No Violation'])[0] != 'No Violation'),
                    'violation_type': ', '.join(matched_rules or analysis_result.get('matched_rules', [])),
                    'violation_details': violation_details or [{
                        'violation_type': ', '.join(analysis_result.get('matched_rules', [])),
                        'legal_provision': 'RBI Master Direction',
                        'circular': 'RBI/2022-23/123',
                        'penalty_min': None,
                        'penalty_max': None,
                        'reason': analysis_result.get('explanation', '')
                    }],
                    
                    # Rule matching information
                    'matched_rules': [
                        {
                            'rule_id': rule['id'],
                            'rule_name': rule['name'],
                            'description': rule['description'],
                            'risk': rule['risk']
                        } for rule in compliance_rules if rule['name'] in matched_rules
                    ] if matched_rules else [],
                    
                    # AI analysis results
                    'ai_analysis': analysis_result if not matched_rules else None,
                    'risk_level': analysis_result.get('risk_level', 'LOW'),
                    
                    # Include all original data for reference
                    'raw_data': {k: str(v) for k, v in transaction_data.items() if v is not None and str(v).strip() != ''}
                }
                
                transactions.append(transaction_record)
                
                # Add a small delay between transactions to avoid rate limiting
                time.sleep(1)  # 1 second delay between transactions
                    
            except Exception as e:
                error_msg = str(e)
                logging.error(f"Error analyzing transaction {idx + 1}: {error_msg}")
                
                # Add the transaction with error details
                transactions.append({
                    'transaction_id': str(transaction_data.get('transaction_id', f"TXN_{idx + 1}")),
                    'date': str(transaction_data.get('date', '')),
                    'sender': str(transaction_data.get('sender_name', transaction_data.get('sender', ''))),
                    'sender_account': str(transaction_data.get('sender_account', '')),
                    'receiver': str(transaction_data.get('receiver_name', transaction_data.get('receiver', ''))),
                    'receiver_account': str(transaction_data.get('receiver_account', '')),
                    'amount': transaction_data.get('amount', ''),
                    'transaction_type': str(transaction_data.get('transaction_type', '')),
                    'description': transaction_text,
                    'balance': transaction_data.get('balance', ''),
                    'has_violation': True,
                    'violation_type': 'Processing Error',
                    'violation_details': [{
                        'violation_type': 'Processing Error',
                        'legal_provision': 'N/A',
                        'circular': 'N/A',
                        'penalty_min': None,
                        'penalty_max': None,
                        'reason': f'Error during analysis: {error_msg}'
                    }],
                    'ai_analysis': {
                        'transaction': transaction_text,
                        'matched_rules': ['Processing Error'],
                        'explanation': f'Error during analysis: {error_msg}'
                    }
                })
                continue
        
        # Count violations
        violations_found = len([t for t in transactions if t.get('has_violation', False)])
        
        response = {
            'success': True,
            'transactions': transactions,
            'kyc_violations': [],  # Empty for compatibility with existing UI
            'total_transactions': total_rows,
            'violation_transactions': violations_found,
            'total_accounts': len({t.get('sender_account') for t in transactions if t.get('sender_account')}),
            'matched_accounts': violations_found,
            'customers_with_violations': len({t.get('sender_account') for t in transactions if t.get('has_violation', False)})
        }
        
        logging.info(f"Successfully analyzed {len(transactions)} transactions, found {violations_found} violations")
        return _clean_for_json(response)
        
    except Exception as e:
        logging.exception('Error processing Excel file')
        return {'error': f'Error processing Excel file: {str(e)}'}

@bp.route('/api/excel/process', methods=['POST'])
def api_excel_process():
    payload = request.get_json(silent=True) or {}
    filename = payload.get('filename')
    logging.info(f'Received Excel process request for filename: {filename}')
    
    if not filename:
        return jsonify({'error': 'No filename provided'}), 400
        
    filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    # Use the new Excel processing function
    result = _process_excel_file(filepath)
    
    if 'error' in result:
        return jsonify({
            'success': False,
            'error': result['error']
        }), 400
        
    try:
        return jsonify({
            'success': True,
            'results': {
                'transactions': result.get('transactions', []),
                'kyc_violations': result.get('kyc_violations', []),
                'summary': {
                    'total_transactions': result.get('total_transactions', 0),
                    'violation_transactions': result.get('violation_transactions', 0),
                    'total_accounts': result.get('total_accounts', 0),
                    'matched_accounts': result.get('matched_accounts', 0),
                    'customers_with_violations': result.get('customers_with_violations', 0)
                }
            },
            'message': f"Found {result.get('violation_transactions', 0)} violation transactions across {result.get('matched_accounts', 0)} accounts"
        })
    finally:
        pass