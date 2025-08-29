"""
Transaction processing module for handling transaction data and linking to violations.
"""
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import logging
from .graph import Neo4jClient

def process_transaction_data(client: Neo4jClient, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process transaction data and link with existing violations.
    
    Args:
        client: Neo4j client instance
        transactions: List of transaction dictionaries with keys:
            - transaction_id: Unique transaction ID
            - account_number: Sender's account number
            - amount: Transaction amount
            - date: Transaction date
            - description: Transaction description
            - transaction_type: Type of transaction
            
    Returns:
        List of dictionaries with matching results
    """
    if not client.enabled or not hasattr(client, '_driver') or not client._driver:
        return []

    results = []
    with client._driver.session(database=getattr(client, '_database', None)) as session:
        for tx_data in transactions:
            try:
                result = session.write_transaction(_process_single_transaction, tx_data)
                if result:
                    results.extend(result)
            except Exception as e:
                logging.error(f"Error processing transaction {tx_data.get('transaction_id')}: {e}")
    return results

def _process_single_transaction(tx, tx_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Process a single transaction and link it to any matching violations.
    
    Args:
        tx: Neo4j transaction object
        tx_data: Transaction data dictionary
        
    Returns:
        List of dictionaries with matching results
    """
    query = """
    // Create or update the transaction
    MERGE (t:Transaction {transaction_id: $transaction_id})
    SET t.amount = toFloat($amount),
        t.date = date($date),
        t.description = $description,
        t.type = $transaction_type,
        t.last_updated = datetime()
    
    // Find or create the account
    MERGE (a:Account {number: $account_number})
    
    // Create relationship between account and transaction
    MERGE (a)-[r:MADE_TRANSACTION]->(t)
    SET r.timestamp = datetime()
    
    // Find any violations for this account
    WITH a, t
    MATCH (a)-[:HAS_VIOLATION]->(v:Violation)
    WHERE v.status = 'ACTIVE' AND 
          (v.date IS NULL OR date($date) >= v.date)
    
    // Create relationship between transaction and violation
    MERGE (t)-[rv:RELATED_TO_VIOLATION]->(v)
    SET rv.matched_at = datetime(),
        rv.matched_by = 'SYSTEM'
    
    // Return transaction and violation details
    RETURN t.transaction_id as tx_id, 
           v.id as violation_id,
           v.type as violation_type
    """
    
    result = tx.run(query,
                   transaction_id=tx_data.get('transaction_id'),
                   account_number=tx_data.get('account_number'),
                   amount=float(tx_data.get('amount', 0)),
                   date=tx_data.get('date'),
                   description=tx_data.get('description', ''),
                   transaction_type=tx_data.get('transaction_type', 'UNKNOWN'))
    
    return [dict(record) for record in result]

def get_transaction_details(client: Neo4jClient, transaction_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed information about a specific transaction and its related violations.
    
    Args:
        client: Neo4j client instance
        transaction_id: The transaction ID to look up
        
    Returns:
        Dictionary with transaction and related violation details, or None if not found
    """
    if not client.enabled or not hasattr(client, '_driver') or not client._driver:
        return None

    query = """
    MATCH (t:Transaction {transaction_id: $transaction_id})
    OPTIONAL MATCH (t)-[:RELATED_TO_VIOLATION]->(v:Violation)
    OPTIONAL MATCH (v)-[:PENALTY_IN_RANGE]->(p:PenaltyRange)
    RETURN t as transaction, v as violation, p as penalty
    """
    
    with client._driver.session(database=getattr(client, '_database', None)) as session:
        result = session.run(query, transaction_id=transaction_id)
        record = result.single()
        
        if not record or not record['transaction']:
            return None
            
        transaction = dict(record['transaction'].items())
        violation = dict(record['violation'].items()) if record['violation'] else None
        penalty = dict(record['penalty'].items()) if record['penalty'] else None
        
        return {
            'transaction': transaction,
            'violation': violation,
            'penalty': penalty
        }

def find_transactions_by_account(transactions_df: pd.DataFrame, account_number: str) -> List[Dict[str, Any]]:
    """Find all transactions for a given account number.
    
    Args:
        transactions_df: DataFrame containing transaction data
        account_number: The account number to search for
        
    Returns:
        List of transaction records matching the account number
    """
    if transactions_df is None or 'Sender Account' not in transactions_df.columns:
        return []
        
    # Convert account number to string for comparison
    account_str = str(account_number).strip()
    
    # Find matching transactions (case-insensitive and strip whitespace)
    mask = transactions_df['Sender Account'].astype(str).str.strip().str.lower() == account_str.lower()
    matching_transactions = transactions_df[mask]
    
    return matching_transactions.to_dict('records')

def process_excel_sheets(excel_file: str) -> Dict[str, Union[pd.DataFrame, Dict]]:
    """Process all sheets in an Excel file and return a dictionary of DataFrames.
    
    Args:
        excel_file: Path to the Excel file
        
    Returns:
        Dictionary with sheet names as keys and DataFrames as values
    """
    try:
        xls = pd.ExcelFile(excel_file)
        sheets = {}
        
        for sheet_name in xls.sheet_names:
            # Read each sheet into a DataFrame
            df = pd.read_excel(xls, sheet_name=sheet_name)
            sheets[sheet_name] = df
            
        return sheets
    except Exception as e:
        print(f"Error processing Excel file: {e}")
        return {}

def get_transaction_details(transaction_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed transaction information including related entities.
    
    Args:
        transaction_id: The ID of the transaction to look up
        
    Returns:
        Dictionary containing transaction details and related entities
    """
    try:
        # This should be implemented to fetch data from your database
        # The following is a placeholder implementation
        result = {}  # Replace with actual database query
        
        if not result:
            return None
            
        return {
            'transaction': dict(result.get('t', {})),
            'account': dict(result.get('a', {})),
            'violation': dict(result.get('v', {})),
            'person': dict(result.get('p', {})),
            'penalty': dict(result.get('pen', {})),
            'relationships': {
                'made_transaction': dict(result.get('mt', {})),
                'related_to_violation': dict(result.get('rtv', {})),
                'has_violation': dict(result.get('hv', {})),
                'violated_by': dict(result.get('vb', {})),
                'penalty_range': dict(result.get('pr', {}))
            }
        }
    except Exception as e:
        logging.error(f"Error getting transaction details for {transaction_id}: {e}")
        return None
