"""
Enhanced transaction analysis for KYC and compliance rule checking.
"""
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
import re

class TransactionAnalyzer:
    """Analyzes transactions for KYC and compliance violations."""
    
    def __init__(self, neo4j_client):
        """Initialize with a Neo4j client."""
        self.neo4j = neo4j_client
        self.monthly_threshold = 30000  # Default monthly threshold in INR
        
    def analyze_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a transaction for compliance with KYC and other rules.
        
        Args:
            transaction: Dictionary containing transaction details
            
        Returns:
            Dictionary with analysis results
        """
        # Basic validation
        if not transaction or 'account_number' not in transaction:
            return {"error": "Invalid transaction data"}
            
        account_number = transaction['account_number']
        amount = float(transaction.get('amount', 0))
        
        # Check for KYC violations first
        kyc_violation = self._check_kyc_violation(account_number)
        if kyc_violation:
            return kyc_violation
            
        # Check for monthly threshold violations
        monthly_analysis = self._check_monthly_threshold(account_number, amount, transaction.get('date'))
        if monthly_analysis:
            return monthly_analysis
            
        # Check for suspicious patterns
        suspicious_activity = self._check_suspicious_patterns(transaction)
        if suspicious_activity:
            return suspicious_activity
            
        return {
            "transaction_id": transaction.get('transaction_id'),
            "status": "Compliant",
            "message": "No violations detected"
        }
        
    def _check_kyc_violation(self, account_number: str) -> Optional[Dict[str, Any]]:
        """Check for existing KYC violations for an account."""
        try:
            query = """
            MATCH (a:Account {number: $account_number})-[:HAS_VIOLATION]->(v:Violation)
            WHERE v.type CONTAINS 'KYC' AND v.status = 'ACTIVE'
            RETURN v.type as violation_type, v.date as violation_date
            LIMIT 1
            """
            with self.neo4j._driver.session() as session:
                result = session.run(query, account_number=account_number)
                record = result.single()
                
                if record:
                    return {
                        "transaction_id": None,
                        "violation_type": record["violation_type"],
                        "status": "Violation",
                        "severity": "HIGH",
                        "explanation": "Customer KYC is incomplete or invalid - HIGH risk",
                        "rule": "Non-compliance with KYC norms"
                    }
        except Exception as e:
            logging.error(f"Error checking KYC violations: {str(e)}")
            
        return None
        
    def _check_monthly_threshold(self, account_number: str, amount: float, date_str: str = None) -> Optional[Dict[str, Any]]:
        """Check if transaction exceeds monthly threshold."""
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
            
        try:
            # Parse the transaction date
            tx_date = datetime.strptime(date_str, '%Y-%m-%d')
            month_start = tx_date.replace(day=1).strftime('%Y-%m-%d')
            month_end = (tx_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            month_end = month_end.strftime('%Y-%m-%d')
            
            # Get total transactions for the month
            query = """
            MATCH (t:Transaction)-[:MADE_BY]->(a:Account {number: $account_number})
            WHERE t.date >= date($month_start) AND t.date <= date($month_end)
            RETURN COALESCE(SUM(toFloat(t.amount)), 0) as monthly_total
            """
            
            with self.neo4j._driver.session() as session:
                result = session.run(
                    query, 
                    account_number=account_number,
                    month_start=month_start,
                    month_end=month_end
                )
                monthly_total = result.single()["monthly_total"] or 0
                
                # Add current transaction amount
                monthly_total += amount
                
                if monthly_total > self.monthly_threshold:
                    return {
                        "transaction_id": None,
                        "violation_type": "MONTHLY_THRESHOLD_EXCEEDED",
                        "status": "Warning",
                        "severity": "MEDIUM",
                        "explanation": f"Customer exceeded the monthly transaction threshold by transacting "
                                    f"₹{monthly_total:,.2f} (threshold: ₹{self.monthly_threshold:,.2f}).",
                        "rule": "Breach of digital lending norms"
                    }
                    
        except Exception as e:
            logging.error(f"Error checking monthly threshold: {str(e)}")
            
        return None
        
    def _check_suspicious_patterns(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check for suspicious transaction patterns."""
        amount = float(transaction.get('amount', 0))
        description = (transaction.get('description') or '').lower()
        
        # Check for round number transactions (potential structuring)
        if amount % 10000 == 0 and amount > 0:
            return {
                "transaction_id": transaction.get('transaction_id'),
                "violation_type": "SUSPICIOUS_ROUND_AMOUNT",
                "status": "Warning",
                "severity": "LOW",
                "explanation": f"Round number transaction of ₹{amount:,.2f} detected. "
                             "This could indicate potential structuring behavior.",
                "rule": "Suspicious Transaction Monitoring"
            }
            
        # Check for high-value transactions
        if amount > 100000:  # 1 lakh threshold
            return {
                "transaction_id": transaction.get('transaction_id'),
                "violation_type": "HIGH_VALUE_TRANSACTION",
                "status": "Alert",
                "severity": "HIGH",
                "explanation": f"High-value transaction of ₹{amount:,.2f} detected. "
                             "This may require additional verification.",
                "rule": "High-Value Transaction Monitoring"
            }
            
        # Check for suspicious keywords in description
        suspicious_keywords = [
            'gambling', 'casino', 'bet', 'lottery', 'crypto', 'bitcoin',
            'forex', 'offshore', 'anonymous', 'prepaid card', 'gift card'
        ]
        
        for keyword in suspicious_keywords:
            if keyword in description:
                return {
                    "transaction_id": transaction.get('transaction_id'),
                    "violation_type": "SUSPICIOUS_KEYWORD_DETECTED",
                    "status": "Warning",
                    "severity": "MEDIUM",
                    "explanation": f"Suspicious keyword '{keyword}' found in transaction description.",
                    "rule": "Suspicious Transaction Monitoring"
                }
                
        return None

    def get_transaction_history(self, account_number: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get recent transaction history for an account."""
        try:
            query = """
            MATCH (t:Transaction)-[:MADE_BY]->(a:Account {number: $account_number})
            WHERE t.date >= date() - duration('P' + $days + 'D')
            RETURN t.transaction_id as transaction_id,
                   t.amount as amount,
                   t.date as date,
                   t.description as description,
                   t.type as type
            ORDER BY t.date DESC
            """
            
            with self.neo4j._driver.session() as session:
                result = session.run(query, account_number=account_number, days=str(days))
                return [dict(record) for record in result]
                
        except Exception as e:
            logging.error(f"Error fetching transaction history: {str(e)}")
            return []

    def is_high_risk_customer(self, account_number: str) -> bool:
        """Check if a customer is high risk based on past violations."""
        try:
            query = """
            MATCH (a:Account {number: $account_number})-[:HAS_VIOLATION]->(v:Violation)
            WHERE v.status = 'ACTIVE' AND v.severity = 'HIGH'
            RETURN count(v) > 0 as is_high_risk
            """
            with self.neo4j._driver.session() as session:
                result = session.run(query, account_number=account_number)
                return result.single()["is_high_risk"]
                
        except Exception as e:
            logging.error(f"Error checking customer risk: {str(e)}")
            return False
