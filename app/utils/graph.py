from __future__ import annotations
import os
import logging
from typing import Dict, Any, Optional, List

from neo4j import GraphDatabase, Driver


class Neo4jClient:
    """Thin wrapper around neo4j.Driver with convenience upsert for violations.

    If required environment variables are missing, the client is disabled and
    calls become no-ops to avoid breaking the existing application flow.
    """

    def __init__(self, uri: Optional[str], user: Optional[str], password: Optional[str], database: Optional[str] = None):
        self._enabled = bool(uri and user and password)
        self._driver: Optional[Driver] = None
        self._database = database or 'neo4j'  # Default to 'neo4j' if not specified
        if self._enabled:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
            
    def get_session(self):
        """Get a new database session with the configured database name."""
        if not self._driver:
            raise RuntimeError("Driver not initialized")
        return self._driver.session(database=self._database)

    @property
    def enabled(self) -> bool:
        return self._enabled and self._driver is not None

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            
    def initialize_schema(self) -> None:
        """Initialize the Neo4j database schema with required constraints and indexes."""
        if not self._enabled or not self._driver:
            return
            
        with self._driver.session(database=self._database) as session:
            # Create constraints for uniqueness
            session.run("""
                CREATE CONSTRAINT account_number IF NOT EXISTS 
                FOR (a:Account) REQUIRE a.number IS UNIQUE;
                
                CREATE CONSTRAINT violation_id IF NOT EXISTS
                FOR (v:Violation) REQUIRE v.id IS UNIQUE;
                
                CREATE CONSTRAINT person_id IF NOT EXISTS
                FOR (p:Person) REQUIRE p.id IS UNIQUE;
                
                CREATE INDEX transaction_id IF NOT EXISTS
                FOR (t:Transaction) ON (t.transaction_id);
                
                CREATE INDEX account_number_idx IF NOT EXISTS
                FOR (a:Account) ON (a.number);
            """)

    def upsert_violation(self, record: Dict[str, Any]) -> None:
        if not self._enabled or not self._driver:
            logging.info("Neo4j not enabled or driver not initialized. Skipping write.")
            return
        has_penalty = (
            record.get("penMin") is not None or record.get("penMax") is not None
        )
        if has_penalty: 
            cypher = (
                """
                MERGE (c:Circular {name: $circular})
                MERGE (v:Violation {slNo: $slNo, page: toInteger($page)})
                  ON CREATE SET v.type = $violationType
                  ON MATCH  SET v.type = coalesce($violationType, v.type)
                MERGE (p:PenaltyRange {min: $penMin, max: $penMax, currency: $currency})
                MERGE (l:LegalProvision {text: coalesce($legal, '')})
                MERGE (r:Reason {text: coalesce($reason, '')})
                MERGE (c)-[:HAS_VIOLATION]->(v)
                MERGE (v)-[:PENALTY_IN_RANGE]->(p)
                MERGE (v)-[:INVOKES]->(l)
                MERGE (v)-[:HAS_REASON]->(r)
                """
            )
        else:
            cypher = (
                """
                MERGE (c:Circular {name: $circular})
                MERGE (v:Violation {slNo: $slNo, page: toInteger($page)})
                  ON CREATE SET v.type = $violationType
                  ON MATCH  SET v.type = coalesce($violationType, v.type)
                MERGE (l:LegalProvision {text: coalesce($legal, '')})
                MERGE (r:Reason {text: coalesce($reason, '')})
                MERGE (c)-[:HAS_VIOLATION]->(v)
                MERGE (v)-[:INVOKES]->(l)
                MERGE (v)-[:HAS_REASON]->(r)
                """
            )
        logging.info(f"Cypher parameters for upsert_violation: {record}")
        with self._driver.session(database=self._database) as session:
            session.execute_write(lambda tx: tx.run(cypher, **record))


def get_client_from_env() -> Neo4jClient:
    uri = os.getenv("NEO4J_URI")
    # Support both NEO4J_USER and NEO4J_USERNAME
    user = os.getenv("NEO4J_USER") or os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE")
    return Neo4jClient(uri, user, password, database)



def find_violations_for_transaction(client: Neo4jClient, account_number: Optional[str], description: str) -> List[Dict[str, Any]]:
    """Attempt to find violations and associated persons for a given transaction description.

    Tries a couple of generic patterns to accommodate unknown graph schemas.
    Returns a list of dicts with keys: violationType, personName, personId, personEmail, personPhone.
    """
    if not client.enabled:
        logging.info("Neo4j not enabled; returning empty matches.")
        return []
    results: List[Dict[str, Any]] = []
    desc = description or ""
    acct = account_number or None
    try:
        with client._driver.session(database=client._database) as session:
            # Pattern 1: Match by violation type text similarity/contains
            query1 = (
                """
                MATCH (v:Violation)
                WHERE toLower(v.type) CONTAINS toLower($desc)
                OPTIONAL MATCH (p:Person)-[:RESPONSIBLE_FOR|ASSOCIATED_WITH|INVOLVED_IN*1..2]->(v)
                RETURN v.type AS violationType,
                       p.name AS personName,
                       p.id AS personId,
                       p.email AS personEmail,
                       p.phone AS personPhone
                LIMIT 5
                """
            )
            for r in session.run(query1, desc=desc):
                results.append(dict(r))

            # Pattern 2: If account is available, try to find via Account->Transaction linkage
            if acct:
                query2 = (
                    """
                    MATCH (v:Violation)
                    OPTIONAL MATCH (a:Account {number: $acct})-[:HAS_TRANSACTION|ASSOCIATED_WITH*0..2]->(t:Transaction)
                    WHERE toLower(t.description) CONTAINS toLower($desc)
                       OR toLower(v.type) CONTAINS toLower($desc)
                    OPTIONAL MATCH (p:Person)-[:OWNS|ASSOCIATED_WITH]->(a)
                    RETURN DISTINCT v.type AS violationType,
                                    p.name AS personName,
                                    p.id AS personId,
                                    p.email AS personEmail,
                                    p.phone AS personPhone
                    LIMIT 5
                    """
                )
                for r in session.run(query2, acct=acct, desc=desc):
                    results.append(dict(r))
    except Exception as e:
        logging.error(f"Neo4j find_violations_for_transaction error: {e}")
        return []

    # Deduplicate by (violationType, personName, personId)
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for item in results:
        key = (item.get('violationType'), item.get('personName'), item.get('personId'))
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def find_violations_by_account(client: Neo4jClient, account_number: str) -> List[Dict[str, Any]]:
    """Find violations associated with a specific account number.
    
    Args:
        client: Neo4j client instance
        account_number: The account number to search for
        
    Returns:
        List of dicts containing violation details with keys: 
        violationType, legalProvision, circular, penMin, penMax, reason,
        personName, personId, personEmail, personPhone
    """
    if not client.enabled or not account_number:
        return []
        
    query = """
    MATCH (a:Account {number: $account_number})-[:HAS_VIOLATION]->(v:Violation)
    OPTIONAL MATCH (v)-[:PENALTY_IN_RANGE]->(p:PenaltyRange)
    OPTIONAL MATCH (v)-[:INVOKES]->(l:LegalProvision)
    OPTIONAL MATCH (v)-[:IN_CIRCULAR]->(c:Circular)
    OPTIONAL MATCH (v)-[:HAS_REASON]->(r:Reason)
    OPTIONAL MATCH (v)-[:VIOLATED_BY]->(per:Person)
    RETURN DISTINCT
        v.type as violationType,
        l.text as legalProvision,
        c.name as circular,
        p.min as penMin,
        p.max as penMax,
        r.text as reason,
        per.name as personName,
        per.id as personId,
        per.email as personEmail,
        per.phone as personPhone
    """
    
    try:
        with client._driver.session() as session:
            result = session.run(query, account_number=account_number)
            return [dict(record) for record in result]
    except Exception as e:
        logging.error(f"Error querying violations by account: {e}")
        return []


def process_kyc_data(client: Neo4jClient, kyc_data: List[Dict[str, Any]]) -> None:
    """Process KYC data and load into Neo4j.
    
    Args:
        client: Neo4j client instance
        kyc_data: List of dictionaries containing KYC data with keys:
            - account_number: The account number
            - customer_name: Name of the customer
            - violation_type: Type of violation
            - kyc_verified: KYC verification status
            - transaction_id: Related transaction ID
            - date: Date of the violation
    """
    if not client.enabled or not client._driver:
        return

    with client._driver.session(database=client._database) as session:
        for record in kyc_data:
            session.execute_write(_create_kyc_violation, record)

def _create_kyc_violation(tx, record):
    query = """
    // Create or update account
    MERGE (a:Account {number: $account_number})
    SET a.name = $customer_name,
        a.kyc_verified = $kyc_verified,
        a.last_updated = datetime()
    
    // Create violation if it doesn't exist
    MERGE (v:Violation {id: $transaction_id})
    SET v.type = $violation_type,
        v.date = date($date),
        v.status = 'ACTIVE',
        v.last_updated = datetime()
    
    // Create relationship between account and violation
    MERGE (a)-[r:HAS_VIOLATION]->(v)
    SET r.detected_date = date($date)
    
    // Create person node if name is available
    WITH a, v
    WHERE $customer_name IS NOT NULL
    MERGE (p:Person {id: 'P' + $account_number})
    SET p.name = $customer_name,
        p.last_updated = datetime()
    
    // Create relationship between violation and person
    WITH v, p
    MERGE (v)-[vp:VIOLATED_BY]->(p)
    SET vp.since = date()
    """
    tx.run(query, 
           account_number=record.get('account_number'),
           customer_name=record.get('customer_name'),
           kyc_verified=record.get('kyc_verified', 'No'),
           transaction_id=record.get('transaction_id'),
           violation_type=record.get('violation_type'),
           date=record.get('date'))


def find_violations_by_type(client: Neo4jClient, violation_type_text: str) -> List[Dict[str, Any]]:
    """Find violations by type text and return associated legal provisions and person details.

    Returns list of dicts with keys: violationType, legalProvision, personName, personId, personEmail, personPhone.
    """
    if not client.enabled:
        logging.info("Neo4j not enabled; returning empty matches.")
        return []
    if not violation_type_text:
        return []
    results: List[Dict[str, Any]] = []
    try:
        with client._driver.session(database=client._database) as session:
            query = (
                """
                MATCH (v:Violation)
                WHERE toLower(v.type) CONTAINS toLower($vtype)
                   OR toLower($vtype) CONTAINS toLower(v.type)
                OPTIONAL MATCH (v)-[:INVOKES]->(l:LegalProvision)
                OPTIONAL MATCH (p:Person)-[:RESPONSIBLE_FOR|:ASSOCIATED_WITH|:INVOLVED_IN*1..2]->(v)
                RETURN DISTINCT v.type AS violationType,
                                coalesce(l.text, l.name, '') AS legalProvision,
                                p.name AS personName,
                                p.id AS personId,
                                p.email AS personEmail,
                                p.phone AS personPhone
                LIMIT 10
                """
            )
            for r in session.run(query, vtype=violation_type_text):
                results.append(dict(r))
    except Exception as e:
        logging.error(f"Neo4j find_violations_by_type error: {e}")
        return []

    # Deduplicate
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for item in results:
        key = (item.get('violationType'), item.get('legalProvision'), item.get('personName'), item.get('personId'))
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def get_compliance_rules(client: Neo4jClient) -> List[Dict[str, Any]]:
    """Fetch all compliance rules from the database.
    
    Args:
        client: Neo4j client instance
        
    Returns:
        List of dictionaries containing rule details with keys:
        - id: Rule identifier
        - name: Rule name
        - description: Detailed description of the rule
        - risk: Risk level (LOW, MEDIUM, HIGH, CRITICAL)
        - condition: Condition to evaluate (if applicable)
    """
    if not client.enabled or not client._driver:
        return []
        
    query = """
    MATCH (r:ComplianceRule)
    RETURN r.id as id,
           r.name as name,
           r.description as description,
           r.risk as risk,
           r.condition as condition
    ORDER BY r.id
    """
    
    try:
        with client._driver.session(database=client._database) as session:
            result = session.run(query)
            return [dict(record) for record in result]
    except Exception as e:
        logging.error(f"Error fetching compliance rules: {str(e)}")
        return []


def initialize_compliance_rules(client: Neo4jClient) -> None:
    """Initialize database constraints for compliance rules."""
    if not client.enabled or not client._driver:
        logging.info("Neo4j client not enabled, skipping compliance rules initialization")
        return
        
    # Create a constraint for rule IDs if it doesn't exist
    constraint_query = """
    CREATE CONSTRAINT compliance_rule_id IF NOT EXISTS
    FOR (r:ComplianceRule) REQUIRE r.id IS UNIQUE;
    """
    
    try:
        with client._driver.session(database=client._database) as session:
            session.run(constraint_query)
            logging.info("Successfully created compliance rule constraints")
    except Exception as e:
        logging.error(f"Error initializing compliance rule constraints: {str(e)}")
        logging.warning("Compliance rules initialization failed, but application will continue")
