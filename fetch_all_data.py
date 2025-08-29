#!/usr/bin/env python3
"""
Script to fetch and display all data from the Neo4j database.
This script will help diagnose data availability issues.
"""
import os
import json
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Neo4jExplorer:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687")
        self.user = os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD", "")
        self.database = os.getenv("NEO4J_DATABASE", "rbi")
        self.driver = None

    def connect(self):
        """Establish connection to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver(
                self.uri, 
                auth=(self.user, self.password)
            )
            # Test the connection
            with self.driver.session(database=self.database) as session:
                result = session.run("RETURN 'Connected successfully' AS message")
                print("✅", result.single()["message"])
            return True
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            return False

    def get_node_counts(self):
        """Get count of nodes by label"""
        query = """
        MATCH (n)
        RETURN labels(n) AS labels, count(*) AS count
        ORDER BY count DESC
        """
        return self._run_query(query)

    def get_relationship_counts(self):
        """Get count of relationships by type"""
        query = """
        MATCH ()-[r]->()
        RETURN type(r) AS type, count(*) AS count
        ORDER BY count DESC
        """
        return self._run_query(query)

    def get_sample_nodes(self, label=None, limit=5):
        """Get sample nodes (optionally filtered by label)"""
        if label:
            query = f"""
            MATCH (n:`{label}`) 
            RETURN properties(n) AS node 
            LIMIT {limit}
            """
        else:
            query = f"""
            MATCH (n) 
            RETURN labels(n) AS labels, properties(n) AS node 
            LIMIT {limit}
            """
        return self._run_query(query)

    def get_schema(self):
        """Get database schema information"""
        query = """
        CALL db.schema.visualization()
        """
        return self._run_query(query)

    def _run_query(self, query, **params):
        """Execute a query and return results"""
        if not self.driver:
            return {"error": "Not connected to database"}
            
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(query, **params)
                return [dict(record) for record in result]
        except Exception as e:
            return {"error": str(e), "query": query}

    def close(self):
        """Close the database connection"""
        if self.driver:
            self.driver.close()
            print("\nDatabase connection closed.")

def print_section(title, data):
    """Print a section with formatted output"""
    print(f"\n{'='*50}")
    print(f"{title.upper()}")
    print("="*50)
    print(json.dumps(data, indent=2, default=str))

def main():
    print("🔍 Neo4j Database Explorer")
    print("="*50)
    
    explorer = Neo4jExplorer()
    
    if not explorer.connect():
        print("Failed to connect to the database. Please check your connection settings.")
        return
    
    try:
        # Get and display node counts
        node_counts = explorer.get_node_counts()
        print_section("Node Counts", node_counts)
        
        # Get and display relationship counts
        rel_counts = explorer.get_relationship_counts()
        print_section("Relationship Counts", rel_counts)
        
        # Get and display schema
        schema = explorer.get_schema()
        print_section("Database Schema", schema)
        
        # If we have node labels, show samples
        if node_counts and not isinstance(node_counts, dict):
            print("\nFetching sample nodes...")
            for item in node_counts:
                if 'labels' in item and item['labels']:
                    label = item['labels'][0]
                    print(f"\nSample {label} nodes:")
                    samples = explorer.get_sample_nodes(label, limit=2)
                    print(json.dumps(samples, indent=2, default=str))
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
    finally:
        explorer.close()

if __name__ == "__main__":
    main()
