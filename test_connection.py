from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get connection details
uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USERNAME", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "")
database = os.getenv("NEO4J_DATABASE", "rbi")

print(f"Attempting to connect to: {uri}")
print(f"Database: {database}")

try:
    # Try to connect
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    # Test connection
    with driver.session(database=database) as session:
        # Test connection
        result = session.run("RETURN 'Connected successfully' AS message")
        print("\n✅ Connection successful!")
        print(f"Response: {result.single()['message']}")
        
        # List all node labels
        print("\n📊 Node labels in the database:")
        result = session.run("""
        CALL db.labels() YIELD label
        RETURN collect(label) AS labels
        """)
        labels = result.single()['labels']
        print(f"Found {len(labels)} node labels: {', '.join(labels)}")
        
        # Count nodes per label
        print("\n📈 Node counts by label:")
        for label in labels:
            result = session.run(f"""
            MATCH (n:`{label}`)
            RETURN count(n) as count
            """)
            count = result.single()['count']
            print(f"- {label}: {count} nodes")
        
        # List relationship types
        print("\n🔗 Relationship types in the database:")
        result = session.run("""
        CALL db.relationshipTypes() YIELD relationshipType
        RETURN collect(relationshipType) AS types
        """)
        rel_types = result.single()['types']
        print(f"Found {len(rel_types)} relationship types: {', '.join(rel_types)}")
        
except Exception as e:
    print(f"\n❌ Connection failed: {str(e)}")
    
finally:
    if 'driver' in locals():
        driver.close()
        print("\nConnection closed.")
