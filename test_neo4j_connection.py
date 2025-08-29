import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    
    print(f"Attempting to connect to: {uri}")
    print(f"Using database: {database}")
    
    try:
        # Try to connect to the database
        with GraphDatabase.driver(uri, auth=(username, password)) as driver:
            # Verify the connection works
            driver.verify_connectivity()
            print("✅ Successfully connected to Neo4j!")
            
            # Get server info
            server_info = driver.get_server_info()
            print(f"\nNeo4j Server Info:")
            print(f"- Version: {server_info.agent}")
            
            # Run a simple query to test
            with driver.session(database=database) as session:
                result = session.run("RETURN 'Hello, Neo4j!' AS message")
                print(f"\nTest query result: {result.single()['message']}")
                
                # Check if database exists
                dbs = session.run("SHOW DATABASES")
                print("\nAvailable databases:")
                for db in dbs:
                    print(f"- {db['name']} (default: {db.get('default', False)})")
                
                # Check if our database exists
                dbs = session.run("SHOW DATABASES")
                db_exists = any(db['name'] == database for db in dbs)
                if not db_exists:
                    print(f"\n⚠️  Warning: Database '{database}' does not exist!")
                    print("Available databases:")
                    dbs = session.run("SHOW DATABASES")
                    for db in dbs:
                        print(f"- {db['name']}")
                else:
                    print(f"\n✅ Database '{database}' exists")
                    
                    # Try to get node count
                    try:
                        result = session.run("MATCH (n) RETURN count(n) AS count")
                        print(f"\nTotal nodes in database: {result.single()['count']}")
                    except Exception as e:
                        print(f"\n⚠️  Error counting nodes: {str(e)}")
                        print("This could be due to insufficient permissions or database access issues.")
                        
    except Exception as e:
        print(f"\n❌ Failed to connect to Neo4j: {str(e)}")
        print("\nTroubleshooting steps:")
        print("1. Make sure Neo4j Desktop or Neo4j Server is running")
        print("2. Verify the connection URI is correct (should be neo4j://host:port)")
        print("3. Check if your username and password are correct")
        print("4. Ensure the database exists and is running")
        print("5. Check if your IP is whitelisted in Neo4j config")
        return False
    
    return True

if __name__ == "__main__":
    test_connection()
