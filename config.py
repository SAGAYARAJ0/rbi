import os
from dotenv import load_dotenv

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or os.urandom(24)
    BASE_DIR = basedir
    UPLOAD_FOLDER = os.path.join(basedir, 'uploads')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
    
    # Rate limiting configuration for Gemini API
    MAX_REQUESTS_PER_BATCH = int(os.environ.get('MAX_REQUESTS_PER_BATCH', 10))  # Max requests per batch
    DELAY_BETWEEN_BATCHES = int(os.environ.get('DELAY_BETWEEN_BATCHES', 60))    # Seconds between batches
    DELAY_BETWEEN_REQUESTS = int(os.environ.get('DELAY_BETWEEN_REQUESTS', 1))   # Seconds between requests
    
    # Test mode for debugging
    TEST_MODE = os.environ.get('TEST_MODE', 'false').lower() == 'true'
    
    # Optional Neo4j settings; app should continue to work when unset
    NEO4J_URI = os.environ.get('NEO4J_URI')
    NEO4J_USER = os.environ.get('NEO4J_USER')
    NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')