import os
import logging
from logging.handlers import RotatingFileHandler
from flask import Flask, session
from config import Config
from app.utils.graph import get_client_from_env, initialize_compliance_rules

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Ensure log directory exists
    log_dir = os.path.join(app.config['BASE_DIR'], 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging
    log_file = os.path.join(log_dir, 'app.log')
    
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Set up file handler with rotation
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10240, backupCount=10, encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    file_handler.setLevel(logging.INFO)
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    console_handler.setLevel(logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler],
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure app logger
    app.logger.handlers.clear()
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)
    
    app.logger.info('RBI Fine Extractor starting...')
    app.logger.info(f'Log file: {log_file}')
    
    # Configure session
    app.config['SECRET_KEY'] = 'your-secret-key-here'  # Change this to a secure secret key in production
    
    # Ensure upload directory exists
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Initialize Neo4j client and compliance rules
    try:
        neo_client = get_client_from_env()
        if neo_client.enabled:
            try:
                initialize_compliance_rules(neo_client)
                logging.info("Successfully initialized compliance rules")
            except Exception as neo_error:
                logging.error(f"Neo4j initialization error: {str(neo_error)}")
                logging.warning("Application will continue without Neo4j functionality")
        else:
            logging.warning("Neo4j client is not enabled. Compliance rules not initialized.")
            logging.info("Application will run without database functionality")
    except Exception as e:
        logging.error(f"Error initializing Neo4j client: {str(e)}")
        logging.warning("Application will continue without database functionality")
    
    # Register blueprints
    from app.routes import bp as main_bp
    from app.auth import auth as auth_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp, url_prefix='/auth')
    
    return app