# RBI Fine Extractor - Configuration Guide

## 🚨 **Critical Issues Fixed**

The following issues have been resolved in the codebase:

1. **Missing logging imports** - Fixed duplicate and missing `import logging` statements
2. **Neo4j connection errors** - Improved error handling for database connection failures
3. **Application startup errors** - Enhanced error handling during initialization

## 🔧 **Environment Configuration**

Create a `.env` file in the root directory with the following configuration:

```bash
# Flask Configuration
SECRET_KEY=your-secret-key-change-this-in-production
FLASK_ENV=development
FLASK_DEBUG=1

# Google Gemini AI Configuration (Required)
GEMINI_API_KEY=your-gemini-api-key-here

# Neo4j Database Configuration (Optional)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
NEO4J_DATABASE=rbi

# Rate Limiting Configuration
MAX_REQUESTS_PER_BATCH=10
DELAY_BETWEEN_BATCHES=60
DELAY_BETWEEN_REQUESTS=1

# Test Mode (set to 'true' for testing without external APIs)
TEST_MODE=false
```

## 🗄️ **Neo4j Database Setup**

### **Option 1: Local Neo4j Desktop**
1. Download and install Neo4j Desktop
2. Create a new database project
3. Set username: `neo4j`, password: `password`
4. Start the database service
5. Update `.env` with your connection details

### **Option 2: Neo4j AuraDB (Cloud)**
1. Sign up at [neo4j.com/aura](https://neo4j.com/aura)
2. Create a new database instance
3. Copy connection details to `.env`

### **Option 3: Run Without Database**
The application will work without Neo4j, but database features will be disabled.

## 🧪 **Testing Database Connection**

Run the test script to verify Neo4j connectivity:

```bash
python test_neo4j_connection.py
```

## 🚀 **Running the Application**

### **1. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **2. Set Environment Variables**
- Copy the `.env` template above
- Fill in your actual API keys and database credentials

### **3. Start the Application**
```bash
flask run
```

The application will be available at: `http://127.0.0.1:5000`

## 🔑 **Default Login Credentials**

- **Username**: `admin`
- **Password**: `admin123`

**⚠️ Change these credentials in production!**

## 📊 **Application Features**

### **Core Functionality**
- PDF document upload and analysis
- RBI fine extraction using AI
- Excel transaction processing
- Compliance rule checking
- Transaction analysis and monitoring

### **Database Features** (when Neo4j is available)
- Compliance rule storage
- Violation tracking
- Transaction history
- KYC validation
- Risk assessment

## 🐛 **Troubleshooting**

### **Common Issues**

1. **"Unable to retrieve routing information"**
   - Neo4j service is not running
   - Incorrect connection URI
   - Firewall blocking connection

2. **"GEMINI_API_KEY not found"**
   - Add your Google Gemini API key to `.env`
   - Get API key from [Google AI Studio](https://makersuite.google.com/app/apikey)

3. **"Permission denied" errors**
   - Check file permissions for uploads/ and logs/ directories
   - Ensure Neo4j user has proper database access

### **Log Files**
- Application logs: `logs/app.log`
- Rotating log files with automatic cleanup

## 🔒 **Security Considerations**

1. **Change default credentials** in `app/auth.py`
2. **Use strong SECRET_KEY** in production
3. **Secure your .env file** - never commit to version control
4. **Enable HTTPS** in production
5. **Implement proper authentication** for production use

## 📈 **Performance Tuning**

### **Rate Limiting**
Adjust these values in `.env` based on your API limits:
- `MAX_REQUESTS_PER_BATCH`: Maximum requests per batch
- `DELAY_BETWEEN_BATCHES`: Seconds between batches
- `DELAY_BETWEEN_REQUESTS`: Seconds between individual requests

### **File Upload Limits**
- Maximum file size: 16MB (configurable in `config.py`)
- Supported formats: PDF, Excel (.xlsx, .xls)

## 🆘 **Support**

If you encounter issues:
1. Check the log files in `logs/` directory
2. Verify your environment configuration
3. Test database connectivity with `test_neo4j_connection.py`
4. Ensure all dependencies are properly installed

## 📝 **Development Notes**

- The application gracefully handles Neo4j connection failures
- Database features are optional and won't break core functionality
- All errors are logged with detailed stack traces
- The application can run in test mode without external APIs
