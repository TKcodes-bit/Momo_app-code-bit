"""
ETL Configuration Module for MoMo App
Handles all configuration settings for the ETL pipeline
"""

import os
from pathlib import Path
from typing import Dict, Any

class ETLConfig:
    """Configuration class for ETL pipeline settings"""
    
    def __init__(self):
        # Project root directory
        self.PROJECT_ROOT = Path(__file__).parent.parent
        
        # Data paths
        self.DSA_DIR = self.PROJECT_ROOT / "dsa"
        self.DATA_DIR = self.PROJECT_ROOT / "data"
        self.DATABASE_DIR = self.PROJECT_ROOT / "database"
        
        # Input files
        self.XML_INPUT_FILE = self.DSA_DIR / "modified_sms_v2.xml"
        self.JSON_OUTPUT_FILE = self.DSA_DIR / "transactions.json"
        
        # Database settings
        self.DATABASE_FILE = self.DATABASE_DIR / "momo.db"
        self.DATABASE_SCHEMA_FILE = self.DATABASE_DIR / "database_setup.sql"
        
        # ETL processing settings
        self.BATCH_SIZE = 1000  # Process transactions in batches
        self.MAX_RETRIES = 3    # Maximum retry attempts for failed operations
        self.RETRY_DELAY = 1    # Delay between retries in seconds
        
        # Data processing settings
        self.CURRENCY_CODE = "RWF"
        self.DEFAULT_CATEGORY_ID = 3  # Money Transfer as default
        
        # Transaction category mappings
        self.CATEGORY_MAPPINGS = {
            "airtime": 1,           # Airtime Purchase
            "bill": 2,              # Bill Payment
            "transfer": 3,           # Money Transfer
            "school": 4,             # School Fees
            "shopping": 5,           # Shopping
            "deposit": 3,            # Bank deposit -> Money Transfer
            "withdrawal": 3,         # Withdrawal -> Money Transfer
            "payment": 2,            # Payment -> Bill Payment
            "receive": 3,            # Receive money -> Money Transfer
        }
        
        # SMS text patterns for categorization
        self.CATEGORY_PATTERNS = {
            "airtime": [
                "airtime", "token", "*182*", "internet", "data bundle"
            ],
            "bill": [
                "bill payment", "utility", "electricity", "water", "rent"
            ],
            "transfer": [
                "transferred", "sent", "received", "mobile money", "momo"
            ],
            "school": [
                "school fees", "tuition", "education", "student"
            ],
            "shopping": [
                "payment", "purchase", "merchant", "shop", "buy"
            ],
            "deposit": [
                "bank deposit", "cash deposit", "deposited"
            ],
            "withdrawal": [
                "withdrawal", "cash out", "withdrawn"
            ]
        }
        
        # Phone number patterns
        self.PHONE_PATTERNS = [
            r"\+250\d{9}",      # +250XXXXXXXXX
            r"250\d{9}",        # 250XXXXXXXXX
            r"0\d{9}",          # 0XXXXXXXXX
            r"\d{9}"            # XXXXXXXXX
        ]
        
        # Amount extraction patterns
        self.AMOUNT_PATTERNS = [
            r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*RWF",
            r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)",
            r"RWF\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"
        ]
        
        # Logging settings
        self.LOG_LEVEL = "INFO"
        self.LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        self.LOG_FILE = self.DATA_DIR / "logs" / "etl.log"
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.DATA_DIR,
            self.DATA_DIR / "raw",
            self.DATA_DIR / "processed", 
            self.DATA_DIR / "logs",
            self.DATABASE_DIR
        ]
        
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except FileExistsError:
                # Directory already exists, which is fine
                pass
    
    def get_database_url(self) -> str:
        """Get SQLite database URL"""
        return f"sqlite:///{self.DATABASE_FILE}"
    
    def validate_config(self) -> bool:
        """Validate configuration settings"""
        errors = []
        
        # Check if input file exists
        if not self.XML_INPUT_FILE.exists():
            errors.append(f"XML input file not found: {self.XML_INPUT_FILE}")
        
        # Check if database schema exists
        if not self.DATABASE_SCHEMA_FILE.exists():
            errors.append(f"Database schema file not found: {self.DATABASE_SCHEMA_FILE}")
        
        # Validate batch size
        if self.BATCH_SIZE <= 0:
            errors.append("Batch size must be greater than 0")
        
        # Validate retry settings
        if self.MAX_RETRIES < 0:
            errors.append("Max retries must be non-negative")
        
        if errors:
            print("Configuration validation errors:")
            for error in errors:
                print(f"  - {error}")
            return False
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "project_root": str(self.PROJECT_ROOT),
            "xml_input_file": str(self.XML_INPUT_FILE),
            "json_output_file": str(self.JSON_OUTPUT_FILE),
            "database_file": str(self.DATABASE_FILE),
            "batch_size": self.BATCH_SIZE,
            "max_retries": self.MAX_RETRIES,
            "currency_code": self.CURRENCY_CODE,
            "category_mappings": self.CATEGORY_MAPPINGS,
            "log_level": self.LOG_LEVEL
        }

# Global configuration instance
config = ETLConfig()

if __name__ == "__main__":
    # Test configuration
    print("ETL Configuration:")
    print(f"Project Root: {config.PROJECT_ROOT}")
    print(f"XML Input: {config.XML_INPUT_FILE}")
    print(f"Database: {config.DATABASE_FILE}")
    print(f"Batch Size: {config.BATCH_SIZE}")
    print(f"Categories: {len(config.CATEGORY_MAPPINGS)}")
    
    if config.validate_config():
        print("✅ Configuration is valid")
    else:
        print("❌ Configuration has errors")
