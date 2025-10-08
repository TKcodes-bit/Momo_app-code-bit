"""
ETL Pipeline Runner for MoMo App
Main orchestrator for the Extract, Transform, Load pipeline
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "dsa"))

from config import config
from parse_xml import parse_sms_xml
from clean_normalize import DataCleaner
from categorize import TransactionCategorizer

class ETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self):
        self.config = config
        self.setup_logging()
        self.cleaner = DataCleaner()
        self.categorizer = TransactionCategorizer()
        self.stats = {
            "start_time": None,
            "end_time": None,
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "errors": []
        }
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=getattr(logging, self.config.LOG_LEVEL),
            format=self.config.LOG_FORMAT,
            handlers=[
                logging.FileHandler(self.config.LOG_FILE),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def extract(self) -> List[Dict[str, Any]]:
        """Extract data from XML source"""
        self.logger.info("Starting data extraction...")
        
        try:
            if not self.config.XML_INPUT_FILE.exists():
                raise FileNotFoundError(f"XML file not found: {self.config.XML_INPUT_FILE}")
            
            # Use existing parser from DSA module
            transactions = parse_sms_xml(str(self.config.XML_INPUT_FILE))
            
            if not transactions:
                raise ValueError("No transactions found in XML file")
            
            self.logger.info(f"Extracted {len(transactions)} transactions from XML")
            return transactions
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            self.stats["errors"].append(f"Extraction error: {str(e)}")
            raise
    
    def transform(self, raw_transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw data through cleaning and categorization"""
        self.logger.info("Starting data transformation...")
        
        processed_transactions = []
        
        for i, transaction in enumerate(raw_transactions):
            try:
                # Clean and normalize the transaction
                cleaned_txn = self.cleaner.clean_transaction(transaction)
                
                # Categorize the transaction
                categorized_txn = self.categorizer.categorize_transaction(cleaned_txn)
                
                # Add processing metadata
                categorized_txn["processed_at"] = datetime.now().isoformat()
                categorized_txn["etl_version"] = "1.0"
                
                processed_transactions.append(categorized_txn)
                
                # Log progress for large datasets
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(raw_transactions)} transactions")
                
            except Exception as e:
                self.logger.warning(f"Failed to process transaction {i}: {str(e)}")
                self.stats["failed"] += 1
                self.stats["errors"].append(f"Transaction {i} processing error: {str(e)}")
                continue
        
        self.logger.info(f"Transformation completed: {len(processed_transactions)} successful, {self.stats['failed']} failed")
        return processed_transactions
    
    def load(self, processed_transactions: List[Dict[str, Any]]) -> bool:
        """Load processed data to JSON file (for now, database integration later)"""
        self.logger.info("Starting data loading...")
        
        try:
            import json
            
            # Save processed data to JSON file
            with open(self.config.JSON_OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(processed_transactions, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Loaded {len(processed_transactions)} transactions to {self.config.JSON_OUTPUT_FILE}")
            
            # Save processing statistics
            stats_file = self.config.DATA_DIR / "processed" / f"etl_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Loading failed: {str(e)}")
            self.stats["errors"].append(f"Loading error: {str(e)}")
            return False
    
    def validate_pipeline(self) -> bool:
        """Validate pipeline configuration and dependencies"""
        self.logger.info("Validating pipeline configuration...")
        
        # Validate configuration
        if not self.config.validate_config():
            return False
        
        # Check if required modules can be imported
        try:
            from parse_xml import parse_sms_xml
            from clean_normalize import DataCleaner
            from categorize import TransactionCategorizer
        except ImportError as e:
            self.logger.error(f"Import error: {str(e)}")
            return False
        
        self.logger.info("Pipeline validation successful")
        return True
    
    def run(self, dry_run: bool = False) -> bool:
        """Run the complete ETL pipeline"""
        self.stats["start_time"] = datetime.now().isoformat()
        self.logger.info("Starting ETL pipeline execution...")
        
        try:
            # Validate pipeline
            if not self.validate_pipeline():
                self.logger.error("Pipeline validation failed")
                return False
            
            if dry_run:
                self.logger.info("DRY RUN MODE - No data will be written")
            
            # Extract
            raw_transactions = self.extract()
            self.stats["total_processed"] = len(raw_transactions)
            
            # Transform
            processed_transactions = self.transform(raw_transactions)
            self.stats["successful"] = len(processed_transactions)
            
            # Load (skip in dry run)
            if not dry_run:
                success = self.load(processed_transactions)
                if not success:
                    return False
            
            self.stats["end_time"] = datetime.now().isoformat()
            
            # Calculate duration
            start_time = datetime.fromisoformat(self.stats["start_time"])
            end_time = datetime.fromisoformat(self.stats["end_time"])
            duration = (end_time - start_time).total_seconds()
            
            self.logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
            self.logger.info(f"Statistics: {self.stats['successful']}/{self.stats['total_processed']} transactions processed")
            
            if self.stats["failed"] > 0:
                self.logger.warning(f"{self.stats['failed']} transactions failed processing")
            
            return True
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}")
            self.stats["end_time"] = datetime.now().isoformat()
            self.stats["errors"].append(f"Pipeline error: {str(e)}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline execution statistics"""
        return self.stats.copy()

def main():
    """Main entry point for ETL pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description="MoMo App ETL Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Run pipeline without writing data")
    parser.add_argument("--validate-only", action="store_true", help="Only validate configuration")
    
    args = parser.parse_args()
    
    # Create and run pipeline
    pipeline = ETLPipeline()
    
    if args.validate_only:
        success = pipeline.validate_pipeline()
        sys.exit(0 if success else 1)
    
    success = pipeline.run(dry_run=args.dry_run)
    
    # Print final statistics
    stats = pipeline.get_stats()
    print("\n" + "="*50)
    print("ETL PIPELINE EXECUTION SUMMARY")
    print("="*50)
    print(f"Start Time: {stats['start_time']}")
    print(f"End Time: {stats['end_time']}")
    print(f"Total Processed: {stats['total_processed']}")
    print(f"Successful: {stats['successful']}")
    print(f"Failed: {stats['failed']}")
    
    if stats['errors']:
        print(f"\nErrors ({len(stats['errors'])}):")
        for error in stats['errors'][:5]:  # Show first 5 errors
            print(f"  - {error}")
        if len(stats['errors']) > 5:
            print(f"  ... and {len(stats['errors']) - 5} more errors")
    
    print("="*50)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
