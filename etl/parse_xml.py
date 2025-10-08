"""
ETL XML Parser Module for MoMo App
Enhanced XML parser with error handling and integration with existing DSA parser
"""

import sys
import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import xml.etree.ElementTree as ET
from datetime import datetime

# Add DSA directory to path to use existing parser
PROJECT_ROOT = Path(__file__).parent.parent
DSA_DIR = PROJECT_ROOT / "dsa"
sys.path.insert(0, str(DSA_DIR))

try:
    from parse_xml import parse_sms_xml as dsa_parse_sms_xml
except ImportError:
    dsa_parse_sms_xml = None

class ETLXMLParser:
    """Enhanced XML parser for ETL pipeline with error handling and validation"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.stats = {
            "total_sms": 0,
            "parsed_successfully": 0,
            "parsing_errors": 0,
            "validation_errors": 0,
            "start_time": None,
            "end_time": None
        }
    
    def parse_xml_file(self, file_path: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Parse XML file and return transactions with statistics"""
        self.stats["start_time"] = datetime.now().isoformat()
        
        try:
            # Validate file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"XML file not found: {file_path}")
            
            # Try to use existing DSA parser first
            if dsa_parse_sms_xml:
                self.logger.info("Using existing DSA XML parser")
                transactions = dsa_parse_sms_xml(file_path)
                if transactions:
                    self.stats["parsed_successfully"] = len(transactions)
                    self.stats["end_time"] = datetime.now().isoformat()
                    return transactions, self.stats.copy()
            
            # Fallback to enhanced parser
            self.logger.info("Using enhanced ETL XML parser")
            transactions = self._parse_xml_enhanced(file_path)
            
            self.stats["parsed_successfully"] = len(transactions)
            self.stats["end_time"] = datetime.now().isoformat()
            
            return transactions, self.stats.copy()
            
        except Exception as e:
            self.logger.error(f"XML parsing failed: {str(e)}")
            self.stats["parsing_errors"] += 1
            self.stats["end_time"] = datetime.now().isoformat()
            raise
    
    def _parse_xml_enhanced(self, file_path: str) -> List[Dict[str, Any]]:
        """Enhanced XML parser with better error handling"""
        transactions = []
        
        try:
            # Parse XML with error handling
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Count total SMS elements
            sms_elements = self._find_sms_elements(root)
            self.stats["total_sms"] = len(sms_elements)
            
            self.logger.info(f"Found {len(sms_elements)} SMS elements")
            
            # Process each SMS element
            for i, sms in enumerate(sms_elements):
                try:
                    transaction = self._parse_sms_element(sms, i)
                    if transaction:
                        transactions.append(transaction)
                    
                    # Log progress for large files
                    if (i + 1) % 1000 == 0:
                        self.logger.info(f"Processed {i + 1}/{len(sms_elements)} SMS elements")
                        
                except Exception as e:
                    self.logger.warning(f"Error parsing SMS element {i}: {str(e)}")
                    self.stats["parsing_errors"] += 1
                    continue
            
            self.logger.info(f"Successfully parsed {len(transactions)} transactions")
            
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
            raise
        except FileNotFoundError as e:
            self.logger.error(f"File not found: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error parsing XML: {e}")
            raise
        
        return transactions
    
    def _find_sms_elements(self, root: ET.Element) -> List[ET.Element]:
        """Find SMS elements in XML structure"""
        sms_elements = []
        
        # Try different possible structures
        if root.tag in ('sms', 'transaction'):
            sms_elements = [root]
        elif root.tag in ('smses', 'transactions', 'data'):
            sms_elements = list(root.findall('sms')) or list(root.findall('transaction'))
        else:
            # Search recursively
            sms_elements = list(root.findall('.//sms')) or list(root.findall('.//transaction'))
        
        return sms_elements
    
    def _parse_sms_element(self, sms: ET.Element, index: int) -> Optional[Dict[str, Any]]:
        """Parse individual SMS element"""
        try:
            # Collect all data from element
            raw_data = {}
            
            # Get attributes
            for attr_name, attr_value in sms.attrib.items():
                raw_data[attr_name] = attr_value
            
            # Get child elements
            for child in sms:
                if child.text is not None:
                    raw_data[child.tag] = child.text.strip()
                else:
                    raw_data[child.tag] = None
            
            # Normalize to standard format
            transaction = self._normalize_transaction_data(raw_data, index)
            
            # Validate transaction
            if self._validate_transaction(transaction):
                return transaction
            else:
                self.stats["validation_errors"] += 1
                self.logger.warning(f"Transaction {index} failed validation")
                return None
                
        except Exception as e:
            self.logger.warning(f"Error parsing SMS element {index}: {str(e)}")
            return None
    
    def _normalize_transaction_data(self, raw_data: Dict[str, Any], index: int) -> Dict[str, Any]:
        """Normalize raw SMS data to standard transaction format"""
        transaction = {}
        
        # ID handling
        transaction["Id"] = self._extract_id(raw_data, index)
        
        # Transaction type
        transaction["transaction_type"] = self._extract_transaction_type(raw_data)
        
        # Amount
        transaction["amount"] = self._extract_amount(raw_data)
        
        # Sender/Receiver
        transaction["sender"] = self._extract_sender(raw_data)
        transaction["receiver"] = self._extract_receiver(raw_data)
        
        # Timestamp
        transaction["timestamp"] = self._extract_timestamp(raw_data)
        
        # SMS body (important for categorization)
        transaction["body"] = raw_data.get("body", "")
        
        # Preserve other fields
        core_fields = {"Id", "transaction_type", "amount", "sender", "receiver", "timestamp", "body"}
        for key, value in raw_data.items():
            if key not in core_fields and value is not None:
                transaction[key] = value
        
        return transaction
    
    def _extract_id(self, raw_data: Dict[str, Any], index: int) -> str:
        """Extract or generate transaction ID"""
        # Try different ID fields
        for id_field in ["Id", "id", "transaction_id", "txn_id"]:
            if id_field in raw_data and raw_data[id_field]:
                return str(raw_data[id_field]).strip()
        
        # Generate ID if none found
        return f"TXN_{index + 1:06d}"
    
    def _extract_transaction_type(self, raw_data: Dict[str, Any]) -> str:
        """Extract transaction type"""
        for type_field in ["transaction_type", "Type", "type"]:
            if type_field in raw_data and raw_data[type_field]:
                return str(raw_data[type_field]).strip()
        
        return "UNKNOWN"
    
    def _extract_amount(self, raw_data: Dict[str, Any]) -> str:
        """Extract amount from various fields"""
        for amount_field in ["amount", "Amount", "value"]:
            if amount_field in raw_data and raw_data[amount_field]:
                return str(raw_data[amount_field]).strip()
        
        return "0"
    
    def _extract_sender(self, raw_data: Dict[str, Any]) -> str:
        """Extract sender information"""
        for sender_field in ["sender", "Sender", "from"]:
            if sender_field in raw_data and raw_data[sender_field]:
                return str(raw_data[sender_field]).strip()
        
        return "Unknown"
    
    def _extract_receiver(self, raw_data: Dict[str, Any]) -> str:
        """Extract receiver information"""
        for receiver_field in ["receiver", "Receiver", "to"]:
            if receiver_field in raw_data and raw_data[receiver_field]:
                return str(receiver_field).strip()
        
        return "Unknown"
    
    def _extract_timestamp(self, raw_data: Dict[str, Any]) -> str:
        """Extract timestamp from various fields"""
        for time_field in ["timestamp", "Timestamp", "date", "Date"]:
            if time_field in raw_data and raw_data[time_field]:
                return str(raw_data[time_field]).strip()
        
        return datetime.now().isoformat()
    
    def _validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validate transaction has required fields"""
        required_fields = ["Id", "transaction_type", "amount", "sender", "receiver", "timestamp"]
        
        for field in required_fields:
            if field not in transaction or not transaction[field]:
                return False
        
        # Additional validations
        if transaction["Id"] == "":
            return False
        
        return True
    
    def get_parsing_statistics(self) -> Dict[str, Any]:
        """Get parsing statistics"""
        return self.stats.copy()
    
    def validate_xml_file(self, file_path: str) -> bool:
        """Validate XML file before parsing"""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"File does not exist: {file_path}")
                return False
            
            # Try to parse XML
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Check if it has SMS elements
            sms_elements = self._find_sms_elements(root)
            if not sms_elements:
                self.logger.error("No SMS elements found in XML file")
                return False
            
            self.logger.info(f"XML file validation successful: {len(sms_elements)} SMS elements found")
            return True
            
        except ET.ParseError as e:
            self.logger.error(f"XML file is not valid: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error validating XML file: {e}")
            return False

def parse_xml(file_path: str) -> List[Dict[str, Any]]:
    """Convenience function for backward compatibility"""
    parser = ETLXMLParser()
    transactions, stats = parser.parse_xml_file(file_path)
    
    # Log statistics
    logger = logging.getLogger(__name__)
    logger.info(f"Parsing completed: {stats['parsed_successfully']}/{stats['total_sms']} transactions parsed")
    
    return transactions

if __name__ == "__main__":
    # Test the parser
    import sys
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample file
    sample_file = str(DSA_DIR / "modified_sms_v2.xml")
    
    if os.path.exists(sample_file):
        parser = ETLXMLParser()
        
        # Validate file first
        if parser.validate_xml_file(sample_file):
            print("✅ XML file validation successful")
            
            # Parse file
            transactions, stats = parser.parse_xml_file(sample_file)
            
            print(f"\nParsing Results:")
            print(f"  Total SMS elements: {stats['total_sms']}")
            print(f"  Successfully parsed: {stats['parsed_successfully']}")
            print(f"  Parsing errors: {stats['parsing_errors']}")
            print(f"  Validation errors: {stats['validation_errors']}")
            
            if transactions:
                print(f"\nSample transaction:")
                print(f"  ID: {transactions[0].get('Id', 'N/A')}")
                print(f"  Type: {transactions[0].get('transaction_type', 'N/A')}")
                print(f"  Amount: {transactions[0].get('amount', 'N/A')}")
                print(f"  Sender: {transactions[0].get('sender', 'N/A')}")
                print(f"  Receiver: {transactions[0].get('receiver', 'N/A')}")
        else:
            print("❌ XML file validation failed")
    else:
        print(f"❌ Sample file not found: {sample_file}")
