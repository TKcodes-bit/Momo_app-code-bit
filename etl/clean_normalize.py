"""
Data Cleaning and Normalization Module for MoMo App ETL
Handles cleaning, validation, and standardization of transaction data
"""

import re
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from decimal import Decimal, InvalidOperation

class DataCleaner:
    """Handles data cleaning and normalization for transactions"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Phone number normalization patterns
        self.phone_patterns = [
            (r"\+250(\d{9})", r"+250\1"),      # +250XXXXXXXXX
            (r"250(\d{9})", r"+250\1"),        # 250XXXXXXXXX -> +250XXXXXXXXX
            (r"0(\d{9})", r"+250\1"),          # 0XXXXXXXXX -> +250XXXXXXXXX
            (r"(\d{9})", r"+250\1"),           # XXXXXXXXX -> +250XXXXXXXXX
        ]
        
        # Amount extraction patterns
        self.amount_patterns = [
            r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*RWF",
            r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)",
            r"RWF\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)",
            r"(\d+)\s*RWF",
            r"RWF\s*(\d+)"
        ]
        
        # Date parsing patterns
        self.date_patterns = [
            r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})",  # 2024-05-10 16:30:51
            r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})",  # 10/05/2024 16:30:51
            r"(\d{2}-\d{2}-\d{4})\s+(\d{2}:\d{2}:\d{2})",  # 10-05-2024 16:30:51
        ]
    
    def clean_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize a single transaction"""
        try:
            cleaned = transaction.copy()
            
            # Clean ID
            cleaned["Id"] = self._clean_id(transaction.get("Id", ""))
            
            # Clean amount
            cleaned["amount"] = self._clean_amount(transaction.get("amount", "0"))
            
            # Clean phone numbers
            cleaned["sender"] = self._clean_phone_number(transaction.get("sender", "Unknown"))
            cleaned["receiver"] = self._clean_phone_number(transaction.get("receiver", "Unknown"))
            
            # Clean timestamp
            cleaned["timestamp"] = self._clean_timestamp(transaction.get("timestamp", ""))
            
            # Clean transaction type
            cleaned["transaction_type"] = self._clean_transaction_type(transaction.get("transaction_type", "UNKNOWN"))
            
            # Extract additional data from SMS body if available
            if "body" in transaction:
                self._extract_from_body(cleaned, transaction["body"])
            
            # Add validation flags
            cleaned["_validation"] = {
                "is_valid": self._validate_transaction(cleaned),
                "cleaned_at": datetime.now().isoformat(),
                "issues": self._get_validation_issues(cleaned)
            }
            
            return cleaned
            
        except Exception as e:
            self.logger.error(f"Error cleaning transaction: {str(e)}")
            # Return original transaction with error flag
            transaction["_validation"] = {
                "is_valid": False,
                "cleaned_at": datetime.now().isoformat(),
                "issues": [f"Cleaning error: {str(e)}"]
            }
            return transaction
    
    def _clean_id(self, transaction_id: str) -> str:
        """Clean and normalize transaction ID"""
        if not transaction_id or transaction_id.strip() == "":
            return f"TXN_{int(datetime.now().timestamp())}"
        
        # Remove whitespace and ensure proper format
        cleaned_id = transaction_id.strip()
        
        # If it doesn't start with TXN_, add it
        if not cleaned_id.startswith("TXN_"):
            cleaned_id = f"TXN_{cleaned_id}"
        
        return cleaned_id
    
    def _clean_amount(self, amount_str: str) -> float:
        """Clean and normalize amount"""
        if not amount_str:
            return 0.0
        
        try:
            # Remove currency symbols and extra whitespace
            amount_str = str(amount_str).strip()
            
            # Try to extract amount using patterns
            for pattern in self.amount_patterns:
                match = re.search(pattern, amount_str, re.IGNORECASE)
                if match:
                    amount_str = match.group(1)
                    break
            
            # Remove commas and convert to float
            amount_str = amount_str.replace(",", "")
            
            # Convert to Decimal first for precision, then to float
            amount_decimal = Decimal(amount_str)
            return float(amount_decimal)
            
        except (ValueError, InvalidOperation, AttributeError) as e:
            self.logger.warning(f"Could not parse amount '{amount_str}': {str(e)}")
            return 0.0
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean and normalize phone number"""
        if not phone or phone.strip() == "" or phone.lower() == "unknown":
            return "Unknown"
        
        phone = str(phone).strip()
        
        # Apply normalization patterns
        for pattern, replacement in self.phone_patterns:
            if re.match(pattern, phone):
                phone = re.sub(pattern, replacement, phone)
                break
        
        # Validate final format
        if re.match(r"\+250\d{9}", phone):
            return phone
        else:
            return "Unknown"
    
    def _clean_timestamp(self, timestamp: str) -> str:
        """Clean and normalize timestamp"""
        if not timestamp:
            return datetime.now().isoformat()
        
        try:
            # Try to parse various timestamp formats
            timestamp_str = str(timestamp).strip()
            
            # Handle Unix timestamp (milliseconds)
            if timestamp_str.isdigit() and len(timestamp_str) == 13:
                timestamp_int = int(timestamp_str)
                dt = datetime.fromtimestamp(timestamp_int / 1000)
                return dt.isoformat()
            
            # Handle Unix timestamp (seconds)
            elif timestamp_str.isdigit() and len(timestamp_str) == 10:
                timestamp_int = int(timestamp_str)
                dt = datetime.fromtimestamp(timestamp_int)
                return dt.isoformat()
            
            # Handle ISO format
            elif "T" in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", ""))
                return dt.isoformat()
            
            # Handle date patterns
            for pattern in self.date_patterns:
                match = re.search(pattern, timestamp_str)
                if match:
                    date_part, time_part = match.groups()
                    # Try different date formats
                    for date_format in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
                        try:
                            dt = datetime.strptime(f"{date_part} {time_part}", f"{date_format} %H:%M:%S")
                            return dt.isoformat()
                        except ValueError:
                            continue
            
            # If all else fails, return current timestamp
            return datetime.now().isoformat()
            
        except Exception as e:
            self.logger.warning(f"Could not parse timestamp '{timestamp}': {str(e)}")
            return datetime.now().isoformat()
    
    def _clean_transaction_type(self, transaction_type: str) -> str:
        """Clean and normalize transaction type"""
        if not transaction_type:
            return "UNKNOWN"
        
        # Normalize to uppercase
        cleaned_type = str(transaction_type).strip().upper()
        
        # Map common variations
        type_mappings = {
            "1": "RECEIVE",
            "0": "SEND", 
            "SENT": "SEND",
            "RECEIVED": "RECEIVE",
            "TRANSFER": "SEND",
            "PAYMENT": "SEND",
            "DEPOSIT": "RECEIVE",
            "WITHDRAWAL": "SEND"
        }
        
        return type_mappings.get(cleaned_type, cleaned_type)
    
    def _extract_from_body(self, transaction: Dict[str, Any], body: str) -> None:
        """Extract additional information from SMS body"""
        if not body:
            return
        
        # Extract amount from body if not already present
        if transaction.get("amount", 0) == 0:
            for pattern in self.amount_patterns:
                match = re.search(pattern, body, re.IGNORECASE)
                if match:
                    try:
                        amount_str = match.group(1).replace(",", "")
                        transaction["amount"] = float(amount_str)
                        break
                    except (ValueError, AttributeError):
                        continue
        
        # Extract transaction ID from body
        tx_id_match = re.search(r"TxId:\s*(\d+)", body, re.IGNORECASE)
        if tx_id_match and not transaction.get("Id"):
            transaction["Id"] = f"TXN_{tx_id_match.group(1)}"
        
        # Extract balance information
        balance_match = re.search(r"balance[:\s]*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)", body, re.IGNORECASE)
        if balance_match:
            try:
                balance_str = balance_match.group(1).replace(",", "")
                transaction["balance_after"] = float(balance_str)
            except ValueError:
                pass
        
        # Extract fee information
        fee_match = re.search(r"fee[:\s]*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)", body, re.IGNORECASE)
        if fee_match:
            try:
                fee_str = fee_match.group(1).replace(",", "")
                transaction["fee"] = float(fee_str)
            except ValueError:
                pass
    
    def _validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validate if transaction has required fields"""
        required_fields = ["Id", "amount", "sender", "receiver", "timestamp"]
        
        for field in required_fields:
            if field not in transaction or not transaction[field]:
                return False
        
        # Additional validations
        if transaction["amount"] < 0:
            return False
        
        if transaction["sender"] == "Unknown" and transaction["receiver"] == "Unknown":
            return False
        
        return True
    
    def _get_validation_issues(self, transaction: Dict[str, Any]) -> List[str]:
        """Get list of validation issues"""
        issues = []
        
        if transaction.get("amount", 0) <= 0:
            issues.append("Invalid or zero amount")
        
        if transaction.get("sender") == "Unknown":
            issues.append("Unknown sender")
        
        if transaction.get("receiver") == "Unknown":
            issues.append("Unknown receiver")
        
        if transaction.get("transaction_type") == "UNKNOWN":
            issues.append("Unknown transaction type")
        
        return issues
    
    def clean_batch(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean a batch of transactions"""
        cleaned_transactions = []
        
        for i, transaction in enumerate(transactions):
            try:
                cleaned = self.clean_transaction(transaction)
                cleaned_transactions.append(cleaned)
                
                # Log progress for large batches
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Cleaned {i + 1}/{len(transactions)} transactions")
                    
            except Exception as e:
                self.logger.error(f"Error cleaning transaction {i}: {str(e)}")
                # Add error transaction
                transaction["_validation"] = {
                    "is_valid": False,
                    "cleaned_at": datetime.now().isoformat(),
                    "issues": [f"Batch cleaning error: {str(e)}"]
                }
                cleaned_transactions.append(transaction)
        
        return cleaned_transactions

if __name__ == "__main__":
    # Test the cleaner
    cleaner = DataCleaner()
    
    # Test transaction
    test_transaction = {
        "Id": "12345",
        "amount": "1,500 RWF",
        "sender": "0788123456",
        "receiver": "250788654321",
        "timestamp": "1715351458724",
        "transaction_type": "1",
        "body": "TxId: 12345. Your payment of 1,500 RWF to John Doe has been completed. Your new balance: 5,000 RWF. Fee was 0 RWF."
    }
    
    cleaned = cleaner.clean_transaction(test_transaction)
    print("Original:", test_transaction)
    print("Cleaned:", cleaned)
    print("Validation:", cleaned.get("_validation", {}))
