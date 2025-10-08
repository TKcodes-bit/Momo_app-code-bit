"""
Transaction Categorization Module for MoMo App ETL
Handles intelligent categorization of transactions based on SMS content and patterns
"""

import re
import logging
from typing import Dict, Any, List, Tuple, Optional
from collections import Counter

class TransactionCategorizer:
    """Handles transaction categorization based on SMS content analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Category definitions with their IDs and patterns
        self.categories = {
            1: {  # Airtime Purchase
                "name": "Airtime Purchase",
                "patterns": [
                    r"airtime", r"token", r"\*182\*", r"internet", r"data bundle",
                    r"mobile data", r"data package", r"bundle", r"units"
                ],
                "keywords": ["airtime", "token", "internet", "data", "bundle", "units"]
            },
            2: {  # Bill Payment
                "name": "Bill Payment", 
                "patterns": [
                    r"bill payment", r"utility", r"electricity", r"water", r"rent",
                    r"insurance", r"tax", r"government", r"council"
                ],
                "keywords": ["bill", "utility", "electricity", "water", "rent", "insurance"]
            },
            3: {  # Money Transfer
                "name": "Money Transfer",
                "patterns": [
                    r"transferred", r"sent", r"received", r"mobile money", r"momo",
                    r"transfer", r"send money", r"receive money", r"payment to"
                ],
                "keywords": ["transfer", "send", "receive", "money", "payment"]
            },
            4: {  # School Fees
                "name": "School Fees",
                "patterns": [
                    r"school fees", r"tuition", r"education", r"student", r"university",
                    r"college", r"academic", r"learning", r"school"
                ],
                "keywords": ["school", "tuition", "education", "student", "university"]
            },
            5: {  # Shopping
                "name": "Shopping",
                "patterns": [
                    r"payment", r"purchase", r"merchant", r"shop", r"buy", r"store",
                    r"retail", r"goods", r"services", r"pos"
                ],
                "keywords": ["payment", "purchase", "merchant", "shop", "buy"]
            }
        }
        
        # Special transaction type patterns
        self.transaction_types = {
            "DEPOSIT": {
                "patterns": [r"bank deposit", r"cash deposit", r"deposited", r"added to your account"],
                "category_id": 3,  # Money Transfer
                "confidence": 0.9
            },
            "WITHDRAWAL": {
                "patterns": [r"withdrawal", r"cash out", r"withdrawn", r"cash withdrawal"],
                "category_id": 3,  # Money Transfer
                "confidence": 0.9
            },
            "AIRTIME": {
                "patterns": [r"airtime", r"token", r"\*182\*", r"internet"],
                "category_id": 1,  # Airtime Purchase
                "confidence": 0.95
            },
            "BILL_PAYMENT": {
                "patterns": [r"bill payment", r"utility", r"electricity", r"water"],
                "category_id": 2,  # Bill Payment
                "confidence": 0.9
            }
        }
    
    def categorize_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Categorize a single transaction"""
        try:
            categorized = transaction.copy()
            
            # Get SMS body for analysis
            sms_body = transaction.get("body", "")
            transaction_type = transaction.get("transaction_type", "").upper()
            amount = transaction.get("amount", 0)
            
            # Determine category and confidence
            category_id, confidence, method = self._determine_category(
                sms_body, transaction_type, amount
            )
            
            # Add categorization results
            categorized["category_id"] = category_id
            categorized["category_name"] = self.categories[category_id]["name"]
            categorized["categorization_confidence"] = confidence
            categorized["categorization_method"] = method
            
            # Add additional extracted information
            self._extract_additional_info(categorized, sms_body)
            
            return categorized
            
        except Exception as e:
            self.logger.error(f"Error categorizing transaction: {str(e)}")
            # Return with default category
            transaction["category_id"] = 3  # Money Transfer as default
            transaction["category_name"] = "Money Transfer"
            transaction["categorization_confidence"] = 0.0
            transaction["categorization_method"] = "error_fallback"
            return transaction
    
    def _determine_category(self, sms_body: str, transaction_type: str, amount: float) -> Tuple[int, float, str]:
        """Determine transaction category with confidence score"""
        
        # Method 1: Check for specific transaction types first
        for txn_type, config in self.transaction_types.items():
            for pattern in config["patterns"]:
                if re.search(pattern, sms_body, re.IGNORECASE):
                    return config["category_id"], config["confidence"], f"transaction_type_{txn_type}"
        
        # Method 2: Pattern matching on SMS body
        category_scores = {}
        
        for category_id, category_info in self.categories.items():
            score = 0
            matches = 0
            
            # Check patterns
            for pattern in category_info["patterns"]:
                if re.search(pattern, sms_body, re.IGNORECASE):
                    score += 1
                    matches += 1
            
            # Check keywords
            for keyword in category_info["keywords"]:
                if keyword.lower() in sms_body.lower():
                    score += 0.5
                    matches += 1
            
            if matches > 0:
                category_scores[category_id] = score / len(category_info["patterns"])
        
        # Method 3: Amount-based heuristics
        amount_category = self._categorize_by_amount(amount)
        if amount_category:
            category_id, confidence = amount_category
            if category_id in category_scores:
                category_scores[category_id] += 0.2
            else:
                category_scores[category_id] = 0.2
        
        # Method 4: Transaction type analysis
        type_category = self._categorize_by_transaction_type(transaction_type)
        if type_category:
            category_id, confidence = type_category
            if category_id in category_scores:
                category_scores[category_id] += 0.1
            else:
                category_scores[category_id] = 0.1
        
        # Determine best category
        if category_scores:
            best_category = max(category_scores.items(), key=lambda x: x[1])
            category_id, confidence = best_category
            return category_id, min(confidence, 1.0), "pattern_matching"
        
        # Default fallback
        return 3, 0.1, "default_fallback"  # Money Transfer
    
    def _categorize_by_amount(self, amount: float) -> Optional[Tuple[int, float]]:
        """Categorize based on transaction amount"""
        if amount <= 0:
            return None
        
        # Airtime purchases are typically small amounts
        if amount <= 5000:
            return 1, 0.3  # Airtime Purchase
        
        # School fees are typically large amounts
        if amount >= 50000:
            return 4, 0.4  # School Fees
        
        # Medium amounts could be various categories
        return None
    
    def _categorize_by_transaction_type(self, transaction_type: str) -> Optional[Tuple[int, float]]:
        """Categorize based on transaction type"""
        if not transaction_type:
            return None
        
        type_mappings = {
            "RECEIVE": (3, 0.6),  # Money Transfer
            "SEND": (3, 0.6),     # Money Transfer
            "DEPOSIT": (3, 0.7),  # Money Transfer
            "WITHDRAWAL": (3, 0.7),  # Money Transfer
            "PAYMENT": (2, 0.5),  # Bill Payment
            "PURCHASE": (5, 0.5),  # Shopping
        }
        
        return type_mappings.get(transaction_type.upper())
    
    def _extract_additional_info(self, transaction: Dict[str, Any], sms_body: str) -> None:
        """Extract additional information from SMS body"""
        
        # Extract merchant/recipient name
        merchant_patterns = [
            r"to\s+([A-Za-z\s]+?)\s+\d+",  # "to John Doe 12345"
            r"payment\s+to\s+([A-Za-z\s]+)",  # "payment to John Doe"
            r"([A-Za-z\s]+)\s+\d{5,}",  # "John Doe 12345"
        ]
        
        for pattern in merchant_patterns:
            match = re.search(pattern, sms_body, re.IGNORECASE)
            if match:
                merchant_name = match.group(1).strip()
                if len(merchant_name) > 2 and len(merchant_name) < 50:
                    transaction["merchant_name"] = merchant_name
                    break
        
        # Extract reference numbers
        ref_patterns = [
            r"TxId:\s*(\d+)",
            r"Transaction\s+Id:\s*(\d+)",
            r"Reference:\s*(\w+)",
            r"Ref:\s*(\w+)"
        ]
        
        for pattern in ref_patterns:
            match = re.search(pattern, sms_body, re.IGNORECASE)
            if match:
                transaction["reference_number"] = match.group(1)
                break
        
        # Extract location information
        location_patterns = [
            r"at\s+([A-Za-z\s]+?)\s+\d{4}-\d{2}-\d{2}",  # "at Kigali 2024-05-10"
            r"from\s+([A-Za-z\s]+?)\s+\d{4}-\d{2}-\d{2}",  # "from Kigali 2024-05-10"
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, sms_body, re.IGNORECASE)
            if match:
                location = match.group(1).strip()
                if len(location) > 2 and len(location) < 30:
                    transaction["location"] = location
                    break
    
    def categorize_batch(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Categorize a batch of transactions"""
        categorized_transactions = []
        
        for i, transaction in enumerate(transactions):
            try:
                categorized = self.categorize_transaction(transaction)
                categorized_transactions.append(categorized)
                
                # Log progress for large batches
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Categorized {i + 1}/{len(transactions)} transactions")
                    
            except Exception as e:
                self.logger.error(f"Error categorizing transaction {i}: {str(e)}")
                # Add default categorization
                transaction["category_id"] = 3
                transaction["category_name"] = "Money Transfer"
                transaction["categorization_confidence"] = 0.0
                transaction["categorization_method"] = "error_fallback"
                categorized_transactions.append(transaction)
        
        return categorized_transactions
    
    def get_category_statistics(self, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get statistics about categorization results"""
        category_counts = Counter()
        confidence_scores = []
        method_counts = Counter()
        
        for transaction in transactions:
            category_id = transaction.get("category_id", 3)
            category_name = self.categories[category_id]["name"]
            category_counts[category_name] += 1
            
            confidence = transaction.get("categorization_confidence", 0)
            confidence_scores.append(confidence)
            
            method = transaction.get("categorization_method", "unknown")
            method_counts[method] += 1
        
        return {
            "total_transactions": len(transactions),
            "category_distribution": dict(category_counts),
            "average_confidence": sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0,
            "method_distribution": dict(method_counts),
            "high_confidence_count": sum(1 for c in confidence_scores if c >= 0.7),
            "low_confidence_count": sum(1 for c in confidence_scores if c < 0.3)
        }
    
    def validate_categorization(self, transaction: Dict[str, Any]) -> bool:
        """Validate if categorization is complete and reasonable"""
        required_fields = ["category_id", "category_name", "categorization_confidence", "categorization_method"]
        
        for field in required_fields:
            if field not in transaction:
                return False
        
        # Check if category_id is valid
        if transaction["category_id"] not in self.categories:
            return False
        
        # Check if confidence is reasonable
        confidence = transaction["categorization_confidence"]
        if confidence < 0 or confidence > 1:
            return False
        
        return True

if __name__ == "__main__":
    # Test the categorizer
    categorizer = TransactionCategorizer()
    
    # Test transactions
    test_transactions = [
        {
            "Id": "TXN_001",
            "amount": 1000,
            "transaction_type": "SEND",
            "body": "TxId: 12345. Your payment of 1,000 RWF to John Doe has been completed. Your new balance: 5,000 RWF."
        },
        {
            "Id": "TXN_002", 
            "amount": 500,
            "transaction_type": "SEND",
            "body": "You have purchased airtime worth 500 RWF. Your new balance: 4,500 RWF."
        },
        {
            "Id": "TXN_003",
            "amount": 25000,
            "transaction_type": "SEND", 
            "body": "School fees payment of 25,000 RWF to University of Rwanda has been processed."
        }
    ]
    
    print("Testing Transaction Categorization:")
    print("=" * 50)
    
    for i, transaction in enumerate(test_transactions):
        categorized = categorizer.categorize_transaction(transaction)
        print(f"\nTransaction {i+1}:")
        print(f"  Category: {categorized['category_name']} (ID: {categorized['category_id']})")
        print(f"  Confidence: {categorized['categorization_confidence']:.2f}")
        print(f"  Method: {categorized['categorization_method']}")
        if 'merchant_name' in categorized:
            print(f"  Merchant: {categorized['merchant_name']}")
    
    # Test batch processing
    categorized_batch = categorizer.categorize_batch(test_transactions)
    stats = categorizer.get_category_statistics(categorized_batch)
    
    print(f"\nBatch Statistics:")
    print(f"  Total: {stats['total_transactions']}")
    print(f"  Average Confidence: {stats['average_confidence']:.2f}")
    print(f"  Category Distribution: {stats['category_distribution']}")
