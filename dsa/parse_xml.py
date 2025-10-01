import xml.etree.ElementTree as ET
from datetime import datetime
import json

def parse_sms_xml(file_path):
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        transactions = []
        
        # Discover likely record elements (sms/transaction)
        if root.tag in ('sms', 'transaction'):
            sms_elements = [root]
        elif root.tag in ('transactions', 'data'):
            sms_elements = list(root.findall('sms')) or list(root.findall('transaction'))
        else:
            sms_elements = list(root.findall('.//sms')) or list(root.findall('.//transaction'))
        
        for sms in sms_elements:
            # Collect raw fields from children and attributes
            raw = {}
            for child in sms:
                raw[child.tag] = (child.text or '').strip() if child.text is not None else None
            for attr_name, attr_value in sms.attrib.items():
                raw[attr_name] = attr_value
            
            # Normalize to required schema
            normalized = {}
            # Id
            if 'Id' in raw:
                normalized['Id'] = raw['Id']
            elif 'id' in raw:
                normalized['Id'] = raw['id']
            elif 'transaction_id' in raw:
                normalized['Id'] = raw['transaction_id']
            else:
                normalized['Id'] = f"txn_{len(transactions) + 1}"
            # transaction_type
            normalized['transaction_type'] = (
                raw.get('transaction_type')
                or raw.get('Type')
                or raw.get('type')
                or 'UNKNOWN'
            )
            # amount
            normalized['amount'] = raw.get('amount') or raw.get('Amount') or '0'
            # sender / receiver
            normalized['sender'] = raw.get('sender') or raw.get('Sender') or 'Unknown'
            normalized['receiver'] = raw.get('receiver') or raw.get('Receiver') or 'Unknown'
            # timestamp
            normalized['timestamp'] = raw.get('timestamp') or raw.get('Timestamp') or datetime.now().isoformat()
            
            # Preserve extra fields not in the core schema
            core_keys = {
                'Id','id','transaction_id','transaction_type','Type','type',
                'amount','Amount','sender','Sender','receiver','Receiver','timestamp','Timestamp'
            }
            for k, v in raw.items():
                if k not in core_keys and k not in normalized:
                    normalized[k] = v
            
            transactions.append(normalized)
        
        return transactions
        
    except ET.ParseError as e:
        print(f"XML parsing error: {e}")
        return []
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []

def create_sample_xml(file_path, num_transactions=50):
    
    import random
    
    # Sample data for generating realistic SMS transactions
    transaction_types = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT', 'RECEIVE']
    senders = ['+250788123456', '+250789123456', '+250787123456', '+250786123456']
    receivers = ['+250788654321', '+250789654321', '+250787654321', '+250786654321']
    
    # Create root element
    root = ET.Element('transactions')
    
    for i in range(num_transactions):
        sms = ET.SubElement(root, 'sms')
        
        # Generate transaction data
        txn_id = f"TXN_{i+1:06d}"
        txn_type = random.choice(transaction_types)
        amount = f"{random.randint(1000, 500000)}"
        sender = random.choice(senders)
        receiver = random.choice(receivers)
        timestamp = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        
        # Add child elements
        ET.SubElement(sms, 'Id').text = txn_id
        ET.SubElement(sms, 'Type').text = txn_type
        ET.SubElement(sms, 'Amount').text = amount
        ET.SubElement(sms, 'Sender').text = sender
        ET.SubElement(sms, 'Receiver').text = receiver
        ET.SubElement(sms, 'Timestamp').text = timestamp
        ET.SubElement(sms, 'Status').text = random.choice(['SUCCESS', 'PENDING', 'FAILED'])
        ET.SubElement(sms, 'Description').text = f"Mobile money {txn_type.lower()}"
    
    # Write to file
    tree = ET.ElementTree(root)
    tree.write(file_path, encoding='utf-8', xml_declaration=True)
    print(f"Sample XML file created: {file_path} with {num_transactions} transactions")

if __name__ == "__main__":
    # Test the parser
    sample_file = "modified_sms_v2.xml"
    
    # Create sample XML if it doesn't exist
    import os
    if not os.path.exists(sample_file):
        print("Creating sample XML file...")
        create_sample_xml(sample_file, 100)
    
    # Test parsing
    print("Testing XML parser...")
    transactions = parse_sms_xml(sample_file)
    
    print(f"Parsed {len(transactions)} transactions")
    if transactions:
        print("Sample transaction:")
        print(json.dumps(transactions[0], indent=2))
