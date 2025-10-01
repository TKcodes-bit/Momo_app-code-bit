import time
import os
import random
import json
from parse_xml import parse_sms_xml

def find_transaction_by_id(transactions, search_id):
    for txn in transactions:
        if txn['Id'] == search_id:
            return txn
    return None

def create_id_map(transactions):
    id_map = {}
    for txn in transactions:
        id_map[txn['Id']] = txn
    return id_map

def benchmark_search_methods(transactions, ids_to_search):
    id_map = create_id_map(transactions)

    # Linear search timing
    start_time = time.perf_counter()
    for tid in ids_to_search:
        find_transaction_by_id(transactions, tid)
    linear_duration = time.perf_counter() - start_time

    # Dictionary lookup timing
    start_time = time.perf_counter()
    for tid in ids_to_search:
        id_map.get(tid)
    dict_duration = time.perf_counter() - start_time

    return linear_duration, dict_duration

if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), "modified_sms_v2.xml")
    transactions = parse_sms_xml(file_path)
    
    # Validate required keys and persist as JSON to demonstrate parsing completion
    required_keys = {"Id", "transaction_type", "amount", "sender", "receiver", "timestamp"}
    missing_records = [t for t in transactions if not required_keys.issubset(t.keys())]
    if missing_records:
        print(f"Warning: {len(missing_records)} records missing required fields: {required_keys}")
    else:
        print("All records contain the required fields.")

    output_json = os.path.join(os.path.dirname(__file__), "transactions.json")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(transactions, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(transactions)} transactions to {output_json}")

    # Select up to 100 random IDs to search
    sample_size = min(100, len(transactions))
    ids_to_search = [txn['Id'] for txn in random.sample(list(transactions), k=sample_size)]

    linear_time, dict_time = benchmark_search_methods(transactions, ids_to_search)

    print(f"Total transactions: {len(transactions)}")
    print(f"IDs searched: {len(ids_to_search)}")
    print(f"Linear search time: {linear_time:.6f} seconds")
    print(f"Dictionary lookup time: {dict_time:.6f} seconds")
    if dict_time > 0:
        print(f"Dictionary lookup is {linear_time/dict_time:.2f} times faster.")
    else:
        print("Dictionary lookup time is zero (infinite speedup).")
        
