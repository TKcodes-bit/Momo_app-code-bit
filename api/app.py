import json
import os
import sys
import base64
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse


# Make `dsa` directory importable to use existing XML parser
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DSA_DIR = os.path.join(PROJECT_ROOT, "dsa")
if DSA_DIR not in sys.path:
    sys.path.insert(0, DSA_DIR)

try:
    from parse_xml import parse_sms_xml  
except Exception:
    parse_sms_xml = None  


DATA_XML_PATH = os.path.join(DSA_DIR, "modified_sms_v2.xml")
DATA_JSON_PATH = os.path.join(DSA_DIR, "transactions.json")


class InMemoryStore:
    def __init__(self) -> None:
        self.transactions = []  
        self.id_map = {}  
        self.next_numeric_suffix = 1

    def load(self) -> None:
        # Prefer loading from JSON if it exists (to preserve prior mutations)
        if os.path.exists(DATA_JSON_PATH):
            try:
                with open(DATA_JSON_PATH, "r", encoding="utf-8") as f:
                    self.transactions = json.load(f)
            except Exception:
                self.transactions = []
        elif parse_sms_xml is not None and os.path.exists(DATA_XML_PATH):
            try:
                self.transactions = parse_sms_xml(DATA_XML_PATH)
            except Exception:
                self.transactions = []
        else:
            self.transactions = []

        # Build id map and next id suffix
        self.id_map = {}
        max_suffix = 0
        for txn in self.transactions:
            txn_id = str(txn.get("Id") or txn.get("id") or "").strip()
            if not txn_id:
                txn_id = self._generate_id()
                txn["Id"] = txn_id
            self.id_map[txn_id] = txn
            # Parse numeric suffix if matches TXN_XXXXXX pattern
            if txn_id.startswith("TXN_"):
                try:
                    suffix = int(txn_id.split("_", 1)[1])
                    if suffix > max_suffix:
                        max_suffix = suffix
                except Exception:
                    pass
        self.next_numeric_suffix = max_suffix + 1

    def _generate_id(self) -> str:
        txn_id = f"TXN_{self.next_numeric_suffix:06d}"
        self.next_numeric_suffix += 1
        return txn_id

    def list(self) -> list:
        return self.transactions

    def get(self, txn_id: str) -> dict | None:
        return self.id_map.get(txn_id)

    def create(self, payload: dict) -> dict:
        txn = dict(payload)
        txn_id = str(txn.get("Id") or "").strip()
        if not txn_id or txn_id in self.id_map:
            txn_id = self._generate_id()
        txn["Id"] = txn_id
        self.transactions.append(txn)
        self.id_map[txn_id] = txn
        return txn

    def update(self, txn_id: str, payload: dict) -> dict | None:
        existing = self.id_map.get(txn_id)
        if not existing:
            return None
        for k, v in payload.items():
            if k == "Id":
                continue
            existing[k] = v
        return existing

    def delete(self, txn_id: str) -> bool:
        existing = self.id_map.get(txn_id)
        if not existing:
            return False
        self.transactions = [t for t in self.transactions if t.get("Id") != txn_id]
        self.id_map.pop(txn_id, None)
        return True

    def persist(self) -> None:
        try:
            with open(DATA_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(self.transactions, f, ensure_ascii=False, indent=2)
        except Exception:
            pass


store = InMemoryStore()
store.load()


def parse_json_body(handler: BaseHTTPRequestHandler) -> tuple[dict, str | None]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except Exception:
        length = 0
    body = handler.rfile.read(length) if length > 0 else b""
    if not body:
        return {}, None
    try:
        return json.loads(body.decode("utf-8")), None
    except Exception as e:
        return {}, f"Invalid JSON: {e}"


class TransactionsHandler(BaseHTTPRequestHandler):
    AUTH_REALM = "Transactions API"

    def _set_cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, status: int, payload: dict | list) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self._set_cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _unauthorized(self) -> None:
        # 401 with WWW-Authenticate for Basic realm
        self.send_response(401)
        self._set_cors()
        self.send_header("WWW-Authenticate", f'Basic realm="{self.AUTH_REALM}", charset="UTF-8"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        body = json.dumps({"error": "Unauthorized"}).encode("utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _require_auth(self) -> bool:
        # Credentials from environment
        expected_user = os.getenv("AUTH_USERNAME", "admin")
        expected_pass = os.getenv("AUTH_PASSWORD", "secret")
        # Read Authorization header
        auth_header = self.headers.get("Authorization", "")
        if not auth_header.startswith("Basic "):
            self._unauthorized()
            return False
        b64_part = auth_header.split(" ", 1)[1].strip()
        try:
            decoded = base64.b64decode(b64_part).decode("utf-8")
        except Exception:
            self._unauthorized()
            return False
        if ":" not in decoded:
            self._unauthorized()
            return False
        username, password = decoded.split(":", 1)
        if username != expected_user or password != expected_pass:
            self._unauthorized()
            return False
        return True

    def do_OPTIONS(self) -> None:  
        self.send_response(204)
        self._set_cors()
        self.end_headers()

    def do_GET(self) -> None:  
        if not self._require_auth():
            return
        parsed = urlparse(self.path)
        path_parts = [p for p in parsed.path.split("/") if p]

        if len(path_parts) == 1 and path_parts[0] == "transactions":
            self._send_json(200, store.list())
            return

        if len(path_parts) == 2 and path_parts[0] == "transactions":
            txn_id = path_parts[1]
            txn = store.get(txn_id)
            if not txn:
                self._send_json(404, {"error": "Transaction not found"})
                return
            self._send_json(200, txn)
            return

        self._send_json(404, {"error": "Not found"})

    def do_POST(self) -> None:  
        if not self._require_auth():
            return
        parsed = urlparse(self.path)
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) == 1 and path_parts[0] == "transactions":
            payload, err = parse_json_body(self)
            if err:
                self._send_json(400, {"error": err})
                return
            if not isinstance(payload, dict):
                self._send_json(400, {"error": "Expected JSON object"})
                return
            txn = store.create(payload)
            store.persist()
            self._send_json(201, txn)
            return
        self._send_json(404, {"error": "Not found"})

    def do_PUT(self) -> None:  
        if not self._require_auth():
            return
        parsed = urlparse(self.path)
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) == 2 and path_parts[0] == "transactions":
            txn_id = path_parts[1]
            payload, err = parse_json_body(self)
            if err:
                self._send_json(400, {"error": err})
                return
            if not isinstance(payload, dict):
                self._send_json(400, {"error": "Expected JSON object"})
                return
            updated = store.update(txn_id, payload)
            if not updated:
                self._send_json(404, {"error": "Transaction not found"})
                return
            store.persist()
            self._send_json(200, updated)
            return
        self._send_json(404, {"error": "Not found"})

    def do_DELETE(self) -> None: 
        if not self._require_auth():
            return
        parsed = urlparse(self.path)
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) == 2 and path_parts[0] == "transactions":
            txn_id = path_parts[1]
            ok = store.delete(txn_id)
            if not ok:
                self._send_json(404, {"error": "Transaction not found"})
                return
            store.persist()
            self._send_json(204, {})
            return
        self._send_json(404, {"error": "Not found"})


def run(host: str = "127.0.0.1", port: int = 8000) -> None:
    httpd = HTTPServer((host, port), TransactionsHandler)
    print(f"Serving on http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    run()


