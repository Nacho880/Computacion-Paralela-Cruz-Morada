#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import random
import re

GENDERS = ["MASCULINO", "FEMENINO"]

class MockHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        if self.path == "/cpyd/v1/login/authenticate":
            length = int(self.headers.get("Content-Length", 0))
            self.rfile.read(length)
            body = json.dumps({"jwt": "mock_token_12345"})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        match = re.match(r"^/cpyd/v1/person/(.+)$", self.path)
        if match:
            uuid = match.group(1)
            gender = GENDERS[int(uuid.replace("-", ""), 16) % 2]
            body = json.dumps({
                "rut": "12.345.678-5",
                "firstName": "MOCK",
                "lastName": "USUARIO",
                "gender": gender,
                "birthDate": "1990-01-01",
                "active": True
            })
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body.encode())
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == "__main__":
    server = HTTPServer(("localhost", 3000), MockHandler)
    print("Mock API corriendo en http://localhost:3000")
    print("  POST /cpyd/v1/login/authenticate -> jwt mock")
    print("  GET  /cpyd/v1/person/<uuid>      -> gender MASCULINO/FEMENINO")
    print("Ctrl+C para detener")
    server.serve_forever()