import http.server, os, sys
os.chdir(sys.argv[1])
class H(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', 'https://app.citify.cl')
        self.send_header('Access-Control-Allow-Private-Network', 'true')
        super().end_headers()
    def do_OPTIONS(self):
        self.send_response(204); self.send_header('Access-Control-Allow-Headers', '*'); self.end_headers()
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(('127.0.0.1', 8765), H).serve_forever()
