#!/usr/bin/env python3

# A simple HTTP server used by /http tests.

import http
from http.server import BaseHTTPRequestHandler, HTTPServer


class Simple(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(http.HTTPStatus.OK)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(
            'Response to GET by simple HTTP server'.encode('utf-8'))

    def do_POST(self):
        self.send_response(http.HTTPStatus.OK)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(
            'Response to POST by simple HTTP server'.encode('utf-8'))


def main():
    HTTPServer(server_address=('', 18000),
               RequestHandlerClass=Simple).serve_forever()


if __name__ == '__main__':
    main()
