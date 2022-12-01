import json
import socket
import socketserver

"""
Problem URL: https://protohackers.com/problem/1
"""

def is_prime(number):
    print("Debug: Checking number " + str(number))
    if int(number) < number:
        return False
    if number > 1:
        for num in range(2, int(number**0.5) + 1):
            if number % num == 0:
                return False
        return True
    return False

class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        for line in self.rfile:
            if not line.endswith(b'}\n'):
                self.wfile.write(b"}bad")
                break
            try:
                data = json.loads(line)
            except ValueError:
                self.wfile.write(b"}bad")
                break
            if not 'method' in data or data['method'] != 'isPrime':
                self.wfile.write(b"}bad")
                break
            if not 'number' in data or type(data['number']) not in (float, int):
                self.wfile.write(b"}bad")
                break
            res = json.dumps({'method': 'isPrime', 'prime': is_prime(data['number'])})
            self.wfile.write((res + "\n").encode('utf8'))
        self.request.close()

class Server(socketserver.ForkingTCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    server = Server(('0.0.0.0', 9999), Handler)
    server.serve_forever()
