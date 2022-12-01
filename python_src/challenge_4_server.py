import socketserver

"""
    Note: Tested out Ubuntu Amazon EC2 with modified security group settings.
"""

store = {
    b'version': b'UDP Store 0.1',
}

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        print("Debug: handle fired!")
        print("Debug: request is " + str(self.request))
        data, sock = self.request
        spl = data.split(b'=', 1)
        if len(spl) == 1:
            key = data
            value = store.get(key, b'')
            sock.sendto(key + b'=' + value, self.client_address)
        else:
            key, value = spl
            if key != b'version':
                store[key] = value

class Server(socketserver.ThreadingUDPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    server = Server(('0.0.0.0', 9999), Handler)
    server.serve_forever()
