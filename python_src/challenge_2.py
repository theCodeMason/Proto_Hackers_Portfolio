
import json
import socket
import socketserver
import struct
import bisect

fmt = struct.Struct('!cii')

class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        timestamps = []
        prices = []
        print("Got connection")
        while True:
            msg = b''
            ex = False
            while len(msg) < 9:
                buf = self.request.recv(9 - len(msg))
                if not buf:
                    ex = True
                    break
                msg += buf
            if ex:
                break
            type, a, b = fmt.unpack(msg)
            if type == b'I':
                timestamp, price = a, b
                i = bisect.bisect_right(timestamps, timestamp)
                timestamps.insert(i, timestamp)
                prices.insert(i, price)
            elif type == b'Q':
                mintime, maxtime = a, b
                if maxtime < mintime:
                    mean = 0
                else:
                    left = bisect.bisect_left(timestamps, mintime)
                    right = bisect.bisect_right(timestamps, maxtime, lo=left)
                    if right == left:
                        mean = 0
                    else:
                        selected = prices[left:right]
                        mean = int(sum(selected) / len(selected))
                self.request.send(struct.pack('!i', mean))
            else:
                self.request.sendall(b'undefined!!!1111!! rm -rf /\n')
                break
        print("Close")
        self.request.close()

class Server(socketserver.ForkingTCPServer):
    allow_reuse_address = True

server = Server(('0.0.0.0', 40000), Handler)
server.serve_forever()
