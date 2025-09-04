from collections import namedtuple
import random
import socketserver

Function = namedtuple('Function', 'encode decode')

class ProtocolError(Exception):
    pass

def reverse(symmetric=False, decode=None):
    assert (symmetric and decode is None) or (not symmetric and decode)
    def decorator(func):
        if symmetric:
            return Function(encode=func, decode=func)
        return Function(encode=func, decode=decode)
    return decorator

class OP:

    @staticmethod
    @reverse(symmetric=True)
    def reversebits(msg, pos):
        for i in range(len(msg)):
            v = msg[i]
            new = (v & 1) << 7
            v >>= 1
            new |= (v & 1) << 6
            v >>= 1
            new |= (v & 1) << 5
            v >>= 1
            new |= (v & 1) << 4
            v >>= 1
            new |= (v & 1) << 3
            v >>= 1
            new |= (v & 1) << 2
            v >>= 1
            new |= (v & 1) << 1
            v >>= 1
            new |= v
            msg[i] = new

    @staticmethod
    def xor_factory(xor):
        if xor == 0:
            return None # no-op
        def func(msg, pos):
            for i in range(len(msg)):
                msg[i] ^= xor
        return Function(encode=func, decode=func)

    @staticmethod
    @reverse(symmetric=True)
    def xorpos(msg, pos):
        for i in range(len(msg)):
            stream_pos = i + pos
            msg[i] ^= stream_pos & 0xFF

    @staticmethod
    def add_factory(add):
        if add == 0:
            return None # no-op
        def encode(msg, pos):
            for i in range(len(msg)):
                msg[i] = (msg[i] + add) & 0xFF
        def decode(msg, pos):
            for i in range(len(msg)):
                msg[i] = (msg[i] - add) & 0xFF
        return Function(encode=encode, decode=decode)

    def decode_addpos(msg, pos):
        for i in range(len(msg)):
            stream_pos = i + pos
            msg[i] = (msg[i] - stream_pos) & 0xFF

    @staticmethod
    @reverse(decode=decode_addpos)
    def addpos(msg, pos):
        for i in range(len(msg)):
            stream_pos = i + pos
            msg[i] = (msg[i] + stream_pos) & 0xFF

class Cipher:

    @staticmethod
    def bake(operations):
        operations = list(filter(lambda op: op is not None, operations))
        if not operations:
            return None
        encode = tuple(op.encode for op in operations)
        decode = [op.decode for op in operations]
        decode.reverse()
        cipher = Cipher(encode, tuple(decode))
        # very dirty and lazy, just generate random input, if encoding
        # returns the same data then this is a no-op
        randmsg = bytes(random.getrandbits(8) for _ in range(200))
        if cipher.encode(randmsg, 0) == randmsg:
            return None
        return cipher

    def __init__(self, encode, decode):
        self.encode_ops = encode
        self.decode_ops = decode

    def encode(self, msg, pos):
        msg = bytearray(msg)
        for op in self.encode_ops:
            op(msg, pos)
        return bytes(msg)

    def decode(self, msg, pos):
        msg = bytearray(msg)
        for op in self.decode_ops:
            op(msg, pos)
        return bytes(msg)

class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        try:
            self.cipher = self.read_spec()
        except ProtocolError:
            print("ProtocolError")
            self.request.close()
            return
        if self.cipher is None:
            print("Bad cipher")
            self.request.close()
            return

        self.in_pos = 0
        self.out_pos = 0

        try:

            while True:
                max_qty = 0
                max_toy = None
                line = self.read_line().decode('ascii').strip()
                if not line:
                    raise ProtocolError()
                print("Recv", line)
                requests = line.split(',')
                for req in requests:
                    idx = req.index('x')
                    qty = int(req[:idx])
                    toy = req[idx + 2:]
                    if qty > max_qty:
                        max_qty = qty
                        max_toy = toy
                print("Send", (max_qty, max_toy))
                self.write('%dx %s\n' % (max_qty, max_toy))

        except ProtocolError:
            pass

        self.request.close()

    def read_line(self):
        data = b''
        while True:
            enc = self.request.recv(1)
            if not enc:
                raise ProtocolError()
            val = self.cipher.decode(enc, self.in_pos)
            data += val
            self.in_pos += 1
            if val == b'\n':
                break
        return data

    def write(self, text):
        msg = text.encode('ascii')
        self.request.sendall(self.cipher.encode(msg, self.out_pos))
        self.out_pos += len(msg)

    def read_spec(self):
        operations = []
        while True:
            op = self.request.recv(1)[0]
            if op == 0:
                break
            elif op == 1:
                operations.append(OP.reversebits)
            elif op == 2:
                operand = self.request.recv(1)[0]
                operations.append(OP.xor_factory(operand))
            elif op == 3:
                operations.append(OP.xorpos)
            elif op == 4:
                operand = self.request.recv(1)[0]
                operations.append(OP.add_factory(operand))
            elif op == 5:
                operations.append(OP.addpos)
            else:
                raise ProtocolError()
        return Cipher.bake(operations)
        

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

server = Server(('0.0.0.0', 40000), Handler)
server.serve_forever()
