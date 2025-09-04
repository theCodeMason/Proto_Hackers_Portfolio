# Import namedtuple from collections to create simple data classes
from collections import namedtuple
# Import random for generating random bytes in testing cipher operations
import random
# Import socketserver to create a TCP server with threaded request handling
import socketserver

# Define a namedtuple called Function with fields 'encode' and 'decode' for cipher operations
Function = namedtuple('Function', 'encode decode')

# Custom exception class for protocol-related errors
class ProtocolError(Exception):
    # Inherits from Exception, no additional implementation needed
    pass

# Factory function to create symmetric or asymmetric Function tuples for encoding/decoding
def reverse(symmetric=False, decode=None):
    # Assert that if symmetric, no separate decode is provided, and vice versa
    assert (symmetric and decode is None) or (not symmetric and decode)
    # Inner decorator function that wraps the provided function
    def decorator(func):
        # If symmetric, use the same function for both encode and decode
        if symmetric:
            return Function(encode=func, decode=func)
        # Otherwise, use the provided func as encode and the given decode
        return Function(encode=func, decode=decode)
    # Return the decorator
    return decorator

# Class containing static methods for various cipher operations
class OP:

    # Static method for reversing bits in each byte of the message
    @staticmethod
    # Use the reverse decorator to make this symmetric (same for encode/decode)
    @reverse(symmetric=True)
    def reversebits(msg, pos):
        # Loop through each byte in the message
        for i in range(len(msg)):
            v = msg[i]  # Get the current byte value
            new = 0  # Initialize new byte value
            # Manually reverse the bits by shifting and masking
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
            msg[i] = new  # Update the byte in the message

    # Static method to create an XOR operation with a fixed value
    @staticmethod
    def xor_factory(xor):
        # If XOR value is 0, it's a no-op, return None
        if xor == 0:
            return None  # no-op
        # Define the XOR function (symmetric)
        def func(msg, pos):
            # XOR each byte with the fixed value
            for i in range(len(msg)):
                msg[i] ^= xor
        # Return a Function with the same func for encode and decode
        return Function(encode=func, decode=func)

    # Static method for position-based XOR operation
    @staticmethod
    # Use reverse decorator for symmetric operation
    @reverse(symmetric=True)
    def xorpos(msg, pos):
        # Loop through each byte
        for i in range(len(msg)):
            # Calculate stream position
            stream_pos = i + pos
            # XOR with lower 8 bits of stream_pos
            msg[i] ^= stream_pos & 0xFF

    # Static method to create an add operation with a fixed value
    @staticmethod
    def add_factory(add):
        # If add value is 0, it's a no-op, return None
        if add == 0:
            return None  # no-op
        # Define encode function: add the value modulo 256
        def encode(msg, pos):
            for i in range(len(msg)):
                msg[i] = (msg[i] + add) & 0xFF
        # Define decode function: subtract the value modulo 256
        def decode(msg, pos):
            for i in range(len(msg)):
                msg[i] = (msg[i] - add) & 0xFF
        # Return Function with encode and decode
        return Function(encode=encode, decode=decode)

    # Separate decode function for position-based add (since it's asymmetric)
    def decode_addpos(msg, pos):
        # Loop through each byte
        for i in range(len(msg)):
            # Calculate stream position
            stream_pos = i + pos
            # Subtract stream_pos modulo 256
            msg[i] = (msg[i] - stream_pos) & 0xFF

    # Static method for position-based add operation
    @staticmethod
    # Use reverse decorator with custom decode
    @reverse(decode=decode_addpos)
    def addpos(msg, pos):
        # Loop through each byte
        for i in range(len(msg)):
            # Calculate stream position
            stream_pos = i + pos
            # Add stream_pos modulo 256
            msg[i] = (msg[i] + stream_pos) & 0xFF

# Class to represent a composed cipher from multiple operations
class Cipher:

    # Static method to create (bake) a cipher from a list of operations
    @staticmethod
    def bake(operations):
        # Filter out None (no-op) operations
        operations = list(filter(lambda op: op is not None, operations))
        # If no operations left, return None
        if not operations:
            return None
        # Collect encode functions in order
        encode = tuple(op.encode for op in operations)
        # Collect decode functions and reverse their order
        decode = [op.decode for op in operations]
        decode.reverse()
        # Create the Cipher instance
        cipher = Cipher(encode, tuple(decode))
        # Test if the cipher is effectively a no-op
        # Generate random message for testing
        randmsg = bytes(random.getrandbits(8) for _ in range(200))
        # If encoding doesn't change the message, it's a no-op
        if cipher.encode(randmsg, 0) == randmsg:
            return None
        # Return the cipher if it's effective
        return cipher

    # Initialize the Cipher with encode and decode operation lists
    def __init__(self, encode, decode):
        self.encode_ops = encode  # Tuple of encode functions
        self.decode_ops = decode  # Tuple of decode functions

    # Encode a message starting from a position
    def encode(self, msg, pos):
        # Convert to bytearray for mutability
        msg = bytearray(msg)
        # Apply each encode operation in order
        for op in self.encode_ops:
            op(msg, pos)
        # Convert back to bytes and return
        return bytes(msg)

    # Decode a message starting from a position
    def decode(self, msg, pos):
        # Convert to bytearray for mutability
        msg = bytearray(msg)
        # Apply each decode operation in reverse order
        for op in self.decode_ops:
            op(msg, pos)
        # Convert back to bytes and return
        return bytes(msg)

# Handler class for socketserver to manage client connections
class Handler(socketserver.BaseRequestHandler):

    # Main handle method for the connection
    def handle(self):
        try:
            # Read and build the cipher spec from client
            self.cipher = self.read_spec()
        except ProtocolError:
            # Print error if protocol violation
            print("ProtocolError")
            # Close the connection
            self.request.close()
            return
        # If cipher baking failed, close
        if self.cipher is None:
            print("Bad cipher")
            self.request.close()
            return

        # Initialize input and output positions for streaming ciphers
        self.in_pos = 0
        self.out_pos = 0

        try:
            # Main loop to process requests
            while True:
                max_qty = 0  # Track maximum quantity
                max_toy = None  # Track toy with max quantity
                # Read a line from the client
                line = self.read_line().decode('ascii').strip()
                # If empty line, protocol error
                if not line:
                    raise ProtocolError()
                # Print received line for debugging
                print("Recv", line)
                # Split requests by comma
                requests = line.split(',')
                # Process each request
                for req in requests:
                    # Find 'x' separator
                    idx = req.index('x')
                    # Parse quantity
                    qty = int(req[:idx])
                    # Parse toy name (skip space after 'x ')
                    toy = req[idx + 2:]
                    # Update max if this qty is higher
                    if qty > max_qty:
                        max_qty = qty
                        max_toy = toy
                # Print sent response for debugging
                print("Send", (max_qty, max_toy))
                # Write the response in format 'qtyx toy\n'
                self.write('%dx %s\n' % (max_qty, max_toy))

        except ProtocolError:
            # Silently handle protocol errors by closing
            pass

        # Close the connection
        self.request.close()

    # Read a line from the client, decoding with cipher
    def read_line(self):
        data = b''  # Buffer for data
        while True:
            # Receive one byte (encrypted)
            enc = self.request.recv(1)
            # If no data, protocol error (unexpected EOF)
            if not enc:
                raise ProtocolError()
            # Decode the byte
            val = self.cipher.decode(enc, self.in_pos)
            data += val  # Append to buffer
            self.in_pos += 1  # Increment input position
            # Stop if newline reached
            if val == b'\n':
                break
        return data  # Return the line

    # Write text to the client, encoding with cipher
    def write(self, text):
        # Encode text to bytes
        msg = text.encode('ascii')
        # Send the encoded message
        self.request.sendall(self.cipher.encode(msg, self.out_pos))
        # Increment output position by message length
        self.out_pos += len(msg)

    # Read the cipher specification from the client
    def read_spec(self):
        operations = []  # List of operations
        while True:
            # Read operation code (1 byte)
            op = self.request.recv(1)[0]
            # 0 terminates the spec
            if op == 0:
                break
            # Operation 1: reverse bits
            elif op == 1:
                operations.append(OP.reversebits)
            # Operation 2: XOR with operand
            elif op == 2:
                operand = self.request.recv(1)[0]
                operations.append(OP.xor_factory(operand))
            # Operation 3: XOR with position
            elif op == 3:
                operations.append(OP.xorpos)
            # Operation 4: Add with operand
            elif op == 4:
                operand = self.request.recv(1)[0]
                operations.append(OP.add_factory(operand))
            # Operation 5: Add with position
            elif op == 5:
                operations.append(OP.addpos)
            else:
                # Unknown op, protocol error
                raise ProtocolError()
        # Bake the cipher from operations
        return Cipher.bake(operations)
        

# Custom server class inheriting from ThreadingTCPServer
class Server(socketserver.ThreadingTCPServer):
    # Allow address reuse for quick restarts
    allow_reuse_address = True

# Create the server on port 40000
server = Server(('0.0.0.0', 40000), Handler)
# Run the server forever
server.serve_forever()
