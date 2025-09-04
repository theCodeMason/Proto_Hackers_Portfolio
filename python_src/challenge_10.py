import socket
import selectors
import re
import json
#TODO alphabetize imports

# ---------------- SlidingBufferReader ----------------
class SlidingBufferReader:
    def __init__(self):
        self.buf = b''  # Buffer to hold incoming data as bytes

    def append(self, chunk: bytes) -> None:
        self.buf += chunk  # Append new chunk to the buffer

    # Read a line terminated by b"\n" (returns line without newline) or None if not yet available
    def read_line(self) -> bytes | None:
        pos = self.buf.find(b'\n')  # Find position of newline
        if pos == -1:
            return None  # No complete line yet
        line = self.buf[:pos]  # Extract line
        self.buf = self.buf[pos + 1:]  # Slide buffer forward
        return line

    # Read exactly n bytes from buffer; returns bytes or None if not enough buffered yet
    def read(self, n: int) -> bytes | None:
        if len(self.buf) < n:
            return None  # Not enough data
        out = self.buf[:n]  # Extract n bytes
        self.buf = self.buf[n:]  # Slide buffer forward
        return out

# ---------------- Validation helpers ----------------
def is_valid_text(s: bytes) -> bool:
    # Check each byte if it's LF (0x0A), TAB (0x09), or printable ASCII (0x20-0x7E)
    for b in s:
        if not (b == 0x0A or b == 0x09 or (0x20 <= b < 0x7F)):
            return False
    return True

def is_valid_file_name(name: str) -> bool:
    # Check if name matches [a-zA-Z0-9\-_.]+
    return bool(re.match(r'^[a-zA-Z0-9\-_.]+$', name))

def is_valid_file_path(input: str) -> bool:
    # File path must start with '/', have a name, no empty segments, valid names
    if input == '/':
        return False  # No name
    if not input.startswith('/'):
        return False
    parts = input.split('/')
    parts = parts[1:]  # Remove leading empty
    for p in parts:
        if p == '':
            return False  # No empty segments
        if not is_valid_file_name(p):
            return False
    return True

def is_valid_dir_path(input: str) -> bool:
    # Dir path starts with '/', allow trailing '/', no empty segments, valid names
    if not input.startswith('/'):
        return False
    parts = input.split('/')
    parts = parts[1:]  # Remove leading empty
    if parts and parts[-1] == '':
        parts = parts[:-1]  # Allow trailing '/', remove last empty
    for p in parts:
        if p == '':
            return False
        if not is_valid_file_name(p):
            return False
    return True

# ---------------- In-memory store ----------------
# filename: last_revision (int)
_META: dict[str, int] = {}
# f"{filename}#{revision}": data (bytes)
_STORE: dict[str, bytes] = {}

def build_file_key(filename: str, revision: int) -> str:
    # Create key as 'filename#revision'
    return f"{filename}#{revision}"

def get_last_revision(filename: str) -> int | None:
    # Get last revision or None
    return _META.get(filename)

def get_file(filename: str, revision: int) -> bytes | None:
    # Get data for specific revision or None
    return _STORE.get(build_file_key(filename, revision))

def save_file(filename: str, data: bytes) -> int:
    # Save data, increment revision if changed, return new/last revision
    last = _META.get(filename)
    if last is None:
        # First revision
        _STORE[build_file_key(filename, 1)] = data
        _META[filename] = 1
        return 1
    last_data = get_file(filename, last)
    if last_data == data:
        return last  # No change
    rev = last + 1
    _STORE[build_file_key(filename, rev)] = data
    _META[filename] = rev
    return rev

# dir must end with '/'
def list_files_in_dir(dir: str) -> list[str]:
    # Get all subpaths starting with dir/
    if not dir.endswith('/'):
        raise RuntimeError('dir must end with "/"')
    result = []
    for key in _STORE:
        filename, _ = key.rsplit('#', 1)  # Split off revision
        if filename.startswith(dir):
            result.append(filename[len(dir):])  # Relative path
    return result

# ---------------- Protocol parsing ----------------
MT_HELP = 'help'
MT_GET = 'get'
MT_PUT = 'put'
MT_LIST = 'list'
MT_ILLEGAL = 'illegal'
MT_ERROR = 'error'

def parse_method(input: str) -> dict:
    # Parse command line, return dict with type and params or error
    parts = re.split(r'\s+', input.rstrip('\r\n').strip())
    if not parts:
        return {'type': MT_ILLEGAL, 'method': ''}

    raw_cmd = parts[0]
    cmd = raw_cmd.lower()

    if cmd == MT_HELP:
        return {'type': MT_HELP}

    if cmd == MT_PUT:
        if len(parts) != 3:
            return {'type': MT_ERROR, 'err': 'usage: PUT file length newline data', 'appendReady': True}
        filename = parts[1]
        length = int(parts[2])  # Invalid -> ValueError, but assume valid
        return {'type': MT_PUT, 'filename': filename, 'length': length}

    if cmd == MT_GET:
        if len(parts) not in (2, 3):
            return {'type': MT_ERROR, 'err': 'usage: GET file [revision]', 'appendReady': True}
        filename = parts[1]
        revision = parts[2] if len(parts) == 3 else None
        return {'type': MT_GET, 'filename': filename, 'revision': revision}

    if cmd == MT_LIST:
        if len(parts) != 2:
            return {'type': MT_ERROR, 'err': 'usage: LIST dir', 'appendReady': True}
        return {'type': MT_LIST, 'dir': parts[1]}

    return {'type': MT_ILLEGAL, 'method': raw_cmd}

# ---------------- Non-blocking multiplexed server ----------------
PORT = 40000
server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_sock.bind(('0.0.0.0', PORT))
server_sock.listen()
server_sock.setblocking(False)
print(f"Server listening on port {PORT}")

class Conn:
    # Connection state class
    def __init__(self, id: int, sock: socket.socket):
        self.id = id  # Unique ID
        self.sock = sock  # Socket
        self.reader = SlidingBufferReader()  # Reader for buffered input
        self.state = 'IDLE'  # State: 'IDLE' or 'PUT_WAIT'
        self.put_filename: str | None = None  # For PUT: filename
        self.put_remaining: int = 0  # For PUT: remaining bytes to read

    def write(self, s: str | bytes, nl: bool = True) -> None:
        # Write string or bytes, append \n if nl
        if isinstance(s, str):
            data = s.encode('utf-8')
        else:
            data = s
        if nl:
            data += b'\n'
        try:
            self.sock.sendall(data)  # Send all data
        except OSError:
            pass  # Ignore errors, like in PHP @fwrite

clients: dict[socket.socket, Conn] = {}  # sock: Conn
client_id_seed = 0  # ID counter
selector = selectors.DefaultSelector()  # Selector for multiplexing
selector.register(server_sock, selectors.EVENT_READ)  # Register server

def close_conn(c: Conn) -> None:
    # Close connection, unregister
    selector.unregister(c.sock)
    c.sock.close()
    del clients[c.sock]

def handle_idle_line(c: Conn, line: str) -> None:
    # Handle command in IDLE state
    method = parse_method(line)
    print(f"[{c.id}] got line {json.dumps({'line': line, 'method': method}, ensure_ascii=False)}")

    if method['type'] == MT_ILLEGAL:
        c.write("ERR illegal method: " + (method.get('method', '')))

    elif method['type'] == MT_HELP:
        c.write("OK usage: HELP|GET|PUT|LIST")
        c.write("READY")

    elif method['type'] == MT_ERROR:
        c.write("ERR " + method['err'])
        if method.get('appendReady'):
            c.write("READY")

    elif method['type'] == MT_PUT:
        filename = method['filename']
        length = max(0, method['length'])
        if not is_valid_file_path(filename):
            c.write("ERR illegal file name")
            c.write("READY")
            return
        # Switch to PUT_WAIT state
        c.state = 'PUT_WAIT'
        c.put_filename = filename
        c.put_remaining = length

    elif method['type'] == MT_GET:
        filename = method['filename']
        if not is_valid_file_path(filename):
            c.write("ERR illegal file name")
            return
        last = get_last_revision(filename)
        if last is None:
            c.write("ERR no such file")
            return
        rev = last
        if 'revision' in method and method['revision'] is not None:
            r = method['revision']
            if r.startswith('r') or r.startswith('R'):
                r = r[1:]
            try:
                rev = int(r)
            except ValueError:
                rev = 0
        data = get_file(filename, rev)
        if data is None:
            c.write("ERR no such revision")
            return
        c.write(f"OK {len(data)}")
        c.write(data, nl=False)
        c.write("READY")

    elif method['type'] == MT_LIST:
        dir = method['dir']
        if not is_valid_dir_path(dir):
            c.write("ERR illegal dir name")
            return
        if not dir.endswith('/') and len(dir) > 1:
            dir += '/'
        all_files = list_files_in_dir(dir)
        # Sort by path length ASC
        all_files.sort(key=len)

        result = set()
        for name in all_files:
            if '/' not in name:
                result.add(name)  # file
            else:
                dir_name = name.split('/')[0] + '/'
                result.add(dir_name)  # subdir

        items = sorted(result)
        c.write(f"OK {len(items)}")
        for item in items:
            if item.endswith('/'):
                rev = 'DIR'
            else:
                rev = 'r' + str(get_last_revision(dir + item))
            c.write(f"{item} {rev}")
        c.write("READY")

def pump_put_body(c: Conn) -> None:
    # Handle reading PUT body if in PUT_WAIT
    if c.state != 'PUT_WAIT' or c.put_remaining <= 0:
        return

    chunk = c.reader.read(c.put_remaining)
    if chunk is None:
        return  # Not enough data yet

    if not is_valid_text(chunk):
        c.write("ERR text files only")
        c.write("READY")
        # Reset state
        c.state = 'IDLE'
        c.put_filename = None
        c.put_remaining = 0
        return

    rev = save_file(c.put_filename, chunk)
    c.write(f"OK r{rev}")
    c.write("READY")

    # Reset state
    c.state = 'IDLE'
    c.put_filename = None
    c.put_remaining = 0

def accept_client() -> None:
    # Accept new client
    global client_id_seed
    try:
        client_sock, addr = server_sock.accept()
    except BlockingIOError:
        return
    client_id_seed += 1
    conn = Conn(client_id_seed, client_sock)
    clients[client_sock] = conn
    selector.register(client_sock, selectors.EVENT_READ)
    print(f"[{conn.id}] client connected")
    conn.write("READY")

# ---------------- Event loop ----------------
while True:
    events = selector.select()  # Wait for events
    for key, mask in events:
        sock = key.fileobj
        if sock is server_sock:
            accept_client()
            continue

        c = clients.get(sock)
        if c is None:
            continue

        try:
            data = c.sock.recv(8192)
        except OSError:
            data = b''

        if not data:
            # Client closed
            close_conn(c)
            continue

        c.reader.append(data)

        # If in PUT_WAIT, pump body first
        if c.state == 'PUT_WAIT':
            pump_put_body(c)
            # If still waiting, skip line parsing
            if c.state == 'PUT_WAIT':
                continue

        # In IDLE, process lines
        while c.state == 'IDLE':
            line_bytes = c.reader.read_line()
            if line_bytes is None:
                break
            line = line_bytes.decode('utf-8')  # Assume UTF-8
            handle_idle_line(c, line)
            if c.state == 'PUT_WAIT':
                pump_put_body(c)
                break
