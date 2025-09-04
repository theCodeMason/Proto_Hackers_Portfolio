import socket
import selectors
import select
import time

def short(d):
    s = str(d)
    if len(s) > 200:
        return s[:100] + " ... " + s[-100:]
    return s

class LineReversal:
    def __init__(self):
        pass

    def poll(self):
        for s in sessions.values():
            nl = s.recv_buf.find(b'\n')
            if nl == -1:
                continue
            spl = s.recv_buf.split(b'\n')
            s.recv_buf = spl.pop()
            for line in spl:
                l = list(line)
                l.reverse()
                rev = bytes(l)
                s.send_data(rev + b'\n')

app = LineReversal()

sessions = {}

def valid_int(s):
    try:
        val = int(s.decode('ascii'))
        if val >= 2147483648:
            return None
        if val < 0:
            return None
        return val
    except ValueError:
        return None

def fmt_args(m_type, *args):
    data = b'/' + m_type.encode('ascii') + b'/'
    for a in args:
        if type(a) == int:
            if a >= 2147483648:
                return None
            a = str(a)
        if type(a) == str:
            a = a.encode('ascii')
        a = a.replace(b'\\', b'\\\\').replace(b'/', b'\\/')
        data += a + b'/'
    return data

def tick():
    t = time.time()
    for session in list(sessions.values()):
        if session.is_expired(t):
            session.close()
            continue
        if session.needs_retry(t):
            session.retry()
    app.poll()

class Session:

    RETRY_TIMEOUT = 1
    EXPIRE_TIMEOUT = 60

    def __init__(self, sess_id, sock, addr):
        self.id = sess_id
        self.sock = sock
        self.addr = addr
        self.closed = False
        self.recv_len = 0
        self.recv_buf = b''
        # amount of acknowledged data
        self.send_ack_len = 0
        # Total size we have buffered to send
        self.send_len = 0
        self.send_buf = b''
        # Timeout counter
        self.ack_timer = None

    def close(self):
        self.send('close', self.id)
        del sessions[self.id]
        self.closed = True

    def update_last_ack(self):
        # Caught up, clear timer
        if self.send_ack_len == self.send_len:
            self.ack_timer = None
        else:
            # ack received, but still waiting for more
            self.ack_timer = time.time()

    def update_last_send(self):
        # Start timer if not already
        if self.ack_timer is None:
            self.ack_timer = time.time()

    def is_expired(self, t):
        if self.ack_timer is None:
            return False
        return t - self.ack_timer > self.EXPIRE_TIMEOUT

    def needs_retry(self, t):
        if self.ack_timer is None:
            return False
        return t - self.ack_timer > self.RETRY_TIMEOUT

    def send(self, m_type, *args):
        data = fmt_args(m_type, *args)
        if data is not None:
            print(">>>", short(data))
            self.sock.sendto(data, self.addr)

    def on_data(self, data, pos):
        if pos > self.recv_len:
            # This is future data, ignore
            self.send('ack', self.id, self.recv_len)
            return
        # How much of this data have we already received
        overlap = self.recv_len - pos
        # any new data from this packet
        new_data = data[overlap:]
        self.recv_len += len(new_data)
        if len(new_data):
            self.recv_buf += new_data
        self.send('ack', self.id, self.recv_len)

    def on_ack(self, l):
        # ack for previous data
        if l <= self.send_ack_len:
            print("Ignoring ack (prev data)")
            return
        # unexpected ack
        if l > self.send_len:
            self.close()
            return
        # length of data confirmed by ack
        conf = l - self.send_ack_len
        self.send_buf = self.send_buf[conf:]
        self.send_ack_len = l
        self.update_last_ack()
        # not all the data was confirmed, send the rest again
        if l < self.send_len:
            self.retry()

    def retry(self):
        trunc = self.send_buf[:950]
        self.send('data', self.id, self.send_ack_len, trunc)

    def send_data(self, data):
        self.send_buf += data
        trunc = self.send_buf[:950]
        self.update_last_send()
        self.send('data', self.id, self.send_ack_len, trunc)
        self.send_len += len(data)

def unescape_split(data):
    esc = False
    cur = b''
    fields = []
    for c in data:
        if c == b'\\'[0]:
            if esc:
                cur += b'\\'
                esc = False
            else:
                esc = True
            continue
        if esc:
            cur += bytes([c])
            esc = False
            continue
        if c == b'/'[0]:
            fields.append(cur)
            cur = b''
        else:
            cur += bytes([c])
    fields.append(cur)
    return fields

def recv_packet(sock, data, address):
    if len(data) < 3 or len(data) >= 1000:
        print("Drop invalid (len)", data)
        return
    if data[0] != b'/'[0] or data[-1] != b'/'[0]:
        print("Drop invalid (slash)", data)
        return
    fields = unescape_split(data)
    m_type = fields[1]
    fields = fields[2:-1]
    print("<<<", m_type, short(fields))
    if m_type == b'connect':
        if len(fields) != 1:
            print("Drop invalid (connect len)", data)
            return
        session = valid_int(fields[0])
        if session is None:
            print("Drop invalid (connect session)", data)
            return
        if session not in sessions:
            s = sessions[session] = Session(session, sock, address)
        sessions[session].send('ack', session, 0)
    elif m_type == b'data':
        if len(fields) != 3:
            print("Drop invalid (data len)", data)
            return
        session = valid_int(fields[0])
        if session is None:
            print("Drop invalid (data session)", data)
            return
        pos = valid_int(fields[1])
        if pos is None:
            print("Drop invalid (data pos int)", data)
            return
        msg_data = fields[2]
        if session not in sessions:
            print("Drop invalid (data no session)", data)
            data = fmt_args('close', session)
            if data is not None:
                print(">>>", data)
                sock.sendto(data, address)
            return
        sessions[session].on_data(msg_data, pos)
    elif m_type == b'ack':
        if len(fields) != 2:
            print("Drop invalid (ack len)", data)
            return
        session = valid_int(fields[0])
        if session is None:
            print("Drop invalid (ack session int)", data)
            return
        a_len = valid_int(fields[1])
        if a_len is None:
            print("Drop invalid (ack len int)", data)
            return
        if session not in sessions:
            print("Drop invalid (ack no session)", data)
            data = fmt_args('close', session)
            if data is not None:
                print(">>>", data)
                sock.sendto(data, address)
            return
        sessions[session].on_ack(a_len)
    elif m_type == b'close':
        if len(fields) != 1:
            print("Drop invalid (close len)", data)
            return
        session = valid_int(fields[0])
        if session is None:
            print("Drop invalid (close session int)", data)
            return
        if session not in sessions:
            print("Drop invalid (close no session)", data)
            return
        sessions[session].close()
    else:
        print("Drop invalid (type)", data)


if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', 40000))
    selector = selectors.EpollSelector()
    selector.register(sock, selectors.EVENT_READ)
    try:
        while True:
            ready = selector.select(timeout=1)
            if len(ready):
                data, address = sock.recvfrom(8192)
                recv_packet(sock, data, address)
            tick()
    finally:
        sock.close()
