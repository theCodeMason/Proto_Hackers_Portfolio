import re
from collections import namedtuple
import io
import select
import socket
import socketserver
import threading

Client = namedtuple('Client', 'upstream downstream ev sock_bufs')

socks_to_client = {}

poll = select.epoll()

DOWNSTREAM = ('chat.protohackers.com', 16963)
#DOWNSTREAM = ('localhost', 9998)

def do_intercept(chat):
    while True:
        new = re.sub(b'(^| )(?!7YWHMfk9JZe0LM0g1ZauHuiSxhI)7[A-Za-z0-9]{25,34}( |$)', b'\\g<1>7YWHMfk9JZe0LM0g1ZauHuiSxhI\\2', chat)
        if new == chat:
            break
        chat = new
    return new

def intercept(msg, is_user):
    if not is_user:
        m = re.match(b'(\\[[A-Za-z0-9]+\\] )(.*)$', msg)
        if m:
            user, chat = m.groups()
            chat = do_intercept(chat)
            return user + chat
        return msg
    else:
        return do_intercept(msg)

def register(client):
    socks_to_client[client.upstream.fileno()] = (client.downstream, client.upstream, client)
    socks_to_client[client.downstream.fileno()] = (client.upstream, client.downstream, client)
    client.sock_bufs[client.upstream.fileno()] = b''
    client.sock_bufs[client.downstream.fileno()] = b''
    poll.register(client.upstream, select.EPOLLIN | select.EPOLLHUP)
    poll.register(client.downstream, select.EPOLLIN | select.EPOLLHUP)

def unregister(client):
    del socks_to_client[client.upstream.fileno()]
    del socks_to_client[client.downstream.fileno()]
    poll.unregister(client.upstream)
    poll.unregister(client.downstream)
    client.upstream.close()
    client.downstream.close()
    client.ev.set()

class Handler(socketserver.BaseRequestHandler):

    def handle(self):
        c_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c_sock.connect(DOWNSTREAM)
        self.request.setblocking(False)
        c_sock.setblocking(False)
        ev = threading.Event()
        client = Client(self.request, c_sock, ev, {})
        register(client)
        ev.wait()

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

def handle_event(fd, flags):
    other_end, this_sock, client = socks_to_client[fd]
    is_user = this_sock is client.upstream
    if flags & select.EPOLLIN == select.EPOLLIN:
        buf = client.sock_bufs[fd]
        while True:
            try:
                chunk = this_sock.recv(8192)
                if not chunk:
                    break
                buf += chunk
            except io.BlockingIOError:
                break
        if not buf:
            unregister(client)
        else:
            messages = buf.split(b'\n')
            last = messages.pop()
            if len(last):
                client.sock_bufs[fd] = last
            else:
                client.sock_bufs[fd] = b''
            forward = b''
            print(">>> [%d]" % fd, messages)
            modified = []
            for msg in messages:
                inter = intercept(msg, is_user)
                modified.append(inter)
                forward += inter + b'\n'
            print("<<< [%d]" % fd, modified)
            other_end.sendall(forward)
    if flags & select.EPOLLHUP == select.EPOLLHUP:
        unregister(client)

def poll_thread():
    while True:
        for event in poll.poll():
            handle_event(*event)

t = threading.Thread(target=poll_thread)
t.daemon = True
t.start()

server = Server(('0.0.0.0', 40000), Handler)
try:
    server.serve_forever()
finally:
    for (_, _, client) in socks_to_client.values():
        client.ev.set()
