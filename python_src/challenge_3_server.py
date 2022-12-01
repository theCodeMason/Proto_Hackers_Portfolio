from collections import namedtuple
import re
import socket
import socketserver

User = namedtuple('User', 'sock name')

user_map = {}

def isascii(data):
    # True if all bytes are in ASCII range.
    return all(b <= 0x7f for b in data)

def register_user(name, sock):
    sock_id = id(sock)
    user_map[sock_id] = User(sock=sock, name=name)

def publish(msg):
    for user in user_map.values():
        send(user.sock, msg)

def send(sock, msg):
    sock.wfile.write((msg + "\n").encode('ascii'))

def broadcast_others(source, msg):
    for user in user_map.values():
        if user.sock != source:
            send(user.sock, msg)

def unregister_user(sock):
    sock_id = id(sock)
    del user_map[sock_id]

class Handler(socketserver.StreamRequestHandler):


    def handle(self):
        # state: new user, not joined
        name = None
        try:
            name = self.handle_new_user()
        except:
            import traceback
            traceback.print_exc()
        if name is None:
            try:
                self.request.close()
            except:
                pass
            return
        # state: user joined
        print("Accept user", name)
        publish("* " + name + " has joined")
        send(self, "* Users online: " + ', '.join([u.name for u in user_map.values()]))
        register_user(name, self)
        try:
            self.joined_handler(name)
        except:
            import traceback
            traceback.print_exc()
        finally:
            print("Disconnect user", name)
            unregister_user(self)
            publish("* " + name + " has left")
            try:
                self.request.close()
            except:
                pass

    def handle_new_user(self):
        send(self, "Welcome. Please enter a name: ")
        name_bin = self.rfile.readline().strip()
        print("Recv name", name_bin)
        if not name_bin or not isascii(name_bin):
            send(self, "Illegal name")
            return None
        name = name_bin.decode('ascii')
        if not re.match('^[a-zA-Z0-9]+$', name):
            send(self, "Illegal name")
            return None
        return name

    def joined_handler(self, name):
        for message_bin in self.rfile:
            message_bin = message_bin.strip()
            if not isascii(message_bin):
                print("Non-ascii from", name)
                return
            message = message_bin.decode('ascii')
            broadcast_others(self, "[" + name + "] " + message)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    server = Server(('0.0.0.0', 9999), Handler)
    server.serve_forever()
