import socket
import select
import struct
import sys
import os

AS_HOST = "pestcontrol.protohackers.com"
AS_PORT = 20547

WRAPPER_SIZE = 6  # type (1) + len (4) + checksum (1)
MAX_LENGTH = 1024 * 1024  # 1 MiB

# Message Types
TY_HELLO = 0x50
TY_ERROR = 0x51
TY_OK = 0x52
TY_DIAL_AUTH = 0x53
TY_TARGET_POPS = 0x54
TY_CREATE_POLICY = 0x55
TY_DELETE_POLICY = 0x56
TY_POLICY_RESULT = 0x57
TY_SITE_VISIT = 0x58

# Policy Actions
ACT_CULL = 0x90
ACT_CONSERVE = 0xA0

class ProtoError(Exception):
    pass

def hex_repr(bytes_data: bytes) -> str:
    out = []
    for b in bytes_data:
        if 0x41 <= b <= 0x7E:
            out.append(chr(b))
        else:
            out.append(f"{b:02x}")
    return " ".join(out)

class OutMsg:
    def __init__(self, msg_ty: int):
        self.buf = bytearray([msg_ty, 0, 0, 0, 0])  # placeholder length
        self.wrapped = False

    def add_u8(self, n: int):
        self.buf.append(n & 0xFF)

    def add_u32(self, n: int):
        self.buf.append((n >> 24) & 0xFF)
        self.buf.append((n >> 16) & 0xFF)
        self.buf.append((n >> 8) & 0xFF)
        self.buf.append(n & 0xFF)

    def add_str(self, s: str):
        b = s.encode('ascii')  # ASCII required by spec
        self.add_u32(len(b))
        self.buf.extend(b)

    def to_bytes(self) -> bytes:
        return bytes(self.buf)

    def send(self, peer):
        if not self.wrapped:
            length = len(self.buf) + 1  # plus checksum
            # write big-endian length into buf[1..4]
            self.buf[1] = (length >> 24) & 0xFF
            self.buf[2] = (length >> 16) & 0xFF
            self.buf[3] = (length >> 8) & 0xFF
            self.buf[4] = length & 0xFF

            total_sum = sum(self.buf) % 256
            checksum = 0 if total_sum == 0 else (256 - total_sum)
            self.add_u8(checksum)
            self.wrapped = True

        b = self.to_bytes()
        peer.debug("-> " + hex_repr(b))
        peer.enqueue_write(b)

class InMsg:
    def __init__(self, ty: int, buf: bytes):
        self.ty = ty
        self.buf = buf
        self.i = 0

    def get_bytes(self, n: int) -> bytes:
        if self.i > len(self.buf) - n:
            raise ProtoError("content exceeds declared length")
        b = self.buf[self.i:self.i + n]
        self.i += n
        return b

    def get_u32(self) -> int:
        b = self.get_bytes(4)
        return struct.unpack("!I", b)[0]

    def get_str(self) -> str:
        size = self.get_u32()
        b = self.get_bytes(size)
        return b.decode('ascii')  # spec says ASCII

    def check_end(self):
        if self.i != len(self.buf):
            raise ProtoError("unused bytes in message")

    @staticmethod
    def try_parse(in_data: bytearray) -> tuple:
        if len(in_data) < 6:
            return None, 0

        ty = in_data[0]
        length = struct.unpack("!I", in_data[1:5])[0]
        if length >= MAX_LENGTH:
            raise ProtoError("message is too long")
        if len(in_data) < length:
            return None, 0

        content_len = length - WRAPPER_SIZE  # payload length
        payload = in_data[5:5 + content_len]
        checksum = in_data[length - 1]

        # Verify checksum over type + 4 len bytes + payload + checksum
        sum_check = ty + sum(in_data[1:5]) + sum(payload) + checksum
        if (sum_check % 256) != 0:
            raise ProtoError("invalid checksum")

        msg = InMsg(ty, payload)
        return msg, length

class Policy:
    def __init__(self, site: int, species: str, ty: str):
        self.site = site
        self.species = species
        self.ty = ty  # 'cull' or 'conserve'
        self.id = None
        self.state = 'pending'  # 'pending', 'exists', 'deleted'

    def set_id(self, id: int, as_conn):
        if self.state == 'exists':
            return
        self.id = id
        if self.state == 'deleted':
            self.send_delete(as_conn)
        elif self.state == 'pending':
            self.state = 'exists'

    def send_delete(self, as_conn):
        if self.id is None:
            return
        m = OutMsg(TY_DELETE_POLICY)
        m.add_u32(self.id)
        m.send(as_conn)

    def delete(self, as_conn):
        if self.state == 'pending':
            self.state = 'deleted'
        elif self.state == 'exists':
            self.state = 'deleted'
            self.send_delete(as_conn)

class Peer:
    def __init__(self, server, sock, role: str):
        self.sock = sock
        self.server = server
        self.role = role  # 'client' or 'as'
        self.got_hello = False

        self.site = None  # only for AS peers
        self.rbuf = bytearray()
        self.wbuf = bytearray()

        self.want_close = False  # defer close until write buffer flushes

        self.sock.setblocking(False)

    def enqueue_write(self, b: bytes):
        self.wbuf.extend(b)

    def has_write(self) -> bool:
        return len(self.wbuf) > 0

    def debug(self, s: str):
        print(f"[DBG] {s}", file=sys.stderr)

    def warn(self, s: str):
        print(f"[WRN] {s}", file=sys.stderr)

    def send_hello(self):
        m = OutMsg(TY_HELLO)
        m.add_str("pestcontrol")
        m.add_u32(1)
        m.send(self)

    def close(self):
        if self.sock is None:
            return
        self.server.remove_peer(self)
        try:
            self.sock.close()
        except:
            pass
        self.sock = None

    def on_readable(self):
        try:
            chunk = self.sock.recv(65536)
        except:
            chunk = b""
        if not chunk:
            # Client closed or error
            self.close()
            return
        self.rbuf.extend(chunk)

        # Parse messages in a loop
        while True:
            try:
                msg, consumed = InMsg.try_parse(self.rbuf)
            except ProtoError as e:
                # IMPORTANT FIX: send Error, then defer close until write buffer is flushed
                self.warn("protocol error: " + str(e))
                m = OutMsg(TY_ERROR)
                m.add_str(str(e))
                m.send(self)
                self.want_close = True  # defer
                return  # exit read loop; writer will flush then close
            if msg is None:
                break

            self.debug("<- " + hex_repr(self.rbuf[:consumed]))
            self.rbuf = self.rbuf[consumed:]

            try:
                if msg.ty == TY_HELLO:
                    protocol = msg.get_str()
                    version = msg.get_u32()
                    msg.check_end()
                    self.debug(f"<- Hello {{ protocol: {protocol!r}, version: {version} }}")
                    if protocol != "pestcontrol" or version != 1:
                        raise ProtoError("unexpected protocol or version")
                    self.got_hello = True
                else:
                    if not self.got_hello:
                        raise ProtoError("did not get Hello")
                    if msg.ty == TY_ERROR:
                        err = msg.get_str()
                        msg.check_end()
                        self.debug(f"<- Error {{ message: {err!r} }}")
                    elif msg.ty == TY_OK:
                        msg.check_end()
                    else:
                        if self.role == 'client':
                            self.server.client_handler(self, msg)
                        else:  # 'as'
                            self.server.as_handler(self, msg)
            except ProtoError as e:
                # For in-message errors, send Error but keep connection alive (spec tests expect that)
                self.warn("protocol error: " + str(e))
                m = OutMsg(TY_ERROR)
                m.add_str(str(e))
                m.send(self)
                # continue parsing subsequent messages if present
            except Exception as t:
                self.warn("fatal: " + str(t))
                self.want_close = True
                return

    def on_writable(self):
        if len(self.wbuf) == 0:
            if self.want_close:
                self.close()
            return
        try:
            n = self.sock.send(self.wbuf)
        except:
            n = 0
        if n == 0:
            self.close()
            return
        if n > 0:
            self.wbuf = self.wbuf[n:]
        if len(self.wbuf) == 0 and self.want_close:
            self.close()  # flush done -> close

class Server:
    def __init__(self, lsock):
        self.lsock = lsock
        self.peers = {}  # id(sock) -> Peer

        # State mirrors PHP
        self.target_pops = {}  # site => {species: [min, max]}
        self.waiting_targets = {}  # site => bool (waiting for target pops)
        self.as_conns = {}  # site => Peer (AS connection)
        self.pending_visits = {}  # site => list of {'peer': Peer, 'populations': dict}
        self.pending_policies = {}  # site => list of Policy
        self.policies = {}  # f"{site}\0{species}" => Policy

        self.lsock.setblocking(False)

    def run(self):
        while True:
            r = [self.lsock]
            w = []
            e = None

            for p in list(self.peers.values()):
                if p.sock is None:
                    continue
                r.append(p.sock)
                if p.has_write():
                    w.append(p.sock)

            try:
                readable, writable, _ = select.select(r, w, [], 1)
            except:
                continue

            for sock in readable:
                if sock == self.lsock:
                    try:
                        cs, _ = self.lsock.accept()
                        peer = Peer(self, cs, 'client')
                        self.register_peer(peer)
                        peer.send_hello()
                    except:
                        pass
                else:
                    peer = self.peer_by_sock(sock)
                    if peer:
                        peer.on_readable()

            for sock in writable:
                peer = self.peer_by_sock(sock)
                if peer:
                    peer.on_writable()

    def key_for(self, site: int, species: str) -> str:
        return f"{site}\0{species}"

    def peer_by_sock(self, sock):
        return self.peers.get(id(sock))

    def register_peer(self, p: Peer):
        self.peers[id(p.sock)] = p

    def remove_peer(self, p: Peer):
        del self.peers[id(p.sock)]
        # Cleanup if it's an AS connection
        if p.role == 'as' and p.site is not None:
            if p.site in self.as_conns:
                del self.as_conns[p.site]
            # pending waits for that site should be retried by reconnecting upon next visit

    # === Authority (AS) handling ===

    def as_handler(self, peer: Peer, msg: InMsg):
        if msg.ty == TY_TARGET_POPS:
            site2 = msg.get_u32()
            pop_cnt = msg.get_u32()
            targets = {}
            for _ in range(pop_cnt):
                species = msg.get_str()
                minp = msg.get_u32()
                maxp = msg.get_u32()
                if species in targets and targets[species] != [minp, maxp]:
                    raise ProtoError(f"conflicting target for species '{species}'")
                targets[species] = [minp, maxp]
            msg.check_end()
            peer.debug(f"<- TargetPopulations {{ site: {site2}, populations: {targets!r} }}")
            if peer.site != site2:
                raise ProtoError("authority site mismatch")
            self.target_pops[site2] = targets
            self.waiting_targets[site2] = False

            # Process any pending site visits for this site now that we have targets:
            if site2 in self.pending_visits and self.pending_visits[site2]:
                for pending in self.pending_visits[site2]:
                    self.process_visit(pending['peer'], site2, pending['populations'])
                self.pending_visits[site2] = []

        elif msg.ty == TY_POLICY_RESULT:
            policy_id = msg.get_u32()
            msg.check_end()

            site = peer.site if peer.site is not None else -1
            if site < 0:
                return

            if site not in self.pending_policies or not self.pending_policies[site]:
                # Unexpected but ignore politely
                return
            pol = self.pending_policies[site].pop(0)
            if isinstance(pol, Policy):
                pol.set_id(policy_id, peer)

        else:
            raise ProtoError(f"unexpected message type {msg.ty:02x}")

    def dial_authority_for_site(self, site: int) -> Peer:
        if site in self.as_conns:
            return self.as_conns[site]
        addr = (AS_HOST, AS_PORT)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(addr)
        except Exception as e:
            raise ProtoError(f"failed to connect AS: {str(e)}")
        sock.setblocking(False)
        peer = Peer(self, sock, 'as')
        peer.site = site
        self.register_peer(peer)
        # hello
        peer.send_hello()

        # DialAuthority
        m = OutMsg(TY_DIAL_AUTH)
        m.add_u32(site)
        m.send(peer)

        self.as_conns[site] = peer
        self.waiting_targets[site] = True
        return peer

    def get_authority_for_site(self, site: int) -> Peer:
        if site in self.target_pops:
            return self.as_conns.get(site) or self.dial_authority_for_site(site)
        # not yet fetched -> ensure connection & request sent
        return self.as_conns.get(site) or self.dial_authority_for_site(site)

    # === Client handling ===

    def client_handler(self, peer: Peer, msg: InMsg):
        if msg.ty != TY_SITE_VISIT:
            raise ProtoError(f"unexpected message type {msg.ty:02x}")
        site = msg.get_u32()
        pop_cnt = msg.get_u32()
        pops = {}
        for _ in range(pop_cnt):
            species = msg.get_str()
            count = msg.get_u32()
            if species in pops and pops[species] != count:
                raise ProtoError(f"conflicting counts for species '{species}'")
            pops[species] = count
        msg.check_end()
        peer.debug(f"<- SiteVisit {{ site: {site}, populations: {pops!r} }}")

        # Ensure AS data:
        try:
            as_conn = self.get_authority_for_site(site)
        except ProtoError as e:
            peer.warn("AS dial failed: " + str(e))
            m = OutMsg(TY_ERROR)
            m.add_str(str(e))
            m.send(peer)
            return

        if site not in self.target_pops:
            # Queue the visit to process after targets arrive.
            if site not in self.pending_visits:
                self.pending_visits[site] = []
            self.pending_visits[site].append({'peer': peer, 'populations': pops})
            return

        self.process_visit(peer, site, pops)

    def process_visit(self, client_peer: Peer, site: int, populations: dict):
        targets = self.target_pops.get(site, {})
        as_conn = self.as_conns.get(site)
        if not as_conn:
            return  # lost AS conn; wait for next visit to re-dial

        for species, (min_pop, max_pop) in targets.items():
            pop = populations.get(species, 0)
            need_policy = None
            if pop < min_pop:
                need_policy = 'conserve'
            if pop > max_pop:
                need_policy = 'cull'

            key = self.key_for(site, species)
            cur_policy = self.policies[key].ty if key in self.policies else None
            if need_policy == cur_policy:
                continue

            # If a different/obsolete policy exists, delete it
            if key in self.policies:
                policy = self.policies.pop(key)
                policy.delete(as_conn)

            if need_policy is not None:
                m = OutMsg(TY_CREATE_POLICY)
                m.add_str(species)
                m.add_u8(ACT_CULL if need_policy == 'cull' else ACT_CONSERVE)
                m.send(as_conn)

                policy = Policy(site, species, need_policy)
                self.policies[key] = policy
                if site not in self.pending_policies:
                    self.pending_policies[site] = []
                self.pending_policies[site].append(policy)

# ---- Main ----
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("listen_port", nargs="?", type=int, default=0)
    args = parser.parse_args()
    port = args.listen_port

    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        lsock.bind(("0.0.0.0", port))
        lsock.listen(128)
    except Exception as e:
        print(f"Failed to bind: {str(e)}", file=sys.stderr)
        sys.exit(1)
    _, actual_port = lsock.getsockname()
    print(f"Listening on 0.0.0.0:{actual_port}", file=sys.stderr)

    server = Server(lsock)
    server.run()
