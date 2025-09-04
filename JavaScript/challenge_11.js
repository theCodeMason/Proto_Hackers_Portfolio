const net = require('net');

const AS_HOST = "pestcontrol.protohackers.com";
const AS_PORT = 20547;

const WRAPPER_SIZE = 6;
const MAX_LENGTH = 1024 * 1024;

// Message Types
const TY_HELLO = 0x50;
const TY_ERROR = 0x51;
const TY_OK = 0x52;
const TY_DIAL_AUTH = 0x53;
const TY_TARGET_POPS = 0x54;
const TY_CREATE_POLICY = 0x55;
const TY_DELETE_POLICY = 0x56;
const TY_POLICY_RESULT = 0x57;
const TY_SITE_VISIT = 0x58;

// Policy Actions
const ACT_CULL = 0x90;
const ACT_CONSERVE = 0xA0;

class ProtoError extends Error {}

function hexRepr(bytes) {
    return Array.from(bytes).map(b => {
        if (b >= 0x41 && b <= 0x7e) return String.fromCharCode(b);
        return b.toString(16).padStart(2, '0');
    }).join(' ');
}

class OutMsg {
    constructor(msgTy) {
        this.buf = [msgTy, 0, 0, 0, 0];
        this.wrapped = false;
    }

    addU8(n) {
        this.buf.push(n & 0xFF);
    }

    addU32(n) {
        this.buf.push((n >> 24) & 0xFF);
        this.buf.push((n >> 16) & 0xFF);
        this.buf.push((n >> 8) & 0xFF);
        this.buf.push(n & 0xFF);
    }

    addStr(s) {
        const b = Buffer.from(s, 'ascii');
        this.addU32(b.length);
        for (let ch of b) {
            this.buf.push(ch);
        }
    }

    toBytes() {
        return Buffer.from(this.buf);
    }

    send(peer) {
        if (!this.wrapped) {
            let len = this.buf.length + 1;
            this.buf[1] = (len >> 24) & 0xFF;
            this.buf[2] = (len >> 16) & 0xFF;
            this.buf[3] = (len >> 8) & 0xFF;
            this.buf[4] = len & 0xFF;
            let sum = this.buf.reduce((a, b) => a + b, 0) % 256;
            let checksum = (sum === 0) ? 0 : (256 - sum);
            this.addU8(checksum);
            this.wrapped = true;
        }
        const b = this.toBytes();
        peer.debug(`-> ${hexRepr(b)}`);
        peer.enqueueWrite(b);
    }
}

class InMsg {
    constructor(ty, buf) {
        this.ty = ty;
        this.buf = buf;
        this.i = 0;
    }

    getBytes(n) {
        if (this.i > this.buf.length - n) {
            throw new ProtoError("content exceeds declared length");
        }
        const b = this.buf.slice(this.i, this.i + n);
        this.i += n;
        return b;
    }

    getU32() {
        const b = this.getBytes(4);
        return b.readUInt32BE(0);
    }

    getStr() {
        const size = this.getU32();
        const b = this.getBytes(size);
        return b.toString('ascii');
    }

    checkEnd() {
        if (this.i !== this.buf.length) {
            throw new ProtoError("unused bytes in message");
        }
    }

    static tryParse(inBuf) {
        if (inBuf.length < 6) return {msg: null, consumed: 0};
        const ty = inBuf.readUInt8(0);
        const len = inBuf.readUInt32BE(1);
        if (len >= MAX_LENGTH) throw new ProtoError("message is too long");
        if (inBuf.length < len) return {msg: null, consumed: 0};
        const contentLen = len - WRAPPER_SIZE;
        const payload = inBuf.slice(5, 5 + contentLen);
        const checksum = inBuf.readUInt8(len - 1);
        let sum = ty + Array.from(inBuf.slice(1, 5)).reduce((a, b) => a + b, 0);
        sum += contentLen > 0 ? Array.from(payload).reduce((a, b) => a + b, 0) : 0;
        sum += checksum;
        if ((sum % 256) !== 0) {
            throw new ProtoError("invalid checksum");
        }
        const msg = new InMsg(ty, payload);
        return {msg, consumed: len};
    }
}

class Policy {
    constructor(site, species, ty) {
        this.site = site;
        this.species = species;
        this.ty = ty;
        this.id = null;
        this.state = 'pending';
    }

    setId(id, asConn) {
        if (this.state === 'exists') return;
        this.id = id;
        if (this.state === 'deleted') {
            this.sendDelete(asConn);
        } else if (this.state === 'pending') {
            this.state = 'exists';
        }
    }

    sendDelete(asConn) {
        if (this.id === null) return;
        const m = new OutMsg(TY_DELETE_POLICY);
        m.addU32(this.id);
        m.send(asConn);
    }

    delete(asConn) {
        if (this.state === 'pending') {
            this.state = 'deleted';
        } else if (this.state === 'exists') {
            this.state = 'deleted';
            this.sendDelete(asConn);
        }
    }
}

class Peer {
    constructor(server, sock, role) {
        this.server = server;
        this.sock = sock;
        this.role = role;
        this.gotHello = false;
        this.site = null;
        this.rbuf = Buffer.alloc(0);
        this.writeQueue = [];
        this.wantClose = false;
        this.isWriting = false;
        this.sock.on('data', chunk => {
            this.onReadable(chunk);
        });
        this.sock.on('error', err => {
            this.debug(`Socket error: ${err.message}`);
            this.close();
        });
        this.sock.on('close', () => {
            this.close();
        });
    }

    enqueueWrite(b) {
        this.writeQueue.push(b);
        this.tryWrite();
    }

    hasWrite() {
        return this.writeQueue.length > 0;
    }

    debug(s) {
        console.error(`[DBG] ${s}`);
    }

    warn(s) {
        console.error(`[WRN] ${s}`);
    }

    sendHello() {
        const m = new OutMsg(TY_HELLO);
        m.addStr("pestcontrol");
        m.addU32(1);
        m.send(this);
    }

    close() {
        if (!this.sock) return;
        this.server.removePeer(this);
        this.sock.destroy();
        this.sock = null;
    }

    onReadable(chunk) {
        this.rbuf = Buffer.concat([this.rbuf, chunk]);
        while (true) {
            let msg, consumed;
            try {
                ({msg, consumed} = InMsg.tryParse(this.rbuf));
            } catch (e) {
                if (e instanceof ProtoError) {
                    this.warn(`protocol error: ${e.message}`);
                    const m = new OutMsg(TY_ERROR);
                    m.addStr(e.message);
                    m.send(this);
                    this.wantClose = true;
                    this.tryWrite();
                    return;
                } else {
                    throw e;
                }
            }
            if (!msg) break;
            this.debug(`<- ${hexRepr(this.rbuf.slice(0, consumed))}`);
            this.rbuf = this.rbuf.slice(consumed);
            try {
                if (msg.ty === TY_HELLO) {
                    const protocol = msg.getStr();
                    const version = msg.getU32();
                    msg.checkEnd();
                    this.debug(`<- Hello { protocol: ${protocol}, version: ${version} }`);
                    if (protocol !== "pestcontrol" || version !== 1) {
                        throw new ProtoError("unexpected protocol or version");
                    }
                    this.gotHello = true;
                } else {
                    if (!this.gotHello) throw new ProtoError("did not get Hello");
                    if (msg.ty === TY_ERROR) {
                        const err = msg.getStr();
                        msg.checkEnd();
                        this.debug(`<- Error { message: ${err} }`);
                    } else if (msg.ty === TY_OK) {
                        msg.checkEnd();
                    } else {
                        if (this.role === 'client') {
                            this.server.clientHandler(this, msg);
                        } else {
                            this.server.asHandler(this, msg);
                        }
                    }
                }
            } catch (e) {
                if (e instanceof ProtoError) {
                    this.warn(`protocol error: ${e.message}`);
                    const m = new OutMsg(TY_ERROR);
                    m.addStr(e.message);
                    m.send(this);
                } else {
                    this.warn(`fatal: ${e.message}`);
                    this.wantClose = true;
                    this.tryWrite();
                    return;
                }
            }
        }
        this.tryWrite();
    }

    tryWrite() {
        if (this.writeQueue.length === 0 || this.isWriting || !this.sock) return;
        this.isWriting = true;
        const writeNext = () => {
            if (this.writeQueue.length === 0 || !this.sock) {
                this.isWriting = false;
                if (this.wantClose) {
                    this.sock.end();
                }
                return;
            }
            const chunk = this.writeQueue.shift();
            const drained = this.sock.write(chunk);
            if (drained) {
                writeNext();
            } else {
                this.sock.once('drain', writeNext);
            }
        };
        writeNext();
    }
}

class Server {
    constructor(lsock) {
        this.lsock = lsock;
        this.peers = new Map();
        this.targetPops = new Map();
        this.waitingTargets = new Map();
        this.asConns = new Map();
        this.pendingVisits = new Map();
        this.pendingPolicies = new Map();
        this.policies = new Map();
    }

    keyFor(site, species) {
        return `${site}\0${species}`;
    }

    registerPeer(p) {
        this.peers.set(p.sock, p);
    }

    removePeer(p) {
        this.peers.delete(p.sock);
        if (p.role === 'as' && p.site !== null) {
            this.asConns.delete(p.site);
        }
    }

    asHandler(peer, msg) {
        if (msg.ty === TY_TARGET_POPS) {
            const site2 = msg.getU32();
            const popCnt = msg.getU32();
            const targets = new Map();
            for (let i = 0; i < popCnt; i++) {
                const species = msg.getStr();
                const minp = msg.getU32();
                const maxp = msg.getU32();
                if (targets.has(species) && (targets.get(species)[0] !== minp || targets.get(species)[1] !== maxp)) {
                    throw new ProtoError(`conflicting target for species '${species}'`);
                }
                targets.set(species, [minp, maxp]);
            }
            msg.checkEnd();
            peer.debug(`<- TargetPopulations { site: ${site2}, populations: ${JSON.stringify(Array.from(targets))} }`);
            if (peer.site !== site2) {
                throw new ProtoError("authority site mismatch");
            }
            this.targetPops.set(site2, targets);
            this.waitingTargets.set(site2, false);
            if (this.pendingVisits.has(site2)) {
                const pendings = this.pendingVisits.get(site2);
                for (let pending of pendings) {
                    this.processVisit(pending.peer, site2, pending.populations);
                }
                this.pendingVisits.set(site2, []);
            }
        } else if (msg.ty === TY_POLICY_RESULT) {
            const policyId = msg.getU32();
            msg.checkEnd();
            const site = peer.site;
            if (site === null || !this.pendingPolicies.has(site)) return;
            const pols = this.pendingPolicies.get(site);
            const pol = pols.shift();
            if (pol) {
                pol.setId(policyId, peer);
            }
        } else {
            throw new ProtoError(`unexpected message type ${msg.ty.toString(16).padStart(2, '0')}`);
        }
    }

    dialAuthorityForSite(site) {
        if (this.asConns.has(site)) {
            return this.asConns.get(site);
        }
        const sock = new net.Socket();
        const peer = new Peer(this, sock, 'as');
        peer.site = site;
        this.registerPeer(peer);
        sock.connect(AS_PORT, AS_HOST, () => {
            peer.sendHello();
            const m = new OutMsg(TY_DIAL_AUTH);
            m.addU32(site);
            m.send(peer);
        });
        sock.on('error', err => {
            peer.warn(`AS connect failed: ${err.message}`);
            peer.close();
        });
        this.asConns.set(site, peer);
        this.waitingTargets.set(site, true);
        return peer;
    }

    getAuthorityForSite(site) {
        if (this.targetPops.has(site)) {
            return this.asConns.get(site) || this.dialAuthorityForSite(site);
        }
        return this.asConns.get(site) || this.dialAuthorityForSite(site);
    }

    clientHandler(peer, msg) {
        if (msg.ty !== TY_SITE_VISIT) {
            throw new ProtoError(`unexpected message type ${msg.ty.toString(16).padStart(2, '0')}`);
        }
        const site = msg.getU32();
        const popCnt = msg.getU32();
        const pops = new Map();
        for (let i = 0; i < popCnt; i++) {
            const species = msg.getStr();
            const count = msg.getU32();
            if (pops.has(species) && pops.get(species) !== count) {
                throw new ProtoError(`conflicting counts for species '${species}'`);
            }
            pops.set(species, count);
        }
        msg.checkEnd();
        peer.debug(`<- SiteVisit { site: ${site}, populations: ${JSON.stringify(Array.from(pops))} }`);
        let asConn;
        try {
            asConn = this.getAuthorityForSite(site);
        } catch (e) {
            peer.warn(`AS dial failed: ${e.message}`);
            const m = new OutMsg(TY_ERROR);
            m.addStr(e.message);
            m.send(peer);
            return;
        }
        if (!this.targetPops.has(site) || this.waitingTargets.get(site)) {
            if (!this.pendingVisits.has(site)) {
                this.pendingVisits.set(site, []);
            }
            this.pendingVisits.get(site).push({peer, populations: pops});
            return;
        }
        this.processVisit(peer, site, pops);
    }

    processVisit(clientPeer, site, populations) {
        const targets = this.targetPops.get(site) || new Map();
        const asConn = this.asConns.get(site);
        if (!asConn) return;
        for (let [species, [minPop, maxPop]] of targets) {
            const pop = populations.get(species) || 0;
            let needPolicy = null;
            if (pop < minPop) needPolicy = 'conserve';
            if (pop > maxPop) needPolicy = 'cull';
            const key = this.keyFor(site, species);
            const curPolicy = this.policies.has(key) ? this.policies.get(key).ty : null;
            if (needPolicy === curPolicy) continue;
            if (this.policies.has(key)) {
                const policy = this.policies.get(key);
                this.policies.delete(key);
                policy.delete(asConn);
            }
            if (needPolicy !== null) {
                const m = new OutMsg(TY_CREATE_POLICY);
                m.addStr(species);
                m.addU8(needPolicy === 'cull' ? ACT_CULL : ACT_CONSERVE);
                m.send(asConn);
                const policy = new Policy(site, species, needPolicy);
                this.policies.set(key, policy);
                if (!this.pendingPolicies.has(site)) {
                    this.pendingPolicies.set(site, []);
                }
                this.pendingPolicies.get(site).push(policy);
            }
        }
    }
}

// Main
const port = process.argv[2] ? parseInt(process.argv[2]) : 0;
const netServer = net.createServer(sock => {
    const peer = new Peer(srv, sock, 'client');
    srv.registerPeer(peer);
    peer.sendHello();
});
const srv = new Server(netServer);
netServer.listen(port, '0.0.0.0', () => {
    const address = netServer.address();
    console.error(`Listening on ${address.address}:${address.port}`);
});
