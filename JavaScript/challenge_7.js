const dgram = require('dgram');
const os = require('os');

// Configuration
const RETRANS_TIMEOUT = 3;     // seconds
const EXPIRY_TIMEOUT = 60;     // seconds
const CHUNK_SIZE = 400;        // bytes per /data/ chunk
const MAX_MSG_LEN = 1000;      // safety cap, like Python
const HOST = process.argv[2] || process.env.UDP_HOST || '0.0.0.0';
const PORT = parseInt(process.argv[3] || process.env.UDP_PORT || '40000', 10);

// Utilities
function now_ts() {
  return Date.now() / 1000;
}

function log_info(msg) {
  console.error(`[${new Date().toISOString()}] INFO: ${msg}`);
}

function log_warn(msg) {
  console.error(`[${new Date().toISOString()}] WARN: ${msg}`);
}

function log_debug(msg) {
  // Toggle verbosity here if desired
  // console.error(`[${new Date().toISOString()}] DEBUG: ${msg}`);
}

function encode_segment(s) {
  // Escape \ as \\ and / as \/
  return s.replace(/\\/g, '\\\\').replace(/\//g, '\\/');
}

function parse_slash_escaped_fields(msg) {
  if (msg === '' || msg[0] !== '/') {
    throw new Error('not starting with /');
  }
  const fields = [''];
  let escaped = false;

  const len = msg.length;
  for (let i = 1; i < len; i++) {
    const c = msg[i];
    if (escaped) {
      fields[fields.length - 1] += c;
      escaped = false;
    } else {
      if (c === '\\') {
        escaped = true;
      } else if (c === '/') {
        fields.push('');
      } else {
        fields[fields.length - 1] += c;
      }
    }
  }

  if (fields[fields.length - 1] !== '') {
    // the Python ensures the message is "finished" (ends with '/')
    throw new Error('unfinished message');
  }
  fields.pop(); // remove final empty field
  if (fields.length === 0) {
    throw new Error('no message type');
  }
  return fields;
}

function to_int32_nonneg(s) {
  const v = parseInt(s, 10);
  if (isNaN(v) || !/^-?\d+$/.test(s)) {
    throw new Error('invalid integer argument');
  }
  if (v < 0) {
    throw new Error('negative integer');
  }
  if (v >= 2147483648) {
    throw new Error('integer too large');
  }
  return v;
}

// UDP I/O
const server = dgram.createSocket('udp4');

server.on('error', (err) => {
  console.error(`server error:\n${err.stack}`);
  server.close();
});

server.on('listening', () => {
  const address = server.address();
  log_info(`UDP server listening on ${address.address}:${address.port}`);
});

server.bind(PORT, HOST);

// Sessions
const sessions = {}; // key: `${id}|${address}:${port}`

class Session {
  constructor(id, address, port, server) {
    this.id = id;
    this.address = address;
    this.port = port;
    this.received = 0;
    this.sent = 0;
    this.acknowledged = 0;
    this.unacknowledged = '';
    this.nextRetransAt = null;
    this.buffer = '';
    this.lastSeen = now_ts();
    this.server = server;
    this.key = Session.keyFor(id, address, port);
  }

  static keyFor(id, address, port) {
    return `${id}|${address}:${port}`;
  }

  touch() {
    this.lastSeen = now_ts();
  }

  log(msg) {
    log_info(`[${this.id}] ${msg}`);
  }

  debug(msg) {
    log_debug(`[${this.id}] ${msg}`);
  }

  send_ack() {
    const out = `/ack/${this.id}/${this.received}/`;
    this.server.send(out, this.port, this.address);
  }

  close() {
    const out = `/close/${this.id}/`;
    this.server.send(out, this.port, this.address);
    delete sessions[this.key];
    this.debug('closed');
  }

  app_receive(data, chunkSize) {
    this.log(`app received: ${JSON.stringify(data)}`);
    this.buffer += data;

    while (true) {
      const pos = this.buffer.indexOf('\n');
      if (pos === -1) break;
      const line = this.buffer.substring(0, pos);

      const reply = line.split('').reverse().join('') + '\n';
      this.app_send(reply, chunkSize);

      this.buffer = this.buffer.substring(pos + 1);
    }
  }

  app_send(data, chunkSize) {
    this.log(`app sent: ${JSON.stringify(data)}`);

    const len = data.length;
    for (let i = 0; i < len; i += chunkSize) {
      const chunk = data.substring(i, i + Math.min(chunkSize, len - i));
      const encoded = encode_segment(chunk);
      this.debug(`sending chunk: ${JSON.stringify(encoded)}`);

      const out = `/data/${this.id}/${this.sent}/${encoded}/`;
      this.server.send(out, this.port, this.address);
      this.sent += chunk.length;
      this.unacknowledged += chunk;

      if (this.nextRetransAt === null) {
        this.nextRetransAt = now_ts(); // schedule soon; main loop adds RETRANS_TIMEOUT
      }
    }
  }

  on_data(position, data, chunkSize) {
    if (position === this.received) {
      this.app_receive(data, chunkSize);
      this.received += data.length;
    }
    this.send_ack();
  }

  on_ack(length) {
    if (length <= this.acknowledged) return;

    if (length > this.sent) {
      // protocol violation -> close
      this.close();
      return;
    }

    const newly = length - this.acknowledged;
    this.unacknowledged = this.unacknowledged.substring(newly);
    this.acknowledged = length;

    if (this.acknowledged < this.sent) {
      // respond to ack with retransmission of still-unacked tail
      const encoded = encode_segment(this.unacknowledged);
      this.debug(`responding to ack with retransmission: ${JSON.stringify(encoded)}`);
      const out = `/data/${this.id}/${this.acknowledged}/${encoded}/`;
      this.server.send(out, this.port, this.address);
      if (this.nextRetransAt === null) {
        this.nextRetransAt = now_ts();
      }
    } else {
      // fully acked
      this.nextRetransAt = null;
    }
  }

  maybe_retransmit(now, retransTimeout, chunkSize) {
    if (this.acknowledged >= this.sent) {
      this.nextRetransAt = null;
      return;
    }
    if (this.nextRetransAt === null || now < this.nextRetransAt + retransTimeout) {
      return;
    }

    const pendingBytes = this.sent - this.acknowledged;
    const data = this.unacknowledged;
    this.log(`retransmitting last ${pendingBytes} bytes`);

    const len = data.length;
    for (let i = 0; i < len; i += chunkSize) {
      const chunk = data.substring(i, i + Math.min(chunkSize, len - i));
      const encoded = encode_segment(chunk);
      const offset = this.acknowledged + i;
      const out = `/data/${this.id}/${offset}/${encoded}/`;
      this.server.send(out, this.port, this.address);
    }

    this.nextRetransAt = now; // schedule next cycle later
  }
}

// Message handler
server.on('message', (msg, rinfo) => {
  const address = rinfo.address;
  const port = rinfo.port;
  const keyPrefix = `${address}:${port}`;
  const data = msg.toString('utf8');

  try {
    if (data.length >= MAX_MSG_LEN) {
      throw new Error('too long');
    }
    const fields = parse_slash_escaped_fields(data);
    log_debug(`received: ${JSON.stringify(fields).substring(0, 200)}`);

    const msg_ty = fields[0];

    const ensure_argc = (n) => {
      if (fields.length !== n + 1) {
        throw new Error(`'${msg_ty}' expects ${n} arguments`);
      }
    };

    if (msg_ty === 'connect') {
      ensure_argc(1);
      const sessionId = to_int32_nonneg(fields[1]);
      const key = Session.keyFor(sessionId, address, port);
      if (!sessions[key]) {
        sessions[key] = new Session(sessionId, address, port, server);
      }
      sessions[key].touch();
      // ack with 0
      server.send(`/ack/${sessionId}/0/`, port, address);

    } else if (msg_ty === 'data') {
      ensure_argc(3);
      const sessionId = to_int32_nonneg(fields[1]);
      const position = to_int32_nonneg(fields[2]);
      const dataField = fields[3];

      const key = Session.keyFor(sessionId, address, port);
      if (!sessions[key]) {
        // unknown session -> close
        server.send(`/close/${sessionId}/`, port, address);
        return;
      }
      const sess = sessions[key];
      sess.touch();
      sess.on_data(position, dataField, CHUNK_SIZE);

    } else if (msg_ty === 'ack') {
      ensure_argc(2);
      const sessionId = to_int32_nonneg(fields[1]);
      const length = to_int32_nonneg(fields[2]);

      const key = Session.keyFor(sessionId, address, port);
      if (!sessions[key]) {
        server.send(`/close/${sessionId}/`, port, address);
        return;
      }
      const sess = sessions[key];
      sess.touch();
      sess.on_ack(length);

    } else if (msg_ty === 'close') {
      ensure_argc(1);
      const sessionId = to_int32_nonneg(fields[1]);

      const key = Session.keyFor(sessionId, address, port);
      if (sessions[key]) {
        sessions[key].close();
      } else {
        server.send(`/close/${sessionId}/`, port, address);
      }
    } else {
      throw new Error('unknown message type');
    }

  } catch (e) {
    log_warn(`invalid message from ${address}:${port}: ${e.message}`);
  }
});

// Periodic tasks: retransmit + expire
setInterval(() => {
  const now = now_ts();

  // Retransmissions and expiry
  Object.values(sessions).forEach((sess) => {
    sess.maybe_retransmit(now, RETRANS_TIMEOUT, CHUNK_SIZE);
    if (now - sess.lastSeen > EXPIRY_TIMEOUT) {
      log_info(`[${sess.id}] session expired`);
      sess.close();
    }
  });
}, 1000); // Check every second
