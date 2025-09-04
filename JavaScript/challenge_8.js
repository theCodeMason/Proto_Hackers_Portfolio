const net = require('net');

// ====== Helpers ======
function sock_id(sock) {
  // In Node.js, we use a Map with socket as key, so no need for ID
  // This function is not used
}

// ====== Cipher primitives ======
const fn_reverse = (b, pos) => {
  let rev = 0;
  for (let i = 0; i < 8; i++) {
    rev = (rev << 1) | (b & 1);
    b = b >>> 1; // Unsigned right shift for positive values
  }
  return rev;
};
const fn_xor_pos = (b, pos) => b ^ (pos & 0xFF);
const fn_add_pos = (b, pos) => (b + (pos & 0xFF)) & 0xFF;

function make_xor_n(n) {
  n &= 0xFF;
  return (b, p) => (b ^ n) & 0xFF;
}
function make_add_n(n) {
  n &= 0xFF;
  return (b, p) => (b + n) & 0xFF;
}

// ====== App layer ======
function toy_priority(s) {
  const items = s.split(',');
  const parsed = [];
  for (const item of items) {
    const trimmed = item.trim();
    const match = /^(\d+)x\s(.+)$/.exec(trimmed);
    if (match) {
      parsed.push([parseInt(match[1], 10), match[2]]);
    }
  }
  parsed.sort((a, b) => b[0] - a[0]);
  if (parsed.length === 0) return '';
  const [n, name] = parsed[0];
  return `${n}x ${name}`;
}

// ====== LUT builder (byte buffers, memory efficient) ======
function build_cipher_tables_bytes(ops) {
  // Returns [enc[256], dec[256], isIdentity]
  const enc = new Array(256).fill(null);
  const dec = new Array(256).fill(null);

  const id = Buffer.from(new Uint8Array(256).map((_, i) => i));
  let isIdentity = true;

  for (let p = 0; p < 256; p++) {
    let vals = new Array(256).fill(0).map((_, i) => i);
    for (const fn of ops) {
      for (let i = 0; i < 256; i++) {
        vals[i] = fn(vals[i], p) & 0xFF;
      }
    }
    const map = Buffer.from(vals);
    enc[p] = map;
    if (isIdentity && !map.equals(id)) isIdentity = false;

    // inverse
    const inv = new Array(256).fill(0);
    for (let i = 0; i < 256; i++) {
      inv[vals[i]] = i;
    }
    dec[p] = Buffer.from(inv);
  }
  return [enc, dec, isIdentity];
}

// ====== Per-connection helpers ======
function close_client(socket) {
  const clients = global.clients; // Assuming clients is global or pass it
  if (!clients.has(socket)) return;
  socket.destroy();
  clients.delete(socket);
}

function process_client(c) {
  while (c.inbuf.length > 0) {
    if (c.stage === 'cipher') {
      const b = c.inbuf[0];
      c.inbuf = c.inbuf.slice(1);

      if (c.cstate === 'xor') {
        c.ops.push(make_xor_n(b));
        c.cstate = null;
        continue;
      }
      if (c.cstate === 'add') {
        c.ops.push(make_add_n(b));
        c.cstate = null;
        continue;
      }

      if (b === 0) {
        // Compile LUTs; disconnect if overall identity (zero-length or no-op combo)
        const [enc, dec, isId] = build_cipher_tables_bytes(c.ops);
        if (isId) {
          c.kill = true;
          return;
        }
        c.enc = enc;
        c.dec = dec;
        c.stage = 'app';
        continue;
      } else if (b === 1) {
        c.ops.push(c.fn_reverse);
      } else if (b === 2) {
        c.cstate = 'xor';
      } else if (b === 3) {
        c.ops.push(c.fn_xor_pos);
      } else if (b === 4) {
        c.cstate = 'add';
      } else if (b === 5) {
        c.ops.push(c.fn_add_pos);
      } else {
        c.kill = true;
        return; // invalid cipher byte
      }
    } else { // stage 'app'
      const cb = c.inbuf[0];
      c.inbuf = c.inbuf.slice(1);
      const plain = c.dec[c.r_pos & 0xFF][cb];
      c.r_pos++;

      if (plain === 10) { // newline terminator
        const resp_text = toy_priority(c.line);
        c.line = '';

        const resp_len = resp_text.length;
        const resp_buf = Buffer.alloc(resp_len + 1);
        for (let i = 0; i < resp_len; i++) {
          resp_buf[i] = c.enc[c.w_pos & 0xFF][resp_text.charCodeAt(i)];
          c.w_pos++;
        }
        resp_buf[resp_len] = c.enc[c.w_pos & 0xFF][10];
        c.w_pos++;
        c.outbuf = Buffer.concat([c.outbuf, resp_buf]);
      } else {
        c.line += String.fromCharCode(plain);
        if (c.line.length > (1 << 20)) {
          c.kill = true;
          return;
        } // 1MB safety
      }
    }
  }
}

// ====== Event-loop server (multi-client with event emitters) ======
function start_server() {
  const port = process.env.PORT || process.env.PROTOHACKERS_PORT || 40000;

  const server = net.createServer();
  global.clients = new Map(); // Make clients accessible
  const IDLE_SECS = 300;

  server.on('connection', (socket) => {
    const c = {
      stage: 'cipher',
      cstate: null,
      ops: [],
      enc: null,
      dec: null,
      r_pos: 0,
      w_pos: 0,
      inbuf: Buffer.alloc(0),
      outbuf: Buffer.alloc(0),
      line: '',
      kill: false,
      last: Math.floor(Date.now() / 1000),
      isDraining: false, // To manage writes
      // bind primitives for quick access
      fn_reverse,
      fn_xor_pos,
      fn_add_pos,
    };
    global.clients.set(socket, c);

    socket.on('data', (buf) => {
      c.inbuf = Buffer.concat([c.inbuf, buf]);
      c.last = Math.floor(Date.now() / 1000);
      process_client(c);
      if (c.kill) {
        close_client(socket);
        return;
      }
      flush_client(socket, c);
    });

    socket.on('end', () => close_client(socket));
    socket.on('close', () => close_client(socket));
    socket.on('error', (err) => {
      // Silent unless critical
      close_client(socket);
    });
  });

  // Idle timeout checker
  setInterval(() => {
    const now = Math.floor(Date.now() / 1000);
    for (const [socket, c] of global.clients.entries()) {
      if (c.kill || (now - c.last > IDLE_SECS)) {
        close_client(socket);
      }
    }
  }, 10000); // Check every 10 seconds

  server.listen(port, '0.0.0.0', () => {
    console.log(`open on :${port}`);
  });
}

function flush_client(socket, c) {
  if (c.outbuf.length === 0 || c.isDraining) return;

  c.isDraining = true;
  const writeChunk = () => {
    const success = socket.write(c.outbuf);
    if (success) {
      c.outbuf = Buffer.alloc(0);
      c.isDraining = false;
      c.last = Math.floor(Date.now() / 1000); // Update last on successful write
    } else {
      socket.once('drain', writeChunk);
    }
  };
  writeChunk();
}

// ====== Main ======
start_server();
