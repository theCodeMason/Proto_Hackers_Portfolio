const net = require('net');

const HOST = '0.0.0.0';
const PORT = 40000;

// ---------- Utilities ----------
function make_error(msg) {
  return JSON.stringify({ status: 'error', error: msg }) + '\n';
}
function make_status_ok() {
  return JSON.stringify({ status: 'ok' }) + '\n';
}
function make_status_ok_id(id) {
  return JSON.stringify({ status: 'ok', id }) + '\n';
}
function make_status_ok_job(q, id, job, pri) {
  return JSON.stringify({ status: 'ok', id, job, pri, queue: q }) + '\n';
}
function make_status_nojob() {
  return JSON.stringify({ status: 'no-job' }) + '\n';
}

// ---------- Client model ----------
class Client {
  constructor(sock) {
    this.sock = sock;
    this.buf = '';
    this.owned = new Set(); // Set of job ids owned by this client
    this.watching = new Set(); // Set of queues this client is currently watching
  }
  write(s) {
    this.sock.write(s);
  }
}

// ---------- Server state ----------
let nextId = 0;
const queues = new Map(); // Map queue => Map(jobId => pri)
const jobs = new Map(); // Map jobId => [queue, job, pri]
const watchers = new Map(); // Map queue => Map(sock => Client)
const clients = new Map(); // Map sock => Client

// ---------- Queue helpers ----------
function pick_from_queues(queues, qs) {
  const candidates = [];
  for (const q of qs) {
    const qMap = queues.get(q);
    if (!qMap || qMap.size === 0) continue;
    let bestId = null;
    let bestPri = null;
    for (const [id, pri] of qMap.entries()) {
      if (bestPri === null || pri > bestPri) {
        bestPri = pri;
        bestId = id;
      }
    }
    if (bestId !== null) {
      candidates.push([bestId, bestPri]);
    }
  }
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => b[1] - a[1]);
  return candidates[0][0];
}

function pop_id_from_queue(queues, q, id) {
  const qMap = queues.get(q);
  if (qMap && qMap.has(id)) {
    qMap.delete(id);
    if (qMap.size === 0) queues.delete(q);
  }
}

function first_watcher_for_queue(watchers, q) {
  const qWatchers = watchers.get(q);
  if (!qWatchers || qWatchers.size === 0) return null;
  const [sock, client] = qWatchers.entries().next().value;
  qWatchers.delete(sock);
  if (qWatchers.size === 0) watchers.delete(q);
  client.watching.delete(q);
  return client;
}

function add_watcher(watchers, client, qs) {
  for (const q of qs) {
    if (!watchers.has(q)) watchers.set(q, new Map());
    watchers.get(q).set(client.sock, client);
    client.watching.add(q);
  }
}

function remove_client_from_all_watchers(watchers, client) {
  for (const q of client.watching) {
    const qWatchers = watchers.get(q);
    if (qWatchers) {
      qWatchers.delete(client.sock);
      if (qWatchers.size === 0) watchers.delete(q);
    }
  }
  client.watching.clear();
}

// ---------- Job operations ----------
function op_put(submitter, q, job, pri, watchers, queues, jobs) {
  const id = nextId++;
  jobs.set(id, [q, job, pri]);

  const client = first_watcher_for_queue(watchers, q);
  if (client) {
    client.write(make_status_ok_job(q, id, job, pri));
    client.owned.add(id);
    submitter.write(make_status_ok_id(id));
  } else {
    if (!queues.has(q)) queues.set(q, new Map());
    queues.get(q).set(id, pri);
    submitter.write(make_status_ok_id(id));
  }
}

function op_get(client, qs, wait, queues, jobs, watchers) {
  const id = pick_from_queues(queues, qs);
  if (id !== null) {
    const [q, job, pri] = jobs.get(id);
    pop_id_from_queue(queues, q, id);
    client.write(make_status_ok_job(q, id, job, pri));
    client.owned.add(id);
  } else {
    if (wait) {
      add_watcher(watchers, client, qs);
    } else {
      client.write(make_status_nojob());
    }
  }
}

function op_abort(client, id, queues, jobs, watchers) {
  if (!jobs.has(id)) {
    client.write(make_status_nojob());
    return;
  }
  if (!client.owned.has(id)) {
    client.write(make_error('not owner'));
    return;
  }
  const [q, job, pri] = jobs.get(id);

  const w = first_watcher_for_queue(watchers, q);
  if (w) {
    w.write(make_status_ok_job(q, id, job, pri));
    w.owned.add(id);
  } else {
    if (!queues.has(q)) queues.set(q, new Map());
    queues.get(q).set(id, pri);
  }
  client.owned.delete(id);
  client.write(make_status_ok());
}

function op_delete(client, id, queues, jobs) {
  if (!jobs.has(id)) {
    client.write(make_status_nojob());
    return;
  }
  const [q, , ] = jobs.get(id);
  pop_id_from_queue(queues, q, id);
  jobs.delete(id);
  client.write(make_status_ok());
}

function abort_all_owned_on_disconnect(client, queues, jobs, watchers) {
  if (client.owned.size === 0) return;
  for (const id of client.owned) {
    if (!jobs.has(id)) continue;
    const [q, job, pri] = jobs.get(id);
    const w = first_watcher_for_queue(watchers, q);
    if (w) {
      w.write(make_status_ok_job(q, id, job, pri));
      w.owned.add(id);
    } else {
      if (!queues.has(q)) queues.set(q, new Map());
      queues.get(q).set(id, pri);
    }
  }
  client.owned.clear();
}

// ---------- Networking ----------
const server = net.createServer((sock) => {
  const client = new Client(sock);
  clients.set(sock, client);

  sock.on('data', (data) => {
    client.buf += data.toString('utf-8');

    while (true) {
      const pos = client.buf.indexOf('\n');
      if (pos === -1) break;
      let line = client.buf.substring(0, pos);
      client.buf = client.buf.substring(pos + 1);

      line = line.replace(/\r$/, '');
      if (line === '') {
        client.write(make_error('empty line'));
        continue;
      }

      let msg;
      try {
        msg = JSON.parse(line);
      } catch (e) {
        client.write(make_error('invalid JSON'));
        continue;
      }

      if (msg.request === 'put') {
        const q = msg.queue;
        const pri = msg.pri;
        const job = msg.job;
        if (typeof q !== 'string' || q === '' || job === undefined || typeof pri !== 'number') {
          client.write(make_error('bad request'));
          continue;
        }
        op_put(client, q, job, pri, watchers, queues, jobs);
      } else if (msg.request === 'get') {
        let qs = msg.queues;
        const wait = !!msg.wait;
        if (!Array.isArray(qs) || qs.length === 0) {
          client.write(make_error('bad request'));
          continue;
        }
        qs = [...new Set(qs.map(String))];
        op_get(client, qs, wait, queues, jobs, watchers);
      } else if (msg.request === 'abort') {
        const id = msg.id;
        if (!Number.isInteger(id)) {
          client.write(make_error('bad request'));
          continue;
        }
        op_abort(client, id, queues, jobs, watchers);
      } else if (msg.request === 'delete') {
        const id = msg.id;
        if (!Number.isInteger(id)) {
          client.write(make_error('bad request'));
          continue;
        }
        op_delete(client, id, queues, jobs);
      } else {
        client.write(make_error('bad request'));
      }
    }
  });

  sock.on('end', () => {
    remove_client_from_all_watchers(watchers, client);
    abort_all_owned_on_disconnect(client, queues, jobs, watchers);
    clients.delete(sock);
  });

  sock.on('close', () => {
    clients.delete(sock);
  });

  sock.on('error', (err) => {
    // Handle silently
  });
});

server.listen(PORT, HOST, () => {
  console.log(`[job-centre] listening on ${HOST}:${PORT}`);
});

let lastStats = 0;
setInterval(() => {
  const now = Math.floor(Date.now() / 1000);
  if (now - lastStats >= 5) {
    lastStats = now;
    console.log(`[stats] queues:${queues.size} jobs:${jobs.size} watchers:${watchers.size} clients:${clients.size}`);
  }
}, 1000);
