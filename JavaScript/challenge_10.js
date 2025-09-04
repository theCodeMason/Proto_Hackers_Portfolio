const net = require('net');

const storage = new Map();

function send(socket, message) {
    socket.write(message + '\n', 'utf8');
}

async function helpHandler(socket, params) {
    send(socket, 'OK usage: HELP|GET|PUT|LIST');
}

function checkName(name) {
    if (!name.startsWith('/') ||
        (name.length > 1 && name.endsWith('/')) ||
        name.includes('//') ||
        !/^[a-zA-Z0-9_./-]*$/.test(name)) {
        return null;
    }
    return name;
}

function checkLength(str) {
    const len = parseInt(str, 10);
    if (isNaN(len)) {
        return null;
    }
    return len;
}

async function putHandler(socket, params, readExact) {
    if (params.length !== 2) {
        send(socket, 'ERR usage: PUT file length newline data');
        return;
    }
    const name = checkName(params[0]);
    if (!name) {
        send(socket, 'ERR illegal file name');
        return;
    }
    const length = checkLength(params[1]);
    if (length === null) {
        send(socket, 'OK');
        return;
    }
    const data = await readData(length, readExact);
    if (data === null) {
        send(socket, 'ERR invalid file data');
        return;
    }
    let s = storage.get(name);
    if (!s) {
        s = new Map();
        storage.set(name, s);
    }
    let rid = `r${s.size + 1}`;
    let lastVal = null;
    if (s.size > 0) {
        const lastEntry = Array.from(s.entries()).pop();
        lastVal = lastEntry[1];
        if (data === lastVal) {
            rid = lastEntry[0];
        }
    }
    if (data !== lastVal) {
        s.set(rid, data);
    }
    send(socket, `OK ${rid}`);
}

async function readData(length, readExact) {
    let buf;
    try {
        buf = await readExact(length);
    } catch {
        return null;
    }
    for (let i = 0; i < buf.length; i++) {
        const c = buf[i];
        if (c < 32 || c > 126) {
            if (![9, 10, 13].includes(c)) {
                return null;
            }
        }
    }
    let data;
    try {
        data = buf.toString('ascii');
    } catch {
        return null;
    }
    return data;
}

async function getHandler(socket, params) {
    if (params.length === 1) {
        params.push('-');
    }
    if (params.length !== 2) {
        send(socket, 'ERR illegal file name');
        return;
    }
    const [name, revision] = params;
    if (!name.startsWith('/')) {
        send(socket, 'ERR illegal file name');
        return;
    }
    if (!storage.has(name)) {
        send(socket, 'ERR no such file');
        return;
    }
    let s = storage.get(name);
    let rev = revision;
    if (rev === '-') {
        rev = Array.from(s.keys()).pop();
    }
    if (!s.has(rev)) {
        send(socket, 'ERR no such revision');
        return;
    }
    const data = s.get(rev);
    send(socket, `OK ${data.length}`);
    socket.write(data, 'utf8');
}

async function listHandler(socket, params) {
    if (params.length !== 1) {
        send(socket, 'ERR usage: list dir');
        return;
    }
    let name = params[0];
    if (!name.endsWith('/')) {
        name += '/';
    }
    const listing = new Map();
    for (const [key, val] of storage.entries()) {
        if (key.startsWith(name)) {
            let subkey = key.slice(name.length);
            let rev;
            if (subkey.includes('/')) {
                subkey = subkey.split('/')[0] + '/';
                rev = 'DIR';
            } else {
                rev = Array.from(val.keys()).pop();
            }
            listing.set(subkey, rev);
        }
    }
    if (listing.size === 0) {
        send(socket, 'ERR file not found');
        return;
    }
    const lines = Array.from(listing.entries()).map(([key, rev]) => `${key} ${rev}`);
    send(socket, `OK ${lines.length}`);
    lines.sort().forEach(fn => send(socket, fn));
}

async function onConnect(socket) {
    const host = socket.remoteAddress;
    const port = socket.remotePort;
    console.log(`connection from ${host}:${port}`);

    let buffer = Buffer.alloc(0);

    const readLine = async () => {
        while (true) {
            let pos = buffer.indexOf(0x0A);
            if (pos !== -1) {
                let lineBuf = buffer.slice(0, pos);
                buffer = buffer.slice(pos + 1);
                if (lineBuf.length > 0 && lineBuf[lineBuf.length - 1] === 0x0D) {
                    lineBuf = lineBuf.slice(0, -1);
                }
                return lineBuf.toString('ascii');
            }
            let chunk;
            try {
                chunk = await new Promise((resolve, reject) => {
                    socket.once('data', resolve);
                    socket.once('error', reject);
                    socket.once('end', () => reject(new Error('EOF')));
                });
            } catch (e) {
                throw e;
            }
            buffer = Buffer.concat([buffer, chunk]);
        }
    };

    const readExact = async (length) => {
        if (length < 0) {
            throw new Error('negative length');
        }
        while (buffer.length < length) {
            let chunk;
            try {
                chunk = await new Promise((resolve, reject) => {
                    socket.once('data', resolve);
                    socket.once('error', reject);
                    socket.once('end', () => reject(new Error('EOF')));
                });
            } catch (e) {
                throw e;
            }
            buffer = Buffer.concat([buffer, chunk]);
        }
        const data = buffer.slice(0, length);
        buffer = buffer.slice(length);
        return data;
    };

    try {
        while (true) {
            send(socket, 'READY');
            let line;
            try {
                line = await readLine();
            } catch (e) {
                if (e.message === 'EOF') break;
                throw e;
            }
            console.log(`line=${line}`);
            const toks = line.trim().split(/\s+/);
            if (toks.length === 0) {
                send(socket, 'ERR illegal method:');
                break;
            }
            const cmd = toks[0].toUpperCase();
            const handler = {
                'HELP': helpHandler,
                'PUT': putHandler,
                'GET': getHandler,
                'LIST': listHandler,
            }[cmd];
            if (handler) {
                if (cmd === 'PUT') {
                    await handler(socket, toks.slice(1), readExact);
                } else {
                    await handler(socket, toks.slice(1));
                }
            } else {
                send(socket, `ERR illegal method: ${cmd}`);
            }
        }
    } finally {
        console.log(`closed connection ${host}:${port}`);
        socket.end();
    }
}

const server = net.createServer(onConnect);
server.listen(40000, () => {
    console.log(`listening on port ${server.address().port}`);
});
