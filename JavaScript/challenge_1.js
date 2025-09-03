const { createServer } = require('net');

const server = createServer();

const isPrime = (num) => {
  for (let i = 2, s = Math.sqrt(num); i <= s; i++) {
    if (num % i === 0) return false;
  }
  return num > 1;
};

/**
 * @param {net.Socket} socket The client socket.
 * @param {string} data The incoming data.
 */
function handleData(socket, data) {
  console.log('got', data);
  try {
    const json = JSON.parse(data);
    if (json.method === 'isPrime' && Number.isFinite(json.number)) {
      if (!Number.isInteger(json.number)) {
        socket.write(
          JSON.stringify({ method: 'isPrime', prime: false }) + '\n'
        );
      } else {
        const prime = isPrime(json.number);
        console.log('isPrime', json.number, prime);
        socket.write(JSON.stringify({ method: 'isPrime', prime }) + '\n');
      }
    } else {
      console.error('unknown method');
      socket.write('malformed\n');
    }
  } catch (e) {
    console.error(e);
    socket.write('malformed\n');
  }
}

server.on('connection', (socket) => {
  console.log('new client', socket.remoteAddress);

  let buffer = '';
  socket.on('data', (raw) => {
    const data = raw.toString();
    if (data.indexOf('\n') === -1) {
      buffer += data;
    } else {
      const reqs = (buffer + data).split('\n');
      const last = reqs.pop();
      if (last !== '') {
        buffer = last;
      } else {
        buffer = '';
      }
      for (let i = 0; i < reqs.length; i++) {
        handleData(socket, reqs[i]);
      }
    }
  });

  socket.on('end', () => {
    console.log('client ended');
  });
});

server.listen(40000, '0.0.0.0', () => {
  const addr = server.address();
  console.log(`listening at ${addr.address}:${addr.port}`);
});
