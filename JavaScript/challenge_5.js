const net = require('net');

const PORT = 40000;
const TONYS_WALLET = '7YWHMfk9JZe0LM0g1ZauHuiSxhI';


function setupServer(connectionHandler) {
  const server = net.createServer()
    .listen(PORT, () => {
      console.log(`server started on ${PORT}`);
    });

  server.on('connection', (socket) => {
    connectionHandler(socket);
  });
}

async function connectionHandler(downstream) {
  const sessionId = `${downstream.remoteAddress}:${downstream.remotePort}`;
  console.log(`${sessionId} | client connected`);

  const upstream = net.connect({ host: 'chat.protohackers.com', port: 16963 });

  downstream.on('error', (err) => {
    console.log(`${sessionId} | error ${err}`);
  });

  downstream.on('close', () => {
    console.log(`${sessionId} | client disconnected`);
    upstream.destroy();
  });

  handleChat(downstream, upstream);
}

function handleChat(downstream, upstream) {

  let messageBuffer = '';
  downstream.on('data', (chunk) => {
    const receivedDataString = Buffer.from(chunk).toString('ascii');
    messageBuffer += receivedDataString;

    if (!receivedDataString.endsWith('\n')) {
      return;
    }

    console.log('message received from downstream', messageBuffer);
    sendMessage({ socket: upstream }, messageBuffer);

    messageBuffer = '';
  });

  let upstreamMessageBuffer = '';
  upstream.on('data', (chunk) => {
    const receivedDataString = Buffer.from(chunk).toString('ascii');
    upstreamMessageBuffer += receivedDataString;

    if (!receivedDataString.endsWith('\n')) {
      return;
    }

    console.log('message received from upstream', upstreamMessageBuffer);

    sendMessage({ socket: downstream }, upstreamMessageBuffer);

    upstreamMessageBuffer = '';
  });
}

function sendMessage(receiverCLient, message) {
  const { socket } = receiverCLient;

  let payload = message
    .replace(/\n/g, '')
    .split(' ')
    .map(split => split.replace(/^7+[A-z|0-9]{25,34}$/g, TONYS_WALLET))
    .join(' ') + '\n';

  socket.write(payload);
}

async function main() {
  setupServer(connectionHandler);
}

main();
