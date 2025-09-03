const net = require('net');

const server = net.createServer(function (socket) {
  socket.on('data', data => {
    console.log(Buffer.from(data).toString('utf8'));
    socket.write(data);
  });
});

server.listen(40000, '0.0.0.0');
