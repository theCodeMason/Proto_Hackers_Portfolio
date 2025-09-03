const net = require('node:net')

const server = net.createServer()

server.listen(40000, '0.0.0.0', () => {
  console.log('server listening on 40000')
})

server.on('connection', (socket) => {
  console.log('client connected')

  const client = net.createConnection(16963, 'chat.protohackers.com')

  client.on('data', (data) => {
    const response = data.toString('utf-8')

    const res = response.replace(/\n/g, '').split(' ').map(split => (
      split.replace(/^7+[A-z|0-9]{25,34}$/g, '7YWHMfk9JZe0LM0g1ZauHuiSxhI'))
    ).join(' ')

    socket.write(res + '\n')
  })

  let request = ''

  socket.on('data', (data) => {
    request += data.toString('utf-8')
    // Wait to get full message
    if (input[input.length - 1] !== '\n') {
      return
    }

    request = request.replace(/\n/g, '')

    const req = request.split(' ').map(split => (
      split.replace(/^7+[A-z|0-9]{25,34}$/g, '7YWHMfk9JZe0LM0g1ZauHuiSxhI'))
    ).join(' ')

    client.write(req + '\n')

    request = ''
  })

  socket.on('close', () => {
    console.log('client disconnected')

    client.destroy()
  })

  socket.on('error', (err) => {
    console.log('error', err)
  })
})
