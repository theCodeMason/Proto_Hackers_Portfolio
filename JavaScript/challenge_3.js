const net = require('node:net')

const server = net.createServer()

server.listen(40000, '0.0.0.0', () => {
  console.log('server listening on 5000')
})

let users = []

server.on('connection', (socket) => {
  console.log('client connected')
  let name;
  let id;
  

  socket.write('Welcome to budgetchat! What shall I call you?\n')

  let request = ''
  socket.on('data', (data) => {
    const input = data.toString('utf-8')

    request += input.replace(/\n/g, '')

    // Wait to get full message
    if (input[input.length - 1] !== '\n') {
      return
    }

    // A new user connected
    if (!name) {
      console.log("1. Setting the user's name", request)
      if(validateName(request.replace(/\n/g, ''))) {
        console.log('name was valid and user joined', request)
        name = request.replace(/\n/g, '')
        id = Math.random().toString(16).slice(2)

        console.log('3. Presence notification', name)
        console.log(`* The room contains: ${users.map(user => user.name).join(', ')}\n`)
        socket.write(`* The room contains: ${users.map(user => user.name).join(', ')}\n`)
        
        users.forEach((user) => {
          console.log('2. sent A user joins to', user.name)
          console.log(`* ${name} has entered the room\n`)
          user.socket.write(`* ${name} has entered the room\n`)
        })

        users.push({
          id,
          name,
          socket
        })

        console.log('added user', users.map(user => ({ name: user.name, id: user.id })))
      } else {
        socket.write('invalid name\n')
        socket.destroy()
      }
      request = ''
      return
    }

    // User has connected and any data is chat messages
    console.log(name, 'sent message', request)
    const otherUsers = users.filter(user => user.id !== id)
    otherUsers.forEach(user => {
      console.log(`[${name}] ${request}\n`)
      user.socket.write(`[${name}] ${request}\n`)
    })

    request = ''
  })

  socket.on('close', () => {
    const user = users.find(user => user.id === id)
    if (user) {
      console.log('user is leaving', user?.name ?? 'user left before joining')
      const otherUsers = users.filter(user => user.id !== id)
      otherUsers.forEach(user => {
        user.socket.write(`* ${name} has left the room\n`)
      })
  
      users = otherUsers
    }
  })

  socket.on('error', (err) => {
    console.log('error', err)
  })
})

function validateName(name) {
  if (name.length < 1) {
    return false 
  }

  if (!name.match(/^[A-z0-9]*$/g)) {
    return false
  }
  
  return true
}
