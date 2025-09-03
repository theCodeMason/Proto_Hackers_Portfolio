const valueStore = {
  version: "Mason's Key-Value Store 1.1"
}

function hasCharacter(str, char) {
  return str.includes(char)
}

function splitOnFirst(str, split) {
  const [first, ...rest] = str.split(split)
  return [first, rest.join(split)]
}

function isInsert(str) {
  return hasCharacter(str, '=')
}

function parseInsert(str) {
  return splitOnFirst(str, '=')
}

function insertValue(str) {
  const [id, value] = parseInsert(str)

  if (id === 'version') {
    return
  }

  valueStore[id] = value
}

function handleRequest(str) {
  if (isInsert(str)) {
    insertValue(str)

    return
  }

  return `${str}=${valueStore[str] || ''}`
}

const dgram = require('node:dgram')

const server = dgram.createSocket('udp4', (msg, rinfo) => {
  const response = handleRequest(msg.toString('utf-8'))

  if(response) {
    server.send(response, rinfo.port, rinfo.address, (err) => {
      if (err) console.log(err)
    })
  }
});

server.on('error', err => {
  console.log(`server error:\n${err.stack}`)
  server.close()
})

server.on('listening', () => {
  const address = server.address();
  console.log(`server listening ${address.address}:${address.port}`);
});

server.bind(40000, '0.0.0.0');
