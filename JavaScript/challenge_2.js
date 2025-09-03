
const net = require('net');

const server = net.createServer();

const handleConnection = (connection) => {
    const address = connection.remoteAddress + ':' + connection.remotePort;
    console.log('New connection with client: ' + address);
    let bufferArray = [];
    let stockPriceMap = {};
    
    connection.on('data', (data) => {
        data.forEach(byte => {
            bufferArray.push(byte);
        });
        while(bufferArray.length >= 9) {
            const command = bufferArray.splice(0, 9);
            let operation = command[0].toString();
            const buffer = Buffer.from(command);

            if (operation == 73) { // Insert
                const timestamp = buffer.readInt32BE(1);
                const price = buffer.readInt32BE(5);
                stockPriceMap[timestamp] = price;
                console.log('I ' + timestamp + ' ' + price);
            } else if (operation == 81) { // Query
                let sum = 0, count = 0;
                const minTime = buffer.readInt32BE(1);
                const maxTime = buffer.readInt32BE(5);
                if(minTime > maxTime) {
                    let response = Buffer.alloc(4);
                    response.fill(0);
                    connection.write(response);
                    return;
                }
                console.log('Q ' + minTime + ' ' + maxTime);
                for(const [key, value] of Object.entries(stockPriceMap)) {
                    if(key >= minTime && key <= maxTime) {
                        sum += value;
                        count++;
                    }
                }
                const average = Math.round(sum/count);
                console.log('sum: ' + sum + ' count: ' + count + ' average: ' + average);
                let response = Buffer.alloc(4);
                response.writeInt32BE(average);
                connection.write(response);
            }
        }        
    });

    connection.on('close', () => {
        console.log('Closed connection with client: '+ address);
    });
}

server.on('connection', handleConnection);

server.listen(40000, () => {
    console.log('Server listening!');
});
