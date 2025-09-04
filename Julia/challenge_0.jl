using Sockets

function handle_client(socket::TCPSocket)
    println("New client connected: $(getsockname(socket)[1])")
    try
        while isopen(socket)
            data = readavailable(socket)
            if isempty(data)
                println("Client ended: $(getsockname(socket)[1])")
                break
            end
            write(socket, data)
        end
    catch e
        println("Client error: $e")
    finally
        close(socket)
        println("Client disconnected: $(getsockname(socket)[1])")
    end
end

function start_server(port::Int=40000)
    server = listen(IPv4(0), port)
    println("Server listening at 0.0.0.0:$port")
    
    while true
        socket = accept(server)
        @async handle_client(socket)
    end
end

# Start the server
start_server(40000)
