<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

// Server configuration
$host = "0.0.0.0";
$port = 40000;

// Create a TCP socket
$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
if ($socket === false) {
    die("Socket creation failed: " . socket_strerror(socket_last_error()));
}

// Set socket options
socket_set_option($socket, SOL_SOCKET, SO_REUSEADDR, 1);

// Bind to host and port
if (!socket_bind($socket, $host, $port)) {
    die("Socket bind failed: " . socket_strerror(socket_last_error($socket)));
}

// Listen for connections
if (!socket_listen($socket, 5)) {
    die("Socket listen failed: " . socket_strerror(socket_last_error($socket)));
}

echo "Server listening on $host:$port\n";

// Accept connections in a loop
while (true) {
    $client = socket_accept($socket);
    if ($client === false) {
        echo "Socket accept failed: " . socket_strerror(socket_last_error($socket)) . "\n";
        continue;
    }

    // Handle each client in a separate process
    $pid = pcntl_fork();
    if ($pid == -1) {
        die("Could not fork process\n");
    } elseif ($pid) {
        // Parent process: close client socket and continue accepting
        socket_close($client);
    } else {
        // Child process: handle client communication
        while (true) {
            $data = socket_read($client, 1024, PHP_BINARY_READ);
            if ($data === false || $data === "") {
                break; // Client disconnected or error
            }
            socket_write($client, $data, strlen($data));
        }
        socket_close($client);
        exit(0); // Exit child process
    }
}

// Close server socket (unreachable in this loop, but good practice)
socket_close($socket);
