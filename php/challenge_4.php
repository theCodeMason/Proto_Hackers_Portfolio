<?php

function start_db($port) {
    // Create UDP socket
    $sock = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
    if ($sock === false) {
        die("Failed to create socket: " . socket_strerror(socket_last_error()) . "\n");
    }

    // Bind socket to address and port
    if (!socket_bind($sock, "0.0.0.0", $port)) {
        die("Failed to bind socket: " . socket_strerror(socket_last_error()) . "\n");
    }

    $data = array();
    $version = "Snapchat for Databases";

    while (true) {
        // Receive data (max 1000 bytes)
        $bytes_received = socket_recvfrom($sock, $raw_request, 1000, 0, $client_ip, $client_port);
        if ($bytes_received === false) {
            continue; // Skip on error
        }

        // Use raw request directly (assuming UTF-8 compatible input)
        $request = $raw_request;
        
        // Find '=' position
        $eq_pos = strpos($request, '=');
        
        if ($eq_pos === false) {
            // Retrieve operation
            $key = $request;
            $val = ($key === "version") ? $version : 
                   (isset($data[$key]) ? $data[$key] : "");
            $response = $key . '=' . $val;
            
            // Send response back to client
            socket_sendto($sock, $response, strlen($response), 0, $client_ip, $client_port);
        } else {
            // Insert operation
            $key = substr($request, 0, $eq_pos);
            $value = substr($request, $eq_pos + 1);
            $data[$key] = $value;
        }
    }

    // Close socket (unreachable due to infinite loop)
    socket_close($sock);
}

// Main execution
$port = getenv('PORT') ?: "40000";
start_db((int)$port);
