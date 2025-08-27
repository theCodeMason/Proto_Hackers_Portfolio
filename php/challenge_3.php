<?php

// Create the server socket
$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
if (!$socket) {
    die("Failed to create socket\n");
}
socket_set_option($socket, SOL_SOCKET, SO_REUSEADDR, 1);
if (!socket_bind($socket, '0.0.0.0', 40000)) {  // Change port as needed
    die("Failed to bind socket\n");
}
if (!socket_listen($socket)) {
    die("Failed to listen on socket\n");
}
socket_set_nonblock($socket);

echo "Server listening on port 10000\n";

// Clients: $clients[$key] = ['socket' => $sock, 'state' => 'pending|joined', 'buffer' => '', 'name' => '']
$clients = [];
$client_id = 0;  // For unique keys

while (true) {
    $read = [$socket];
    foreach ($clients as $c) {
        $read[] = $c['socket'];
    }

    $write = null;
    $except = null;
    $num_changed = socket_select($read, $write, $except, null);
    if ($num_changed === false) {
        break;
    }

    foreach ($read as $r) {
        if ($r === $socket) {
            // Accept new client
            $client_sock = socket_accept($socket);
            if ($client_sock) {
                socket_set_nonblock($client_sock);
                $key = $client_id++;
                $clients[$key] = [
                    'socket' => $client_sock,
                    'state' => 'pending',
                    'buffer' => '',
                    'name' => ''
                ];
                // Send welcome message
                $welcome = "Welcome to budgetchat! What shall I call you?\n";
                socket_send($client_sock, $welcome, strlen($welcome), 0);
            }
        } else {
            // Find the key for this client
            $key = null;
            foreach ($clients as $k => $c) {
                if ($c['socket'] === $r) {
                    $key = $k;
                    break;
                }
            }
            if ($key === null) continue;

            // Read data
            $buf = '';
            $bytes = socket_recv($r, $buf, 2048, 0);
            if ($bytes === false || $bytes === 0) {
                // Connection closed or error
                handle_disconnect($key, $clients);
            } else {
                $clients[$key]['buffer'] .= $buf;
                process_buffer($key, $clients);
            }
        }
    }
}

// Cleanup
socket_close($socket);

// Functions

function handle_disconnect($key, &$clients) {
    $client = $clients[$key];
    if ($client['state'] === 'joined') {
        // Announce leave
        $leave_msg = "* " . $client['name'] . " has left the room\n";
        foreach ($clients as $other_key => $other) {
            if ($other_key !== $key && $other['state'] === 'joined') {
                socket_send($other['socket'], $leave_msg, strlen($leave_msg), 0);
            }
        }
    }
    socket_close($client['socket']);
    unset($clients[$key]);
}

function process_buffer($key, &$clients) {
    $client =& $clients[$key];
    while (($pos = strpos($client['buffer'], "\n")) !== false) {
        $line = substr($client['buffer'], 0, $pos);
        $client['buffer'] = substr($client['buffer'], $pos + 1);
        $line = rtrim($line, "\r");  // Strip trailing whitespace like \r

        if ($client['state'] === 'pending') {
            // Validate name
            if (preg_match('/^[a-zA-Z0-9]+$/', $line) && strlen($line) >= 1 && strlen($line) <= 20) {
                // Check uniqueness
                $unique = true;
                foreach ($clients as $other) {
                    if ($other['state'] === 'joined' && strtolower($other['name']) === strtolower($line)) {
                        $unique = false;
                        break;
                    }
                }
                if ($unique) {
                    $client['name'] = $line;
                    $client['state'] = 'joined';
                    // Send user list to new user
                    $user_list = get_user_list($clients, $key);
                    $list_msg = "* The room contains: " . $user_list . "\n";
                    socket_send($client['socket'], $list_msg, strlen($list_msg), 0);
                    // Announce join to others
                    $join_msg = "* " . $client['name'] . " has entered the room\n";
                    foreach ($clients as $other_key => $other) {
                        if ($other_key !== $key && $other['state'] === 'joined') {
                            socket_send($other['socket'], $join_msg, strlen($join_msg), 0);
                        }
                    }
                } else {
                    // Duplicate name
                    $error = "Name already taken\n";
                    socket_send($client['socket'], $error, strlen($error), 0);
                    handle_disconnect($key, $clients);
                    return;
                }
            } else {
                // Invalid name
                $error = "Invalid name\n";
                socket_send($client['socket'], $error, strlen($error), 0);
                handle_disconnect($key, $clients);
                return;
            }
        } elseif ($client['state'] === 'joined') {
            // Chat message, limit to 1000 chars
            if (strlen($line) > 1000) {
                $line = substr($line, 0, 1000);
            }
            if (strlen($line) > 0) {
                $chat_msg = "[" . $client['name'] . "] " . $line . "\n";
                // Relay to other joined clients
                foreach ($clients as $other_key => $other) {
                    if ($other_key !== $key && $other['state'] === 'joined') {
                        socket_send($other['socket'], $chat_msg, strlen($chat_msg), 0);
                    }
                }
            }
        }
    }
}

function get_user_list(&$clients, $exclude_key) {
    $names = [];
    foreach ($clients as $k => $c) {
        if ($k !== $exclude_key && $c['state'] === 'joined') {
            $names[] = $c['name'];
        }
    }
    return implode(', ', $names);
}
