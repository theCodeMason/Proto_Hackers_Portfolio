<?php
/**
 * ProtoHackers Challenge 5: Mob in the Middle
 *
 * A transparent TCP proxy that intercepts Boguscoin addresses and replaces them
 * with Tony's address. This is a proper man-in-the-middle proxy - each client
 * gets their own upstream connection to maintain chat protocol integrity.
 */

class MobProxy {
    private $listenSocket;
    private $upstreamHost = 'chat.protohackers.com';
    private $upstreamPort = 16963;
    private $tonyAddress = '7YWHMfk9JZe0LM0g1ZauHuiSxhI';
    private $connections = [];

    public function __construct($port = 8080) {
        $this->listenSocket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if (!$this->listenSocket) {
            die("Failed to create socket: " . socket_strerror(socket_last_error()));
        }

        socket_set_option($this->listenSocket, SOL_SOCKET, SO_REUSEADDR, 1);

        if (!socket_bind($this->listenSocket, '0.0.0.0', $port)) {
            die("Failed to bind socket: " . socket_strerror(socket_last_error()));
        }

        if (!socket_listen($this->listenSocket, 10)) {
            die("Failed to listen on socket: " . socket_strerror(socket_last_error()));
        }

        socket_set_nonblock($this->listenSocket);
        echo "Mob proxy listening on port $port...\n";
    }

    public function run() {
        while (true) {
            // Accept new connections
            $clientSocket = @socket_accept($this->listenSocket);
            if ($clientSocket !== false) {
                $this->handleNewClient($clientSocket);
            }

            // Handle existing connections
            $this->processConnections();

            usleep(1000); // Small delay to prevent busy waiting
        }
    }

    private function handleNewClient($clientSocket) {
        echo "New client connected\n";

        // Create upstream connection
        $upstreamSocket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if (!$upstreamSocket) {
            echo "Failed to create upstream socket\n";
            socket_close($clientSocket);
            return;
        }

        // Connect to upstream (blocking for simplicity)
        if (!socket_connect($upstreamSocket, $this->upstreamHost, $this->upstreamPort)) {
            echo "Failed to connect to upstream: " . socket_strerror(socket_last_error()) . "\n";
            socket_close($clientSocket);
            socket_close($upstreamSocket);
            return;
        }

        // Set both sockets to non-blocking
        socket_set_nonblock($clientSocket);
        socket_set_nonblock($upstreamSocket);

        $connId = spl_object_id($clientSocket);
        $this->connections[$connId] = [
            'client' => $clientSocket,
            'upstream' => $upstreamSocket,
            'client_buffer' => '',
            'upstream_buffer' => ''
        ];

        echo "Connection $connId established\n";
    }

    private function processConnections() {
        if (empty($this->connections)) {
            return;
        }

        // Prepare sockets for select
        $readSockets = [];
        foreach ($this->connections as $conn) {
            $readSockets[] = $conn['client'];
            $readSockets[] = $conn['upstream'];
        }

        $write = null;
        $except = null;
        $result = @socket_select($readSockets, $write, $except, 0, 1000);

        if ($result === false || $result === 0) {
            return;
        }

        foreach ($readSockets as $socket) {
            $this->handleSocketData($socket);
        }
    }

    private function handleSocketData($socket) {
        // Find the connection this socket belongs to
        $connId = null;
        $isClientSocket = false;

        foreach ($this->connections as $id => $conn) {
            if ($conn['client'] === $socket) {
                $connId = $id;
                $isClientSocket = true;
                break;
            } elseif ($conn['upstream'] === $socket) {
                $connId = $id;
                $isClientSocket = false;
                break;
            }
        }

        if ($connId === null) {
            return;
        }

        $conn = &$this->connections[$connId];

        // Read data from socket
        $data = @socket_read($socket, 4096, PHP_BINARY_READ);

        if ($data === false) {
            $error = socket_last_error($socket);
            if ($error !== SOCKET_EWOULDBLOCK && $error !== SOCKET_EAGAIN) {
                echo "Read error on connection $connId: " . socket_strerror($error) . "\n";
                $this->closeConnection($connId);
            }
            return;
        }

        if ($data === '') {
            echo "Connection $connId closed by " . ($isClientSocket ? 'client' : 'upstream') . "\n";
            $this->closeConnection($connId);
            return;
        }

        // Add to buffer and process
        if ($isClientSocket) {
            $conn['client_buffer'] .= $data;
            $this->processBuffer($connId, true);
        } else {
            $conn['upstream_buffer'] .= $data;
            $this->processBuffer($connId, false);
        }
    }

    private function processBuffer($connId, $isFromClient) {
        if (!isset($this->connections[$connId])) {
            return;
        }

        $conn = &$this->connections[$connId];
        $bufferKey = $isFromClient ? 'client_buffer' : 'upstream_buffer';
        $targetSocket = $isFromClient ? $conn['upstream'] : $conn['client'];

        // Process any complete lines
        while (($pos = strpos($conn[$bufferKey], "\n")) !== false) {
            $line = substr($conn[$bufferKey], 0, $pos + 1);
            $conn[$bufferKey] = substr($conn[$bufferKey], $pos + 1);

            // Replace Boguscoin addresses
            $modifiedLine = $this->replaceBoguscoinAddresses($line);

            // Forward to target
            if (!$this->sendData($targetSocket, $modifiedLine)) {
                $this->closeConnection($connId);
                return;
            }

            $direction = $isFromClient ? "Client -> Server" : "Server -> Client";
            echo "$direction ($connId): " . trim($modifiedLine) . "\n";
        }
    }

    private function sendData($socket, $data) {
        $totalLength = strlen($data);
        $sent = 0;

        while ($sent < $totalLength) {
            $result = @socket_write($socket, substr($data, $sent), $totalLength - $sent);
            if ($result === false) {
                $error = socket_last_error($socket);
                if ($error !== SOCKET_EWOULDBLOCK && $error !== SOCKET_EAGAIN) {
                    echo "Write error: " . socket_strerror($error) . "\n";
                    return false;
                }
                usleep(1000); // Brief pause before retry
                continue;
            }
            $sent += $result;
        }

        return true;
    }

    private function closeConnection($connId) {
        if (!isset($this->connections[$connId])) {
            return;
        }

        $conn = $this->connections[$connId];
        @socket_close($conn['client']);
        @socket_close($conn['upstream']);
        unset($this->connections[$connId]);

        echo "Connection $connId closed\n";
    }

    private function replaceBoguscoinAddresses($message) {
        // Match Boguscoin addresses: 26-35 alphanumeric chars starting with '7'
        // Must be word-bounded (preceded/followed by space or line boundary)
        $pattern = '/(?<=^|\s)(7[a-zA-Z0-9]{25,34})(?=\s|$)/';

        return preg_replace_callback($pattern, function($matches) {
            $address = $matches[1];

            // Double-check it meets all criteria
            if (strlen($address) >= 26 && strlen($address) <= 35 &&
                ctype_alnum($address) && $address[0] === '7') {

                echo "Replacing Boguscoin address: $address -> {$this->tonyAddress}\n";
                return $this->tonyAddress;
            }

            return $address;
        }, $message);
    }

    public function __destruct() {
        foreach ($this->connections as $connId => $conn) {
            $this->closeConnection($connId);
        }

        if ($this->listenSocket) {
            socket_close($this->listenSocket);
        }
    }
}

// Main execution
if (php_sapi_name() === 'cli') {
    $port = isset($argv[1]) ? intval($argv[1]) : 40000;

    echo "Starting Mob in the Middle proxy...\n";
    echo "Listening on port: $port\n";
    echo "Upstream: chat.protohackers.com:16963\n";
    echo "Tony's address: 7YWHMfk9JZe0LM0g1ZauHuiSxhI\n\n";

    $proxy = new MobProxy($port);
    $proxy->run();
} else {
    echo "This script must be run from the command line.\n";
}
?>
