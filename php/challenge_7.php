<?php
// Remember to run it as: 'php challenge_7.php 0.0.0.0 40000'
/**
 * olleh_udp_server.php
 * A self-contained PHP UDP server that mirrors the given Python asyncio version.
 * - No third-party libraries
 * - Handles: /connect/{id}/, /data/{id}/{pos}/{encoded}/, /ack/{id}/{len}/, /close/{id}/
 * - Per-session retransmission every $RETRANS_TIMEOUT seconds if unacked bytes remain
 * - Session expiry after $EXPIRY_TIMEOUT seconds of inactivity
 * - App logic: collects lines and replies with the reversed line + "\n"
 *
 * Run: php olleh_udp_server.php [host] [port]
 * Default: host=0.0.0.0, port=40000
 */

ini_set('display_errors', '1');
error_reporting(E_ALL);

// ---------------------- Configuration ----------------------
$RETRANS_TIMEOUT = 3;     // seconds
$EXPIRY_TIMEOUT  = 60;    // seconds
$CHUNK_SIZE      = 400;   // bytes per /data/ chunk
$MAX_MSG_LEN     = 1000;  // safety cap, like Python
$HOST = $argv[1] ?? getenv('UDP_HOST') ?? '0.0.0.0';
$PORT = intval($argv[2] ?? getenv('UDP_PORT') ?? '40000');

// ---------------------- Utilities --------------------------
function now_ts(): float { return microtime(true); }

function log_info(string $msg): void {
    fwrite(STDERR, '[' . date('c') . "] INFO: $msg\n");
}
function log_warn(string $msg): void {
    fwrite(STDERR, '[' . date('c') . "] WARN: $msg\n");
}
function log_debug(string $msg): void {
    // Toggle verbosity here if desired
    // fwrite(STDERR, '[' . date('c') . "] DEBUG: $msg\n");
}

function encode_segment(string $s): string {
    // Escape \ as \\ and / as \/
    return str_replace(['\\','/'], ['\\\\','\\/'], $s);
}

/**
 * Parse a message of the form "/.../.../.../" where
 * '/' is a separator and '\' escapes the next character.
 * Returns string[] of fields without the leading/trailing empty parts.
 * Throws InvalidArgumentException on format problems.
 */
function parse_slash_escaped_fields(string $msg): array {
    if ($msg === '' || $msg[0] !== '/') {
        throw new InvalidArgumentException('not starting with /');
    }
    $fields = [''];
    $escaped = false;

    $len = strlen($msg);
    for ($i = 1; $i < $len; $i++) {
        $c = $msg[$i];
        if ($escaped) {
            $fields[count($fields)-1] .= $c;
            $escaped = false;
        } else {
            if ($c === '\\') {
                $escaped = true;
            } elseif ($c === '/') {
                $fields[] = '';
            } else {
                $fields[count($fields)-1] .= $c;
            }
        }
    }

    if ($fields[count($fields)-1] !== '') {
        // the Python ensures the message is "finished" (ends with '/')
        throw new InvalidArgumentException('unfinished message');
    }
    array_pop($fields); // remove final empty field
    if (count($fields) === 0) {
        throw new InvalidArgumentException('no message type');
    }
    return $fields;
}

function to_int32_nonneg(string $s): int {
    if (!preg_match('/^-?\d+$/', $s)) {
        throw new InvalidArgumentException('invalid integer argument');
    }
    $v = intval($s);
    if ($v < 0) {
        throw new InvalidArgumentException('negative integer');
    }
    if ($v >= 2147483648) {
        throw new InvalidArgumentException('integer too large');
    }
    return $v;
}

// ---------------------- UDP I/O ----------------------------
/** Create UDP server socket */
$server = @stream_socket_server("udp://{$HOST}:{$PORT}", $errno, $errstr, STREAM_SERVER_BIND);
if ($server === false) {
    fwrite(STDERR, "Failed to bind UDP socket on {$HOST}:{$PORT} - $errstr ($errno)\n");
    exit(1);
}
stream_set_blocking($server, false);
log_info("UDP server listening on {$HOST}:{$PORT}");

/** send datagram to 'udp://ip:port' */
function send_dgram($socket, string $payload, string $addrUri): void {
    $bytes = @stream_socket_sendto($socket, $payload, 0, $addrUri);
    if ($bytes === false) {
        log_warn("send_dgram failed to {$addrUri}");
    }
}

// ---------------------- Session ----------------------------
class Session {
    public int $id;
    public string $addrUri;        // "udp://ip:port"
    public int $received = 0;
    public int $sent = 0;
    public int $acknowledged = 0;
    public string $unacknowledged = '';
    public ?float $nextRetransAt = null;
    public string $buffer = '';
    public float $lastSeen;
    public $socket;

    public function __construct(int $id, string $addrUri, $socket) {
        $this->id = $id;
        $this->addrUri = $addrUri;
        $this->socket = $socket;
        $this->lastSeen = now_ts();
    }

    public function touch(): void {
        $this->lastSeen = now_ts();
    }

    public function log(string $msg): void {
        log_info("[{$this->id}] $msg");
    }
    public function debug(string $msg): void {
        log_debug("[{$this->id}] $msg");
    }

    public function send_ack(): void {
        $out = "/ack/{$this->id}/{$this->received}/";
        send_dgram($this->socket, $out, $this->addrUri);
    }

    public function close(array &$sessions): void {
        $out = "/close/{$this->id}/";
        send_dgram($this->socket, $out, $this->addrUri);
        $key = Session::keyFor($this->id, $this->addrUri);
        unset($sessions[$key]);
        $this->debug("closed");
    }

    public static function keyFor(int $id, string $addrUri): string {
        return "{$id}|{$addrUri}";
    }

    /** Application: receive raw data payload; split by \n and echo reversed line + \n */
    public function app_receive(string $data, int $chunkSize): void {
        $this->log("app received: " . var_export($data, true));
        $this->buffer .= $data;

        while (true) {
            $pos = strpos($this->buffer, "\n");
            if ($pos === false) break;
            $line = substr($this->buffer, 0, $pos);

            $reply = strrev($line) . "\n";
            $this->app_send($reply, $chunkSize);

            $this->buffer = substr($this->buffer, $pos + 1);
        }
    }

    /** Application: send data in chunks with escaping */
    public function app_send(string $data, int $chunkSize): void {
        $this->log("app sent: " . var_export($data, true));

        $len = strlen($data);
        for ($i = 0; $i < $len; $i += $chunkSize) {
            $chunk = substr($data, $i, min($chunkSize, $len - $i));
            $encoded = encode_segment($chunk);
            $this->debug("sending chunk: " . var_export($encoded, true));

            $out = "/data/{$this->id}/{$this->sent}/{$encoded}/";
            send_dgram($this->socket, $out, $this->addrUri);
            $this->sent += strlen($chunk);
            $this->unacknowledged .= $chunk;

            if ($this->nextRetransAt === null) {
                $this->nextRetransAt = now_ts(); // schedule soon; main loop adds RETRANS_TIMEOUT
            }
        }
    }

    /** Data delivery from remote: enforces position, acks */
    public function on_data(int $position, string $data, int $chunkSize): void {
        if ($position === $this->received) {
            $this->app_receive($data, $chunkSize);
            $this->received += strlen($data);
        }
        $this->send_ack();
    }

    /** Ack handling */
    public function on_ack(int $length, array &$sessions): void {
        if ($length <= $this->acknowledged) return;

        if ($length > $this->sent) {
            // protocol violation -> close
            $this->close($sessions);
            return;
        }

        $newly = $length - $this->acknowledged;
        $this->unacknowledged = substr($this->unacknowledged, $newly);
        $this->acknowledged = $length;

        if ($this->acknowledged < $this->sent) {
            // respond to ack with retransmission of still-unacked tail
            $encoded = encode_segment($this->unacknowledged);
            $this->debug("responding to ack with retransmission: " . var_export($encoded, true));
            $out = "/data/{$this->id}/{$this->acknowledged}/{$encoded}/";
            send_dgram($this->socket, $out, $this->addrUri);
            if ($this->nextRetransAt === null) {
                $this->nextRetransAt = now_ts();
            }
        } else {
            // fully acked
            $this->nextRetransAt = null;
        }
    }

    /** Periodic retransmission if needed */
    public function maybe_retransmit(float $now, int $retransTimeout, int $chunkSize): void {
        if ($this->acknowledged >= $this->sent) {
            $this->nextRetransAt = null;
            return;
        }
        if ($this->nextRetransAt === null || $now < $this->nextRetransAt + $retransTimeout) {
            return;
        }

        $pendingBytes = $this->sent - $this->acknowledged;
        $data = $this->unacknowledged;
        $this->log("retransmitting last {$pendingBytes} bytes");

        $len = strlen($data);
        for ($i = 0; $i < $len; $i += $chunkSize) {
            $chunk = substr($data, $i, min($chunkSize, $len - $i));
            $encoded = encode_segment($chunk);
            $offset = $this->acknowledged + $i;
            $out = "/data/{$this->id}/{$offset}/{$encoded}/";
            send_dgram($this->socket, $out, $this->addrUri);
        }

        $this->nextRetransAt = $now; // schedule next cycle later
    }
}

// ---------------------- Server State -----------------------
/** @var array<string, Session> */
$sessions = []; // key: "{id}|udp://ip:port"

// ---------------------- Main Event Loop --------------------
$read = [$server];
$write = [];
$except = [];
$lastHousekeep = now_ts();

while (true) {
    $r = [$server];
    $w = $write;
    $e = $except;

    // 100ms tick to allow retransmissions/expiry
    $num = @stream_select($r, $w, $e, 0, 100000);
    if ($num === false) {
        // transient select failure; continue
        continue;
    }

    // Receive path
    if (!empty($r)) {
        $peer = '';
        $data = @stream_socket_recvfrom($server, 8192, 0, $peer);
        if ($data !== false && $data !== '') {
            // $peer like "udp://1.2.3.4:56789"
            $addrUri = $peer;
            try {
                if (strlen($data) >= $MAX_MSG_LEN) {
                    throw new InvalidArgumentException('too long');
                }
                // ASCII enforcement similar to Python (we’ll allow binary but expect ASCII control path)
                $msg = $data;

                $fields = parse_slash_escaped_fields($msg);
                log_debug("received: " . substr(var_export($fields, true), 0, 200));

                $msg_ty = $fields[0];

                $ensure_argc = function(int $n) use ($fields, $msg_ty) {
                    if (count($fields) !== $n + 1) {
                        throw new InvalidArgumentException("'$msg_ty' expects $n arguments");
                    }
                };

                if ($msg_ty === 'connect') {
                    $ensure_argc(1);
                    $sessionId = to_int32_nonneg($fields[1]);
                    $key = Session::keyFor($sessionId, $addrUri);
                    if (!isset($GLOBALS['sessions'][$key])) {
                        $GLOBALS['sessions'][$key] = new Session($sessionId, $addrUri, $GLOBALS['server']);
                    }
                    $GLOBALS['sessions'][$key]->touch();
                    // ack with 0
                    send_dgram($GLOBALS['server'], "/ack/{$sessionId}/0/", $addrUri);

                } elseif ($msg_ty === 'data') {
                    $ensure_argc(3);
                    $sessionId = to_int32_nonneg($fields[1]);
                    $position  = to_int32_nonneg($fields[2]);
                    $dataField = $fields[3];

                    $key = Session::keyFor($sessionId, $addrUri);
                    if (!isset($GLOBALS['sessions'][$key])) {
                        // unknown session -> close
                        send_dgram($GLOBALS['server'], "/close/{$sessionId}/", $addrUri);
                        continue;
                    }
                    $sess = $GLOBALS['sessions'][$key];
                    $sess->touch();
                    $sess->on_data($position, $dataField, $GLOBALS['CHUNK_SIZE']);

                } elseif ($msg_ty === 'ack') {
                    $ensure_argc(2);
                    $sessionId = to_int32_nonneg($fields[1]);
                    $length    = to_int32_nonneg($fields[2]);

                    $key = Session::keyFor($sessionId, $addrUri);
                    if (!isset($GLOBALS['sessions'][$key])) {
                        send_dgram($GLOBALS['server'], "/close/{$sessionId}/", $addrUri);
                        continue;
                    }
                    $sess = $GLOBALS['sessions'][$key];
                    $sess->touch();
                    $sess->on_ack($length, $GLOBALS['sessions']);

                } elseif ($msg_ty === 'close') {
                    $ensure_argc(1);
                    $sessionId = to_int32_nonneg($fields[1]);

                    $key = Session::keyFor($sessionId, $addrUri);
                    if (isset($GLOBALS['sessions'][$key])) {
                        $GLOBALS['sessions'][$key]->close($GLOBALS['sessions']);
                    } else {
                        send_dgram($GLOBALS['server'], "/close/{$sessionId}/", $addrUri);
                    }
                } else {
                    throw new InvalidArgumentException('unknown message type');
                }

            } catch (InvalidArgumentException $e) {
                log_warn("invalid message from {$addrUri}: " . $e->getMessage());
            }
        }
    }

    // Periodic tasks: retransmit + expire
    $now = now_ts();

    // Retransmissions
    foreach ($sessions as $key => $sess) {
        $sess->maybe_retransmit($now, $RETRANS_TIMEOUT, $CHUNK_SIZE);
    }

    // Housekeeping (expiry) every ~1s
    if ($now - $lastHousekeep >= 1.0) {
        foreach ($sessions as $key => $sess) {
            if ($now - $sess->lastSeen > $EXPIRY_TIMEOUT) {
                log_info("[{$sess->id}] session expired");
                $sess->close($sessions);
            }
        }
        $lastHousekeep = $now;
    }
}
