<?php
/**
 * Means to an End — PHP server
 * Protohackers-style: 9-byte messages
 *  - 'I' <int32 timestamp> <int32 price> : insert/update
 *  - 'Q' <int32 min> <int32 max>        : query mean in [min, max]
 * Respond to Q with a 4-byte big-endian int32 of the mean (truncate toward zero).
 *
 * Port: 5514 (to match the provided Clojure code)
 */

error_reporting(E_ALL);
ini_set('display_errors', '1');

const HOST = '0.0.0.0';
const PORT = 40000;

// ---- 32-bit helpers (two's complement like the Clojure version) ----

/** Convert unsigned 32-bit (0..2^32-1) to signed 32-bit (-2^31..2^31-1) */
function u32_to_s32(int $u): int {
    // Bring into 0..2^32-1 range (PHP ints are 64-bit on most platforms)
    $u &= 0xFFFFFFFF;
    if ($u & 0x80000000) {
        return $u - 0x100000000; // negative
    }
    return $u; // non-negative
}

/** Convert signed 32-bit (-2^31..2^31-1) to unsigned domain for pack('N', ...) */
function s32_to_u32(int $s): int {
    // Normalize to 32-bit
    $s &= 0xFFFFFFFF;
    return $s;
}

/** Read big-endian uint32 from 4 bytes to PHP int, then to signed int */
function be_bytes_to_s32(string $bytes): int {
    $arr = unpack('Nn', $bytes);
    return u32_to_s32($arr['n']);
}

/** Pack signed int32 as big-endian 4 bytes */
function s32_to_be_bytes(int $s): string {
    return pack('N', s32_to_u32($s));
}

// ---- Networking setup ----

$errno = 0;
$errstr = '';
$server = @stream_socket_server("tcp://" . HOST . ":" . PORT, $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
if ($server === false) {
    fwrite(STDERR, "Failed to bind: $errstr ($errno)\n");
    exit(1);
}
stream_set_blocking($server, false);
fwrite(STDERR, "[means] listening on " . HOST . ":" . PORT . PHP_EOL);

// Per-connection state:
// $clients[(int)$sock] = [
//   'sock' => resource,
//   'buf'  => string (incoming),
//   'out'  => string (pending outgoing),
//   'db'   => [timestamp(int)=>price(int)]
// ]
$clients = [];

/** Accept a new client */
function accept_client($server, array &$clients): void {
    $sock = @stream_socket_accept($server, 0);
    if ($sock === false) return;
    stream_set_blocking($sock, false);
    $id = (int)$sock;
    $peer = stream_socket_get_name($sock, true) ?: 'unknown';
    $clients[$id] = [
        'sock' => $sock,
        'buf'  => '',
        'out'  => '',
        'db'   => [], // per-connection state
        'peer' => $peer,
    ];
    fwrite(STDERR, "[means] + client #$id from $peer\n");
}

/** Close and remove a client */
function drop_client(int $id, array &$clients): void {
    if (!isset($clients[$id])) return;
    $peer = $clients[$id]['peer'] ?? '';
    @fclose($clients[$id]['sock']);
    unset($clients[$id]);
    fwrite(STDERR, "[means] - client #$id $peer\n");
}

/** Process complete 9-byte frames in the client's input buffer */
function process_frames(int $id, array &$clients): void {
    $c =& $clients[$id];
    while (strlen($c['buf']) >= 9) {
        $frame = substr($c['buf'], 0, 9);
        $c['buf'] = substr($c['buf'], 9);

        $cmd = $frame[0];
        $arg1 = be_bytes_to_s32(substr($frame, 1, 4));
        $arg2 = be_bytes_to_s32(substr($frame, 5, 4));

        // Uncomment if you want verbose tracing:
        // fwrite(STDERR, "[#{$id}] cmd={$cmd} arg1={$arg1} arg2={$arg2}\n");

        if ($cmd === 'I') {
            // Insert/update timestamp -> price
            $c['db'][$arg1] = $arg2;
        } elseif ($cmd === 'Q') {
            $min = $arg1;
            $max = $arg2;
            if ($min > $max) {
                // Empty range -> mean 0 (common interpretation)
                $mean = 0;
            } else {
                $sum = 0;
                $count = 0;
                // Iterate only the entries we have
                foreach ($c['db'] as $ts => $price) {
                    if ($ts >= $min && $ts <= $max) {
                        $sum += $price;
                        $count++;
                    }
                }
                if ($count === 0) {
                    $mean = 0;
                } else {
                    // Truncate toward zero like Clojure's (quot)
                    // In PHP 7+, intdiv truncates toward zero.
                    $mean = intdiv($sum, $count);
                }
            }
            $c['out'] .= s32_to_be_bytes($mean);
        } else {
            // Unknown command — per spec we can ignore or disconnect.
            // We'll ignore gracefully.
        }
    }
}

// ---- Main event loop ----

while (true) {
    $read = [$server];
    $write = [];
    $except = null;

    foreach ($clients as $id => $c) {
        $read[] = $c['sock'];
        if ($c['out'] !== '') {
            $write[] = $c['sock'];
        }
    }

    // 1-second timeout to avoid busy loop; adjust as desired
    $num_changed = @stream_select($read, $write, $except, 1, 0);
    if ($num_changed === false) {
        // Interrupted or error — continue
        continue;
    }

    // New connections
    if (in_array($server, $read, true)) {
        accept_client($server, $clients);
        // Remove server from read list for per-client processing
        $idx = array_search($server, $read, true);
        if ($idx !== false) {
            unset($read[$idx]);
        }
    }

    // Handle readable clients
    foreach ($clients as $id => $_) {
        $sock = $clients[$id]['sock'];
        if (!in_array($sock, $read, true)) {
            continue;
        }

        $data = @fread($sock, 8192);
        if ($data === '' || $data === false) {
            // EOF or error -> drop
            drop_client($id, $clients);
            continue;
        }

        $clients[$id]['buf'] .= $data;
        process_frames($id, $clients);
    }

    // Handle writable clients (pending responses)
    foreach ($clients as $id => $_) {
        $sock = $clients[$id]['sock'];
        if ($clients[$id]['out'] === '' || !in_array($sock, $write, true)) {
            continue;
        }
        $to_write = $clients[$id]['out'];
        $written = @fwrite($sock, $to_write);
        if ($written === false) {
            // On write error, drop client
            drop_client($id, $clients);
            continue;
        }
        if ($written === strlen($to_write)) {
            $clients[$id]['out'] = '';
        } else {
            // Partial write; keep the remainder
            $clients[$id]['out'] = substr($to_write, $written);
        }
    }
}

