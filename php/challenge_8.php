<?php
// ProtoHackers #8 — Priority Toy Workshop (PHP, non-blocking, multi-client)

// ====== Helpers ======
function sock_id($sock) {
    // PHP 8 sockets are objects; use a stable ID for array keys
    if (is_object($sock)) return spl_object_id($sock);
    return (int)$sock; // resources (PHP 7) fall back to int cast
}

// ====== Cipher primitives ======
$fn_reverse = function($b, $pos) {
    $rev = 0;
    for ($i = 0; $i < 8; $i++) { $rev = ($rev << 1) | ($b & 1); $b >>= 1; }
    return $rev;
};
$fn_xor_pos = function($b, $pos) { return $b ^ ($pos & 0xFF); };
$fn_add_pos = function($b, $pos) { return ($b + ($pos & 0xFF)) & 0xFF; };
// (sub_pos not needed directly; dec is handled via inverse LUTs)

function make_xor_n($n) { $n &= 0xFF; return fn($b,$p)=>($b ^ $n) & 0xFF; }
function make_add_n($n) { $n &= 0xFF; return fn($b,$p)=>(($b + $n) & 0xFF); }

// ====== App layer ======
function toy_priority($s) {
    $items = explode(',', $s);
    $parsed = [];
    foreach ($items as $item) {
        if (preg_match('/^(\d+)x\s(.+)$/', trim($item), $m)) {
            $parsed[] = [(int)$m[1], $m[2]];
        }
    }
    usort($parsed, fn($a,$b)=>$b[0] <=> $a[0]);
    if (empty($parsed)) return '';
    [$n, $name] = $parsed[0];
    return sprintf("%dx %s", $n, $name);
}

// ====== LUT builder (byte strings, memory efficient) ======
function build_cipher_tables_bytes($ops) {
    // Returns [$enc[256], $dec[256], $isIdentity]
    $enc = array_fill(0,256,null);
    $dec = array_fill(0,256,null);

    $id = pack('C*', ...range(0,255));
    $isIdentity = true;

    for ($p = 0; $p < 256; $p++) {
        $vals = range(0,255);
        foreach ($ops as $fn) {
            for ($i = 0; $i < 256; $i++) {
                $vals[$i] = $fn($vals[$i], $p) & 0xFF;
            }
        }
        $map = pack('C*', ...$vals);
        $enc[$p] = $map;
        if ($isIdentity && $map !== $id) $isIdentity = false;

        // inverse
        $inv = array_fill(0,256,0);
        for ($i = 0; $i < 256; $i++) { $inv[$vals[$i]] = $i; }
        $dec[$p] = pack('C*', ...$inv);
    }
    return [$enc, $dec, $isIdentity];
}

// ====== Per-connection helpers ======
function close_client(&$clients, $id) {
    if (!isset($clients[$id])) return;
    @socket_close($clients[$id]['sock']);
    unset($clients[$id]);
}

function process_client(&$c) {
    while ($c['inbuf'] !== '') {
        if ($c['stage'] === 'cipher') {
            $b = ord($c['inbuf'][0]); $c['inbuf'] = substr($c['inbuf'],1);

            if ($c['cstate'] === 'xor') { $c['ops'][] = make_xor_n($b); $c['cstate'] = null; continue; }
            if ($c['cstate'] === 'add') { $c['ops'][] = make_add_n($b); $c['cstate'] = null; continue; }

            if ($b === 0) {
                // Compile LUTs; disconnect if overall identity (zero-length or no-op combo)
                [$enc, $dec, $isId] = build_cipher_tables_bytes($c['ops']);
                if ($isId) { $c['kill'] = true; return; }
                $c['enc'] = $enc; $c['dec'] = $dec; $c['stage'] = 'app';
                continue;
            } elseif ($b === 1) {
                $c['ops'][] = $c['fn_reverse'];
            } elseif ($b === 2) {
                $c['cstate'] = 'xor';
            } elseif ($b === 3) {
                $c['ops'][] = $c['fn_xor_pos'];
            } elseif ($b === 4) {
                $c['cstate'] = 'add';
            } elseif ($b === 5) {
                $c['ops'][] = $c['fn_add_pos'];
            } else {
                $c['kill'] = true; return; // invalid cipher byte
            }
        } else { // stage 'app'
            $cb = ord($c['inbuf'][0]); $c['inbuf'] = substr($c['inbuf'],1);
            $plain = ord($c['dec'][$c['r_pos'] & 0xFF][$cb]);
            $c['r_pos']++;

            if ($plain === 10) { // newline terminator
                $resp_text = toy_priority($c['line']);
                $c['line'] = '';

                $len = strlen($resp_text);
                for ($i=0; $i<$len; $i++) {
                    $c['outbuf'] .= $c['enc'][$c['w_pos'] & 0xFF][ord($resp_text[$i])];
                    $c['w_pos']++;
                }
                $c['outbuf'] .= $c['enc'][$c['w_pos'] & 0xFF][10];
                $c['w_pos']++;
                $c['want_write'] = true;
            } else {
                $c['line'] .= chr($plain);
                if (strlen($c['line']) > (1<<20)) { $c['kill'] = true; return; } // 1MB safety
            }
        }
    }
}

// ====== Event-loop server (multi-client with socket_select) ======
function start_server() {
    $port = getenv('PORT') ?: (getenv('PROTOHACKERS_PORT') ?: 40000);

    $server = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
    if (!$server) die("socket_create failed: ".socket_strerror(socket_last_error())."\n");
    socket_set_option($server, SOL_SOCKET, SO_REUSEADDR, 1);
    if (!@socket_bind($server, '0.0.0.0', (int)$port)) {
        die("socket_bind failed: ".socket_strerror(socket_last_error($server))."\n");
    }
    if (!@socket_listen($server, 128)) {
        die("socket_listen failed: ".socket_strerror(socket_last_error($server))."\n");
    }
    socket_set_nonblock($server);
    echo "open on :$port\n";

    $clients = [];
    $IDLE_SECS = 300;

    while (true) {
        $read = [$server];
        $write = [];
        foreach ($clients as $id => $c) {
            $read[] = $c['sock'];
            if ($c['want_write'] && $c['outbuf'] !== '') $write[] = $c['sock'];
        }

        $except = null;
        @socket_select($read, $write, $except, 1, 0);

        // Accept all pending
        if (in_array($server, $read, true)) {
            while (true) {
                $sock = @socket_accept($server);
                if ($sock === false) break;
                socket_set_nonblock($sock);
                $id = sock_id($sock);
                $clients[$id] = [
                    'sock' => $sock,
                    'stage' => 'cipher',
                    'cstate' => null,
                    'ops' => [],
                    'enc' => null,
                    'dec' => null,
                    'r_pos' => 0,
                    'w_pos' => 0,
                    'inbuf' => '',
                    'outbuf' => '',
                    'line' => '',
                    'want_write' => false,
                    'kill' => false,
                    'last' => time(),
                    // bind primitives for quick access
                    'fn_reverse' => $GLOBALS['fn_reverse'],
                    'fn_xor_pos' => $GLOBALS['fn_xor_pos'],
                    'fn_add_pos' => $GLOBALS['fn_add_pos'],
                ];
            }
        }

        // Reads
        foreach ($clients as $id => &$c) {
            if (!in_array($c['sock'], $read, true)) continue;

            $buf = '';
            $n = @socket_recv($c['sock'], $buf, 8192, 0);
            if ($n === false) {
                $err = socket_last_error($c['sock']);
                if ($err !== SOCKET_EWOULDBLOCK && (!defined('SOCKET_EAGAIN') || $err !== SOCKET_EAGAIN)) {
                    $c['kill'] = true;
                }
                continue;
            }
            if ($n === 0) { $c['kill'] = true; continue; } // peer closed

            $c['inbuf'] .= $buf;
            $c['last'] = time();
            process_client($c);
        }
        unset($c);

        // Writes
        foreach ($clients as $id => &$c) {
            if ($c['outbuf'] === '' || !in_array($c['sock'], $write, true)) continue;
            $sent = @socket_write($c['sock'], $c['outbuf']);
            if ($sent === false) {
                $err = socket_last_error($c['sock']);
                if ($err !== SOCKET_EWOULDBLOCK && (!defined('SOCKET_EAGAIN') || $err !== SOCKET_EAGAIN)) {
                    $c['kill'] = true;
                }
                continue;
            }
            if ($sent > 0) {
                $c['outbuf'] = substr($c['outbuf'], $sent);
                if ($c['outbuf'] === '') $c['want_write'] = false;
                $c['last'] = time();
            }
        }
        unset($c);

        // Cleanup / idle timeouts
        $now = time();
        foreach (array_keys($clients) as $id) {
            if (!isset($clients[$id])) continue;
            if ($clients[$id]['kill'] || ($now - $clients[$id]['last'] > $IDLE_SECS)) {
                close_client($clients, $id);
            }
        }
    }
}

// ====== Main ======
start_server();
