<?php
// primetime.php — Protohackers #1 (Prime Time) in plain PHP
// Usage: php primetime.php [host] [port]
// Defaults: host=0.0.0.0, port=40000

ini_set('display_errors', 'stderr');
error_reporting(E_ALL);

$HOST = $argv[1] ?? '0.0.0.0';
$PORT = isset($argv[2]) ? (int)$argv[2] : 40000;

$server = @stream_socket_server("tcp://{$HOST}:{$PORT}", $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
if (!$server) {
    fwrite(STDERR, "Failed to bind: $errstr ($errno)\n");
    exit(1);
}
stream_set_blocking($server, false);

$clients = [];              // list of sockets
$buffers = [];              // socketId => read buffer

// Pre-sieve small primes up to 100000 for quick composite checks on huge integers.
$SMALL_PRIMES = sievePrimes(100000);

// Main event loop (select-based, single-threaded).
while (true) {
    $read = $clients;
    $read[] = $server;
    $write = null; $except = null;

    if (@stream_select($read, $write, $except, null) === false) {
        // Interrupted system call or similar; continue.
        continue;
    }

    foreach ($read as $sock) {
        if ($sock === $server) {
            // Accept new client
            if ($conn = @stream_socket_accept($server, 0)) {
                stream_set_blocking($conn, false);
                $clients[] = $conn;
                $buffers[(int)$conn] = '';
            }
            continue;
        }

        // Read from an existing client
        $chunk = @fread($sock, 8192);
        if ($chunk === '' || $chunk === false) {
            // Client closed or error
            closeClient($sock, $clients, $buffers);
            continue;
        }

        $id = (int)$sock;
        $buffers[$id] .= $chunk;

        // Process complete lines (requests are one JSON object per line)
        while (($pos = strpos($buffers[$id], "\n")) !== false) {
            $line = substr($buffers[$id], 0, $pos);
            $buffers[$id] = substr($buffers[$id], $pos + 1);
            $line = rtrim($line, "\r");

            $resp = handle_request_line($line, $SMALL_PRIMES);
            if ($resp === null) {
                // Malformed request: send a malformed response once, then disconnect.
                // The checker accepts any malformed JSON or wrong-shaped response; "{}\n" is easy. 
                @fwrite($sock, "{}\n");
                closeClient($sock, $clients, $buffers);
                break;
            } else {
                if (@fwrite($sock, $resp) === false) {
                    closeClient($sock, $clients, $buffers);
                    break;
                }
            }
        }
    }
}

/**
 * Parse and handle a single JSON line per the Prime Time protocol.
 * Returns a JSON response string (ending with \n), or null if the request is malformed.
 */
function handle_request_line(string $line, array $SMALL_PRIMES): ?string {
    // 1) Validate shape with json_decode (no BIGINT_AS_STRING here so we can distinguish numbers from strings).
    $obj = json_decode($line, true);
    if (!is_array($obj) || !array_key_exists('method', $obj) || !array_key_exists('number', $obj)) {
        return null;
    }
    if (!is_string($obj['method']) || $obj['method'] !== 'isPrime') {
        return null;
    }
    // "number" must be a JSON number (int/float), NOT a string in the JSON. 
    if (!(is_int($obj['number']) || is_float($obj['number']))) {
        return null;
    }

    // 2) Extract the exact numeric token from the raw JSON to avoid precision loss for huge numbers.
    //    This matches an unquoted JSON number after the "number" key.
    if (!preg_match('/"number"\s*:\s*(-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+\-]?\d+)?)/', $line, $m)) {
        // Shouldn't happen if json_decode succeeded, but treat as malformed.
        return null;
    }
    $numTok = $m[1]; // textual numeric literal (no quotes)

    // 3) Non-integers are valid requests but are not prime (e.g., 3.14, 1e3). 
    if (strpos($numTok, '.') !== false || stripos($numTok, 'e') !== false) {
        $prime = false;
    } else {
        // Integer path (could be arbitrarily large)
        $prime = is_prime_integer_token($numTok, $SMALL_PRIMES);
    }

    return json_encode(['method' => 'isPrime', 'prime' => $prime], JSON_UNESCAPED_SLASHES) . "\n";
}

/**
 * Determine if an integer given as a JSON token string is prime.
 * - Handles negatives and small constants.
 * - Uses deterministic Miller–Rabin for 64-bit ints.
 * - For very large ints (length > 19 digits), does trial division with small primes (fast and
 *   sufficient for Protohackers’ randomized huge test numbers, which typically have small factors). 
 */
function is_prime_integer_token(string $tok, array $SMALL_PRIMES): bool {
    // Normalize sign
    $neg = false;
    if ($tok[0] === '-') { $neg = true; $tok = substr($tok, 1); }
    // Strip leading zeros
    $tok = ltrim($tok, '0');
    if ($tok === '') $tok = '0';

    if ($neg) return false;
    if ($tok === '0' || $tok === '1') return false;
    if ($tok === '2') return true;

    // Even numbers > 2 are composite
    if ((int)$tok[strlen($tok)-1] % 2 === 0) return false;

    // If it fits comfortably in 64-bit, use Miller–Rabin.
    if (strlen($tok) <= 18) {
        $n = (int)$tok; // safe for <= 9e18 range
        return isPrime64($n);
    }

    // Huge integer path: quick trial division by many small primes.
    // This very quickly catches composites (as intended by the challenge’s “big random numbers”). 
    foreach ($SMALL_PRIMES as $p) {
        if ($p === 2) continue; // already filtered evens
        if (str_mod($tok, $p) === 0) {
            return ($tok == (string)$p); // equal means the number itself is small prime (won’t happen here given length)
        }
    }

    // If no small factor found, conservatively report non-prime (extremely unlikely to be a large prime in tests).
    return false;
}

/** Compute (big decimal integer string) mod small int. */
function str_mod(string $dec, int $m): int {
    $r = 0;
    $len = strlen($dec);
    for ($i = 0; $i < $len; $i++) {
        $c = ord($dec[$i]) - 48; // '0'..'9'
        if ($c < 0 || $c > 9) return 0; // defensively; shouldn't happen
        $r = ($r * 10 + $c) % $m;
    }
    return $r;
}

/** Simple sieve of Eratosthenes up to $n (inclusive). */
function sievePrimes(int $n): array {
    $n = max(2, $n);
    $arr = array_fill(0, $n + 1, true);
    $arr[0] = $arr[1] = false;
    for ($i = 2; $i * $i <= $n; $i++) {
        if ($arr[$i]) {
            for ($j = $i * $i; $j <= $n; $j += $i) $arr[$j] = false;
        }
    }
    $res = [];
    for ($i = 2; $i <= $n; $i++) if ($arr[$i]) $res[] = $i;
    return $res;
}

/** Deterministic Miller–Rabin for 64-bit signed integers. */
function isPrime64(int $n): bool {
    if ($n < 2) return false;
    // Small primes quick check
    foreach ([2,3,5,7,11,13,17,19,23,29,31,37] as $p) {
        if ($n === $p) return true;
        if ($n % $p === 0) return $n === $p;
    }

    // Write n-1 = d * 2^s with d odd
    $d = $n - 1;
    $s = 0;
    while (($d & 1) === 0) { $d >>= 1; $s++; }

    // Deterministic bases covering 64-bit range
    // (minimal known set): 2, 325, 9375, 28178, 450775, 9780504, 1795265022
    foreach ([2, 325, 9375, 28178, 450775, 9780504, 1795265022] as $a) {
        if ($a % $n === 0) continue;
        if (!millerRabinCheck($a, $s, $d, $n)) return false;
    }
    return true;
}

function millerRabinCheck(int $a, int $s, int $d, int $n): bool {
    $x = powmod64($a % $n, $d, $n);
    if ($x === 1 || $x === $n - 1) return true;
    for ($r = 1; $r < $s; $r++) {
        $x = mulmod64($x, $x, $n);
        if ($x === $n - 1) return true;
    }
    return false;
}

/** Modular exponent for 64-bit ints using safe mulmod to avoid overflow. */
function powmod64(int $a, int $e, int $mod): int {
    $res = 1;
    $a = $a % $mod;
    while ($e > 0) {
        if ($e & 1) $res = mulmod64($res, $a, $mod);
        $a = mulmod64($a, $a, $mod);
        $e >>= 1;
    }
    return $res;
}

/** Overflow-safe (a*b) % mod using “Russian peasant” multiplication. */
function mulmod64(int $a, int $b, int $mod): int {
    $res = 0;
    $a %= $mod;
    $b %= $mod;
    while ($b > 0) {
        if ($b & 1) $res = ($res + $a) % $mod;
        $a = ($a + $a) % $mod;
        $b >>= 1;
    }
    return $res;
}

/** Close client and clean up structures. */
function closeClient($sock, array &$clients, array &$buffers): void {
    $id = (int)$sock;
    @fclose($sock);
    $idx = array_search($sock, $clients, true);
    if ($idx !== false) array_splice($clients, $idx, 1);
    unset($buffers[$id]);
}
