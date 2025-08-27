<?php
// newc.php — PHP 8+
// Multiplexed TCP server implementing HELP/GET/PUT/LIST with in-memory revisions.
// Non-blocking sockets + stream_select() so one client's slow PUT body won't block others.

// ---------------- SlidingBufferReader ----------------
class SlidingBufferReader {
    private string $buf = '';

    public function append(string $chunk): void {
        $this->buf .= $chunk;
    }

    // Read a line terminated by "\n" (returns line w/o newline) or null if not yet available
    public function readLine(): ?string {
        $pos = strpos($this->buf, "\n");
        if ($pos === false) return null;
        $line = substr($this->buf, 0, $pos);
        $this->buf = substr($this->buf, $pos + 1);
        return $line;
    }

    // Read exactly $n bytes from buffer; returns string or null if not enough buffered yet
    public function read(int $n): ?string {
        if (strlen($this->buf) < $n) return null;
        $out = substr($this->buf, 0, $n);
        $this->buf = substr($this->buf, $n);
        return $out;
    }
}

// ---------------- Validation helpers ----------------
function isValidText(string $s): bool {
    $len = strlen($s);
    for ($i = 0; $i < $len; $i++) {
        $b = ord($s[$i]);
        if (!($b === 0x0A || $b === 0x09 || ($b >= 0x20 && $b < 0x7F))) {
            return false;
        }
    }
    return true;
}
function isValidFileName(string $name): bool {
    return preg_match('/^[a-zA-Z0-9\-_.]+$/', $name) === 1;
}
function isValidFilePath(string $input): bool {
    if ($input === '/') return false;           // file path must have a name
    if (!str_starts_with($input, '/')) return false;
    $parts = explode('/', $input);
    array_shift($parts); // first empty before leading '/'
    foreach ($parts as $p) {
        if ($p === '') return false;           // no empty segments
        if (!isValidFileName($p)) return false;
    }
    return true;
}
function isValidDirPath(string $input): bool {
    if (!str_starts_with($input, '/')) return false;
    $parts = explode('/', $input);
    array_shift($parts); // remove leading empty
    if (end($parts) === '') array_pop($parts); // allow trailing '/'
    foreach ($parts as $p) {
        if ($p === '') return false;
        if (!isValidFileName($p)) return false;
    }
    return true;
}

// ---------------- In-memory store ----------------
// filename => last revision (int)
$_META = [];
// "filename#revision" => data (string)
$_STORE = [];

function buildFileKey(string $filename, int $revision): string {
    return $filename . '#' . $revision;
}
function getLastRevision(string $filename): ?int {
    global $_META;
    return $_META[$filename] ?? null;
}
function getFile(string $filename, int $revision): ?string {
    global $_STORE;
    return $_STORE[buildFileKey($filename, $revision)] ?? null;
}
function saveFile(string $filename, string $data): int {
    global $_META, $_STORE;
    $last = $_META[$filename] ?? null;
    if ($last === null) {
        $_STORE[buildFileKey($filename, 1)] = $data;
        $_META[$filename] = 1;
        return 1;
    }
    $lastData = getFile($filename, $last);
    if ($lastData === $data) {
        return $last; // no change
    }
    $rev = $last + 1;
    $_STORE[buildFileKey($filename, $rev)] = $data;
    $_META[$filename] = $rev;
    return $rev;
}
// dir must end with '/'
function listFilesInDir(string $dir): array {
    global $_STORE;
    if (!str_ends_with($dir, '/')) {
        throw new RuntimeException('dir must end with "/"');
    }
    $result = [];
    foreach (array_keys($_STORE) as $key) {
        $filename = explode('#', $key, 2)[0];
        if (str_starts_with($filename, $dir)) {
            $result[] = substr($filename, strlen($dir));
        }
    }
    return $result;
}

// ---------------- Protocol parsing ----------------
const MT_HELP    = 'help';
const MT_GET     = 'get';
const MT_PUT     = 'put';
const MT_LIST    = 'list';
const MT_ILLEGAL = 'illegal';
const MT_ERROR   = 'error';

function parseMethod(string $input): array {
    $parts = preg_split('/\s+/', rtrim($input, "\r\n"), -1, PREG_SPLIT_NO_EMPTY);
    if (!$parts) return ['type' => MT_ILLEGAL, 'method' => ''];

    $rawCmd = $parts[0];
    $cmd = strtolower($rawCmd);

    if ($cmd === MT_HELP) {
        return ['type' => MT_HELP];
    }
    if ($cmd === MT_PUT) {
        if (count($parts) !== 3) {
            return ['type' => MT_ERROR, 'err' => 'usage: PUT file length newline data', 'appendReady' => true];
        }
        $filename = $parts[1];
        $length   = intval($parts[2]); // NaN -> 0
        return ['type' => MT_PUT, 'filename' => $filename, 'length' => $length];
    }
    if ($cmd === MT_GET) {
        if (count($parts) !== 2 && count($parts) !== 3) {
            return ['type' => MT_ERROR, 'err' => 'usage: GET file [revision]', 'appendReady' => true];
        }
        $filename = $parts[1];
        $revision = $parts[2] ?? null;
        return ['type' => MT_GET, 'filename' => $filename, 'revision' => $revision];
    }
    if ($cmd === MT_LIST) {
        if (count($parts) !== 2) {
            return ['type' => MT_ERROR, 'err' => 'usage: LIST dir', 'appendReady' => true];
        }
        return ['type' => MT_LIST, 'dir' => $parts[1]];
    }

    return ['type' => MT_ILLEGAL, 'method' => $rawCmd];
}

// ---------------- Non-blocking multiplexed server ----------------
$PORT = 40000;
$server = @stream_socket_server("tcp://0.0.0.0:$PORT", $errno, $errstr);
if ($server === false) {
    fwrite(STDERR, "Failed to bind: $errstr ($errno)\n");
    exit(1);
}
stream_set_blocking($server, false);
echo "Server listening on port $PORT\n";

class Conn {
    public int $id;
    /** @var resource */
    public $sock;
    public SlidingBufferReader $reader;
    public string $state = 'IDLE'; // IDLE | PUT_WAIT
    public ?string $putFilename = null;
    public int $putRemaining = 0;

    public function __construct(int $id, $sock) {
        $this->id = $id;
        $this->sock = $sock;
        stream_set_blocking($this->sock, false);
        $this->reader = new SlidingBufferReader();
    }
    public function write(string $s, bool $nl = true): void {
        @fwrite($this->sock, $s . ($nl ? "\n" : ""));
    }
}

$clients = [];        // (int)$sock => Conn
$clientIdSeed = 0;

function closeConn(array &$clients, Conn $c): void {
    @fclose($c->sock);
    unset($clients[(int)$c->sock]);
}

function handleIdleLine(Conn $c, string $line): void {
    $method = parseMethod($line);
    echo "[{$c->id}] got line " . json_encode(['line'=>$line,'method'=>$method], JSON_UNESCAPED_SLASHES) . PHP_EOL;

    switch ($method['type']) {
        case MT_ILLEGAL:
            $c->write("ERR illegal method: " . ($method['method'] ?? ''));
            break;

        case MT_HELP:
            $c->write("OK usage: HELP|GET|PUT|LIST");
            $c->write("READY");
            break;

        case MT_ERROR:
            $c->write("ERR " . $method['err']);
            if (!empty($method['appendReady'])) $c->write("READY");
            break;

        case MT_PUT:
            $filename = $method['filename'];
            $length   = max(0, (int)$method['length']);
            if (!isValidFilePath($filename)) {
                $c->write("ERR illegal file name");
                $c->write("READY");
                break;
            }
            // Enter PUT_WAIT: do not parse further commands on this socket until body is read
            $c->state = 'PUT_WAIT';
            $c->putFilename = $filename;
            $c->putRemaining = $length;
            break;

        case MT_GET:
            $filename = $method['filename'];
            if (!isValidFilePath($filename)) { $c->write("ERR illegal file name"); break; }
            $last = getLastRevision($filename);
            if ($last === null) { $c->write("ERR no such file"); break; }
            $rev = $last;
            if (!empty($method['revision'])) {
                $r = $method['revision'];
                if ($r[0] === 'r' || $r[0] === 'R') $r = substr($r, 1);
                $rev = (int)$r;
            }
            $data = getFile($filename, $rev);
            if ($data === null) { $c->write("ERR no such revision"); break; }
            $c->write("OK " . strlen($data));
            $c->write($data, false);
            $c->write("READY");
            break;

        case MT_LIST:
            $dir = $method['dir'];
            if (!isValidDirPath($dir)) { $c->write("ERR illegal dir name"); break; }
            if (!str_ends_with($dir, '/') && strlen($dir) > 1) $dir .= '/';
            $all = listFilesInDir($dir);
            // sort by path length ASC (matches TS behavior)
            usort($all, fn($a, $b) => strlen($a) <=> strlen($b));

            $result = [];
            foreach ($all as $name) {
                if (!str_contains($name, '/')) {
                    $result[$name] = true; // file
                } else {
                    $dirName = explode('/', $name, 2)[0];
                    $result[$dirName . '/'] = true; // subdir (only once)
                }
            }

            $items = array_keys($result);
            sort($items, SORT_STRING);
            $c->write("OK " . count($items));
            foreach ($items as $item) {
                $rev = str_ends_with($item, '/')
                    ? 'DIR'
                    : ('r' . getLastRevision($dir . $item));
                $c->write($item . ' ' . $rev);
            }
            $c->write("READY");
            break;
    }
}

function pumpPutBody(Conn $c): void {
    if ($c->state !== 'PUT_WAIT' || $c->putRemaining <= 0) return;

    // Try to read exactly the remaining bytes (or as many as are buffered)
    $chunk = $c->reader->read($c->putRemaining);
    if ($chunk === null) return; // not enough buffered yet

    if (!isValidText($chunk)) {
        $c->write("ERR text files only");
        $c->write("READY");
        // reset state
        $c->state = 'IDLE'; $c->putFilename = null; $c->putRemaining = 0;
        return;
    }

    $rev = saveFile($c->putFilename, $chunk);
    $c->write("OK r$rev");
    $c->write("READY");

    // reset state
    $c->state = 'IDLE'; $c->putFilename = null; $c->putRemaining = 0;
}

function acceptClient($server, array &$clients, int &$clientIdSeed): void {
    $sock = @stream_socket_accept($server, 0); // non-blocking accept
    if ($sock === false) return;
    $id = $clientIdSeed++;
    $conn = new Conn($id, $sock);
    $clients[(int)$sock] = $conn;
    echo "[$id] client connected\n";
    $conn->write("READY");
}

// ---------------- Event loop ----------------
while (true) {
    $read = [$server];
    foreach ($clients as $c) $read[] = $c->sock;

    $write = null; $except = null;
    $n = @stream_select($read, $write, $except, null, null);
    if ($n === false) {
        // interrupted system call or similar
        continue;
    }

    foreach ($read as $r) {
        if ($r === $server) {
            acceptClient($server, $clients, $clientIdSeed);
            continue;
        }

        $c = $clients[(int)$r] ?? null;
        if (!$c) continue;

        $data = @fread($c->sock, 8192);
        if ($data === '' || $data === false) {
            // client closed
            closeConn($clients, $c);
            continue;
        }
        $c->reader->append($data);

        // If waiting for PUT body, try to consume that first
        if ($c->state === 'PUT_WAIT') {
            pumpPutBody($c);
            // If still waiting, do not parse more commands on this socket
            if ($c->state === 'PUT_WAIT') continue;
        }

        // In IDLE, consume as many complete lines as are buffered
        while ($c->state === 'IDLE') {
            $line = $c->reader->readLine();
            if ($line === null) break;
            handleIdleLine($c, $line);
            if ($c->state === 'PUT_WAIT') {
                // Just switched to PUT_WAIT due to a PUT; try to consume any already-buffered body bytes
                pumpPutBody($c);
                break;
            }
        }
    }
}
