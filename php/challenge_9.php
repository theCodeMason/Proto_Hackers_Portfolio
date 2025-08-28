<?php
/**
 * Job Centre server (PHP, single file, no third-party libs)
 * Protocol: line-delimited JSON requests, line-delimited JSON responses
 * Port: 5514 (TCP)
 *
 * Supported requests:
 *  {"request":"put","queue":"queueName","job":<any JSON>,"pri":<number>}
 *  {"request":"get","queues":["q1","q2",...], "wait": true|false}
 *  {"request":"abort","id":<number>}
 *  {"request":"delete","id":<number>}
 *
 * Responses:
 *  {"status":"ok"}                                  // generic OK
 *  {"status":"ok","id":<id>}                        // for put OK
 *  {"status":"ok","id":<id>,"job":<job>,"pri":<p>,"queue":"q"} // job delivery
 *  {"status":"no-job"}                              // no job available for get
 *  {"status":"error","error":"..."}                 // on bad input / not owner, etc.
 */

error_reporting(E_ALL);
ini_set('display_errors', '1');

const HOST = '0.0.0.0';
const PORT = 5514;

// ---------- Utilities ----------
function make_error(string $msg): string {
    return json_encode(['status' => 'error', 'error' => $msg], JSON_UNESCAPED_SLASHES) . "\n";
}
function make_status_ok(): string {
    return json_encode(['status' => 'ok'], JSON_UNESCAPED_SLASHES) . "\n";
}
function make_status_ok_id(int $id): string {
    return json_encode(['status' => 'ok', 'id' => $id], JSON_UNESCAPED_SLASHES) . "\n";
}
function make_status_ok_job(string $q, int $id, $job, $pri): string {
    return json_encode(['status' => 'ok', 'id' => $id, 'job' => $job, 'pri' => $pri, 'queue' => $q], JSON_UNESCAPED_SLASHES) . "\n";
}
function make_status_nojob(): string {
    return json_encode(['status' => 'no-job'], JSON_UNESCAPED_SLASHES) . "\n";
}
function set_nonblock($sock): void {
    stream_set_blocking($sock, false);
}

// ---------- Client model ----------
class Client {
    /** @var resource */
    public $sock;
    /** @var string */
    public $buf = '';
    /** @var array<int,true> job ids owned by this client */
    public $owned = [];
    /** @var array<string,true> queues this client is currently watching */
    public $watching = [];

    public function __construct($sock) {
        $this->sock = $sock;
    }
    public function id(): string {
        return spl_object_hash($this);
    }
    public function write(string $s): void {
        @fwrite($this->sock, $s);
    }
}

// ---------- Server state ----------
$nextId   = 0;
/** @var array<string,array<int,float|int>> Map queue => (jobId => pri) */
$queues   = [];
/** @var array<int,array{0:string,1:mixed,2:float|int}> Map jobId => [queue, job, pri] */
$jobs     = [];
/** @var array<string,array<string,Client>> Map queue => (clientId => Client) */
$watchers = [];

/** @var array<string,Client> */
$clients  = [];

// ---------- Queue helpers ----------
function pick_from_queues(array &$queues, array $qs): ?int {
    $candidates = [];
    foreach ($qs as $q) {
        if (!isset($queues[$q]) || !$queues[$q]) { continue; }
        $bestId = null; $bestPri = null;
        foreach ($queues[$q] as $id => $pri) {
            if ($bestPri === null || $pri > $bestPri) {
                $bestPri = $pri;
                $bestId = $id;
            }
        }
        if ($bestId !== null) {
            $candidates[] = [$bestId, $bestPri];
        }
    }
    if (!$candidates) return null;
    usort($candidates, function($a, $b){ return $b[1] <=> $a[1]; });
    return $candidates[0][0];
}

function pop_id_from_queue(array &$queues, string $q, int $id): void {
    if (isset($queues[$q][$id])) {
        unset($queues[$q][$id]);
        if (!$queues[$q]) unset($queues[$q]);
    }
}

function first_watcher_for_queue(array &$watchers, string $q): ?Client {
    if (!isset($watchers[$q]) || !$watchers[$q]) return null;
    foreach ($watchers[$q] as $cid => $client) {
        unset($watchers[$q][$cid]);
        if (!$watchers[$q]) unset($watchers[$q]);
        unset($client->watching[$q]);
        return $client;
    }
    return null;
}
function add_watcher(array &$watchers, Client $client, array $qs): void {
    foreach ($qs as $q) {
        if (!isset($watchers[$q])) $watchers[$q] = [];
        $watchers[$q][$client->id()] = $client;
        $client->watching[$q] = true;
    }
}
function remove_client_from_all_watchers(array &$watchers, Client $client): void {
    foreach ($watchers as $q => $_) {
        unset($watchers[$q][$client->id()]);
        if (!$watchers[$q]) unset($watchers[$q]);
    }
    $client->watching = [];
}

// ---------- Job operations ----------
function op_put(Client $submitter, string $q, $job, $pri, int &$nextId, array &$queues, array &$jobs, array &$watchers): void {
    $id = $nextId++;
    $jobs[$id] = [$q, $job, $pri];

    if ($client = first_watcher_for_queue($watchers, $q)) {
        $client->write(make_status_ok_job($q, $id, $job, $pri));
        $client->owned[$id] = true;
        $submitter->write(make_status_ok_id($id));
    } else {
        if (!isset($queues[$q])) $queues[$q] = [];
        $queues[$q][$id] = $pri;
        $submitter->write(make_status_ok_id($id));
    }
}

function op_get(Client $client, array $qs, bool $wait, array &$queues, array &$jobs, array &$watchers): void {
    $id = pick_from_queues($queues, $qs);
    if ($id !== null) {
        [$q, $job, $pri] = $jobs[$id];
        pop_id_from_queue($queues, $q, $id);
        $client->write(make_status_ok_job($q, $id, $job, $pri));
        $client->owned[$id] = true;
    } else {
        if ($wait) {
            add_watcher($watchers, $client, $qs);
        } else {
            $client->write(make_status_nojob());
        }
    }
}

function op_abort(Client $client, int $id, array &$queues, array &$jobs, array &$watchers): void {
    if (!isset($jobs[$id])) {
        $client->write(make_status_nojob());
        return;
    }
    if (!isset($client->owned[$id])) {
        $client->write(make_error('not owner'));
        return;
    }
    [$q, $job, $pri] = $jobs[$id];

    if ($w = first_watcher_for_queue($watchers, $q)) {
        $w->write(make_status_ok_job($q, $id, $job, $pri));
        $w->owned[$id] = true;
    } else {
        if (!isset($queues[$q])) $queues[$q] = [];
        $queues[$q][$id] = $pri;
    }
    unset($client->owned[$id]);
    $client->write(make_status_ok());
}

function op_delete(Client $client, int $id, array &$queues, array &$jobs): void {
    if (!isset($jobs[$id])) {
        $client->write(make_status_nojob());
        return;
    }
    [$q, $job, $pri] = $jobs[$id];
    if (isset($queues[$q][$id])) {
        unset($queues[$q][$id]);
        if (!$queues[$q]) unset($queues[$q]);
    }
    unset($jobs[$id]);
    $client->write(make_status_ok());
}

function abort_all_owned_on_disconnect(Client $client, array &$queues, array &$jobs, array &$watchers): void {
    if (!$client->owned) return;
    foreach (array_keys($client->owned) as $id) {
        if (!isset($jobs[$id])) continue;
        [$q, $job, $pri] = $jobs[$id];
        if ($w = first_watcher_for_queue($watchers, $q)) {
            $w->write(make_status_ok_job($q, $id, $job, $pri));
            $w->owned[$id] = true;
        } else {
            if (!isset($queues[$q])) $queues[$q] = [];
            $queues[$q][$id] = $pri;
        }
        unset($client->owned[$id]);
    }
}

// ---------- Networking ----------
$server = @stream_socket_server("tcp://" . HOST . ":" . PORT, $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN);
if (!$server) {
    fwrite(STDERR, "Failed to bind " . HOST . ":" . PORT . " - $errstr ($errno)\n");
    exit(1);
}
set_nonblock($server);
echo "[job-centre] listening on " . HOST . ":" . PORT . PHP_EOL;

$lastStats = 0;

while (true) {
    $read = [$server];
    foreach ($clients as $c) { $read[] = $c->sock; }
    $write = null; $except = null;

    @stream_select($read, $write, $except, 1, 0);

    if (in_array($server, $read, true)) {
        $sock = @stream_socket_accept($server, 0);
        if ($sock) {
            set_nonblock($sock);
            $client = new Client($sock);
            $clients[$client->id()] = $client;
        }
        $read = array_filter($read, fn($s) => $s !== $server);
    }

    foreach ($clients as $cid => $client) {
        if (!in_array($client->sock, $read, true)) continue;

        $data = @fread($client->sock, 8192);
        if ($data === '' || $data === false) {
            if (feof($client->sock)) {
                remove_client_from_all_watchers($watchers, $client);
                abort_all_owned_on_disconnect($client, $queues, $jobs, $watchers);
                @fclose($client->sock);
                unset($clients[$cid]);
            }
            continue;
        }

        $client->buf .= $data;

        while (true) {
            $pos = strpos($client->buf, "\n");
            if ($pos === false) break;
            $line = substr($client->buf, 0, $pos);
            $client->buf = substr($client->buf, $pos + 1);

            $line = rtrim($line, "\r");
            if ($line === '') { $client->write(make_error('empty line')); continue; }

            $msg = json_decode($line, true);
            if (!is_array($msg)) {
                $client->write(make_error('invalid JSON'));
                continue;
            }

            if (($msg['request'] ?? null) === 'put') {
                $q = $msg['queue'] ?? null;
                $pri = $msg['pri'] ?? null;
                if (!is_string($q) || $q === '' || !array_key_exists('job',$msg) || !is_numeric($pri)) {
                    $client->write(make_error('bad request'));
                    continue;
                }
                op_put($client, $q, $msg['job'], $pri, $nextId, $queues, $jobs, $watchers);
            }
            elseif (($msg['request'] ?? null) === 'get') {
                $qs = $msg['queues'] ?? null;
                $wait = (bool)($msg['wait'] ?? false);
                if (!is_array($qs) || !$qs) {
                    $client->write(make_error('bad request'));
                    continue;
                }
                $qs = array_values(array_unique(array_map(fn($x)=>strval($x), $qs)));
                op_get($client, $qs, $wait, $queues, $jobs, $watchers);
            }
            elseif (($msg['request'] ?? null) === 'abort') {
                $id = $msg['id'] ?? null;
                if (!is_int($id)) {
                    $client->write(make_error('bad request'));
                    continue;
                }
                op_abort($client, $id, $queues, $jobs, $watchers);
            }
            elseif (($msg['request'] ?? null) === 'delete') {
                $id = $msg['id'] ?? null;
                if (!is_int($id)) {
                    $client->write(make_error('bad request'));
                    continue;
                }
                op_delete($client, $id, $queues, $jobs);
            }
            else {
                $client->write(make_error('bad request'));
            }
        }
    }

    $now = time();
    if ($now - $lastStats >= 5) {
        $lastStats = $now;
        echo sprintf(
            "[stats] queues:%d jobs:%d watchers:%d clients:%d\n",
            count($queues), count($jobs), count($watchers), count($clients)
        );
    }
}
