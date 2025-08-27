<?php
/**
 * Protohackers "Pest Control" — pure-PHP implementation (no third-party libs)
 * Usage: php pestcontrol.php [listen_port]
 *
 * Listens for client connections, dials the Authority Service (AS) at
 * pestcontrol.protohackers.com:20547 per-site, enforces targets, and manages policies.
 */

error_reporting(E_ALL);
ini_set('display_errors', '1');

const AS_HOST = "pestcontrol.protohackers.com";
const AS_PORT = 20547;

const WRAPPER_SIZE = 6;              // type (1) + len (4) + checksum (1)
const MAX_LENGTH   = 1024*1024;      // 1 MiB

// Message Types
const TY_HELLO           = 0x50;
const TY_ERROR           = 0x51;
const TY_OK              = 0x52;
const TY_DIAL_AUTH       = 0x53;
const TY_TARGET_POPS     = 0x54;
const TY_CREATE_POLICY   = 0x55;
const TY_DELETE_POLICY   = 0x56;
const TY_POLICY_RESULT   = 0x57;
const TY_SITE_VISIT      = 0x58;

// Policy Actions
const ACT_CULL     = 0x90;
const ACT_CONSERVE = 0xA0;

class ProtoError extends Exception {}

function hex_repr(string $bytes): string {
    $out = [];
    foreach (unpack("C*", $bytes) as $b) {
        if ($b >= 0x41 && $b <= 0x7e) $out[] = chr($b);
        else $out[] = sprintf("%02x", $b);
    }
    return implode(" ", $out);
}

class OutMsg {
    private array $buf;    // array of bytes (ints 0..255)
    private bool $wrapped = false;

    public function __construct(int $msg_ty) {
        $this->buf = [$msg_ty, 0,0,0,0]; // placeholder length
    }
    public function add_u8(int $n): void { $this->buf[] = $n & 0xFF; }
    public function add_u32(int $n): void {
        $this->buf[] = ($n >> 24) & 0xFF;
        $this->buf[] = ($n >> 16) & 0xFF;
        $this->buf[] = ($n >> 8)  & 0xFF;
        $this->buf[] = ($n)       & 0xFF;
    }
    public function add_str(string $s): void {
        $b = $s; // ASCII required by spec
        $this->add_u32(strlen($b));
        foreach (unpack("C*", $b) as $ch) $this->buf[] = $ch;
    }
    private function to_bytes(): string {
        return pack("C*", ...$this->buf);
    }
    public function send(Peer $peer): void {
        if (!$this->wrapped) {
            $len = count($this->buf) + 1; // plus checksum
            // write big-endian length into buf[1..4]
            $this->buf[1] = ($len >> 24) & 0xFF;
            $this->buf[2] = ($len >> 16) & 0xFF;
            $this->buf[3] = ($len >> 8)  & 0xFF;
            $this->buf[4] = ($len)       & 0xFF;

            $sum = array_sum($this->buf) % 256;
            $checksum = ($sum === 0) ? 0 : (256 - $sum);
            $this->add_u8($checksum);
            $this->wrapped = true;
        }
        $b = $this->to_bytes();
        $peer->debug("-> " . hex_repr($b));
        $peer->enqueueWrite($b);
    }
}

class InMsg {
    public int $ty;
    private string $buf;
    private int $i = 0;

    public function __construct(int $ty, string $buf) {
        $this->ty = $ty;
        $this->buf = $buf;
    }
    public function get_bytes(int $n): string {
        if ($this->i > strlen($this->buf) - $n) {
            throw new ProtoError("content exceeds declared length");
        }
        $b = substr($this->buf, $this->i, $n);
        $this->i += $n;
        return $b;
    }
    public function get_u32(): int {
        $b = $this->get_bytes(4);
        $arr = unpack("N", $b);
        return $arr[1];
    }
    public function get_str(): string {
        $size = $this->get_u32();
        $b = $this->get_bytes($size);
        // spec says ASCII (we don't re-encode)
        return $b;
    }
    public function check_end(): void {
        if ($this->i !== strlen($this->buf)) {
            throw new ProtoError("unused bytes in message");
        }
    }

    /**
     * Try to parse a single message from $in (by reference).
     * Returns [InMsg, total_bytes_consumed] or [null, 0] if incomplete.
     * Throws ProtoError on invalid checksum or length.
     */
    public static function tryParse(string &$in): array {
        if (strlen($in) < 6) return [null, 0];

        $hdr = unpack("Cty/Nlen", substr($in, 0, 5));
        $ty = $hdr['ty'];
        $len = $hdr['len'];
        if ($len >= MAX_LENGTH) throw new ProtoError("message is too long");
        if (strlen($in) < $len) return [null, 0];

        $content_len = $len - WRAPPER_SIZE; // payload length
        $payload = substr($in, 5, $content_len);
        $checksum = ord($in[$len - 1]);

        // Verify checksum over type + 4 len bytes + payload + checksum
        $sum = $ty
            + array_sum(unpack("C*", substr($in, 1, 4)))
            + (($content_len>0) ? array_sum(unpack("C*", $payload)) : 0)
            + $checksum;
        if (($sum % 256) !== 0) {
            throw new ProtoError("invalid checksum");
        }

        $msg = new InMsg($ty, $payload);
        return [$msg, $len];
    }
}

class Policy {
    public int $site;
    public string $species;
    /** @var 'cull'|'conserve' */
    public string $ty;
    public ?int $id = null;
    /** @var 'pending'|'exists'|'deleted' */
    public string $state = 'pending';

    public function __construct(int $site, string $species, string $ty) {
        $this->site = $site;
        $this->species = $species;
        $this->ty = $ty;
    }
    public function set_id(int $id, Peer $as_conn): void {
        if ($this->state === 'exists') return;
        $this->id = $id;
        if ($this->state === 'deleted') {
            $this->send_delete($as_conn);
        } elseif ($this->state === 'pending') {
            $this->state = 'exists';
        }
    }
    public function send_delete(Peer $as_conn): void {
        if ($this->id === null) return;
        $m = new OutMsg(TY_DELETE_POLICY);
        $m->add_u32($this->id);
        $m->send($as_conn);
    }
    public function delete(Peer $as_conn): void {
        if ($this->state === 'pending') {
            $this->state = 'deleted';
        } elseif ($this->state === 'exists') {
            $this->state = 'deleted';
            $this->send_delete($as_conn);
        }
    }
}

class Peer {
    public $sock;
    public Server $server;
    public string $role;   // 'client' or 'as'
    public bool $gotHello = false;

    public ?int $site = null; // only for AS peers
    private string $rbuf = "";
    private string $wbuf = "";

    public bool $wantClose = false;   // <-- NEW: defer close until write buffer flushes

    public function __construct(Server $srv, $sock, string $role) {
        $this->server = $srv;
        $this->sock = $sock;
        $this->role = $role;
        stream_set_blocking($this->sock, false);
    }
    public function enqueueWrite(string $b): void { $this->wbuf .= $b; }
    public function hasWrite(): bool { return $this->wbuf !== ""; }

    public function debug(string $s): void { fwrite(STDERR, "[DBG] {$s}\n"); }
    public function warn(string $s): void  { fwrite(STDERR, "[WRN] {$s}\n"); }

    public function sendHello(): void {
        $m = new OutMsg(TY_HELLO);
        $m->add_str("pestcontrol");
        $m->add_u32(1);
        $m->send($this);
    }

    public function close(): void {
        if ($this->sock === null) return;
        $this->server->removePeer($this);
        @fclose($this->sock);
        $this->sock = null;
    }

    public function onReadable(): void {
        $chunk = @fread($this->sock, 65536);
        if ($chunk === "" || $chunk === false) {
            // Client closed or error
            $this->close();
            return;
        }
        $this->rbuf .= $chunk;

        // Parse messages in a loop
        while (true) {
            try {
                [$msg, $consumed] = InMsg::tryParse($this->rbuf);
            } catch (ProtoError $e) {
                // *** IMPORTANT FIX: send Error, then defer close until write buffer is flushed
                $this->warn("protocol error: " . $e->getMessage());
                $m = new OutMsg(TY_ERROR);
                $m->add_str($e->getMessage());
                $m->send($this);
                $this->wantClose = true;  // defer
                return;                    // exit read loop; writer will flush then close
            }
            if ($msg === null) break;

            $this->debug("<- " . hex_repr(substr($this->rbuf, 0, $consumed)));
            $this->rbuf = substr($this->rbuf, $consumed);

            try {
                if ($msg->ty === TY_HELLO) {
                    $protocol = $msg->get_str();
                    $version  = $msg->get_u32();
                    $msg->check_end();
                    $this->debug("<- Hello { protocol: " . var_export($protocol,true) . ", version: $version }");
                    if ($protocol !== "pestcontrol" || $version !== 1) {
                        throw new ProtoError("unexpected protocol or version");
                    }
                    $this->gotHello = true;
                } else {
                    if (!$this->gotHello) throw new ProtoError("did not get Hello");
                    if ($msg->ty === TY_ERROR) {
                        $err = $msg->get_str();
                        $msg->check_end();
                        $this->debug("<- Error { message: " . var_export($err,true) . " }");
                    } elseif ($msg->ty === TY_OK) {
                        $msg->check_end();
                    } else {
                        if ($this->role === 'client') {
                            $this->server->client_handler($this, $msg);
                        } else { // 'as'
                            $this->server->as_handler($this, $msg);
                        }
                    }
                }
            } catch (ProtoError $e) {
                // For in-message errors, send Error but keep connection alive (spec tests expect that)
                $this->warn("protocol error: " . $e->getMessage());
                $m = new OutMsg(TY_ERROR);
                $m->add_str($e->getMessage());
                $m->send($this);
                // continue parsing subsequent messages if present
            } catch (Throwable $t) {
                $this->warn("fatal: " . $t->getMessage());
                $this->wantClose = true;
                return;
            }
        }
    }

    public function onWritable(): void {
        if ($this->wbuf === "") {
            if ($this->wantClose) { $this->close(); }   // close after flush
            return;
        }
        $n = @fwrite($this->sock, $this->wbuf);
        if ($n === false) { $this->close(); return; }
        if ($n > 0) $this->wbuf = substr($this->wbuf, $n);
        if ($this->wbuf === "" && $this->wantClose) { $this->close(); }  // flush done -> close
    }
}

class Server {
    /** @var resource */
    private $lsock;
    /** @var array<int,Peer> */
    private array $peers = [];

    // === State mirrors Python ===

    /** site => array species => [min, max] */
    private array $target_pops = [];
    /** site => bool (waiting for target pops) */
    private array $waiting_targets = [];
    /** site => Peer (AS connection) */
    private array $as_conns = [];
    /** site => list of pending site visits [ ['peer'=>Peer,'populations'=>array] ] */
    private array $pending_visits = [];
    /** site => list<Policy> pending (awaiting id) */
    private array $pending_policies = [];
    /** (site|species) key => Policy  */
    private array $policies = []; // key: "{$site}\0{$species}"

    public function __construct($lsock) {
        $this->lsock = $lsock;
        stream_set_blocking($this->lsock, false);
    }

    public function run(): void {
        while (true) {
            $r = [$this->lsock];
            $w = [];
            $e = null;

            foreach ($this->peers as $p) {
                if ($p->sock === null) continue;
                $r[] = $p->sock;
                if ($p->hasWrite()) $w[] = $p->sock;
            }

            $num = @stream_select($r, $w, $e, 1, 0);
            if ($num === false) continue;

            foreach ($r as $sock) {
                if ($sock === $this->lsock) {
                    $cs = @stream_socket_accept($this->lsock, 0);
                    if ($cs) {
                        $peer = new Peer($this, $cs, 'client');
                        $this->registerPeer($peer);
                        $peer->sendHello();
                    }
                } else {
                    $peer = $this->peerBySock($sock);
                    if ($peer) $peer->onReadable();
                }
            }
            foreach ($w as $sock) {
                $peer = $this->peerBySock($sock);
                if ($peer) $peer->onWritable();
            }
        }
    }

    private function keyFor(int $site, string $species): string {
        return $site . "\0" . $species;
    }

    private function peerBySock($sock): ?Peer {
        $id = (int)$sock;
        return $this->peers[$id] ?? null;
    }
    public function registerPeer(Peer $p): void {
        $this->peers[(int)$p->sock] = $p;
    }
    public function removePeer(Peer $p): void {
        unset($this->peers[(int)$p->sock]);
        // Cleanup if it's an AS connection
        if ($p->role === 'as' && $p->site !== null) {
            unset($this->as_conns[$p->site]);
            // pending waits for that site should be retried by reconnecting upon next visit
        }
    }

    // === Authority (AS) handling ===

    public function as_handler(Peer $peer, InMsg $msg): void {
        if ($msg->ty === TY_TARGET_POPS) {
            $site2   = $msg->get_u32();
            $pop_cnt = $msg->get_u32();
            $targets = [];
            for ($i=0; $i<$pop_cnt; $i++) {
                $species = $msg->get_str();
                $minp    = $msg->get_u32();
                $maxp    = $msg->get_u32();
                if (isset($targets[$species]) && $targets[$species] !== [$minp, $maxp]) {
                    throw new ProtoError("conflicting target for species '$species'");
                }
                $targets[$species] = [$minp, $maxp];
            }
            $msg->check_end();
            $peer->debug("<- TargetPopulations { site: $site2, populations: " . var_export($targets,true) . " }");
            if ($peer->site !== $site2) {
                throw new ProtoError("authority site mismatch");
            }
            $this->target_pops[$site2] = $targets;
            $this->waiting_targets[$site2] = false;

            // Process any pending site visits for this site now that we have targets:
            if (!empty($this->pending_visits[$site2])) {
                foreach ($this->pending_visits[$site2] as $pending) {
                    $this->processVisit($pending['peer'], $site2, $pending['populations']);
                }
                $this->pending_visits[$site2] = [];
            }
        } elseif ($msg->ty === TY_POLICY_RESULT) {
            $policy_id = $msg->get_u32();
            $msg->check_end();

            $site = $peer->site ?? -1;
            if ($site < 0) return;

            if (empty($this->pending_policies[$site])) {
                // Unexpected but ignore politely
                return;
            }
            $pol = array_shift($this->pending_policies[$site]);
            if ($pol instanceof Policy) {
                $pol->set_id($policy_id, $peer);
            }
        } else {
            throw new ProtoError(sprintf("unexpected message type %02x", $msg->ty));
        }
    }

    private function dialAuthorityForSite(int $site): Peer {
        if (isset($this->as_conns[$site])) {
            return $this->as_conns[$site];
        }
        $addr = sprintf("tcp://%s:%d", AS_HOST, AS_PORT);
        $sock = @stream_socket_client($addr, $errno, $errstr, 10, STREAM_CLIENT_CONNECT);
        if (!$sock) {
            throw new ProtoError("failed to connect AS: $errstr");
        }
        stream_set_blocking($sock, false);
        $peer = new Peer($this, $sock, 'as');
        $peer->site = $site;
        $this->registerPeer($peer);
        // hello
        $peer->sendHello();

        // DialAuthority
        $m = new OutMsg(TY_DIAL_AUTH);
        $m->add_u32($site);
        $m->send($peer);

        $this->as_conns[$site] = $peer;
        $this->waiting_targets[$site] = true;
        return $peer;
    }

    private function getAuthorityForSite(int $site): Peer {
        if (isset($this->target_pops[$site])) {
            return $this->as_conns[$site] ?? $this->dialAuthorityForSite($site);
        }
        // not yet fetched -> ensure connection & request sent
        return $this->as_conns[$site] ?? $this->dialAuthorityForSite($site);
    }

    // === Client handling ===

    public function client_handler(Peer $peer, InMsg $msg): void {
        if ($msg->ty !== TY_SITE_VISIT) {
            throw new ProtoError(sprintf("unexpected message type %02x", $msg->ty));
        }
        $site    = $msg->get_u32();
        $pop_cnt = $msg->get_u32();
        $pops    = [];
        for ($i=0; $i<$pop_cnt; $i++) {
            $species = $msg->get_str();
            $count   = $msg->get_u32();
            if (isset($pops[$species]) && $pops[$species] !== $count) {
                throw new ProtoError("conflicting counts for species '$species'");
            }
            $pops[$species] = $count;
        }
        $msg->check_end();
        $peer->debug("<- SiteVisit { site: $site, populations: " . var_export($pops, true) . " }");

        // Ensure AS data:
        try {
            $as_conn = $this->getAuthorityForSite($site);
        } catch (ProtoError $e) {
            $peer->warn("AS dial failed: ".$e->getMessage());
            $m = new OutMsg(TY_ERROR);
            $m->add_str($e->getMessage());
            $m->send($peer);
            return;
        }

        if (!isset($this->target_pops[$site])) {
            // Queue the visit to process after targets arrive.
            $this->pending_visits[$site][] = ['peer' => $peer, 'populations' => $pops];
            return;
        }

        $this->processVisit($peer, $site, $pops);
    }

    private function processVisit(Peer $clientPeer, int $site, array $populations): void {
        $targets = $this->target_pops[$site] ?? [];
        $as_conn = $this->as_conns[$site] ?? null;
        if (!$as_conn) return; // lost AS conn; wait for next visit to re-dial

        foreach ($targets as $species => [$min_pop, $max_pop]) {
            $pop = $populations[$species] ?? 0;
            $need_policy = null;
            if ($pop < $min_pop) $need_policy = 'conserve';
            if ($pop > $max_pop) $need_policy = 'cull';

            $key = $this->keyFor($site, $species);
            $cur_policy = isset($this->policies[$key]) ? $this->policies[$key]->ty : null;
            if ($need_policy === $cur_policy) continue;

            // If a different/obsolete policy exists, delete it
            if (isset($this->policies[$key])) {
                $policy = $this->policies[$key];
                unset($this->policies[$key]);
                $policy->delete($as_conn);
            }

            if ($need_policy !== null) {
                $m = new OutMsg(TY_CREATE_POLICY);
                $m->add_str($species);
                $m->add_u8($need_policy === 'cull' ? ACT_CULL : ACT_CONSERVE);
                $m->send($as_conn);

                $policy = new Policy($site, $species, $need_policy);
                $this->policies[$key] = $policy;
                $this->pending_policies[$site][] = $policy;
            }
        }
    }
}

// ---- Main ----
$port = isset($argv[1]) ? intval($argv[1]) : 0;
$listen = @stream_socket_server("tcp://0.0.0.0:$port", $errno, $errstr);
if (!$listen) {
    fwrite(STDERR, "Failed to bind: $errstr\n");
    exit(1);
}
$meta = stream_socket_get_name($listen, false);
fwrite(STDERR, "Listening on $meta\n");

$server = new Server($listen);
$server->run();
