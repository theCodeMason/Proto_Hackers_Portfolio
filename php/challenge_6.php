<?php

function u16_to_bytes($n) {
    return chr($n >> 8) . chr($n & 0xFF);
}

function bytes_to_u16($bytes) {
    return (ord($bytes[0]) << 8) | ord($bytes[1]);
}

function u32_to_bytes($n) {
    return chr($n >> 24) . chr(($n >> 16) & 0xFF) . chr(($n >> 8) & 0xFF) . chr($n & 0xFF);
}

function bytes_to_u32($bytes) {
    return (ord($bytes[0]) << 24) | (ord($bytes[1]) << 16) | (ord($bytes[2]) << 8) | ord($bytes[3]);
}

function str_to_bytes($s) {
    $len = strlen($s);
    if ($len > 255) {
        throw new Exception("String too long for single-byte length");
    }
    return chr($len) . $s;
}

function make_error($msg) {
    return chr(0x10) . str_to_bytes($msg);
}

function make_ticket($plate, $road, $p1, $p2, $speed) {
    list($m1, $t1) = $p1;
    list($m2, $t2) = $p2;
    if ($t1 > $t2) {
        list($m1, $t1, $m2, $t2) = [$m2, $t2, $m1, $t1];
    }
    return chr(0x21) .
           str_to_bytes($plate) .
           u16_to_bytes($road) .
           u16_to_bytes($m1) .
           u32_to_bytes($t1) .
           u16_to_bytes($m2) .
           u32_to_bytes($t2) .
           u16_to_bytes(round($speed * 100));
}

function make_heartbeat() {
    return chr(0x41);
}

function decode_bytes($buffer) {
    if (strlen($buffer) < 1) {
        return ['status' => 'incomplete'];
    }
    $type = ord($buffer[0]);
    if (!in_array($type, [0x20, 0x40, 0x80, 0x81])) {
        return ['status' => 'error'];
    }
    $bs = substr($buffer, 1);
    $len = strlen($bs);
    switch ($type) {
        case 0x20:
            if ($len < 1) {
                return ['status' => 'incomplete'];
            }
            $l = ord($bs[0]);
            $expected = 1 + 1 + $l + 4; // type + l + plate + timestamp
            if (strlen($buffer) < $expected) {
                return ['status' => 'incomplete'];
            }
            $msg = [
                'type' => 'plate',
                'plate' => substr($bs, 1, $l),
                'timestamp' => bytes_to_u32(substr($bs, 1 + $l, 4))
            ];
            return ['status' => 'ok', 'msg' => $msg, 'consumed' => $expected];
        case 0x40:
            $expected = 1 + 4;
            if (strlen($buffer) < $expected) {
                return ['status' => 'incomplete'];
            }
            $msg = [
                'type' => 'want-heartbeat',
                'interval' => bytes_to_u32($bs)
            ];
            return ['status' => 'ok', 'msg' => $msg, 'consumed' => $expected];
        case 0x80:
            $expected = 1 + 6;
            if (strlen($buffer) < $expected) {
                return ['status' => 'incomplete'];
            }
            $msg = [
                'type' => 'i-am-camera',
                'road' => bytes_to_u16(substr($bs, 0, 2)),
                'mile' => bytes_to_u16(substr($bs, 2, 2)),
                'limit' => bytes_to_u16(substr($bs, 4, 2))
            ];
            return ['status' => 'ok', 'msg' => $msg, 'consumed' => $expected];
        case 0x81:
            if ($len < 1) {
                return ['status' => 'incomplete'];
            }
            $num = ord($bs[0]);
            $expected = 1 + 1 + $num * 2;
            if (strlen($buffer) < $expected) {
                return ['status' => 'incomplete'];
            }
            $roads = [];
            for ($i = 0; $i < $num; $i++) {
                $roads[] = bytes_to_u16(substr($bs, 1 + $i * 2, 2));
            }
            $msg = [
                'type' => 'i-am-dispatcher',
                'roads' => $roads
            ];
            return ['status' => 'ok', 'msg' => $msg, 'consumed' => $expected];
    }
    return ['status' => 'incomplete'];
}

function day($ts) {
    return floor($ts / 86400);
}

function avg_speed($p1, $p2) {
    list($m1, $t1) = $p1;
    list($m2, $t2) = $p2;
    return round(abs(($m2 - $m1) / ($t2 - $t1)) * 3600);
}

function scrub_logbook($entries, $days_covered) {
    $days_set = array_flip($days_covered);
    return array_filter($entries, function ($e) use ($days_set) {
        return !isset($days_set[day($e[1])]);
    });
}

$server = stream_socket_server('tcp://0.0.0.0:40000', $errno, $errstr);
if (!$server) {
    die("Failed to create server: $errstr ($errno)\n");
}
stream_set_blocking($server, false);

$connections = [];
$buffers = [];
$states = [];
$heartbeats = [];
$pending = array_fill(0, 65536, []);
$logbook = [];
$ticketed = [];

echo "open!\n";

while (true) {
    $reads = [$server];
    foreach ($connections as $conn) {
        $reads[] = $conn;
    }
    $writes = null;
    $except = null;
    $tv_sec = 0;
    $tv_usec = 100000; // 0.1s for heartbeat checks
    if (stream_select($reads, $writes, $except, $tv_sec, $tv_usec) === false) {
        break;
    }

    if (in_array($server, $reads)) {
        $conn = stream_socket_accept($server, 0);
        if ($conn) {
            stream_set_blocking($conn, false);
            $key = (int)$conn;
            $connections[$key] = $conn;
            $buffers[$key] = '';
            $states[$key] = [];
            $heartbeats[$key] = null;
            echo "..accept\n";
        }
    }

    foreach ($reads as $read) {
        if ($read === $server) {
            continue;
        }
        $data = fread($read, 1024);
        if ($data === false || strlen($data) === 0) {
            close_conn($read);
            continue;
        }
        $key = (int)$read;
        $buffers[$key] .= $data;
        while (true) {
            $dec = decode_bytes($buffers[$key]);
            if ($dec['status'] === 'incomplete') {
                break;
            }
            if ($dec['status'] === 'error') {
                fwrite($read, make_error("nope"));
                close_conn($read);
                break;
            }
            $msg = $dec['msg'];
            $buffers[$key] = substr($buffers[$key], $dec['consumed']);
            handle_msg($read, $msg);
        }
    }

    $now = microtime(true);
    foreach ($heartbeats as $key => $hb) {
        if ($hb !== null && $hb['interval'] > 0 && $now >= $hb['next']) {
            if (isset($connections[$key])) {
                $conn = $connections[$key];
                fwrite($conn, make_heartbeat());
                fflush($conn);
            }
            $heartbeats[$key]['next'] += $hb['interval'] / 10;
        }
    }

    foreach ($states as $key => $state) {
        if (isset($state['role']) && $state['role'] === 'dispatcher' && isset($connections[$key])) {
            $conn = $connections[$key];
            foreach ($state['roads'] as $road) {
                while (!empty($GLOBALS['pending'][$road])) {
                    $msg = array_shift($GLOBALS['pending'][$road]);
                    fwrite($conn, $msg);
                    fflush($conn);
                }
            }
        }
    }
}

function close_conn($conn) {
    $key = (int)$conn;
    @fclose($conn);
    unset($GLOBALS['connections'][$key], $GLOBALS['buffers'][$key], $GLOBALS['states'][$key], $GLOBALS['heartbeats'][$key]);
    echo "..close\n";
}

function handle_msg($conn, $msg) {
    $key = (int)$conn;
    $state = &$GLOBALS['states'][$key];
    switch ($msg['type']) {
        case 'i-am-camera':
            if (isset($state['role'])) {
                fwrite($conn, make_error("can't switch roles"));
                fflush($conn);
                close_conn($conn);
                return;
            }
            $state['role'] = 'camera';
            $state['camera'] = [
                'road' => $msg['road'],
                'mile' => $msg['mile'],
                'limit' => $msg['limit']
            ];
            break;
        case 'i-am-dispatcher':
            if (isset($state['role'])) {
                fwrite($conn, make_error("can't switch roles"));
                fflush($conn);
                close_conn($conn);
                return;
            }
            $state['role'] = 'dispatcher';
            $state['roads'] = $msg['roads'];
            break;
        case 'plate':
            if (!isset($state['role']) || $state['role'] !== 'camera') {
                fwrite($conn, make_error("not a camera"));
                fflush($conn);
                close_conn($conn);
                return;
            }
            $camera = $state['camera'];
            $plate = $msg['plate'];
            $ts = $msg['timestamp'];
            $road = $camera['road'];
            $mile = $camera['mile'];
            $limit = $camera['limit'];
            $day_ts = day($ts);
            if (isset($GLOBALS['ticketed'][$plate][$day_ts])) {
                break;
            }
            if (!isset($GLOBALS['logbook'][$plate])) {
                $GLOBALS['logbook'][$plate] = [];
            }
            if (!isset($GLOBALS['logbook'][$plate][$road])) {
                $GLOBALS['logbook'][$plate][$road] = [];
            }
            $poss = [];
            foreach ($GLOBALS['logbook'][$plate][$road] as $entry) {
                $avg = avg_speed([$mile, $ts], $entry);
                if ($avg > $limit) {
                    $poss[] = $entry;
                }
            }
            if (empty($poss)) {
                $GLOBALS['logbook'][$plate][$road][] = [$mile, $ts];
                break;
            }
            usort($poss, function ($a, $b) use ($ts) {
                return abs($ts - $a[1]) <=> abs($ts - $b[1]);
            });
            list($mile2, $ts2) = $poss[0];
            $speed = avg_speed([$mile, $ts], [$mile2, $ts2]);
            $min_ts = min($ts, $ts2);
            $max_ts = max($ts, $ts2);
            $min_day = day($min_ts);
            $max_day = day($max_ts);
            $days_covered = range($min_day, $max_day);
            if (!isset($GLOBALS['ticketed'][$plate])) {
                $GLOBALS['ticketed'][$plate] = [];
            }
            foreach ($days_covered as $d) {
                $GLOBALS['ticketed'][$plate][$d] = true;
            }
            $GLOBALS['logbook'][$plate][$road] = scrub_logbook(
                $GLOBALS['logbook'][$plate][$road],
                array_keys($GLOBALS['ticketed'][$plate])
            );
            $ticket_msg = make_ticket($plate, $road, [$mile, $ts], [$mile2, $ts2], $speed);
            $GLOBALS['pending'][$road][] = $ticket_msg;
            break;
        case 'want-heartbeat':
            if (isset($GLOBALS['heartbeats'][$key])) {
                fwrite($conn, make_error("heartbeat already set"));
                fflush($conn);
                close_conn($conn);
                return;
            }
            $interval = $msg['interval'];
            $GLOBALS['heartbeats'][$key] = [
                'interval' => $interval,
                'next' => microtime(true) + $interval / 10
            ];
            break;
    }
}
