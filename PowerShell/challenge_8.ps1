# This is ittermittent depending on how bad your server is ...but it's worked for me to pass the challenge with it! 
# ph8.ps1 — ProtoHackers #8 (PowerShell 7.5.2)
# Non-blocking multi-client server with fast LUT builds and filterable debug logs.

using namespace System
using namespace System.Net
using namespace System.Net.Sockets
using namespace System.Text
using namespace System.Collections.Generic
using namespace System.Diagnostics

$ErrorActionPreference = 'Stop'

# ---------------- Config ----------------
$PORT = if ($env:PORT) { [int]$env:PORT }
        elseif ($env:PROTOHACKERS_PORT) { [int]$env:PROTOHACKERS_PORT }
        else { 40000 }

$IDLE_SECS          = 300
$LINE_MAXLEN        = 1048576  # 1 MiB guard
$SELECT_TIMEOUT_US  = 20000    # 20 ms for responsive multi-client behavior

# Debug controls
$DEBUG_ENABLED = if ($env:DEBUG -and $env:DEBUG -eq '0') { $false } else { $true }
# Categories you can enable via:  DBG=accept,recv,app,send   (comma-separated, case-insensitive)
$DEFAULT_DBG = @('accept','recv','cipher','compile','app','send','close','select')
$DBG_SET = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
if ($DEBUG_ENABLED) {
    if ($env:DBG) {
        foreach ($c in ($env:DBG -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })) { [void]$DBG_SET.Add($c) }
    } else {
        foreach ($c in $DEFAULT_DBG) { [void]$DBG_SET.Add($c) }
    }
}

function D([string]$cat, [string]$msg) {
    if ($DEBUG_ENABLED -and $DBG_SET.Contains($cat)) {
        [Console]::Error.WriteLine("{0:o} DEBUG [{1}] {2}", (Get-Date), $cat, $msg)
    }
}
function Log([string]$msg) {
    [Console]::Error.WriteLine("{0:o} {1}", (Get-Date), $msg)
}
function Get-NowTs {
    [int][Math]::Floor((Get-Date).ToUniversalTime().Subtract([datetime]"1970-01-01").TotalSeconds)
}
function Get-SockId([Socket]$s) { [int64]$s.Handle }
function Get-Remote([Socket]$s) {
    try {
        $ep = [IPEndPoint]$s.RemoteEndPoint
        return "{0}:{1}" -f $ep.Address, $ep.Port
    } catch { return "unknown" }
}

# ---------------- App layer: toy_priority ----------------
function Invoke-ToyPriority([string]$s) {
    # "3x car, 1x doll, 5x train" -> "5x train"
    $items  = $s -split ','
    $bestN  = -1
    $bestNm = ''
    foreach ($item in $items) {
        $t = $item.Trim()
        if ($t -match '^(?<n>\d+)x\s+(?<name>.+)$') {
            $n  = [int]$Matches['n']
            $nm = $Matches['name']
            if ($n -gt $bestN) { $bestN = $n; $bestNm = $nm }
        }
    }
    if ($bestN -lt 0) { return "" }
    "{0}x {1}" -f $bestN, $bestNm
}

# ---------------- Build LUTs (fast, no scriptblocks in hot loop) ----------------
# Ops are descriptors: @{ T='rev' } | @{ T='xorPos' } | @{ T='addPos' } | @{ T='xorN'; N=<byte> } | @{ T='addN'; N=<byte> }
function New-CipherTables([System.Collections.IList]$ops) {
    $enc = New-Object 'object[]' 256
    $dec = New-Object 'object[]' 256
    $isIdentity = $true

    $idRow = New-Object 'byte[]' 256
    for ($i=0; $i -lt 256; $i++) { $idRow[$i] = [byte]$i }

    for ($p=0; $p -lt 256; $p++) {
        $vals = New-Object 'int[]' 256
        for ($i=0; $i -lt 256; $i++) { $vals[$i] = $i }

        foreach ($op in $ops) {
            switch ($op.T) {
                'rev' {
                    for ($i=0; $i -lt 256; $i++) {
                        $b = $vals[$i]; $rev = 0
                        for ($k=0; $k -lt 8; $k++) { $rev = (($rev -shl 1) -bor ($b -band 1)); $b = $b -shr 1 }
                        $vals[$i] = $rev -band 0xFF
                    }
                }
                'xorPos' {
                    $pp = $p -band 0xFF
                    for ($i=0; $i -lt 256; $i++) { $vals[$i] = (($vals[$i] -bxor $pp) -band 0xFF) }
                }
                'addPos' {
                    $pp = $p -band 0xFF
                    for ($i=0; $i -lt 256; $i++) { $vals[$i] = (($vals[$i] + $pp) -band 0xFF) }
                }
                'xorN' {
                    $n = [int]($op.N -band 0xFF)
                    for ($i=0; $i -lt 256; $i++) { $vals[$i] = (($vals[$i] -bxor $n) -band 0xFF) }
                }
                'addN' {
                    $n = [int]($op.N -band 0xFF)
                    for ($i=0; $i -lt 256; $i++) { $vals[$i] = (($vals[$i] + $n) -band 0xFF) }
                }
                default { throw "Unknown op type: $($op.T)" }
            }
        }

        $map = New-Object 'byte[]' 256
        for ($i=0; $i -lt 256; $i++) { $map[$i] = [byte]($vals[$i]) }
        $enc[$p] = $map

        if ($isIdentity) {
            for ($i=0; $i -lt 256; $i++) { if ($map[$i] -ne $idRow[$i]) { $isIdentity = $false; break } }
        }

        $inv = New-Object 'byte[]' 256
        for ($i=0; $i -lt 256; $i++) { $inv[$vals[$i]] = [byte]$i }
        $dec[$p] = $inv
    }

    ,@($enc,$dec,$isIdentity)
}

# ---------------- Per-connection state ----------------
function New-ClientState([Socket]$s) {
    [pscustomobject]@{
        Sock      = $s
        Stage     = 'cipher'
        CState    = $null
        Ops       = [System.Collections.ArrayList]::new()  # list of op descriptors
        Enc       = $null
        Dec       = $null
        RPos      = 0
        WPos      = 0
        InBuf     = [System.Collections.Generic.Queue[byte]]::new()
        OutBuf    = [System.Collections.Generic.List[byte]]::new()
        LineSB    = [System.Text.StringBuilder]::new()
        WantWrite = $false
        Kill      = $false
        LastTs    = Get-NowTs
    }
}

function Close-Client([hashtable]$clients, [int64]$id, [string]$reason="") {
    if (-not $clients.ContainsKey($id)) { return }
    $c = $clients[$id]
    $remote = Get-Remote $c.Sock
    if ($reason -and $DEBUG_ENABLED) { D 'close' ("Close {0} (id={1}): {2}" -f $remote, $id, $reason) }
    try { $c.Sock?.Dispose() } catch {}
    $null = $clients.Remove($id)
}

function Process-ClientState($c) {
    $preOut = $c.OutBuf.Count
    while ($c.InBuf.Count -gt 0) {
        if ($c.Stage -eq 'cipher') {
            $b = $c.InBuf.Dequeue()
            D 'cipher' ("[{0}] byte={1}" -f (Get-Remote $c.Sock), $b)

            if ($c.CState -eq 'xor') {
                $null = $c.Ops.Add(@{ T='xorN'; N=($b -band 0xFF) }); $c.CState = $null
                D 'cipher' ("[{0}] add xor {1}" -f (Get-Remote $c.Sock), $b)
                continue
            }
            if ($c.CState -eq 'add') {
                $null = $c.Ops.Add(@{ T='addN'; N=($b -band 0xFF) }); $c.CState = $null
                D 'cipher' ("[{0}] add add {1}" -f (Get-Remote $c.Sock), $b)
                continue
            }

            switch ($b) {
                0 {
                    D 'compile' ("[{0}] compile LUTs" -f (Get-Remote $c.Sock))
                    $sw = [Stopwatch]::StartNew()
                    $res   = New-CipherTables $c.Ops
                    $sw.Stop()
                    $c.Enc = $res[0]; $c.Dec = $res[1]
                    $isId  = [bool]$res[2]
                    D 'compile' ("[{0}] LUT {1} ms; identity={2}; ops={3}" -f (Get-Remote $c.Sock), $sw.ElapsedMilliseconds, $isId, $c.Ops.Count)
                    if ($isId) { $c.Kill = $true; return }
                    $c.Stage = 'app'
                    D 'cipher' ("[{0}] -> stage=app" -f (Get-Remote $c.Sock))
                }
                1 { $null = $c.Ops.Add(@{ T='rev'    }); D 'cipher' ("[{0}] add reverse" -f (Get-Remote $c.Sock)) }
                2 { $c.CState = 'xor';                   D 'cipher' ("[{0}] expect xor param" -f (Get-Remote $c.Sock)) }
                3 { $null = $c.Ops.Add(@{ T='xorPos' }); D 'cipher' ("[{0}] add xor_pos" -f (Get-Remote $c.Sock)) }
                4 { $c.CState = 'add';                   D 'cipher' ("[{0}] expect add param" -f (Get-Remote $c.Sock)) }
                5 { $null = $c.Ops.Add(@{ T='addPos' }); D 'cipher' ("[{0}] add add_pos" -f (Get-Remote $c.Sock)) }
                default { D 'cipher' ("[{0}] invalid byte {1}" -f (Get-Remote $c.Sock), $b); $c.Kill = $true; return }
            }
            continue
        }

        # ---- app stage ----
        $cb = $c.InBuf.Dequeue()
        $plain = [int]$c.Dec[$c.RPos -band 0xFF][$cb]
        $c.RPos++

        if ($plain -eq 10) {
            $raw = $c.LineSB.ToString()
            D 'app' ("[{0}] line len={1}: {2}" -f (Get-Remote $c.Sock), $raw.Length, $raw)
            $resp = Invoke-ToyPriority $raw
            $null = $c.LineSB.Clear()

            if ($resp.Length -gt 0) {
                $bytes = [Text.Encoding]::ASCII.GetBytes($resp)
                for ($i=0; $i -lt $bytes.Length; $i++) {
                    $mapped = $c.Enc[$c.WPos -band 0xFF][ $bytes[$i] ]
                    $c.OutBuf.Add($mapped) | Out-Null
                    $c.WPos++
                }
                D 'app' ("[{0}] queued resp={1} bytes" -f (Get-Remote $c.Sock), $bytes.Length)
            } else {
                D 'app' ("[{0}] queued resp=0 (empty)" -f (Get-Remote $c.Sock))
            }
            # newline
            $mappedLF = $c.Enc[$c.WPos -band 0xFF][10]
            $c.OutBuf.Add($mappedLF) | Out-Null
            $c.WPos++
            $c.WantWrite = $true
        }
        else {
            if ($c.LineSB.Length -ge $LINE_MAXLEN) { $c.Kill = $true; return }
            $null = $c.LineSB.Append([char]$plain)
        }
    }
    if ($c.OutBuf.Count -gt $preOut) { $c.WantWrite = $true }
}

# ---------------- Event loop server ----------------
$listen = [Socket]::new([AddressFamily]::InterNetwork, [SocketType]::Stream, [ProtocolType]::Tcp)
$listen.SetSocketOption([SocketOptionLevel]::Socket, [SocketOptionName]::ReuseAddress, $true)
$listen.SetSocketOption([SocketOptionLevel]::Tcp,    [SocketOptionName]::NoDelay,     $true)
$listen.Bind([IPEndPoint]::new([IPAddress]::Any, $PORT))
$listen.Listen(128)
$listen.Blocking = $false
Log "Open on :$PORT"
D 'tick' ("Debug {0}. Categories: {1}" -f ($(if($DEBUG_ENABLED){'ON'}else{'OFF'})), ($(if($DBG_SET.Count){[string]::Join(',', $DBG_SET)}else{'(none)'})))

$clients = @{}

try {
    while ($true) {
        $readList  = [System.Collections.ArrayList]::new()
        $writeList = [System.Collections.ArrayList]::new()
        $errList   = [System.Collections.ArrayList]::new()

        $null = $readList.Add($listen)
        foreach ($kv in $clients.GetEnumerator()) {
            $null = $readList.Add($kv.Value.Sock)
            if ($kv.Value.OutBuf.Count -gt 0) { $null = $writeList.Add($kv.Value.Sock) }
        }

        try {
            [Socket]::Select($readList, $writeList, $errList, $SELECT_TIMEOUT_US)
        } catch {
            D 'select' "Select() threw; sleeping 5ms"
            Start-Sleep -Milliseconds 5
        }

        #if ($DEBUG_ENABLED -and $DBG_SET.Contains('select')) {
        #    D 'select' ("R={0} W={1}" -f $readList.Count, $writeList.Count)
        #}

        # Accept all pending
        if ($readList.Contains($listen)) {
            while ($true) {
                try { $sock = $listen.Accept() } catch [System.Net.Sockets.SocketException] { break }
                if ($sock -eq $null) { break }
                $sock.Blocking = $false
                # IMPORTANT: also disable Nagle per connection
                $sock.SetSocketOption([SocketOptionLevel]::Tcp, [SocketOptionName]::NoDelay, $true)
                $id = Get-SockId $sock
                $clients[$id] = New-ClientState $sock
                D 'accept' ("id={0} from {1}" -f $id, (Get-Remote $sock))
            }
        }

        # Reads
        foreach ($kv in @($clients.GetEnumerator())) {
            $c = $kv.Value
            if (-not $readList.Contains($c.Sock)) { continue }

            $tmp = New-Object 'byte[]' 8192
            try {
                $n = $c.Sock.Receive($tmp, 0, $tmp.Length, [SocketFlags]::None)
            } catch [System.Net.Sockets.SocketException] {
                if ($_.Exception.ErrorCode -ne 10035) {
                    D 'recv' ("[{0}] recv exception code={1}" -f (Get-Remote $c.Sock), $_.Exception.ErrorCode)
                    $c.Kill = $true
                }
                continue
            }
            if ($n -eq 0) { $c.Kill = $true; D 'recv' ("[{0}] peer closed" -f (Get-Remote $c.Sock)); continue }

            D 'recv' ("[{0}] +{1}B (stage={2})" -f (Get-Remote $c.Sock), $n, $c.Stage)
            for ($i=0; $i -lt $n; $i++) { $c.InBuf.Enqueue($tmp[$i]) }
            $c.LastTs = Get-NowTs
            Process-ClientState $c
        }

        # Writes
        foreach ($kv in @($clients.GetEnumerator())) {
            $c = $kv.Value
            if ($c.OutBuf.Count -eq 0 -or -not $writeList.Contains($c.Sock)) { continue }

            do {
                $toSend = [Math]::Min($c.OutBuf.Count, 8192)
                if ($toSend -le 0) { break }
                $buf = New-Object 'byte[]' $toSend
                for ($i=0; $i -lt $toSend; $i++) { $buf[$i] = $c.OutBuf[$i] }

                try {
                    $sent = $c.Sock.Send($buf, 0, $buf.Length, [SocketFlags]::None)
                } catch [System.Net.Sockets.SocketException] {
                    if ($_.Exception.ErrorCode -ne 10035) {
                        D 'send' ("[{0}] send exception code={1}" -f (Get-Remote $c.Sock), $_.Exception.ErrorCode)
                        $c.Kill = $true
                    }
                    break
                }

                if ($sent -gt 0) {
                    $c.OutBuf.RemoveRange(0, $sent)
                    $c.LastTs = Get-NowTs
                    D 'send' ("[{0}] -{1}B, remain {2}" -f (Get-Remote $c.Sock), $sent, $c.OutBuf.Count)
                }
            } while ($sent -gt 0)

            if ($c.OutBuf.Count -eq 0) { $c.WantWrite = $false }
        }

        # Cleanup
        $now = Get-NowTs
        foreach ($id in @($clients.Keys)) {
            $c = $clients[$id]
            if ($c.Kill) { Close-Client $clients $id "kill flag"; continue }
            if ($now - $c.LastTs -gt $IDLE_SECS) { Close-Client $clients $id "idle timeout" }
        }
    }
}
finally {
    try { $listen?.Dispose() } catch {}
    foreach ($id in @($clients.Keys)) { Close-Client $clients $id "shutdown" }
}
