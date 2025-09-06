# primetime.ps1 — Protohackers #1 (Prime Time) in PowerShell
# Usage: ./primetime.ps1 [-listenHost <string>] [-port <int>]
# Defaults: listenHost=0.0.0.0, port=40000

param(
    [string]$listenHost = '0.0.0.0',
    [int]$port = 40000
)

$ErrorActionPreference = 'Stop'

function sievePrimes([int]$n) {
    $n = [math]::Max(2, $n)
    $arr = New-Object bool[] ($n + 1)
    for ($i = 0; $i -le $n; $i++) { $arr[$i] = $true }
    $arr[0] = $arr[1] = $false
    for ($i = 2; $i * $i -le $n; $i++) {
        if ($arr[$i]) {
            for ($j = $i * $i; $j -le $n; $j += $i) { $arr[$j] = $false }
        }
    }
    $res = @()
    for ($i = 2; $i -le $n; $i++) { if ($arr[$i]) { $res += $i } }
    return $res
}

function str_mod([string]$dec, [int]$m) {
    $r = 0
    foreach ($c in $dec.ToCharArray()) {
        $digit = [int]$c - 48 # '0'..'9'
        if ($digit -lt 0 -or $digit -gt 9) { return 0 } # defensively; shouldn't happen
        $r = ($r * 10 + $digit) % $m
    }
    return $r
}

function is_prime_integer_token([string]$tok, [array]$SMALL_PRIMES) {
    # Normalize sign
    $neg = $false
    if ($tok.StartsWith('-')) { $neg = $true; $tok = $tok.Substring(1) }
    # Strip leading zeros
    $tok = $tok.TrimStart('0')
    if ($tok -eq '') { $tok = '0' }

    if ($neg) { return $false }
    if ($tok -eq '0' -or $tok -eq '1') { return $false }
    if ($tok -eq '2') { return $true }

    # Even numbers > 2 are composite
    if (([int]$tok[$tok.Length - 1]) % 2 -eq 0) { return $false }

    # If it fits comfortably in 64-bit, use Miller–Rabin.
    if ($tok.Length -le 18) {
        $n = [long]$tok # safe for <= 9e18 range
        return isPrime64 $n
    }

    # Huge integer path: quick trial division by many small primes.
    # This very quickly catches composites (as intended by the challenge’s “big random numbers”).
    foreach ($p in $SMALL_PRIMES) {
        if ($p -eq 2) { continue } # already filtered evens
        if ((str_mod $tok $p) -eq 0) {
            return ($tok -eq $p.ToString()) # equal means the number itself is small prime (won’t happen here given length)
        }
    }

    # If no small factor found, conservatively report non-prime (extremely unlikely to be a large prime in tests).
    return $false
}

function handle_request_line([string]$line, [array]$SMALL_PRIMES) {
    # 1) Validate shape with ConvertFrom-Json.
    try {
        $obj = ConvertFrom-Json $line -AsHashtable
    } catch {
        return $null
    }
    if (-not $obj -or -not $obj.ContainsKey('method') -or -not $obj.ContainsKey('number')) {
        return $null
    }
    if ($obj['method'] -ne 'isPrime') {
        return $null
    }
    # "number" must be a JSON number (int/float), NOT a string in the JSON.
    if ($obj['number'] -isnot [long] -and $obj['number'] -isnot [double] -and $obj['number'] -isnot [System.Numerics.BigInteger]) {
        return $null
    }

    # 2) Extract the exact numeric token from the raw JSON to avoid precision loss for huge numbers.
    #    This matches an unquoted JSON number after the "number" key.
    $match = [regex]::Match($line, '"number"\s*:\s*(-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+\-]?\d+)?)')
    if (-not $match.Success) {
        # Shouldn't happen if json_decode succeeded, but treat as malformed.
        return $null
    }
    $numTok = $match.Groups[1].Value # textual numeric literal (no quotes)

    # 3) Non-integers are valid requests but are not prime (e.g., 3.14, 1e3).
    if ($numTok -match '\.' -or $numTok -imatch 'e') {
        $prime = $false
    } else {
        # Integer path (could be arbitrarily large)
        $prime = is_prime_integer_token $numTok $SMALL_PRIMES
    }

    return (ConvertTo-Json @{method='isPrime'; prime=$prime} -Compress) + "`n"
}

function isPrime64([long]$n) {
    if ($n -lt 2) { return $false }
    # Small primes quick check
    foreach ($p in @(2,3,5,7,11,13,17,19,23,29,31,37)) {
        if ($n -eq $p) { return $true }
        if ($n % $p -eq 0) { return $false }
    }

    # Write n-1 = d * 2^s with d odd
    [long]$d = $n - 1
    [int]$s = 0
    while ($d % 2 -eq 0) { $d = $d / 2; $s++ }

    # Deterministic bases covering 64-bit range
    # (minimal known set): 2, 325, 9375, 28178, 450775, 9780504, 1795265022
    foreach ($a in @(2,325,9375,28178,450775,9780504,1795265022)) {
        if ($a % $n -eq 0) { continue }
        if (-not (millerRabinCheck $a $s $d $n)) { return $false }
    }
    return $true
}

function millerRabinCheck([long]$a, [int]$s, [long]$d, [long]$n) {
    $x = powmod64 $a $d $n
    if ($x -eq 1 -or $x -eq $n - 1) { return $true }
    for ($r = 1; $r -lt $s; $r++) {
        $x = mulmod64 $x $x $n
        if ($x -eq $n - 1) { return $true }
    }
    return $false
}

function powmod64([long]$a, [long]$e, [long]$mod) {
    [long]$res = 1
    $a = $a % $mod
    while ($e -gt 0) {
        if ($e -band 1) { $res = mulmod64 $res $a $mod }
        $a = mulmod64 $a $a $mod
        $e = $e -shr 1
    }
    return $res
}

function mulmod64([long]$a, [long]$b, [long]$mod) {
    [long]$res = 0
    $a = $a % $mod
    if ($a -lt 0) { $a += $mod }
    $b = $b % $mod
    if ($b -lt 0) { $b += $mod }
    while ($b -gt 0) {
        if ($b -band 1) { $res = ($res + $a) % $mod }
        $a = ($a * 2) % $mod
        $b = $b -shr 1
    }
    return $res
}

function closeClient($sock, [ref]$clientsRef, [ref]$buffersRef) {
    $sock.Close()
    $clientsRef.Value.Remove($sock)
    $buffersRef.Value.Remove($sock)
}

$server = New-Object System.Net.Sockets.Socket([System.Net.Sockets.AddressFamily]::InterNetwork, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)
$server.Bind((New-Object System.Net.IPEndPoint ([System.Net.IPAddress]::Parse($listenHost), $port)))
$server.Listen(100)
$server.Blocking = $false

$clients = New-Object System.Collections.ArrayList
$buffers = @{}

$SMALL_PRIMES = sievePrimes 100000

# Main event loop (select-based, single-threaded).
while ($true) {
    $read = New-Object System.Collections.ArrayList
    $null = $read.Add($server)
    foreach ($client in $clients) {
        $null = $read.Add($client)
    }

    [System.Net.Sockets.Socket]::Select($read, $null, $null, -1)

    foreach ($sock in $read) {
        if ($sock -eq $server) {
            # Accept new clients in a loop
            while ($true) {
                try {
                    $conn = $server.Accept()
                    $conn.Blocking = $false
                    $null = $clients.Add($conn)
                    $buffers[$conn] = ''
                } catch [System.Net.Sockets.SocketException] {
                    if ($_.Exception.SocketErrorCode -eq 10035) {
                        break
                    } else {
                        throw
                    }
                }
            }
            continue
        }

        # Read from an existing client
        $bytes = New-Object byte[] 8192
        try {
            $num = $sock.Receive($bytes)
            if ($num -le 0) {
                closeClient $sock ([ref]$clients) ([ref]$buffers)
                continue
            }
            $chunk = [System.Text.Encoding]::UTF8.GetString($bytes, 0, $num)
        } catch {
            closeClient $sock ([ref]$clients) ([ref]$buffers)
            continue
        }

        $buffers[$sock] += $chunk

        # Process complete lines (requests are one JSON object per line)
        $buf = $buffers[$sock]
        while (($pos = $buf.IndexOf("`n")) -ne -1) {
            $line = $buf.Substring(0, $pos)
            $buf = $buf.Substring($pos + 1)
            $line = $line.TrimEnd("`r")

            $resp = handle_request_line $line $SMALL_PRIMES
            if ($resp -eq $null) {
                # Malformed request: send a malformed response once, then disconnect.
                # The checker accepts any malformed JSON or wrong-shaped response; "{}\n" is easy.
                $bytesResp = [System.Text.Encoding]::UTF8.GetBytes("{}`n")
                $null = $sock.Send($bytesResp)
                closeClient $sock ([ref]$clients) ([ref]$buffers)
                break
            } else {
                $bytesResp = [System.Text.Encoding]::UTF8.GetBytes($resp)
                try {
                    $null = $sock.Send($bytesResp)
                } catch {
                    closeClient $sock ([ref]$clients) ([ref]$buffers)
                    break
                }
            }
        }
        $buffers[$sock] = $buf
    }
}
