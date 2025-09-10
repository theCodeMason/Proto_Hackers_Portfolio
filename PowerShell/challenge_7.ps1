# Configuration
$RETRANS_TIMEOUT = 3     # seconds
$EXPIRY_TIMEOUT = 60     # seconds
$CHUNK_SIZE = 400        # bytes per /data/ chunk
$MAX_MSG_LEN = 1000      # safety cap, like Python
$BindAddress = if ($args.Length -ge 1) { $args[0] } elseif ($env:UDP_HOST) { $env:UDP_HOST } else { '0.0.0.0' }
$PORT = if ($args.Length -ge 2) { [int]$args[1] } elseif ($env:UDP_PORT) { [int]$env:UDP_PORT } else { 40000 }

# Utilities
function Now-Ts {
    return [math]::Floor(((Get-Date).ToUniversalTime() - (Get-Date "1970-01-01")).TotalSeconds)
}

function Log-Info {
    param($msg)
    [Console]::Error.WriteLine("[$(Get-Date -Format o)] INFO: $msg")
}

function Log-Warn {
    param($msg)
    [Console]::Error.WriteLine("[$(Get-Date -Format o)] WARN: $msg")
}

function Log-Debug {
    param($msg)
    # Toggle verbosity here if desired
    # [Console]::Error.WriteLine("[$(Get-Date -Format o)] DEBUG: $msg")
}

function Encode-Segment {
    param([string]$s)
    # Escape \ as \\ and / as \/
    # Use string.Replace to avoid regex subtleties of -replace
    return ($s.Replace('\', '\\')).Replace('/', '\/')
}

function Parse-SlashEscapedFields {
    param([string]$msg)
    if ([string]::IsNullOrEmpty($msg) -or $msg[0] -ne '/') {
        throw 'not starting with /'
    }

    $fields = @('')
    $escaped = $false
    $len = $msg.Length

    for ($i = 1; $i -lt $len; $i++) {
        $c = $msg[$i]
        if ($escaped) {
            $fields[-1] += $c
            $escaped = $false
        } else {
            if ($c -eq '\') {
                $escaped = $true
            } elseif ($c -eq '/') {
                $fields += ''
            } else {
                $fields[-1] += $c
            }
        }
    }

    if ($escaped) {
        throw 'unfinished escape'
    }

    if ($fields[-1] -ne '') {
        # ensure message is "finished" (ends with '/')
        throw 'unfinished message'
    }

    $fields = $fields[0..($fields.Length - 2)]  # remove final empty field
    if ($fields.Length -eq 0) {
        throw 'no message type'
    }
    return $fields
}

function To-Int32NonNeg {
    param([string]$s)
    try {
        $v = [int]$s
    } catch {
        throw 'invalid integer argument'
    }
    if ($v -lt 0) { throw 'negative integer' }
    if ($v -ge 2147483648) { throw 'integer too large' }
    return $v
}

# Sessions
$sessions = @{}

class Session {
    [int]$id
    [string]$address
    [int]$port
    [int]$received = 0
    [int]$sent = 0
    [int]$acknowledged = 0
    [string]$unacknowledged = ''
    [Nullable[double]]$nextRetransAt = $null
    [string]$buffer = ''
    [int]$lastSeen
    [System.Net.Sockets.UdpClient]$server
    [string]$key

    Session([int]$id, [string]$address, [int]$port, [System.Net.Sockets.UdpClient]$server) {
        $this.id = $id
        $this.address = $address
        $this.port = $port
        $this.lastSeen = [int](Now-Ts)
        $this.server = $server
        $this.key = [Session]::KeyFor($id, $address, $port)
    }

    static [string] KeyFor([int]$id, [string]$address, [int]$port) {
        # The backtick before ':' prevents any odd parsing if $address contains $
        return "$id|$address`:$port"
    }

    [void] Touch() {
        $this.lastSeen = [int](Now-Ts)
    }

    [void] Log([string]$msg) {
        Log-Info "[$($this.id)] $msg"
    }

    [void] Debug([string]$msg) {
        Log-Debug "[$($this.id)] $msg"
    }

    [void] SendAck() {
        $out = "/ack/$($this.id)/$($this.received)/"
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($out)
        [void]$this.server.Send($bytes, $bytes.Length, $this.address, $this.port)
    }

    [void] Close() {
        $out = "/close/$($this.id)/"
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($out)
        [void]$this.server.Send($bytes, $bytes.Length, $this.address, $this.port)
        $script:sessions.Remove($this.key) | Out-Null
        $this.Debug('closed')
    }

    [void] AppReceive([string]$data, [int]$chunkSize) {
        $this.Log("app received: $(ConvertTo-Json -Compress $data)")
        $this.buffer += $data

        while ($true) {
            $pos = $this.buffer.IndexOf("`n")
            if ($pos -eq -1) { break }

            $line = $this.buffer.Substring(0, $pos)

            # Reverse line safely (allow empty line)
            if ($line.Length -gt 0) {
                $replyCore = ($line[($line.Length-1)..0] -join '')
            } else {
                $replyCore = ''
            }
            $reply = $replyCore + "`n"
            $this.AppSend($reply, $chunkSize)

            $this.buffer = $this.buffer.Substring($pos + 1)
        }
    }

    [void] AppSend([string]$data, [int]$chunkSize) {
        $this.Log("app sent: $(ConvertTo-Json -Compress $data)")

        $len = $data.Length
        for ($i = 0; $i -lt $len; $i += $chunkSize) {
            $chunkLen = [math]::Min($chunkSize, $len - $i)
            $chunk = $data.Substring($i, $chunkLen)
            $encoded = Encode-Segment $chunk
            $this.Debug("sending chunk: $(ConvertTo-Json -Compress $encoded)")

            $out = "/data/$($this.id)/$($this.sent)/$encoded/"
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($out)
            [void]$this.server.Send($bytes, $bytes.Length, $this.address, $this.port)
            $this.sent += $chunkLen
            $this.unacknowledged += $chunk

            if ($null -eq $this.nextRetransAt) {
                # schedule soon; main loop adds RETRANS_TIMEOUT
                $this.nextRetransAt = Now-Ts
            }
        }
    }

    [void] OnData([int]$position, [string]$data, [int]$chunkSize) {
        if ($position -eq $this.received) {
            $this.AppReceive($data, $chunkSize)
            $this.received += $data.Length
        }
        $this.SendAck()
    }

    [void] OnAck([int]$length) {
        if ($length -le $this.acknowledged) { return }

        if ($length -gt $this.sent) {
            # protocol violation -> close
            $this.Close()
            return
        }

        $newly = $length - $this.acknowledged
        if ($newly -gt 0 -and $newly -le $this.unacknowledged.Length) {
            $this.unacknowledged = $this.unacknowledged.Substring($newly)
        } else {
            $this.unacknowledged = ''
        }
        $this.acknowledged = $length

        if ($this.acknowledged -lt $this.sent) {
            # respond to ack with retransmission of still-unacked tail
            $encoded = Encode-Segment $this.unacknowledged
            $this.Debug("responding to ack with retransmission: $(ConvertTo-Json -Compress $encoded)")
            $out = "/data/$($this.id)/$($this.acknowledged)/$encoded/"
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($out)
            [void]$this.server.Send($bytes, $bytes.Length, $this.address, $this.port)
            if ($null -eq $this.nextRetransAt) {
                $this.nextRetransAt = Now-Ts
            }
        } else {
            # fully acked
            $this.nextRetransAt = $null
        }
    }

    [void] MaybeRetransmit([double]$now, [int]$retransTimeout, [int]$chunkSize) {
        if ($this.acknowledged -ge $this.sent) {
            $this.nextRetransAt = $null
            return
        }
        if ($null -eq $this.nextRetransAt -or $now -lt ($this.nextRetransAt + $retransTimeout)) {
            return
        }

        $pendingBytes = $this.sent - $this.acknowledged
        $data = $this.unacknowledged
        $this.Log("retransmitting last $pendingBytes bytes")

        $len = $data.Length
        for ($i = 0; $i -lt $len; $i += $chunkSize) {
            $chunkLen = [math]::Min($chunkSize, $len - $i)
            $chunk = $data.Substring($i, $chunkLen)
            $encoded = Encode-Segment $chunk
            $offset = $this.acknowledged + $i
            $out = "/data/$($this.id)/$offset/$encoded/"
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($out)
            [void]$this.server.Send($bytes, $bytes.Length, $this.address, $this.port)
        }

        $this.nextRetransAt = $now  # schedule next cycle later
    }
}

# UDP I/O
$ipEndpoint = New-Object System.Net.IPEndPoint ([System.Net.IPAddress]::Parse($BindAddress), $PORT)
$server = New-Object System.Net.Sockets.UdpClient $ipEndpoint
$server.Client.ReceiveTimeout = 1000  # 1 second for periodic checks

Log-Info "UDP server listening on $($BindAddress):$($PORT)"

# Message handler loop
while ($true) {
    $remoteEP = New-Object System.Net.IPEndPoint ([System.Net.IPAddress]::Any, 0)
    $address = $null
    $port = $null
    try {
        $bytes = $server.Receive([ref]$remoteEP)
        $data = [System.Text.Encoding]::UTF8.GetString($bytes)
        $address = $remoteEP.Address.ToString()
        $port = $remoteEP.Port

        if ($data.Length -ge $MAX_MSG_LEN) { throw 'too long' }
        $fields = Parse-SlashEscapedFields $data
        Log-Debug "received: $(ConvertTo-Json -Compress $fields -Depth 1)"

        $msg_ty = $fields[0]

        function Ensure-Argc([int]$n) {
            if ($fields.Length -ne ($n + 1)) {
                throw "'$msg_ty' expects $n arguments"
            }
        }

        if ($msg_ty -eq 'connect') {
            Ensure-Argc 1
            $sessionId = To-Int32NonNeg $fields[1]
            $key = [Session]::KeyFor($sessionId, $address, $port)
            if (-not $sessions.ContainsKey($key)) {
                $sessions[$key] = [Session]::new($sessionId, $address, $port, $server)
            }
            $sessions[$key].Touch()
            # ack with 0
            $out = "/ack/$sessionId/0/"
            $bytesOut = [System.Text.Encoding]::UTF8.GetBytes($out)
            [void]$server.Send($bytesOut, $bytesOut.Length, $address, $port)

        } elseif ($msg_ty -eq 'data') {
            Ensure-Argc 3
            $sessionId = To-Int32NonNeg $fields[1]
            $position = To-Int32NonNeg $fields[2]
            $dataField = $fields[3]

            $key = [Session]::KeyFor($sessionId, $address, $port)
            if (-not $sessions.ContainsKey($key)) {
                # unknown session -> close
                $out = "/close/$sessionId/"
                $bytesOut = [System.Text.Encoding]::UTF8.GetBytes($out)
                [void]$server.Send($bytesOut, $bytesOut.Length, $address, $port)
                continue
            }
            $sess = $sessions[$key]
            $sess.Touch()
            $sess.OnData($position, $dataField, $CHUNK_SIZE)

        } elseif ($msg_ty -eq 'ack') {
            Ensure-Argc 2
            $sessionId = To-Int32NonNeg $fields[1]
            $length = To-Int32NonNeg $fields[2]

            $key = [Session]::KeyFor($sessionId, $address, $port)
            if (-not $sessions.ContainsKey($key)) {
                $out = "/close/$sessionId/"
                $bytesOut = [System.Text.Encoding]::UTF8.GetBytes($out)
                [void]$server.Send($bytesOut, $bytesOut.Length, $address, $port)
                continue
            }
            $sess = $sessions[$key]
            $sess.Touch()
            $sess.OnAck($length)

        } elseif ($msg_ty -eq 'close') {
            Ensure-Argc 1
            $sessionId = To-Int32NonNeg $fields[1]

            $key = [Session]::KeyFor($sessionId, $address, $port)
            if ($sessions.ContainsKey($key)) {
                $sessions[$key].Close()
            } else {
                $out = "/close/$sessionId/"
                $bytesOut = [System.Text.Encoding]::UTF8.GetBytes($out)
                [void]$server.Send($bytesOut, $bytesOut.Length, $address, $port)
            }
        } else {
            throw 'unknown message type'
        }
    } catch [System.Net.Sockets.SocketException] {
        if ($_.Exception.SocketErrorCode -eq [System.Net.Sockets.SocketError]::TimedOut) {
            # Periodic tasks: retransmit + expire
            $now = Now-Ts
            foreach ($sess in @($sessions.Values)) {  # Copy to array to avoid modification during enumeration
                $sess.MaybeRetransmit($now, $RETRANS_TIMEOUT, $CHUNK_SIZE)
                if (($now - $sess.lastSeen) -gt $EXPIRY_TIMEOUT) {
                    Log-Info "[$($sess.id)] session expired"
                    $sess.Close()
                }
            }
        } else {
            Log-Warn "server error: $($_.Exception.Message)"
            break
        }
    } catch {
        if ($null -ne $address -and $null -ne $port) {
            Log-Warn "invalid message from ${address}:${port}: $($_.Exception.Message)"
        } else {
            Log-Warn "error: $($_.Exception.Message)"
        }
    }
}

$server.Close()
