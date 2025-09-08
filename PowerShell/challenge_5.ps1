# mob5.ps1 — ProtoHackers Challenge 5 (Mob-in-the-Middle) — PowerShell

class MobProxy {
    [System.Net.Sockets.TcpListener] $Listener
    [string]      $UpstreamHost = 'chat.protohackers.com'
    [int]         $UpstreamPort = 16963
    [string]      $TonyAddress  = '7YWHMfk9JZe0LM0g1ZauHuiSxhI'
    [hashtable]   $Connections  = @{}
    [int]         $ConnIdCounter = 0

    MobProxy([int]$Port) {
        $this.Listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, $Port)
        $this.Listener.Start()
        Write-Host ("Mob proxy listening on port {0}..." -f $Port)
    }

    [void]Run() {
        while ($true) {
            if ($this.Listener.Pending()) {
                $client = $this.Listener.AcceptTcpClient()
                $this.HandleNewClient($client)
            }
            $this.ProcessConnections()
            Start-Sleep -Milliseconds 1
        }
    }

    [void]HandleNewClient([System.Net.Sockets.TcpClient]$Client) {
        try { $Client.NoDelay = $true } catch {}

        $up = [System.Net.Sockets.TcpClient]::new()
        try {
            $up.Connect($this.UpstreamHost, $this.UpstreamPort)
            $up.NoDelay = $true
        } catch {
            Write-Host ("Upstream connect failed: {0}" -f $_.Exception.Message)
            try { $Client.Close() } catch {}
            return
        }

        $id = $this.ConnIdCounter++
        $this.Connections[$id] = @{
            Client             = $Client
            Upstream           = $up
            ClientBuf          = ''
            UpstreamBuf        = ''
            ClientReadClosed   = $false
            UpstreamReadClosed = $false
        }
        Write-Host ("Connection {0} established" -f $id)
    }

    [void]ProcessConnections() {
        if ($this.Connections.Count -eq 0) { return }
        $ids = @($this.Connections.Keys)
        foreach ($id in $ids) {
            if (-not $this.Connections.ContainsKey($id)) { continue }
            $conn = $this.Connections[$id]

            # If either TcpClient is gone, close
            if (-not $conn.Client -or -not $conn.Upstream) {
                $this.CloseConnection($id)
                continue
            }

            $this.PumpSide($id, $true)   # client -> upstream
            $this.PumpSide($id, $false)  # upstream -> client

            if ($conn.ClientReadClosed -and $conn.UpstreamReadClosed) {
                $this.CloseConnection($id)
            }
        }
    }

    [void]PumpSide([int]$id, [bool]$fromClient) {
        if (-not $this.Connections.ContainsKey($id)) { return }
        $conn = $this.Connections[$id]

        $srcTcp    = if ($fromClient) { $conn.Client } else { $conn.Upstream }
        $dstTcp    = if ($fromClient) { $conn.Upstream } else { $conn.Client }
        if (-not $srcTcp -or -not $dstTcp) { $this.CloseConnection($id); return }

        $srcSock   = $srcTcp.Client
        $dstSock   = $dstTcp.Client
        if (-not $srcSock -or -not $dstSock) { $this.CloseConnection($id); return }

        $bufKey    = if ($fromClient) { 'ClientBuf' } else { 'UpstreamBuf' }
        $closedKey = if ($fromClient) { 'ClientReadClosed' } else { 'UpstreamReadClosed' }

        if ($conn[$closedKey]) { return }

        # Ready to read? Poll detects readable or EOF; disambiguate with Available.
        if (-not $srcSock.Poll(0, [System.Net.Sockets.SelectMode]::SelectRead)) { return }

        if ($srcSock.Available -eq 0) {
            # EOF / FIN
            $conn[$closedKey] = $true
            $side = "upstream"; if ($fromClient) { $side = "client" }
            Write-Host ("{0} EOF on conn {1}" -f $side, $id)

            # Flush any partial line (unterminated) to dst if possible
            $partial = $conn[$bufKey]
            if ($partial -ne '') {
                $forward = $this.ReplaceBoguscoinAddresses($partial)
                $bytes = [System.Text.Encoding]::UTF8.GetBytes($forward)
                try {
                    $dstStream = $dstTcp.GetStream()
                    $dstStream.Write($bytes, 0, $bytes.Length)
                } catch {
                    Write-Host ("Flush-on-EOF write failed on conn {0}: {1}" -f $id, $_.Exception.Message)
                    $this.CloseConnection($id)
                    return
                }
                $conn[$bufKey] = ''
            }

            # Half-close opposite send so it can still receive remaining broadcasts
            try { $dstSock.Shutdown([System.Net.Sockets.SocketShutdown]::Send) } catch {}
            return
        }

        # Drain pending bytes non-blocking
        $tmp = New-Object byte[] 4096
        while ($srcSock.Available -gt 0) {
            $toRead = [Math]::Min($tmp.Length, $srcSock.Available)
            $n = 0
            try {
                $n = $srcTcp.GetStream().Read($tmp, 0, $toRead)
            } catch {
                Write-Host ("Read error on conn {0}: {1}" -f $id, $_.Exception.Message)
                $this.CloseConnection($id)
                return
            }
            if ($n -le 0) { break }
            $conn[$bufKey] += [System.Text.Encoding]::UTF8.GetString($tmp, 0, $n)
        }

        $this.ProcessBuffer($id, $fromClient)
    }

    [void]ProcessBuffer([int]$id, [bool]$fromClient) {
        if (-not $this.Connections.ContainsKey($id)) { return }
        $conn = $this.Connections[$id]

        $srcKey = if ($fromClient) { 'ClientBuf' } else { 'UpstreamBuf' }
        # Delay GetStream() until we actually need to write a line
        while (($pos = $conn[$srcKey].IndexOf("`n")) -ne -1) {
            $raw = $conn[$srcKey].Substring(0, $pos + 1)   # includes \n
            $conn[$srcKey] = $conn[$srcKey].Substring($pos + 1)

            $newline = "`n"
            $body = $raw.TrimEnd("`n")
            if ($body.EndsWith("`r")) {
                $body = $body.Substring(0, $body.Length - 1)
                $newline = "`r`n"
            }

            $modified = $this.ReplaceBoguscoinAddresses($body) + $newline
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($modified)

            try {
                $dstTcp = if ($fromClient) { $conn.Upstream } else { $conn.Client }
                if (-not $dstTcp) { $this.CloseConnection($id); return }
                $dstStream = $dstTcp.GetStream()
                $dstStream.Write($bytes, 0, $bytes.Length)
            } catch {
                Write-Host ("Write error on conn {0}: {1}" -f $id, $_.Exception.Message)
                $this.CloseConnection($id)
                return
            }

            $dir = 'Server -> Client'; if ($fromClient) { $dir = 'Client -> Server' }
            Write-Host ("{0} ({1}): {2}" -f $dir, $id, $body)
        }
    }

    [void]CloseConnection([int]$id) {
        if (-not $this.Connections.ContainsKey($id)) { return }
        $c = $this.Connections[$id]
        try { try { $c.Client.Client.Shutdown([System.Net.Sockets.SocketShutdown]::Both) } catch {} } catch {}
        try { try { $c.Upstream.Client.Shutdown([System.Net.Sockets.SocketShutdown]::Both) } catch {} } catch {}
        try { $c.Client.Close() } catch {}
        try { $c.Upstream.Close() } catch {}
        $this.Connections.Remove($id) | Out-Null
        Write-Host ("Connection {0} closed" -f $id)
    }

    [string]ReplaceBoguscoinAddresses([string]$Message) {
        # .NET-safe boundary handling; keep left boundary group
        $pattern = '(^|\s)(7[a-zA-Z0-9]{25,34})(?=\s|$)'
        $e = {
            param([System.Text.RegularExpressions.Match]$m)
            $prefix  = $m.Groups[1].Value
            $address = $m.Groups[2].Value
            if ($address.Length -ge 26 -and $address.Length -le 35 -and $address -cmatch '^7[a-zA-Z0-9]+$') {
                Write-Host ("Replacing Boguscoin address: {0} -> {1}" -f $address, $this.TonyAddress)
                return $prefix + $this.TonyAddress
            }
            return $prefix + $address
        }
        return [System.Text.RegularExpressions.Regex]::Replace($Message, $pattern, $e)
    }
}

# ---------------- Main ----------------

$portStr = $env:PORT
if ([string]::IsNullOrWhiteSpace($portStr)) { $portStr = '40000' }
$port = [int]$portStr

Write-Host "Starting Mob in the Middle proxy..."
Write-Host ("Listening on port: {0}" -f $port)
Write-Host ("Upstream: {0}:{1}" -f 'chat.protohackers.com', 16963)
Write-Host "Tony's address: 7YWHMfk9JZe0LM0g1ZauHuiSxhI`n"

$proxy = [MobProxy]::new($port)
$proxy.Run()
