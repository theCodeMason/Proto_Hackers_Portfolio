$ErrorActionPreference = 'Continue'

$BindHost = '0.0.0.0'
$PORT = 40000

# ---------- Utilities ----------
function Make-Error {
    param([string]$msg)
    @{status = 'error'; error = $msg} | ConvertTo-Json -Compress | ForEach-Object { "$_`n" }
}

function Make-StatusOk {
    @{status = 'ok'} | ConvertTo-Json -Compress | ForEach-Object { "$_`n" }
}

function Make-StatusOkId {
    param([long]$id)
    @{status = 'ok'; id = $id} | ConvertTo-Json -Compress | ForEach-Object { "$_`n" }
}

function Make-StatusOkJob {
    param([string]$q, [long]$id, $job, $pri)
    @{status = 'ok'; id = $id; job = $job; pri = $pri; queue = $q} | ConvertTo-Json -Compress -Depth 10 | ForEach-Object { "$_`n" }
}

function Make-StatusNoJob {
    @{status = 'no-job'} | ConvertTo-Json -Compress | ForEach-Object { "$_`n" }
}

# ---------- Client model ----------
class Client {
    [System.Net.Sockets.TcpClient]$sock
    [System.Net.Sockets.NetworkStream]$stream
    [string]$buf = ''
    [hashtable]$owned = @{}
    [hashtable]$watching = @{}

    Client([System.Net.Sockets.TcpClient]$sock) {
        $this.sock = $sock
        $this.stream = $this.sock.GetStream()
    }

    [string] Id() {
        return $this.GetHashCode().ToString()
    }

    [void] Write([string]$s) {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($s)
        try {
            $this.stream.Write($bytes, 0, $bytes.Length)
        } catch {}
    }
}

# ---------- Priority Queue for jobs ----------
class PriorityQueue {
    [hashtable]$ids = @{}  # id -> pri (double)
    [System.Collections.Generic.SortedSet[double]]$negPris = [System.Collections.Generic.SortedSet[double]]::new()
    [hashtable]$priSets = @{}  # negpri -> HashSet<long>

    [void] Add([long]$id, [double]$pri) {
        $this.ids[$id] = $pri
        $neg = -$pri
        if (-not $this.priSets.ContainsKey($neg)) {
            $this.priSets[$neg] = [System.Collections.Generic.HashSet[long]]::new()
        }
        $this.priSets[$neg].Add($id) > $null
        $this.negPris.Add($neg) > $null
    }

    [psobject] Peek() {
        if ($this.negPris.Count -eq 0) { return $null }
        $maxNeg = $this.negPris.Min
        $set = $this.priSets[$maxNeg]
        if ($set.Count -eq 0) { return $null }
        $enum = $set.GetEnumerator()
        $enum.MoveNext() > $null
        $id = $enum.Current
        return @{id = $id; pri = -$maxNeg}
    }

    [psobject] Pop() {
        $peek = $this.Peek()
        if ($peek -eq $null) { return $null }
        $id = $peek.id
        $pri = $peek.pri
        $neg = -$pri
        $this.priSets[$neg].Remove($id) > $null
        $this.ids.Remove($id)
        if ($this.priSets[$neg].Count -eq 0) {
            $this.priSets.Remove($neg)
            $this.negPris.Remove($neg) > $null
        }
        return @{id = $id; pri = $pri}
    }

    [void] Remove([long]$id) {
        if (-not $this.ids.ContainsKey($id)) { return }
        $pri = $this.ids[$id]
        $neg = -$pri
        if ($this.priSets.ContainsKey($neg)) {
            $this.priSets[$neg].Remove($id) > $null
            if ($this.priSets[$neg].Count -eq 0) {
                $this.priSets.Remove($neg)
                $this.negPris.Remove($neg) > $null
            }
        }
        $this.ids.Remove($id)
    }

    [int] Count() {
        return $this.ids.Count
    }
}

# ---------- Server state ----------
$nextId = 0L
$queues = @{}   # Map queue => PriorityQueue
$jobs = @{}     # Map jobId => [queue, job, pri]
$watchers = @{} # Map queue => (clientId => Client)
$clients = @{}  # Map clientId => Client
$clientHandles = @{}  # Map socket.Handle => Client

# ---------- Queue helpers ----------
function Pick-FromQueues {
    param([array]$qs)
    $candidates = @()
    foreach ($q in $qs) {
        if (-not $queues.ContainsKey($q)) { continue }
        $pq = $queues[$q]
        if ($pq.Count() -eq 0) { continue }
        $peek = $pq.Peek()
        $candidates += , @($peek.id, $peek.pri)
    }
    if ($candidates.Count -eq 0) { return $null }
    $candidates = $candidates | Sort-Object { $_[1] } -Descending
    return $candidates[0][0]
}

function Pop-IdFromQueue {
    param([string]$q, [long]$id)
    if ($queues.ContainsKey($q)) {
        $queues[$q].Remove($id)
        if ($queues[$q].Count() -eq 0) { $queues.Remove($q) }
    }
}

function First-WatcherForQueue {
    param([string]$q)
    if (-not $watchers.ContainsKey($q) -or $watchers[$q].Count -eq 0) { return $null }
    $cid = $watchers[$q].Keys | Select-Object -First 1
    $client = $watchers[$q][$cid]
    $watchers[$q].Remove($cid)
    if ($watchers[$q].Count -eq 0) { $watchers.Remove($q) }
    $client.watching.Remove($q)
    return $client
}

function Add-Watcher {
    param([Client]$client, [array]$qs)
    foreach ($q in $qs) {
        if (-not $watchers.ContainsKey($q)) { $watchers[$q] = @{} }
        $watchers[$q][$client.Id()] = $client
        $client.watching[$q] = $true
    }
}

function Remove-ClientFromAllWatchers {
    param([Client]$client)
    foreach ($q in @($watchers.Keys)) {
        if ($watchers.ContainsKey($q)) {
            $watchers[$q].Remove($client.Id())
            if ($watchers[$q].Count -eq 0) { $watchers.Remove($q) }
        }
    }
    $client.watching = @{}
}

# ---------- Job operations ----------
function Op-Put {
    param([Client]$submitter, [string]$q, $job, $pri)
    $id = $script:nextId++
    $jobs[$id] = @($q, $job, $pri)
    $client = First-WatcherForQueue $q
    if ($client) {
        $client.Write((Make-StatusOkJob $q $id $job $pri))
        $client.owned[$id] = $true
        $submitter.Write((Make-StatusOkId $id))
    } else {
        if (-not $queues.ContainsKey($q)) { $queues[$q] = [PriorityQueue]::new() }
        $queues[$q].Add($id, $pri)
        $submitter.Write((Make-StatusOkId $id))
    }
}

function Op-Get {
    param([Client]$client, [array]$qs, [bool]$wait)
    $id = Pick-FromQueues $qs
    if ($id -ne $null) {
        $q, $job, $pri = $jobs[$id]
        Pop-IdFromQueue $q $id
        $client.Write((Make-StatusOkJob $q $id $job $pri))
        $client.owned[$id] = $true
    } else {
        if ($wait) {
            Add-Watcher $client $qs
        } else {
            $client.Write((Make-StatusNoJob))
        }
    }
}

function Op-Abort {
    param([Client]$client, [long]$id)
    if (-not $jobs.ContainsKey($id)) {
        $client.Write((Make-StatusNoJob))
        return
    }
    if (-not $client.owned.ContainsKey($id)) {
        $client.Write((Make-Error 'not owner'))
        return
    }
    $q, $job, $pri = $jobs[$id]
    $w = First-WatcherForQueue $q
    if ($w) {
        $w.Write((Make-StatusOkJob $q $id $job $pri))
        $w.owned[$id] = $true
    } else {
        if (-not $queues.ContainsKey($q)) { $queues[$q] = [PriorityQueue]::new() }
        $queues[$q].Add($id, $pri)
    }
    $client.owned.Remove($id)
    $client.Write((Make-StatusOk))
}

function Op-Delete {
    param([Client]$client, [long]$id)
    if (-not $jobs.ContainsKey($id)) {
        $client.Write((Make-StatusNoJob))
        return
    }
    $q, $job, $pri = $jobs[$id]
    Pop-IdFromQueue $q $id
    $jobs.Remove($id)
    $client.Write((Make-StatusOk))
}

function AbortAllOwnedOnDisconnect {
    param([Client]$client)
    if ($client.owned.Count -eq 0) { return }
    foreach ($id in @($client.owned.Keys)) {
        if (-not $jobs.ContainsKey($id)) { continue }
        $q, $job, $pri = $jobs[$id]
        $w = First-WatcherForQueue $q
        if ($w) {
            $w.Write((Make-StatusOkJob $q $id $job $pri))
            $w.owned[$id] = $true
        } else {
            if (-not $queues.ContainsKey($q)) { $queues[$q] = [PriorityQueue]::new() }
            $queues[$q].Add($id, $pri)
        }
        $client.owned.Remove($id)
    }
}

function Remove-Client {
    param([Client]$client)
    $cid = $client.Id()
    $handle = $client.sock.Client.Handle
    Remove-ClientFromAllWatchers $client
    AbortAllOwnedOnDisconnect $client
    $client.sock.Close()
    $clients.Remove($cid)
    $clientHandles.Remove($handle)
}

# ---------- Networking ----------
$listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Any, $PORT)
try {
    $listener.Start()
} catch {
    Write-Error "Failed to bind "
    exit 1
}
Write-Host "[job-centre] listening on "

while ($true) {
    $readlist = New-Object System.Collections.ArrayList
    $readlist.Add($listener.Server) > $null
    foreach ($c in $clients.Values) {
        $readlist.Add($c.sock.Client) > $null
    }

    [System.Net.Sockets.Socket]::Select($readlist, $null, $null, -1)

    if ($readlist.Contains($listener.Server)) {
        $sock = $listener.AcceptTcpClient()
        $client = [Client]::new($sock)
        $clients[$client.Id()] = $client
        $clientHandles[$sock.Client.Handle] = $client
    }

    foreach ($readySock in $readlist) {
        if ($readySock -eq $listener.Server) { continue }
        $client = $clientHandles[$readySock.Handle]
        if (-not $client) { continue }

        $buffer = New-Object byte[] 8192
        try {
            $read = $client.stream.Read($buffer, 0, 8192)
            if ($read -eq 0) {
                Remove-Client $client
                continue
            }
            $client.buf += [System.Text.Encoding]::UTF8.GetString($buffer, 0, $read)
        } catch {
            Remove-Client $client
            continue
        }

        # Process buffer
        while (($pos = $client.buf.IndexOf("`n")) -ge 0) {
            $line = $client.buf.Substring(0, $pos)
            $client.buf = $client.buf.Substring($pos + 1)
            $line = $line.TrimEnd("`r")
            if ($line -eq '') { $client.Write((Make-Error 'empty line')); continue }

            try {
                $msg = $line | ConvertFrom-Json
            } catch {
                $client.Write((Make-Error 'invalid JSON'))
                continue
            }

            if ($msg.request -eq 'put') {
                $q = $msg.queue
                $pri = $msg.pri
                if (-not ($q -is [string]) -or $q -eq '' -or -not $msg.PSObject.Properties['job'] -or -not ($pri -is [double] -or $pri -is [int] -or $pri -is [int64])) {
                    $client.Write((Make-Error 'bad request'))
                    continue
                }
                Op-Put $client $q $msg.job $pri
            } elseif ($msg.request -eq 'get') {
                $qs = $msg.queues
                $wait = [bool]($msg.wait)
                if (-not ($qs -is [array]) -or $qs.Count -eq 0) {
                    $client.Write((Make-Error 'bad request'))
                    continue
                }
                $qs = $qs | Select-Object -Unique | ForEach-Object { [string]$_ }
                Op-Get $client $qs $wait
            } elseif ($msg.request -eq 'abort') {
                $id = $msg.id
                if (-not ($id -is [int] -or $id -is [int64])) {
                    $client.Write((Make-Error 'bad request'))
                    continue
                }
                Op-Abort $client $id
            } elseif ($msg.request -eq 'delete') {
                $id = $msg.id
                if (-not ($id -is [int] -or $id -is [int64])) {
                    $client.Write((Make-Error 'bad request'))
                    continue
                }
                Op-Delete $client $id
            } else {
                $client.Write((Make-Error 'bad request'))
            }
        }
    }
}
