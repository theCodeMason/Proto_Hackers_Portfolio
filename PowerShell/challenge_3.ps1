function Handle-Disconnect {
    param (
        $Key,
        [ref]$Clients
    )
    $clientObj = $Clients.Value | Where-Object { $_.Key -eq $Key }
    if ($null -eq $clientObj) { return }
    if ($clientObj.State -eq 'joined') {
        # Announce leave
        $leaveMsg = "* $($clientObj.Name) has left the room`n"
        $bytes = [Text.Encoding]::UTF8.GetBytes($leaveMsg)
        foreach ($other in $Clients.Value) {
            if ($other.Key -ne $Key -and $other.State -eq 'joined') {
                try {
                    # Debug: Log leave message
                    if ($other.Name -eq 'watchman') {
                        Write-Host "Sending to watchman: $leaveMsg"
                    }
                    $other.Stream.Write($bytes, 0, $bytes.Length)
                } catch {
                    Handle-Disconnect -Key $other.Key -Clients $Clients
                }
            }
        }
    }
    Write-Host "Disconnecting client $($clientObj.Key) ($($clientObj.Name))"
    $clientObj.Client.Close()
    $Clients.Value = @($Clients.Value | Where-Object { $_.Key -ne $Key })
}

function Process-Buffer {
    param (
        $Key,
        [ref]$Clients
    )
    $clientObj = $Clients.Value | Where-Object { $_.Key -eq $Key }
    if ($null -eq $clientObj) { return }
    while ($clientObj.Buffer.IndexOf("`n") -ne -1) {
        $pos = $clientObj.Buffer.IndexOf("`n")
        $line = $clientObj.Buffer.Substring(0, $pos)
        $clientObj.Buffer = $clientObj.Buffer.Substring($pos + 1)
        $line = $line.TrimEnd("`r")
        Write-Host "Processing buffer for client $($clientObj.Key) ($($clientObj.Name)): $line"

        if ($clientObj.State -eq 'pending') {
            # Validate name
            if ($line -match '^[a-zA-Z0-9]+$') {
                # Check uniqueness (case-insensitive)
                $unique = $true
                foreach ($other in $Clients.Value) {
                    if ($other.State -eq 'joined' -and $other.Name.ToLower() -eq $line.ToLower()) {
                        $unique = $false
                        break
                    }
                }
                if ($unique) {
                    $clientObj.Name = $line
                    # Send user list to new user (before setting state to 'joined')
                    $userList = Get-UserList -Clients $Clients.Value -ExcludeKey $Key
                    $listMsg = "* The room contains:" + $(if ($userList) { " $userList" } else { "" }) + "`n"
                    $bytes = [Text.Encoding]::UTF8.GetBytes($listMsg)
                    # Debug: Log message sent to client
                    if ($clientObj.Name -eq 'watchman') {
                        Write-Host "Sending to watchman: $listMsg"
                    }
                    $clientObj.Stream.Write($bytes, 0, $bytes.Length)
                    # Now set state to 'joined'
                    $clientObj.State = 'joined'
                    Write-Host "Client $($clientObj.Key) joined as $($clientObj.Name)"
                    # Announce join to others
                    $joinMsg = "* $($clientObj.Name) has entered the room`n"
                    $bytes = [Text.Encoding]::UTF8.GetBytes($joinMsg)
                    foreach ($other in $Clients.Value) {
                        if ($other.Key -ne $Key -and $other.State -eq 'joined') {
                            try {
                                # Debug: Log join message
                                if ($other.Name -eq 'watchman') {
                                    Write-Host "Sending to watchman: $joinMsg"
                                }
                                $other.Stream.Write($bytes, 0, $bytes.Length)
                            } catch {
                                Handle-Disconnect -Key $other.Key -Clients $Clients
                            }
                        }
                    }
                } else {
                    Handle-Disconnect -Key $Key -Clients $Clients
                    return
                }
            } else {
                Handle-Disconnect -Key $Key -Clients $Clients
                return
            }
        } elseif ($clientObj.State -eq 'joined') {
            # Chat message, limit to 1000 chars
            if ($line.Length -gt 1000) {
                $line = $line.Substring(0, 1000)
            }
            if ($line.Length -gt 0) {
                $chatMsg = "[$($clientObj.Name)] $line`n"
                $bytes = [Text.Encoding]::UTF8.GetBytes($chatMsg)
                foreach ($other in $Clients.Value) {
                    if ($other.Key -ne $Key -and $other.State -eq 'joined') {
                        try {
                            # Debug: Log chat message
                            if ($other.Name -eq 'watchman') {
                                Write-Host "Sending to watchman: $chatMsg"
                            }
                            $other.Stream.Write($bytes, 0, $bytes.Length)
                        } catch {
                            Handle-Disconnect -Key $other.Key -Clients $Clients
                        }
                    }
                }
            }
        }
    }
}

function Get-UserList {
    param (
        $Clients,
        $ExcludeKey
    )
    $names = @()
    foreach ($c in $Clients) {
        if ($c.Key -ne $ExcludeKey -and $c.State -eq 'joined') {
            $names += $c.Name
        }
    }
    $names = $names | Sort-Object
    return $names -join ', '
}

# Create the TCP listener
$listener = New-Object System.Net.Sockets.TcpListener('0.0.0.0', 40000)
$listener.Start()

Write-Host "Server listening on port 40000"

# Clients: array of PSCustomObjects with properties: Client, State, Buffer, Name
$clients = @()
$clientId = 0

while ($true) {
    # Check for new connections
    if ($listener.Pending()) {
        $tcpClient = $listener.AcceptTcpClient()
        $stream = $tcpClient.GetStream()
        $stream.ReadTimeout = 1
        $key = $clientId++
        $clientObj = [PSCustomObject]@{
            Key     = $key
            Client  = $tcpClient
            Stream  = $stream
            State   = 'pending'
            Buffer  = ''
            Name    = ''
        }
        $clients += $clientObj
        # Send welcome message
        $welcome = "Welcome to budgetchat! What shall I call you?`n"
        $bytes = [Text.Encoding]::UTF8.GetBytes($welcome)
        # Debug: Log welcome message
        Write-Host "Sending welcome to client ${key}: $welcome"
        $stream.Write($bytes, 0, $bytes.Length)
    }

    # Process existing clients
    for ($i = $clients.Count - 1; $i -ge 0; $i--) {
        $clientObj = $clients[$i]
        $stream = $clientObj.Stream
        $bytes = New-Object byte[] 2048
        try {
            $numBytes = $stream.Read($bytes, 0, 2048)
        } catch {
            if ($_.Exception.Message -match "timed out") {
                # No data available
                continue
            } else {
                # Other error, disconnect
                Write-Host "Error reading from client $($clientObj.Key): $($_.Exception.Message)"
                Handle-Disconnect -Key $clientObj.Key -Clients ([ref]$clients)
                continue
            }
        }
        if ($numBytes -eq 0) {
            # Connection closed
            Write-Host "Client $($clientObj.Key) ($($clientObj.Name)) disconnected"
            Handle-Disconnect -Key $clientObj.Key -Clients ([ref]$clients)
            continue
        }
        $data = [Text.Encoding]::UTF8.GetString($bytes, 0, $numBytes)
        # Debug: Log received data
        Write-Host "Received from client $($clientObj.Key) ($($clientObj.Name)): $data"
        $clientObj.Buffer += $data
        Process-Buffer -Key $clientObj.Key -Clients ([ref]$clients)
    }

    # Small sleep to prevent CPU hogging
    Start-Sleep -Milliseconds 10
}

# Cleanup (though infinite loop, for completeness)
$listener.Stop()
