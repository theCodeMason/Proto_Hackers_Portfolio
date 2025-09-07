# Create the TCP listener
$listener = New-Object System.Net.Sockets.TcpListener('0.0.0.0', 40000)
$listener.Start()

Write-Host "Server listening on port 40000"

# Clients: array of PSCustomObjects
$clients = @()
$clientId = 0

while ($true) {
    # Check for new connections
    if ($listener.Pending()) {
        $tcpClient = $listener.AcceptTcpClient()
        $stream = $tcpClient.GetStream()
        $key = $clientId++
        $clientObj = [PSCustomObject]@{
            Key      = $key
            Client   = $tcpClient
            Stream   = $stream
            Buffer   = New-Object System.Collections.ArrayList
            Database = New-Object System.Collections.SortedList
        }
        $clients += $clientObj
    }

    # Process existing clients
    for ($i = $clients.Count - 1; $i -ge 0; $i--) {
        $clientObj = $clients[$i]
        $stream = $clientObj.Stream

        if ($stream.DataAvailable) {
            $bytes = New-Object byte[] 2048
            $numBytes = $stream.Read($bytes, 0, 2048)
            if ($numBytes -eq 0) {
                Handle-Disconnect -Key $clientObj.Key -Clients ([ref]$clients)
                continue
            }
            [void]$clientObj.Buffer.AddRange($bytes[0..($numBytes - 1)])
        }

        # Process messages from buffer
        while ($clientObj.Buffer.Count -ge 9) {
            $msgBytes = $clientObj.Buffer.GetRange(0, 9).ToArray()
            [void]$clientObj.Buffer.RemoveRange(0, 9)

            $type = [char]$msgBytes[0]

            $val1Bytes = $msgBytes[1..4]
            if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($val1Bytes) }
            $val1 = [BitConverter]::ToInt32($val1Bytes, 0)

            $val2Bytes = $msgBytes[5..8]
            if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($val2Bytes) }
            $val2 = [BitConverter]::ToInt32($val2Bytes, 0)

            if ($type -eq 'I') {
                $clientObj.Database[$val1] = $val2
            } elseif ($type -eq 'Q') {
                $sum = 0L
                $count = 0
                for ($j = 0; $j -lt $clientObj.Database.Count; $j++) {
                    $key = $clientObj.Database.GetKey($j)
                    if ($key -ge $val1 -and $key -le $val2) {
                        $sum += [long]$clientObj.Database.GetByIndex($j)
                        $count++
                    }
                }
                if ($count -eq 0) {
                    $avg = 0
                } else {
                    $avg = [int]($sum / $count)
                }
                $avgBytes = [BitConverter]::GetBytes($avg)
                if ([BitConverter]::IsLittleEndian) { [Array]::Reverse($avgBytes) }
                try {
                    $stream.Write($avgBytes, 0, 4)
                } catch {
                    Handle-Disconnect -Key $clientObj.Key -Clients ([ref]$clients)
                    break
                }
            } else {
                # Invalid type, disconnect
                Handle-Disconnect -Key $clientObj.Key -Clients ([ref]$clients)
                break
            }
        }
    }

    # Small sleep to prevent CPU hogging
    Start-Sleep -Milliseconds 10
}

# Cleanup (though infinite loop, for completeness)
$listener.Stop()

function Handle-Disconnect {
    param (
        $Key,
        [ref]$Clients
    )
    $clientObj = $Clients.Value | Where-Object { $_.Key -eq $Key }
    if ($null -eq $clientObj) { return }
    try {
        $clientObj.Client.Close()
    } catch {}
    $Clients.Value = $Clients.Value | Where-Object { $_.Key -ne $Key }
}
