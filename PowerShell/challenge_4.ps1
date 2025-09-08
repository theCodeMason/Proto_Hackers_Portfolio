function Start-Db {
    param (
        [int]$Port
    )

    # Create UDP client
    $udpClient = New-Object System.Net.Sockets.UdpClient($Port)

    $data = @{}
    $version = "Snapchat for Databases"

    while ($true) {
        try {
            $remoteEP = New-Object System.Net.IPEndPoint([System.Net.IPAddress]::Any, 0)
            $bytes = $udpClient.Receive([ref]$remoteEP)
            $request = [Text.Encoding]::UTF8.GetString($bytes)

            # Find '=' position
            $eqPos = $request.IndexOf('=')

            if ($eqPos -eq -1) {
                # Retrieve operation
                $key = $request
                if ($key -eq "version") {
                    $val = $version
                } else {
                    $val = if ($data.ContainsKey($key)) { $data[$key] } else { "" }
                }
                $response = "$key=$val"
                $bytesOut = [Text.Encoding]::UTF8.GetBytes($response)
                $udpClient.Send($bytesOut, $bytesOut.Length, $remoteEP) | Out-Null
            } else {
                # Insert operation
                $key = $request.Substring(0, $eqPos)
                $value = $request.Substring($eqPos + 1)
                $data[$key] = $value
            }
        } catch {
            # Skip on error
        }
    }

    # Close client (unreachable due to infinite loop)
    $udpClient.Close()
}

# Main execution
$portStr = $env:PORT ?? "40000"
$port = [int]$portStr
Start-Db -Port $port
