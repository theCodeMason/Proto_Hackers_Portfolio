# Optimized TCP Echo Server for ProtoHackers Smoke Test
$port = 40000
$listener = New-Object System.Net.Sockets.TcpListener('0.0.0.0', $port)
$listener.Start()
Write-Host "Echo server listening on port $port..."

try {
    while ($true) {
        # Asynchronously accept client connections
        $clientTask = $listener.AcceptTcpClientAsync()
        $client = $clientTask.GetAwaiter().GetResult()
        Write-Host "New connection from $($client.Client.RemoteEndPoint)"

        # Handle each client in a separate runspace for lightweight concurrency
        $runspace = [PowerShell]::Create()
        $scriptBlock = {
            param($tcpClient)
            try {
                $stream = $tcpClient.GetStream()
                $buffer = New-Object byte[] 8192  # Larger buffer for efficiency
                while ($tcpClient.Connected) {
                    $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
                    if ($bytesRead -eq 0) { break }  # Connection closed by client
                    $stream.Write($buffer, 0, $bytesRead)
                    $stream.Flush()  # Ensure data is sent immediately
                }
            } catch {
                Write-Host "Error in connection from $($tcpClient.Client.RemoteEndPoint): $($_.Exception.Message)"
            } finally {
                $stream.Close()
                $tcpClient.Close()
                Write-Host "Connection closed from $($tcpClient.Client.RemoteEndPoint)"
            }
        }
        $runspace.AddScript($scriptBlock).AddArgument($client) | Out-Null
        $runspace.BeginInvoke() | Out-Null
    }
} catch {
    Write-Host "Server error: $($_.Exception.Message)"
} finally {
    $listener.Stop()
    Write-Host "Server stopped"
}
