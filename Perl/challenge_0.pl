#!/usr/bin/perl
use strict;
use warnings;
use IO::Socket::INET;

# Configuration
my $port = 40000;
my $buffer_size = 1024;

# Create TCP server socket
my $server = IO::Socket::INET->new(
    LocalPort => $port,
    Proto     => 'tcp',
    Listen    => 10,
    ReuseAddr => 1,
) or die "Cannot create server socket: $!\n";

print "Server listening on port $port\n";

# Main server loop
while (my $client = $server->accept()) {
    my $peer_addr = $client->peerhost();
    my $peer_port = $client->peerport();
    print "[@{[scalar localtime]}] Connected: $peer_addr:$peer_port\n";

    # Fork a new process to handle the client
    if (fork() == 0) {
        # Child process
        while (1) {
            my $data;
            my $bytes_read = $client->recv($data, $buffer_size);
            if (!defined $bytes_read || length($data) == 0) {
                # Client disconnected
                print "[@{[scalar localtime]}] Disconnected: $peer_addr:$peer_port\n";
                last;
            }
            # Echo data back
            $client->send($data) or do {
                print "[@{[scalar localtime]}] Send error: $!\n";
                last;
            };
        }
        $client->close();
        exit; # Exit child process
    }
    $client->close(); # Parent closes client socket
}

# Cleanup
$server->close();
