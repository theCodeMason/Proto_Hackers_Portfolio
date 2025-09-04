program SmokeTest;

{$mode objfpc}{$H+}

uses
  Sockets, SysUtils, BaseUnix;  // BaseUnix for ESysEINTR

const
  BUFFER_SIZE = 1024;

var
  ServerSocket, ClientSocket: Integer;
  Buffer: array[0..BUFFER_SIZE-1] of Byte;
  BytesRead: LongInt;

function CreateServer(Port: Word): Integer;
var
  Sock: Integer;
  Opt: LongInt;
  Addr: TInetSockAddr;
begin
  // Create TCP socket
  Sock := fpSocket(AF_INET, SOCK_STREAM, 0);
  if Sock = -1 then
  begin
    WriteLn('Error creating socket: ', GetLastOSError);
    Halt(1);
  end;

  // Allow socket to reuse address
  Opt := 1;
  if fpSetSockOpt(Sock, SOL_SOCKET, SO_REUSEADDR, @Opt, SizeOf(Opt)) = -1 then
  begin
    WriteLn('Error setting socket options: ', GetLastOSError);
    CloseSocket(Sock);
    Halt(1);
  end;

  // Bind socket to address and port
  FillChar(Addr, SizeOf(Addr), 0);
  Addr.sin_family := AF_INET;
  Addr.sin_port   := htons(Port);
  Addr.sin_addr.s_addr := htonl(INADDR_ANY);

  if fpBind(Sock, @Addr, SizeOf(Addr)) = -1 then
  begin
    WriteLn('Error binding socket: ', GetLastOSError);
    CloseSocket(Sock);
    Halt(1);
  end;

  // Listen for incoming connections
  if fpListen(Sock, 5) = -1 then
  begin
    WriteLn('Error listening on socket: ', GetLastOSError);
    CloseSocket(Sock);
    Halt(1);
  end;

  Result := Sock;
end;

procedure HandleClient(ClientSock: Integer);
var
  BytesSent: LongInt;
begin
  while True do
  begin
    // Receive data from client
    BytesRead := fpRecv(ClientSock, @Buffer, BUFFER_SIZE, 0);
    if BytesRead <= 0 then
    begin
      if BytesRead = -1 then
        WriteLn('Error receiving data: ', GetLastOSError);
      Break; // Client disconnected or error
    end;

    // Echo back the received data
    BytesSent := fpSend(ClientSock, @Buffer, BytesRead, 0);
    if BytesSent = -1 then
    begin
      WriteLn('Error sending data: ', GetLastOSError);
      Break;
    end;
  end;

  // Close client socket
  CloseSocket(ClientSock);
end;

begin
  // Create server on port 40000 (change if needed)
  ServerSocket := CreateServer(40000);
  WriteLn('Server listening on port 40000...');

  // Main server loop
  while True do
  begin
    // Accept incoming connection
    ClientSocket := fpAccept(ServerSocket, nil, nil);
    if ClientSocket = -1 then
    begin
      // Ignore interrupted system call errors, log others
      if GetLastOSError <> ESysEINTR then
        WriteLn('Error accepting connection: ', GetLastOSError);
      Continue;
    end;

    WriteLn('Client connected.');
    // Handle client synchronously (simple echo)
    HandleClient(ClientSocket);
    WriteLn('Client disconnected.');
  end;

  // Close server socket (unreachable here)
  CloseSocket(ServerSocket);
end.
