with Ada.Text_IO; use Ada.Text_IO;
with GNAT.Sockets; use GNAT.Sockets;
with Ada.Streams; use Ada.Streams;

procedure Challenge_0 is
   -- Server port
   Port : constant Port_Type := 5000;

   -- Task to handle each client connection
   task type Client_Handler is
      entry Start (Socket : Socket_Type);
   end Client_Handler;

   task body Client_Handler is
      Client_Socket : Socket_Type;
      Buffer        : Stream_Element_Array (1 .. 1024); -- Buffer for reading data
      Last          : Stream_Element_Offset;
   begin
      accept Start (Socket : Socket_Type) do
         Client_Socket := Socket;
      end Start;
      loop
         -- Read data from the client
         Receive_Socket (Client_Socket, Buffer, Last);
         exit when Last = 0; -- Client disconnected
         -- Echo the data back to the client
         Send_Socket (Client_Socket, Buffer (1 .. Last), Last);
      end loop;
      -- Close the client socket
      Close_Socket (Client_Socket);
   exception
      when others =>
         Close_Socket (Client_Socket);
   end Client_Handler;

begin
   -- Initialize the socket library
   Initialize;

   -- Create a TCP server socket
   declare
      Server_Socket : Socket_Type;
      Address       : Sock_Addr_Type;
   begin
      -- Set up the server address
      Address.Addr := Any_Inet_Addr;
      Address.Port := Port;

      -- Create and bind the server socket
      Create_Socket (Server_Socket, Family_Inet, Socket_Stream);
      Set_Socket_Option (Server_Socket, Socket_Level, (Reuse_Address, True));
      Bind_Socket (Server_Socket, Address);
      Listen_Socket (Server_Socket);

      Put_Line ("Server listening on port " & Port_Type'Image (Port));

      -- Main server loop: accept client connections
      loop
         declare
            Client_Socket : Socket_Type;
            Client_Addr   : Sock_Addr_Type;
            Handler       : Client_Handler;
         begin
            -- Accept a new connection
            Accept_Socket (Server_Socket, Client_Socket, Client_Addr);
            Put_Line ("Accepted connection from " & Image (Client_Addr));
            -- Start a new task to handle the client
            Handler.Start (Client_Socket);
         end;
      end loop;
   exception
      when others =>
         Put_Line ("Server error. Shutting down.");
         Close_Socket (Server_Socket);
   end;

   -- Finalize the socket library
   Finalize;
exception
   when others =>
      Put_Line ("Unexpected error. Exiting.");
      Finalize;
end Challenge_0;
