# Import json for parsing and serializing JSON data
import json
# Import PriorityQueue for priority-based job queuing and Empty for handling empty queue exceptions
from queue import PriorityQueue, Empty
# Import socket for creating and managing network sockets
import socket
# Import selectors for efficient I/O multiplexing using epoll
import selectors
# Import namedtuple for creating lightweight object types and defaultdict for dictionary with default values
from collections import namedtuple, defaultdict

# Define a namedtuple for Client, holding socket, line buffer, working jobs, and waits
Client = namedtuple('Client', 'sock line_buf working_on waits')
# Define a namedtuple for Wait, holding client ID and queues it's waiting on
Wait = namedtuple('Wait', 'client_id queues')

# Dictionary to store all connected clients, keyed by socket file descriptor
clients = {}
# Set to track valid job IDs that are still in queues
valid_jobs = set()
# Dictionary to track assigned jobs, mapping job ID to client
assigned = {}

# Global counter for generating unique job IDs
job_counter = 0

# Class representing a priority queue for jobs
class Queue:
    # Initialize the queue
    def __init__(self):
        # Internal PriorityQueue; note: uses negative priority for max-heap behavior since Python's PriorityQueue is min-heap
        self.q = PriorityQueue()
        # Counter for internal use (not used in code, possibly legacy)
        self.counter = 0
        # Set of waiters (clients waiting for jobs in this queue)
        self.waiters = set()

    # Put a new job into the queue with priority and data
    def put(self, pri, data):
        # Access global job_counter
        global job_counter
        # Assign current job_counter as ID
        id = job_counter
        # Increment job_counter for next job
        job_counter += 1
        # Put into priority queue with negative priority for max-heap
        self.q.put((-pri, id, data))
        # Add ID to valid_jobs set
        valid_jobs.add(id)
        # Return the job ID
        return id

    # Insert an existing job back into the queue (e.g., after abort)
    def insert(self, id, pri, data):
        # Put into queue without negating priority (wait, this seems inconsistent; might be a bug? Should be -pri?)
        self.q.put((pri, id, data))
        # Add ID to valid_jobs
        valid_jobs.add(id)

    # Peek at the highest priority job without removing it
    def peek(self):
        # Loop to skip invalid jobs
        while True:
            try:
                # Try to get job without blocking
                job = self.q.get_nowait()
            except Empty:
                # If queue empty, return None
                return None
            # Get job ID
            id = job[1]
            # Check if still valid
            if id in valid_jobs:
                break
        # Put the job back into the queue
        self.q.put(job)
        # Unpack job
        pri, id, data = job
        # Return ID, priority (note: pri is negative), data
        return id, pri, data

    # Pop the highest priority job from the queue
    def pop(self):
        # Get the job
        pri, id, data = self.q.get_nowait()
        # Assert it's valid
        assert id in valid_jobs
        # Remove from valid_jobs
        valid_jobs.remove(id)
        # Return ID, priority (negative), data
        return id, pri, data

    # Add a waiter to the set
    def add_wait(self, wait):
        self.waiters.add(wait)

    # Remove a waiter from the set
    def remove_wait(self, wait):
        self.waiters.discard(wait)

# Defaultdict of Queue instances, keyed by queue name
queues = defaultdict(Queue)

# Function to register a new client connection
def register_client(sock):
    # Get file descriptor
    no = sock.fileno()
    # Create Client tuple with socket, empty bytearray buffer, empty dict for working_on, empty set for waits
    clients[no] = Client(sock, bytearray(), {}, set())

# Function called when a client disconnects
def on_disconnect(client):
    # Requeue any jobs the client was working on
    for id, job in client.working_on.items():
        # Remove from assigned
        del assigned[id]
        # Insert back into the queue
        queues[job['queue']].insert(id, job['pri'], job['data'])
    # Remove any waits the client had
    for wait in client.waits:
        for queue in wait.queues:
            # Remove wait from each queue's waiters
            queues[queue].remove_wait(wait)
    # Get file descriptor
    no = client.sock.fileno()
    # Close the socket
    client.sock.close()
    # Remove from clients
    del clients[no]
    # For each job that was requeued, check if there are waiters and process one
    for job in client.working_on.values():
        if queues[job['queue']].waiters:
            # Pop a waiter
            wait = queues[job['queue']].waiters.pop()
            # Process the wait
            process_wait(wait, job['queue'])

# Function to process a wait when a job becomes available
def process_wait(wait, queue):
    # Get the client from wait's client_id
    client = clients[wait.client_id]
    # Remove the wait from client's waits
    client.waits.remove(wait)
    # Remove the wait from all queues it was waiting on
    for queue in wait.queues:
        queues[queue].remove_wait(wait)
    # Pop a job and assign to the client
    pop_job_to_client(client, queue)

# Function to pop a job from a queue and assign to a client
def pop_job_to_client(client, queue):
    # Pop the job
    job_id, pri, data = queues[queue].pop()
    # Store in client's working_on
    client.working_on[job_id] = { 'id': job_id, 'pri': pri, 'data': data, 'queue': queue }
    # Add to assigned
    assigned[job_id] = client
    # Send the job to client with positive priority
    send(client, id=job_id, job=data, pri=-pri, queue=queue)

# Function to send a response to the client
def send(client, status='ok', **kwargs):
    # Create JSON data with status and kwargs
    data = json.dumps({ **kwargs, 'status': status })
    # Print for debugging
    print(">>>", client.sock.fileno(), data)
    # Send JSON encoded with newline
    client.sock.sendall(data.encode('utf8') + b'\n')

# Function to send an error response
def send_error(client, msg):
    # Use send with status 'error' and error message
    send(client, status='error', error=msg)

# Function called when a socket is ready for reading
def on_response(sock):
    # Get file descriptor
    no = sock.fileno()
    # Get client
    client = clients[no]
    # Loop to read all available data
    while True:
        try:
            # Receive up to 4096 bytes
            buf = client.sock.recv(4096)
        except BlockingIOError:
            # No more data, break
            break
        except IOError:
            # Error, disconnect
            on_disconnect(client)
            return True
        # If no data, disconnect
        if not buf:
            on_disconnect(client)
            return True
        # Split by newline
        lines = buf.split(b'\n')
        if len(lines) == 1:
            # No full line, append to buffer
            client.line_buf.extend(buf)
        else:
            # Process first line with buffer
            first = client.line_buf + lines[0]
            # Clear buffer
            client.line_buf.clear()
            # Extend buffer with last partial line
            client.line_buf.extend(lines[-1])
            # Process the first full line
            process_line(client, first)
            # Process middle full lines
            for line in lines[1:-1]:
                process_line(client, line)

# Function to process a single line (JSON request) from client
def process_line(client, line):
    try:
        # Parse JSON
        req = json.loads(line)
    except:
        # Invalid, send error
        send_error(client, "Invalid JSON")
        return
    # Print for debugging
    print("<<<", client.sock.fileno(), req)
    # Check for 'request' key
    if 'request' not in req:
        send_error(client, 'Missing \"request\" key')
        return
    # Get request type
    req_type = req['request']
    # Handle 'put' request: add job to queue
    if req_type == 'put':
        try:
            # Extract queue, priority, job data
            q = req['queue']
            pri = req['pri']
            data = req['job']
        except KeyError:
            # Missing fields
            send_error(client, 'Missing data')
            return
        # Validate priority
        if type(pri) is not int or pri < 0:
            send_error(client, 'bad priority')
            return
        # Validate queue name
        if type(q) is not str:
            send_error(client, 'bad queue')
            return
        # Validate job data
        if type(data) is not dict:
            send_error(client, 'bad job')
            return
        # Put into queue
        job_id = queues[q].put(pri, data)
        # If waiters, process one
        if queues[q].waiters:
            wait = queues[q].waiters.pop()
            process_wait(wait, q)
        # Send ID back
        send(client, id=job_id)
    # Handle 'get' request: get a job from queues
    elif req_type == 'get':
        # Validate queues list
        if 'queues' not in req or type(req['queues']) is not list:
            send_error(client, 'bad request')
            return
        highest = None  # Highest priority (lowest number since negated)
        highest_queue = None
        # Find the highest priority job across queues
        for queue in req['queues']:
            job = queues[queue].peek()
            if job is not None:
                id, pri, data = job
                if highest is None or pri < highest:  # Lower pri value means higher priority since negated
                    highest = pri
                    highest_queue = queue
        # If found, pop and assign
        if highest is not None:
            pop_job_to_client(client, highest_queue)
        # If wait flag, set up wait
        elif req.get('wait', False) == True:
            # Create Wait
            wait = Wait(client.sock.fileno(), tuple(req['queues']))
            # Add to client's waits
            client.waits.add(wait)
            # Add to each queue's waiters
            for queue in req['queues']:
                queues[queue].add_wait(wait)
        else:
            # No job available
            send(client, status='no-job')
    # Handle 'delete' request: delete a job
    elif req_type == 'delete':
        # Validate ID
        if 'id' not in req or type(req['id']) is not int:
            send_error(client, 'bad job ID')
            return
        id = req['id']
        # If in valid_jobs, remove
        if id in valid_jobs:
            valid_jobs.remove(id)
            send(client)
        # If assigned, remove from client's working_on
        elif id in assigned:
            assigned.pop(id).working_on.pop(id)
            send(client)
        else:
            # Not found
            send(client, status='no-job')
    # Handle 'abort' request: abort a job the client is working on
    elif req_type == 'abort':
        # Validate ID
        if 'id' not in req or type(req['id']) is not int:
            send_error(client, 'bad job ID')
            return
        id = req['id']
        # If not in client's working_on, no-job
        if id not in client.working_on:
            send(client, status='no-job')
        else:
            # Remove from assigned
            assert assigned.pop(id) is client
            # Pop from working_on
            job = client.working_on.pop(id)
            # Insert back into queue
            queues[job['queue']].insert(id, job['pri'], job['data'])
            # Send ok
            send(client)
    else:
        # Unknown request type
        send_error(client, 'Invalid request type')
        return

# Main block
if __name__ == '__main__':
    # Create server socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow reuse address
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Bind to all interfaces on port 40000
    server_sock.bind(('0.0.0.0', 40000))
    # Listen for connections
    server_sock.listen()

    # Create Epoll selector
    selector = selectors.EpollSelector()
    # Register server socket for read events
    selector.register(server_sock, selectors.EVENT_READ)
    try:
        # Main loop
        while True:
            # Select ready sockets with timeout
            ready = selector.select(timeout=30)
            # For each ready key
            for key, _ in ready:
                sock = key.fileobj
                # If server socket, accept new client
                if sock is server_sock:
                    client, addr = sock.accept()
                    # Set non-blocking
                    client.setblocking(False)
                    # Register client
                    register_client(client)
                    # Register with selector for read
                    selector.register(client, selectors.EVENT_READ)
                else:
                    # Handle response from client
                    disconnect = on_response(sock)
                    # If disconnected, unregister
                    if disconnect:
                        selector.unregister(sock)
    finally:
        # Close server socket
        server_sock.close()
