# Import bisect for efficient insertion into sorted lists using binary search
import bisect
# Import struct for packing and unpacking binary data
import struct
# Import socketserver for creating a TCP server with request handling
import socketserver
# Import time for handling sleep intervals and timestamps
import time
# Import threading for running background threads, like the heartbeat thread
import threading

# Custom exception class for protocol-related errors
class ProtocolError(Exception):
    # Initialize the exception with a message
    def __init__(self, msg):
        self.msg = msg

# Global dictionary to store BeatCounter instances, keyed by client ID
beat_counter = {}

# Class to manage heartbeat counters for clients
class BeatCounter:
    # Initialize with client object and heartbeat interval
    def __init__(self, client, interval):
        self.client = client  # Reference to the client handler
        self.interval = interval  # Heartbeat interval in beats
        self.acc = 0  # Accumulator to count beats

    # Increment the beat counter and send heartbeat if interval is reached
    def beat(self):
        self.acc += 1
        if self.acc == self.interval:
            self.acc = 0  # Reset accumulator
            self.send_beat()  # Send the heartbeat

    # Send a heartbeat message to the client
    def send_beat(self):
        self.client.send(0x41, b'')  # Send message type 0x41 (heartbeat) with no data

# Function to register a heartbeat for a client
def register_heartbeat(client, interval):
    # Create and store a BeatCounter for the client
    beat_counter[id(client)] = BeatCounter(client, interval)

# Function to unregister a heartbeat for a client
def unregister_heartbeat(client):
    # Remove the BeatCounter if it exists
    if id(client) in beat_counter:
        del beat_counter[id(client)]

# Background thread function to handle periodic heartbeats
def heartbeat_thread():
    # Infinite loop to periodically trigger beats
    while True:
        time.sleep(.1)  # Sleep for 0.1 seconds between checks
        # Make a copy of the counters to avoid runtime errors if the dictionary changes during iteration
        counters = list(beat_counter.values())
        # Trigger beat for all registered counters, catching exceptions to prevent thread crash
        for counter in counters:
            try:
                counter.beat()
            except Exception as e:
                print("Heartbeat error:", e)

# Class representing a road with associated cameras, dispatchers, and observations
class Road:
    # Initialize the road with an ID
    def __init__(self, road_id):
        self.id = road_id  # Unique road identifier
        self.limit = None  # Speed limit (in mph, set later)
        self.camera_to_pos = {}  # Map camera ID to position (mile marker)
        self.position_to_camera = {}  # Map position to camera handler
        self.dispatchers = {}  # Map dispatcher ID to dispatcher handler
        self.stored_tickets = []  # List to store tickets if no dispatchers are available
        # Dictionary of car observations, keyed by plate; each value is a tuple of (timestamps, positions) lists
        self.car_observations = {}
        self.lock = threading.Lock()  # Lock for thread safety

    # Set the speed limit for the road
    def set_limit(self, limit):
        # Check if limit is already set and matches; raise error if mismatch
        with self.lock:
            if self.limit is not None and self.limit != limit:
                raise ProtocolError('Limit different to previous limit')
            self.limit = limit  # Set the limit

    # Add a camera to the road at a specific position
    def add_camera(self, camera, position):
        # Check if position already has a camera; raise error if so
        with self.lock:
            if position in self.position_to_camera:
                raise ProtocolError('Camera already exists at this location')
            self.position_to_camera[position] = camera  # Map position to camera
            self.camera_to_pos[id(camera)] = position  # Map camera ID to position

    # Add a dispatcher to the road
    def add_dispatcher(self, dispatcher):
        with self.lock:
            self.dispatchers[id(dispatcher)] = dispatcher  # Add to dispatchers map
            tickets = list(self.stored_tickets)  # Copy stored tickets
            self.stored_tickets = []  # Clear stored tickets
        for ticket in tickets:
            self.send_ticket(ticket)  # Send each stored ticket

    # Remove a camera from the road
    def remove_camera(self, camera):
        with self.lock:
            pos = self.camera_to_pos[id(camera)]  # Get position from camera ID
            del self.camera_to_pos[id(camera)]  # Remove from camera_to_pos
            del self.position_to_camera[pos]  # Remove from position_to_camera

    # Remove a dispatcher from the road
    def remove_dispatcher(self, dispatcher):
        with self.lock:
            del self.dispatchers[id(dispatcher)]  # Remove from dispatchers map

    # Process an observation from a camera
    def camera_observation(self, camera, plate, timestamp):
        with self.lock:
            pos = self.camera_to_pos[id(camera)]  # Get position of the camera
            # Initialize observations for the plate if not present
            if plate not in self.car_observations:
                # Store timestamp and position in two synchronized lists
                self.car_observations[plate] = ([], [])
            obs_ts, obs_pos = self.car_observations[plate]  # Get timestamps and positions
            idx = bisect.bisect(obs_ts, timestamp)  # Find insertion index using binary search
            obs_ts.insert(idx, timestamp)  # Insert timestamp
            obs_pos.insert(idx, pos)  # Insert position
            # Print observations for debugging
            print("Observations:", plate, list(zip(obs_ts, obs_pos)))
            # Calculate speeds and check for violations
            for speed, obs1, obs2 in get_speeds(idx, obs_ts, obs_pos):
                # Print speed for debugging
                print("Speed", speed, obs1, obs2)
                # If speed exceeds limit, create a ticket
                if round(speed) > self.limit:
                    self.create_ticket(plate, speed, obs1, obs2)

    # Create a ticket for a speed violation
    def create_ticket(self, plate, speed, obs1, obs2):
        speed_int = int(round(speed * 100))  # Convert speed to integer (mph * 100)
        ticket = (plate, speed_int, obs1, obs2)  # Tuple representing the ticket
        # Send immediately if dispatchers available, else store
        if self.dispatchers:
            self.send_ticket(ticket)
        else:
            # Print for debugging
            print("Store ticket", ticket)
            self.stored_tickets.append(ticket)  # Store the ticket

    # Send a ticket to a dispatcher
    def send_ticket(self, ticket):
        # Print for debugging
        print("Maybe send ticket", ticket)
        plate, speed, obs1, obs2 = ticket  # Unpack ticket
        pos1, time1 = obs1  # Unpack first observation
        pos2, time2 = obs2  # Unpack second observation
        # Check if ticket should be sent (no duplicate days)
        if should_send_ticket(plate, time1, time2):
            # Print for debugging
            print("Will send ticket", ticket)
            # Select the first dispatcher arbitrarily
            dispatcher = next(iter(self.dispatchers.values()))
            plate_bytes = plate.encode('ascii')  # Encode plate as ASCII
            # Pack the message: length of plate, plate, road ID, pos1, time1, pos2, time2, speed
            msg = struct.pack('!B', len(plate_bytes))
            msg += plate_bytes
            msg += struct.pack('!HHIHIH', self.id, pos1, time1, pos2, time2, speed)
            dispatcher.send(0x21, msg)  # Send message type 0x21 (ticket)

# Function to calculate speeds between observations
# Yields up to two speed observations: previous and next
def get_speeds(idx, times, positions):
    # If only one observation, no speeds
    if len(times) == 1:
        return []
    pos = positions[idx]  # Current position
    time_ = times[idx]  # Current timestamp
    # Calculate speed with previous observation if exists
    if idx > 0:
        prev_pos = positions[idx - 1]
        prev_time = times[idx - 1]
        dist = abs(pos - prev_pos)  # Distance in miles
        t = time_ - prev_time  # Time in seconds
        if t > 0:
            # Yield speed in mph: (distance / time) * 3600
            yield ((dist / t) * 3600, (prev_pos, prev_time), (pos, time_))
    # Calculate speed with next observation if exists
    if idx < len(times) - 1:
        new_pos = positions[idx + 1]
        new_time = times[idx + 1]
        dist = abs(new_pos - pos)  # Distance in miles
        t = new_time - time_  # Time in seconds
        if t > 0:
            # Yield speed in mph
            yield ((dist / t) * 3600, (pos, time_), (new_pos, new_time))

# Global dictionary mapping camera IDs to Road objects
camera_to_road = {}
# Global dictionary mapping dispatcher IDs to lists of Road objects
dispatcher_roads = {}
# Global dictionary mapping road IDs to Road objects
roads = {}
# Global dictionary mapping car plates to sets of days they were ticketed
car_tickets = {}
# Lock for car_tickets access
car_lock = threading.Lock()

# Function to determine if a ticket should be sent (avoids duplicates on the same day)
def should_send_ticket(plate, time1, time2):
    with car_lock:
        # Initialize set for plate if not present
        if plate not in car_tickets:
            car_tickets[plate] = set()
        day_start = time1 // 86400  # Start day (seconds in a day)
        day_end = time2 // 86400  # End day
        days = set()  # Set of days spanned by the observations
        # Add all days from start to end
        for day in range(day_start, day_end + 1):
            days.add(day)
            # If any day already ticketed, don't send
            if day in car_tickets[plate]:
                return False
        # Update ticketed days
        car_tickets[plate].update(days)
        return True

# Function to register a camera client
def register_camera(client, road, mile, limit):
    # Print for debugging
    print("Register camera", { 'client': id(client), 'road': road, 'mile': mile, 'limit': limit })
    # Create road if not exists
    if road not in roads:
        roads[road] = Road(road)
    road_obj = roads[road]  # Get road object
    road_obj.set_limit(limit)  # Set limit
    road_obj.add_camera(client, mile)  # Add camera
    camera_to_road[id(client)] = road_obj  # Map client to road

# Function to unregister a camera client
def unregister_camera(client):
    camera_to_road[id(client)].remove_camera(client)  # Remove from road
    del camera_to_road[id(client)]  # Remove mapping

# Function to register a dispatcher client for multiple roads
def register_dispatcher(client, in_roads):
    # Print for debugging
    print("Register dispatcher", { 'client': id(client), 'roads': in_roads })
    road_objs = []  # List of road objects
    for road in in_roads:
        # Create road if not exists
        if road not in roads:
            roads[road] = Road(road)
        road_objs.append(roads[road])  # Add to list
        roads[road].add_dispatcher(client)  # Add dispatcher to road
    dispatcher_roads[id(client)] = road_objs  # Map client to roads

# Function to unregister a dispatcher client
def unregister_dispatcher(client):
    road_objs = dispatcher_roads[id(client)]  # Get roads
    for road in road_objs:
        road.remove_dispatcher(client)  # Remove from each road
    del dispatcher_roads[id(client)]  # Remove mapping

# Function to process a camera observation
def camera_observation(camera, plate, timestamp):
    # Print for debugging
    print("Observation", { 'camera': id(camera), 'plate': plate, 'timestamp': timestamp })
    road = camera_to_road[id(camera)]  # Get road
    road.camera_observation(camera, plate, timestamp)  # Delegate to road

# Handler class for socketserver to manage client connections
class Handler(socketserver.BaseRequestHandler):
    # Main handle method for the connection
    def handle(self):
        self.client_type = None  # Type: 'camera' or 'dispatcher'
        self.heartbeat_known = False  # Flag if heartbeat request processed
        self._send_lock = threading.Lock()  # Lock to serialize sends on this socket

        # Main loop to process messages
        while True:
            try:
                self.main_loop()  # Process one message
            except ProtocolError as e:
                # Handle protocol error: send error message
                err = e.msg.encode('ascii')
                try:
                    self.send(0x10, struct.pack('!B', len(err)) + err)  # Send error type 0x10
                    # Sleep (possibly to allow send to complete before close)
                    time.sleep(1)
                except Exception:
                    pass
                break
            except Exception as e:
                # Print unexpected exception
                print("Exception:", e)
                # Break to handle teardown
                break

        # Close the socket
        self.request.close()
        # Unregister based on type
        if self.client_type == 'camera':
            unregister_camera(self)
        elif self.client_type == 'dispatcher':
            unregister_dispatcher(self)
        # Always unregister heartbeat
        unregister_heartbeat(self)

    # Process one incoming message
    def main_loop(self):
        msg_type = self.read_u8()  # Read message type
        if msg_type == 0x20:
            # Plate observation message
            # Check if client is a camera
            if self.client_type != 'camera':
                raise ProtocolError('not a camera')
            plate = self.read_str()  # Read plate string
            timestamp = self.read_u32()  # Read timestamp
            camera_observation(self, plate, timestamp)  # Process observation
        elif msg_type == 0x40:
            # WantHeartbeat message
            # Check if already set
            if self.heartbeat_known:
                raise ProtocolError('Heartbeat already set')
            interval = self.read_u32()  # Read interval
            if interval != 0:
                register_heartbeat(self, interval)  # Register if non-zero
            self.heartbeat_known = True  # Mark as known
        elif msg_type == 0x80:
            # IAmCamera message
            # Check if type already set
            if self.client_type is not None:
                raise ProtocolError('Already classified as another type')
            road = self.read_u16()  # Read road ID
            mile = self.read_u16()  # Read mile position
            limit = self.read_u16()  # Read speed limit
            register_camera(self, road, mile, limit)  # Register camera
            self.client_type = 'camera'  # Set type
        elif msg_type == 0x81:
            # IAmDispatcher message
            # Check if type already set
            if self.client_type is not None:
                raise ProtocolError('Already classified as another type')
            numroads = self.read_u8()  # Read number of roads
            roads = []  # List of roads
            for _ in range(numroads):
                roads.append(self.read_u16())  # Read each road ID
            register_dispatcher(self, roads)  # Register dispatcher
            self.client_type = 'dispatcher'  # Set type
        else:
            # Unknown message type
            raise ProtocolError('Unknown message type')

    # Helper to read unsigned 8-bit integer
    def read_u8(self):
        return self._read(1)[0]

    # Helper to read unsigned 16-bit integer (big-endian)
    def read_u16(self):
        (val,) = struct.unpack('!H', self._read(2))
        return val

    # Helper to read unsigned 32-bit integer (big-endian)
    def read_u32(self):
        (val,) = struct.unpack('!I', self._read(4))
        return val

    # Helper to read string (length-prefixed)
    def read_str(self):
        l = self.read_u8()  # Read length
        return self._read(l).decode('ascii')  # Read and decode

    # Internal read method to read exactly 'size' bytes
    def _read(self, size):
        buf = b''  # Buffer for data
        while len(buf) < size:
            remaining = size - len(buf)  # Bytes left
            recv = self.request.recv(remaining)  # Receive
            # If no data, raise exception (connection closed)
            if not len(recv):
                raise Exception('Unable to read')
            buf += recv  # Append
        return buf

    # Send a message with ID and data
    def send(self, msg_id, data):
        with self._send_lock:  # Acquire lock to ensure thread-safe sending
            self.request.send(struct.pack('!B', msg_id))  # Send message ID
            self.request.sendall(data)  # Send data

# Custom server class inheriting from ThreadingTCPServer
class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True  # Allow address reuse for quick restarts

# Create and start the heartbeat thread
t = threading.Thread(target=heartbeat_thread)
t.daemon = True  # Daemon thread to exit with main
t.start()  # Start the thread

# Create and start the server
server = Server(('0.0.0.0', 40000), Handler)  # Bind to all interfaces on port 9999
server.serve_forever()  # Run the server loop
