import threading
import socket
import queue

class DecentralizedNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.peers = []
        self.task_queue = queue.Queue()

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)

        print(f"Node started at {self.host}:{self.port}")
        
        self.listen_thread = threading.Thread(target=self.listen_for_connections)
        self.listen_thread.start()

    def listen_for_connections(self):
        while True:
            client_socket, client_address = self.socket.accept()
            self.peers.append(client_socket)
            print(f"Connected to {client_address}")
            
            peer_thread = threading.Thread(target=self.handle_peer, args=(client_socket,))
            peer_thread.start()

    def handle_peer(self, peer_socket):
        while True:
            try:
                data = peer_socket.recv(1024)
                if not data:
                    self.peers.remove(peer_socket)
                    print(f"Connection closed by {peer_socket.getpeername()}")
                    break

                decoded_data = data.decode()
                if decoded_data == "request_task":
                    if not self.task_queue.empty():
                        task = self.task_queue.get()
                        peer_socket.send(task.encode())
                    else:
                        peer_socket.send("no_task".encode())
                elif decoded_data.startswith("result:"):
                    result = decoded_data.split(":", 1)[1]
                    print(f"Received result from {peer_socket.getpeername()}: {result}")
                else:
                    print(f"Received unknown data from {peer_socket.getpeername()}: {decoded_data}")

            except Exception as e:
                print(f"Error handling peer {peer_socket.getpeername()}: {e}")
                self.peers.remove(peer_socket)
                break

    def distribute_task(self, task):
        for peer_socket in self.peers:
            try:
                peer_socket.send(task.encode())
            except:
                self.peers.remove(peer_socket)

    def add_task_to_queue(self, task):
        self.task_queue.put(task)

    def start_task_processing(self):
        while True:
            if not self.task_queue.empty():
                task = self.task_queue.get()
                self.distribute_task(task)
            # Implement some delay to avoid busy-waiting

if __name__ == "__main__":
    node = DecentralizedNode('localhost', 5000)
    node.start()

    # Add tasks to the task queue
    node.add_task_to_queue("Task 1")
    node.add_task_to_queue("Task 2")

    # Start processing tasks
    processing_thread = threading.Thread(target=node.start_task_processing)
    processing_thread.start()
