import socket

# Replace with the master node's public or elastic IP
HOST = '13.61.146.93'  
PORT = 65432

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    message = "Hello from the client!"
    print(f"Sending: {message}")
    s.sendall(message.encode())
    data = s.recv(1024)
    print("Received back:", data.decode())