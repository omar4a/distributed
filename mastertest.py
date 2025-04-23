import socket

HOST = ''  # Listen on all interfaces
PORT = 65432  # You can choose any unused port

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"Server is listening on port {PORT}...")
    conn, addr = s.accept()  # Wait for a connection
    with conn:
        print('Connected by', addr)
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print("Received:", data.decode())
            # Echo the data back to the client
            conn.sendall(data)