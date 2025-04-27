import socket
import threading
import time

def server_thread(listen_port, node_name):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', listen_port))
    s.listen(5)
    print(f"{node_name} server listening on port {listen_port}")
    while True:
        conn, addr = s.accept()
        data = conn.recv(1024)
        if data:
            print(f"{node_name} received: '{data.decode()}' from {addr}")
            response = f"Pong from {node_name}"
            conn.send(response.encode())
        conn.close()

def ping_target(target_ip, target_port, message, node_name):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_ip, target_port))
            print(f"{node_name} sending: '{message}' to {target_ip}:{target_port}")
            s.send(message.encode())
            data = s.recv(1024)
            print(f"{node_name} got reply: '{data.decode()}' from {target_ip}:{target_port}")
    except Exception as e:
        print(f"{node_name} failed to connect to {target_ip}:{target_port} - {e}")

if __name__ == '__main__':
    node_name = "Master"
    listen_port = 55000  # Unique port for Master
    # Start server thread
    threading.Thread(target=server_thread, args=(listen_port, node_name), daemon=True).start()
    time.sleep(1)  # Brief delay to ensure the server is listening

    # Define targets (replace 'localhost' with appropriate IPs in production)
    targets = [
        ("localhost", 55001, "Ping from Master to Crawler"),
        ("localhost", 55002, "Ping from Master to Indexer"),
    ]
    for target_ip, target_port, message in targets:
        ping_target(target_ip, target_port, message, node_name)