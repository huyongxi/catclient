import socket
import json
import time

if __name__ == "__main__":
    ip_port = ("127.0.0.1", 2280)
    sock = socket.socket()
    sock.connect(ip_port)

    json_data = {"name":"test","type":"A","args":[1,2,3,4,5]}

    while True:
        buf = bytearray(json.dumps(json_data).encode())
        buf.append(0)
        sock.sendall(buf)
        print(sock.recv(64).decode())
        time.sleep(1)
