#!/usr/bin/env python3
"""Forward a port so Windows can reach a service that bound only to 127.0.0.1
inside WSL.

The Spark application interface binds to 127.0.0.1 on an IPv4-mapped socket,
which the WSL localhost forwarder does not always publish to Windows. Listening
on 0.0.0.0 and relaying to it makes the page reachable from a Windows browser.

    python3 port_forward.py 4050 4040
"""
import socket
import sys
import threading

LISTEN_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 4050
TARGET_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 4040
TARGET_HOST = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"


def pump(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            s.close()


def handle(client):
    try:
        upstream = socket.create_connection((TARGET_HOST, TARGET_PORT), timeout=10)
    except OSError:
        client.close()
        return
    threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
    threading.Thread(target=pump, args=(upstream, client), daemon=True).start()


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("0.0.0.0", LISTEN_PORT))
server.listen(64)
print(f"forwarding 0.0.0.0:{LISTEN_PORT} -> {TARGET_HOST}:{TARGET_PORT}", flush=True)

while True:
    conn, _ = server.accept()
    handle(conn)
