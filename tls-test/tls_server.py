
import socket
import ssl
import threading

context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("cert.pem", "key.pem")


def handle_client(conn, addr):
    try:
        with context.wrap_socket(conn, server_side=True) as tls:
            print(f"TLS handshake succeeded: {addr}")
            data = tls.recv(4096)
            print(f"Received {len(data)} bytes from {addr}")

    except ssl.SSLError as e:
        print(f"TLS error from {addr}: {e}")
    except OSError as e:
        print(f"Connection error from {addr}: {e}")
    finally:
        conn.close()


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("127.0.0.1", 8443))
server.listen(5)

print("TLS-only test server listening on port 8443")

while True:
    conn, addr = server.accept()

    thread = threading.Thread(
        target=handle_client,
        args=(conn, addr),
        daemon=True
    )
    thread.start()
