import socket
import ssl

context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("cert.pem", "key.pem")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 8443))
    server.listen(5)

    print("TLS-only test server listening on port 8443")

    while True:
        conn, addr = server.accept()

        try:
            with context.wrap_socket(conn, server_side=True) as tls:
                print("TLS handshake succeeded:", addr)
                tls.recv(4096) 
        except (ssl.SSLError, OSError) as error:
            print("Connection ended:", error)