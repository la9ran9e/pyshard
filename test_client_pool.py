import socket


class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
 
    def handle_client(self, client):
        # Server will just close the connection after it opens it
        client.close()
        return

    def start_listening(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(5)

        client, addr = sock.accept()
        client_handler = threading.Thread(target=self.handle_client, args=(client,))
        client_handler.start()