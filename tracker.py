import socket
import threading
import json
import hashlib
from tracker_dao import TrackerDao


class Tracker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.users = {}
        self.active_peers = set()
        self.db = TrackerDao()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

    def start(self):
        print(f"Tracker iniciado em {self.host}:{self.port}")
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"Nova conexão de {addr}")
            handler = threading.Thread(target=self.handle_client, args=(client_socket,))
            handler.start()

    def hash_password(self, password, salt = None):
        # if salt is None:
        #     salt = os.urandom(16)
        # implementar lógica de hash com salt dps (for fun)
        # salt deve ser armazenado junto com o hash, função para retorno do hash + salt deve ser implementada (verficar password no login) 
        # pwd_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 100000)

        return hashlib.sha256(password.encode()).hexdigest()


    def handle_client(self, client_socket):
        username = None
        try:
            buffer = ''
            while True:
                data = client_socket.recv(1024 *1024).decode()
                if not data:
                    break
                buffer += data
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    request = json.loads(message)
                    response = self.process_request(request, username)
                    if response.get('status') == 'login_success':
                        username = request['username']
                        self.active_peers.add(username)
                    client_socket.send(json.dumps(response).encode() + b'\n')
        except Exception as e:
            print(f"Erro ao lidar com cliente: {e}")
        finally:
            client_socket.close()
            if username:
                self.remove_peer(username)

    def process_request(self, request, current_username):
        method = request.get('method')
        if method == 'register':
            return self.handle_register(request)
        elif method == 'login':
            return self.handle_login(request)
        elif method == 'announce':
            if not current_username:
                return {'status': 'error', 'message': 'Não autenticado'}
            return self.handle_announce(request, current_username)
        else:
            return {'status': 'error', 'message': 'Ação inválida'}

    def handle_register(self, request):
        username = request.get('username')
        password = request.get('password')
        if not username or username == '' or not password or password == '':
            return {'status': 'error', 'message': 'username ou password faltando'}
        
        hashed_password = self.hash_password(password)

        if self.db.register_user(username, hashed_password):
            return {'status': 'success', 'message': 'Registro bem-sucedido'}
        else:
            return {'status': 'error', 'message': 'Usuário já existe'}

    def handle_login(self, request):
        username = request.get('username')
        password = request.get('password')
        if not username or username == '' or not password or password == '':
            return {'status': 'error', 'message': 'username ou password faltando'}
        
        hashed_password = self.hash_password(password)

        if not self.db.verify_user(username, hashed_password):
            return {'status': 'error', 'message': 'Credenciais inválidas'}
        
        self.db.add_active_peer(username)
        return {'status': 'success', 'message': 'Login bem-sucedido'}
    
    def remove_peer(self, username):
        self.db.remove_peer_files(username)
        self.db.remove_active_peer(username)

    def handle_announce(self, request, username):
        if self.db.verify_active_peer(username):
            file_name = request.get('name')
            file_size = request.get('size')
            file_hash = request.get('hash')
            
            if not file_name or not file_size or not file_hash:
                return {'status': 'error', 'message': 'Detalhes do arquivo faltando'}
            
            if self.db.register_file(username, file_name, file_size, file_hash):
                return {'status': 'success', 'message': 'Arquivo anunciado com sucesso'}
            else:
                return {'status': 'error', 'message': 'Erro ao registrar arquivo'}
        else:
            return {'status': 'error', 'message': 'Peer não autenticado'}
        
    def get_active_peers(self):
        # TODO: Implementar lógica para retornar peers ativos
        return None
    
    def get_peer_addr(self, username):
        # TODO: Implementar lógica para retornar endereços de peers
        return None
    
    def get_file_info(self, file_hash):
        # TODO: Implementar lógica para retornar informações de arquivos e/ou usuários com arquivo
        return None

if __name__ == '__main__':
    tracker = Tracker('localhost', 5000)
    tracker.start()