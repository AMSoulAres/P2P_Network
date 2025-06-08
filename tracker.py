import socket
import threading
import json
import hashlib
from tracker_dao import TrackerDao
from datetime import datetime, timedelta
TEMPO_LOGIN = 1 # Tempo de login em minutos

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
        """Lida com a comunicação com o cliente"""
        username = None
        try:
            buffer = ''
            while True: # Mantém o loop até que o cliente desconecte
                data = client_socket.recv(1024 *1024).decode()
                if not data:
                    break
                buffer += data
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    request = json.loads(message)
                    response = self.process_request(request, username)
                    if response.get('status') == 'success' and request.get('method') == 'login': # Workaround pra salvar o usuario da sessão e "manter login" (melhor forma seria implementar um sistema de sessão com token)
                        username = request.get('username')
                    client_socket.send(json.dumps(response).encode() + b'\n') # Envia a resposta para o cliente
        except Exception as e:
            print(f"Erro na comunicação com o cliente: {e}")
        finally:
            client_socket.close()
            if username:
                self.remove_peer(username) # Remove o peer do banco de dados quando desconecta

    def process_request(self, request, current_username):
        method = request.get('method')
        if method == 'register':
            return self.handle_register(request)
        elif method == 'login':
            return self.handle_login(request)
        elif method == 'announce':
            return self.handle_announce(request, current_username)
        elif method == 'get_peers': 
            return self.handle_get_active_peers_w_file(request)
        elif method == 'get_file_metadata':
            return self.handle_get_file_metadata(request)
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
        """Anuncia um arquivo para o tracker"""

        # Verifica se o usuario fazendo a requisição está logado e ativo (o peer é ativado no login e o usuário salvo na sessão do socket)
        dao_result = self.db.verify_active_peer(username)
        is_peer_active, message = self.verify_active_peer(username, dao_result)

        if is_peer_active:
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
            return {'status': 'error', 'message': message}
        
    def verify_active_peer(self, username, result):
        if result:
            peer_status = result[0]
            peer_last_seen = result[1]

            # Verifica se o peer está ativo (active_peer = 1)
            if peer_status == 1:
                peer_last_seen = datetime.strptime(peer_last_seen, "%Y-%m-%d %H:%M:%S")
                # Verifica timestamp do último login. Se o timestamp for maior que 5 min, desativa o peer
                if peer_last_seen < (datetime.now() - timedelta(minutes=TEMPO_LOGIN)):
                    self.remove_peer(username)
                    return False, "Login expirado"
                else:
                    return True, "Peer autenticado"
            else:
                # Verifica se o peer está ativo (active_peer = 0)
                return False, "Peer não autenticado"
        else:
            return False, "Usuário não encontrado"

    def handle_active_peers_w_file(self, request):
        file_hash = request.get('file_hash')
        if not file_hash:
            return {'status': 'error', 'message': 'Hash do arquivo faltando'}
        
        try:
            # Obter peers ativos com o arquivo
            cursor = self.db.conn.execute(
                "SELECT ip, port FROM peer_info WHERE username IN "
                "(SELECT username FROM peer_files WHERE file_hash = ?)",
                (file_hash,)
            )
            peers = cursor.fetchall()
            return {'status': 'success', 'peers': peers}
        except sqlite3.Error as e:
            return {'status': 'error', 'message': f"Database error: {str(e)}"}
    
    def get_peer_addr(self, username):
        # TODO: Implementar lógica para retornar endereços de peers
        return None
    
    def get_file_info(self, file_hash):
        # TODO: Implementar lógica para retornar informações de arquivos e/ou usuários com arquivo
        return None

if __name__ == '__main__':
    tracker = Tracker('localhost', 5000)
    tracker.start()