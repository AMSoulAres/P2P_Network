import socket
import threading
import json
import hashlib
from tracker_dao import TrackerDao
from datetime import datetime, timedelta
TEMPO_LOGIN = 1000 # Tempo de login em minutos
W_TIME = 0.01      # w1: peso do tempo conectado
W_CHUNKS = 1       # w3: peso dos chunks servidos

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
        # implementar lógica de hash com salt dps
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
        elif method == 'heartbeat':
            return self.handle_heartbeat(request, current_username)
        elif method == 'announce':
            return self.handle_announce(request, current_username)
        elif method == 'get_peers': 
            return self.handle_get_active_peers_w_file(request)
        elif method == 'get_file_metadata':
            return self.handle_get_file_metadata(request)
        elif method == 'partial_announce':
            return self.handle_partial_announce(request, current_username)
        elif method == 'list_online_users':
            return self.handle_list_online_users()
        elif method == 'get_peer_address':
            return self.handle_get_peer_address(request)
        elif method == 'get_peer_chat_address':
            return self.handle_get_peer_chat_address(request)
        elif method == 'list_files':
            return self.handle_list_files()
        
        #Sala de Chat
        elif method == 'room_create':
            return self.handle_room_create(request, current_username)
        elif method == 'room_delete':
            return self.handle_room_delete(request, current_username)
        elif method == 'room_invite':
            return self.handle_room_invite(request, current_username)
        elif method == 'room_kick':
            return self.handle_room_kick(request, current_username)
        elif method == 'room_list':
            return self.handle_room_list()
        elif method == 'room_details':
            return self.handle_room_details(request, current_username)
        else:
            return {'status': 'error', 'message': 'Ação inválida'}

    def handle_register(self, request):
        username = request.get('username')
        password = request.get('password')
        ip = request.get('ip')
        port = request.get('port')
        chat_port = request.get('chat_port')

        if not username or username == '' or not password or password == '':
            return {'status': 'error', 'message': 'username ou password faltando'}
        
        hashed_password = self.hash_password(password)

        if self.db.register_user(username, hashed_password, ip, port, chat_port):
            return {'status': 'success', 'message': 'Registro bem-sucedido'}
        else:
            return {'status': 'error', 'message': 'Usuário já existe'}

    def handle_login(self, request):
        username = request.get('username')
        password = request.get('password')
        ip = request.get('ip')
        port = request.get('port')
        chat_port = request.get('chat_port')

        if not username or username == '' or not password or password == '':
            return {'status': 'error', 'message': 'username ou password faltando'}
        
        hashed_password = self.hash_password(password)

        if not self.db.verify_user(username, hashed_password):
            return {'status': 'error', 'message': 'Credenciais inválidas'}
        
        self.db.add_active_peer(username, ip, port, chat_port)
        return {'status': 'success', 'message': 'Login bem-sucedido'}

    def handle_heartbeat(self, request, username):
        hashes = request.get('file_hashes', [])
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        # atualiza timestamp e refresh de arquivos do peer
        self.db.refresh_peer_files(username, hashes)

        metrics = request.get('metrics', {})
        time_online = metrics.get('time_online', 0)
        chunks_served = metrics.get('chunks_served', 0)
        self.db.update_peer_score(
            username,
            time_online,
            chunks_served
        )
        time_online, chunks_served = self.db.get_peer_score(username)

        score = (W_TIME * time_online) + (W_CHUNKS * chunks_served)
        return {'status': 'success', 'score': score}
    
    def remove_peer(self, username):
        self.db.remove_peer_files(username)
        self.db.remove_active_peer(username)

    def handle_announce(self, request, username):
        """Anuncia um arquivo para o tracker"""

        # Verifica se o usuario fazendo a requisição está logado e ativo (o peer é ativado no login e o usuário salvo na sessão do socket)
        is_peer_active, message = self.verify_active_peer(username)

        if is_peer_active:
            file_name = request.get('name')
            file_size = request.get('size')
            file_hash = request.get('hash')
            file_chunks = request.get('chunks')
            
            if not file_name or not file_size or not file_hash:
                return {'status': 'error', 'message': 'Detalhes do arquivo faltando'}
            
            if self.db.register_file(username, file_name, file_size, file_hash, json.dumps(file_chunks)):
                return {'status': 'success', 'message': 'Arquivo anunciado com sucesso'}
            else:
                return {'status': 'error', 'message': 'Erro ao registrar arquivo'}
        else:
            return {'status': 'error', 'message': message}

    def handle_partial_announce(self, request, username):
        file_hash = request.get('file_hash')
        if not file_hash or not username:
            return {'status': 'error', 'message': 'Dados faltando'}

        is_peer_active, message = self.verify_active_peer(username)
        if not is_peer_active:
            return {'status': 'error', 'message': message}

        if self.db.register_partial_file(username, file_hash):
            return {'status': 'success', 'message': 'Anúncio parcial registrado'}
        else:
            return {'status': 'error', 'message': 'Erro ao registrar parcial'}

        
    def verify_active_peer(self, username):
        dao_result = self.db.verify_active_peer(username)
        if dao_result:
            peer_status = dao_result[0]
            peer_last_seen = dao_result[1]

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

    def handle_get_active_peers_w_file(self, request):
        file_hash = request.get('file_hash')
        if not file_hash:
            return {'status': 'error', 'message': 'Hash do arquivo faltando'}

        # Obter peers ativos com o arquivo
        peers = self.db.get_active_peers_with_file(file_hash)
        peers = sorted(peers, key=lambda x: self._calculate_peer_score(x), reverse=True)
        peers = [(p[0], p[1], p[2], self._calculate_peer_score(p)) for p in peers]
        if not peers:
            return {'status': 'error', 'message': 'Nenhum peer ativo com o arquivo encontrado'}
        return {'status': 'success', 'peers': peers}

    def _calculate_peer_score(self, peer):
        time_online = peer[3] or 0
        chunks_served = peer[4] or 0
        return (W_TIME * time_online) + (W_CHUNKS * chunks_served)
    
    def handle_get_file_metadata(self, request):
        file_hash = request.get('file_hash')
        if not file_hash:
            return {'status': 'error', 'message': 'Hash do arquivo faltando'}
        
        result = self.db.get_file_metadata(file_hash)
        if not result:
            return {'status': 'error', 'message': 'Arquivo não encontrado'}
        
        metadata = {
            'name': result[0],
            'size': result[1],
            'chunk_hashes': json.loads(result[2]) if result[2] else []
        }
        return {'status': 'success', 'metadata': metadata}
    
    def handle_list_online_users(self):
        users = self.db.get_online_users()
        return {'status': 'success', 'users': users}
    
    def handle_get_peer_address(self, request):
        username = request.get('username')
        address = self.db.get_peer_address(username)
        if address:
            return {'status': 'success', 'ip': address[0], 'port': address[1]}
        return {'status': 'error', 'message': 'Usuário não encontrado'}
    
    def handle_get_peer_chat_address(self, request):
        username = request.get('username')
        address = self.db.get_peer_chat_address(username)
        if address:
            return {'status': 'success', 'ip': address[0], 'port': address[1]}
        return {'status': 'error', 'message': 'Usuário não encontrado'}
    
    def handle_list_files(self):
        files = self.db.list_files()
        if not files:
            return {'status': 'error', 'message': 'Nenhum arquivo encontrado'}
        
        file_list = []
        for file in files:
            file_list.append({
                'hash': file[0],
                'name': file[1],
                'size': file[2],
                'chunks': json.loads(file[3]) if file[3] else []
            })
        
        return {'status': 'success', 'files': file_list}
    
    def handle_room_create(self, request, username):
        is_active, msg = self.verify_active_peer(username)
        if not is_active: return {'status': 'error', 'message': msg}
        
        room_name = request.get('room_name')
        if (not room_name or room_name == ''): return {'status': 'error', 'message': 'Nome da sala é obrigatório'}

        if self.db.create_room(room_name, username):
            return {'status': 'success', 'message': f"Sala '{room_name}' criada com sucesso."}
        else:
            return {'status': 'error', 'message': f"Sala '{room_name}' já existe."}

    def handle_room_delete(self, request, username):
        is_active, msg = self.verify_active_peer(username)
        if not is_active: return {'status': 'error', 'message': msg}
        
        room_name = request.get('room_name')
        if not self.db.is_moderator(username, room_name):
            return {'status': 'error', 'message': 'Apenas o moderador pode excluir a sala.'}

        if self.db.delete_room(room_name):
            return {'status': 'success', 'message': f"Sala '{room_name}' excluída."}
        else:
            return {'status': 'error', 'message': 'Erro ao excluir a sala.'}

    def handle_room_invite(self, request, username):
        is_active, msg = self.verify_active_peer(username)
        if not is_active: return {'status': 'error', 'message': msg}

        room_name = request.get('room_name')
        target_user = request.get('target_user')
        if not self.db.is_moderator(username, room_name):
            return {'status': 'error', 'message': 'Apenas o moderador pode convidar usuários.'}

        if self.db.add_room_member(room_name, target_user):
            return {'status': 'success', 'message': f"'{target_user}' convidado para a sala '{room_name}'."}
        else:
            return {'status': 'error', 'message': f"Não foi possível convidar '{target_user}'. Verifique se o usuário e a sala existem."}

    def handle_room_kick(self, request, username):
        is_active, msg = self.verify_active_peer(username)
        if not is_active: return {'status': 'error', 'message': msg}

        room_name = request.get('room_name')
        target_user = request.get('target_user')
        if not self.db.is_moderator(username, room_name):
            return {'status': 'error', 'message': 'Apenas o moderador pode remover usuários.'}
        
        room_info = self.db.get_room_info(room_name)
        if room_info and room_info[0] == target_user:
            return {'status': 'error', 'message': 'O moderador não pode ser removido da sala.'}

        if self.db.remove_room_member(room_name, target_user):
            return {'status': 'success', 'message': f"'{target_user}' removido da sala '{room_name}'."}
        else:
            return {'status': 'error', 'message': f"Erro ao remover '{target_user}'."}

    def handle_room_list(self):
        rooms = self.db.list_all_rooms()
        room_list = [{'name': r[0], 'moderator': r[1]} for r in rooms]
        return {'status': 'success', 'rooms': room_list}

    def handle_room_details(self, request, username):
        is_active, msg = self.verify_active_peer(username)
        if not is_active: return {'status': 'error', 'message': msg}

        room_name = request.get('room_name')
        if not self.db.is_user_in_room(username, room_name):
            return {'status': 'error', 'message': 'Você não tem permissão para ver detalhes desta sala.'}
            
        info = self.db.get_room_info(room_name)
        if not info: return {'status': 'error', 'message': 'Sala não encontrada.'}

        members = self.db.get_room_members(room_name)
        details = {
            'moderator': info[0],
            'created_at': info[1],
            'members': members
        }
        return {'status': 'success', 'details': details}

if __name__ == '__main__':
    tracker = Tracker('localhost', 5000)
    tracker.start()