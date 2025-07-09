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
        elif method == 'create_room':
            return self.handle_create_room(request, current_username)
        elif method == 'delete_room':
            return self.handle_delete_room(request, current_username)
        elif method == 'add_member':
            return self.handle_add_member(request, current_username)
        elif method == 'remove_member':
            return self.handle_remove_member(request, current_username)
        elif method == 'list_rooms':
            return self.handle_list_rooms(request, current_username)
        elif method == 'get_room_members':
            return self.handle_get_room_members(request, current_username)
        elif method == 'get_room_info':
            return self.handle_get_room_info(request, current_username)
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

    # Handlers para salas de chat
    
    def handle_create_room(self, request, username):
        """Cria uma nova sala de chat"""
        if not username:
            return {'status': 'error', 'message': 'Usuário não autenticado'}
        
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        
        room_id = request.get('room_id')
        name = request.get('name')
        max_history = request.get('max_history', 100)
        
        if not room_id or not name:
            return {'status': 'error', 'message': 'ID e nome da sala são obrigatórios'}
        
        if self.db.create_chat_room(room_id, name, username, max_history):
            return {'status': 'success', 'message': f'Sala {name} criada com sucesso'}
        else:
            return {'status': 'error', 'message': 'Sala já existe ou erro interno'}

    def handle_delete_room(self, request, username):
        """Remove uma sala de chat"""
        if not username:
            return {'status': 'error', 'message': 'Usuário não autenticado'}
        
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        
        room_id = request.get('room_id')
        if not room_id:
            return {'status': 'error', 'message': 'ID da sala é obrigatório'}
        
        if self.db.delete_chat_room(room_id, username):
            return {'status': 'success', 'message': f'Sala removida com sucesso'}
        else:
            return {'status': 'error', 'message': 'Apenas o moderador pode remover a sala'}

    def handle_add_member(self, request, username):
        """Adiciona membro à sala"""
        if not username:
            return {'status': 'error', 'message': 'Usuário não autenticado'}
        
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        
        room_id = request.get('room_id')
        member_username = request.get('username')
        
        if not room_id or not member_username:
            return {'status': 'error', 'message': 'ID da sala e nome do usuário são obrigatórios'}
        
        if self.db.add_member_to_room(room_id, member_username, username):
            return {'status': 'success', 'message': f'Usuário {member_username} adicionado à sala'}
        else:
            return {'status': 'error', 'message': 'Apenas o moderador pode adicionar membros'}

    def handle_remove_member(self, request, username):
        """Remove membro da sala"""
        if not username:
            return {'status': 'error', 'message': 'Usuário não autenticado'}
        
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        
        room_id = request.get('room_id')
        member_username = request.get('username')
        
        if not room_id or not member_username:
            return {'status': 'error', 'message': 'ID da sala e nome do usuário são obrigatórios'}
        
        if self.db.remove_member_from_room(room_id, member_username, username):
            return {'status': 'success', 'message': f'Usuário {member_username} removido da sala'}
        else:
            return {'status': 'error', 'message': 'Apenas o moderador ou o próprio usuário pode sair da sala'}

    def handle_list_rooms(self, request, username):
        """Lista salas disponíveis"""
        if not username:
            return {'status': 'error', 'message': 'Usuário não autenticado'}
        
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        
        rooms = self.db.list_chat_rooms(username)
        room_list = []
        for room in rooms:
            room_list.append({
                'room_id': room[0],
                'name': room[1],
                'moderator': room[2],
                'created_at': room[3],
                'is_member': bool(room[4])
            })
        
        return {'status': 'success', 'rooms': room_list}

    def handle_get_room_members(self, request, username):
        """Lista membros da sala"""
        if not username:
            return {'status': 'error', 'message': 'Usuário não autenticado'}
        
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        
        room_id = request.get('room_id')
        if not room_id:
            return {'status': 'error', 'message': 'ID da sala é obrigatório'}
        
        members = self.db.get_room_members(room_id, username)
        if members is None:
            return {'status': 'error', 'message': 'Sala não encontrada ou acesso negado'}
        
        member_list = []
        for member in members:
            member_list.append({
                'username': member[0],
                'joined_at': member[1]
            })
        
        return {'status': 'success', 'members': member_list}

    def handle_get_room_info(self, request, username):
        """Obtém informações da sala"""
        if not username:
            return {'status': 'error', 'message': 'Usuário não autenticado'}
        
        is_active, msg = self.verify_active_peer(username)
        if not is_active:
            return {'status': 'error', 'message': msg}
        
        room_id = request.get('room_id')
        if not room_id:
            return {'status': 'error', 'message': 'ID da sala é obrigatório'}
        
        info = self.db.get_room_info(room_id, username)
        if info is None:
            return {'status': 'error', 'message': 'Sala não encontrada ou acesso negado'}
        
        return {
            'status': 'success',
            'room_info': {
                'name': info[0],
                'moderator': info[1],
                'created_at': info[2],
                'max_history': info[3]
            }
        }