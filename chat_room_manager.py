import json
import os
import socket
import threading
import time
import hashlib
import json
import socket
import threading
import os
import time
from datetime import datetime
from room_message_manager import RoomMessageManager

class ChatRoomManager:
    def __init__(self, peer):
        self.peer = peer
        self.active_rooms = {}  # room_id -> room_info
        self.room_members = {}  # room_id -> {username -> address}
        self.room_connections = {}  # room_id -> {username -> socket}
        self.room_lock = threading.Lock()
        self.chat_logs_dir = "chat_logs"
        os.makedirs(self.chat_logs_dir, exist_ok=True)
        self.message_manager = RoomMessageManager(peer)
        
        # Thread para sincronização periódica
        self.sync_thread = threading.Thread(target=self.sync_rooms_periodically, daemon=True)
        self.sync_thread.start()

    def create_room(self, room_id, max_history=100):
        """Cria uma nova sala de chat"""
        request = {
            'method': 'create_room',
            'room_id': room_id,
            'max_history': max_history
        }
        response = self.peer.send_request(request)
        
        if response and response.get('status') == 'success':
            # Criar arquivo de log local
            self.create_room_log_file(room_id)
            return True, response.get('message')
        else:
            return False, response.get('message', 'Erro desconhecido')

    def delete_room(self, room_id):
        """Remove uma sala de chat (apenas moderador)"""
        request = {
            'method': 'delete_room',
            'room_id': room_id
        }
        response = self.peer.send_request(request)
        
        if response and response.get('status') == 'success':
            # Limpar dados locais
            with self.room_lock:
                self.active_rooms.pop(room_id, None)
                self.room_members.pop(room_id, None)
                self.room_connections.pop(room_id, None)
            
            # Limpar mensagens locais
            self.message_manager.delete_room_messages(room_id)
            
            return True, response.get('message')
        else:
            return False, response.get('message', 'Erro desconhecido')

    def add_member(self, room_id, username):
        """Adiciona um membro à sala (apenas moderador)"""
        request = {
            'method': 'add_member',
            'room_id': room_id,
            'username': username
        }
        response = self.peer.send_request(request)
        
        if response and response.get('status') == 'success':
            # Atualizar lista de membros
            self.refresh_room_members(room_id)
            return True, response.get('message')
        else:
            return False, response.get('message', 'Erro desconhecido')

    def remove_member(self, room_id, username):
        """Remove um membro da sala"""
        request = {
            'method': 'remove_member',
            'room_id': room_id,
            'username': username
        }
        response = self.peer.send_request(request)
        
        if response and response.get('status') == 'success':
            # Atualizar lista de membros
            self.refresh_room_members(room_id)
            return True, response.get('message')
        else:
            return False, response.get('message', 'Erro desconhecido')

    def list_rooms(self):
        """Lista todas as salas disponíveis"""
        request = {'method': 'list_rooms'}
        response = self.peer.send_request(request)
        
        if response and response.get('status') == 'success':
            return True, response.get('rooms', [])
        else:
            return False, response.get('message', 'Erro desconhecido')

    def join_room(self, room_id):
        """Ingressa em uma sala (atualiza dados locais)"""
        request = {'method': 'get_room_info', 'room_id': room_id}
        response = self.peer.send_request(request)
        
        if not (response and response.get('status') == 'success'):
            return False, response.get('message', 'Sala não encontrada')
        
        room_info = response.get('room_info')
        
        with self.room_lock:
            self.active_rooms[room_id] = room_info
        
        # Obter membros da sala
        success, members = self.get_room_members(room_id)
        if success:
            with self.room_lock:
                self.room_members[room_id] = {}
                for member in members:
                    self.room_members[room_id][member['username']] = None
        
        # Carregar histórico de mensagens
        self.load_room_history(room_id)
        
        # Criar arquivo de log se não existir
        self.create_room_log_file(room_id)
        
        return True, f"Ingressou na sala {room_info['room_id']}"

    def leave_room(self, room_id):
        """Sai de uma sala"""
        success, message = self.remove_member(room_id, self.peer.username)
        
        if success:
            with self.room_lock:
                self.active_rooms.pop(room_id, None)
                self.room_members.pop(room_id, None)
                # Fechar conexões da sala
                if room_id in self.room_connections:
                    for sock in self.room_connections[room_id].values():
                        try:
                            sock.close()
                        except:
                            pass
                    self.room_connections.pop(room_id, None)
        
        return success, message

    def get_room_members(self, room_id):
        """Obtém lista de membros da sala"""
        request = {'method': 'get_room_members', 'room_id': room_id}
        response = self.peer.send_request(request)
        
        if response and response.get('status') == 'success':
            return True, response.get('members', [])
        else:
            return False, response.get('message', 'Erro desconhecido')

    def send_message(self, room_id, message):
        """Envia mensagem para a sala"""
        request = {'method': 'get_room_info', 'room_id': room_id}
        response = self.peer.send_request(request)
        
        if not (response and response.get('status') == 'success'):
            return False, "Erro ao obter informações da sala"
        
        room_info = response.get('room_info')

        # Salvar mensagem usando o gerenciador de mensagens
        success, message_hash, error = self.message_manager.save_message(
            room_id, self.peer.username, message, room_info
        )
        
        if not success:
            return False, error or "Erro ao salvar mensagem"
        
        # Broadcast para membros conectados
        self.broadcast_to_room(room_id, {
            'action': 'room_message',
            'room_id': room_id,
            'sender': self.peer.username,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'hash': message_hash
        })
        
        # Salvar no log local para compatibilidade
        self.save_message_to_log(room_id, self.peer.username, message, message_hash)
        
        return True, "Mensagem enviada"

    def broadcast_to_room(self, room_id, message_obj):
        """Envia mensagem para todos os membros conectados da sala"""
        if room_id not in self.room_members:
            return

        with self.room_lock:
            room_connections = self.room_connections.get(room_id, {})
        
        # Enviar para cada membro conectado
        for username in self.room_members[room_id]:
            if username == self.peer.username:
                continue  # Não enviar para si mesmo
            
            try:
                # Verificar se há conexão ativa
                if username not in room_connections:
                    # Tentar reconectar
                    if self.connect_to_member(room_id, username):
                        room_connections = self.room_connections.get(room_id, {})
                
                if username in room_connections:
                    sock = room_connections[username]
                    sock.sendall(json.dumps(message_obj).encode() + b'\n')
                else:
                    # Se não conseguiu conectar, tentar broadcast direto
                    self.send_direct_message(username, message_obj)
                    
            except Exception as e:
                print(f"Erro ao enviar mensagem para {username}: {e}")
                # Remover conexão inválida e tentar reconectar
                if username in room_connections:
                    room_connections.pop(username, None)
                # Tentar broadcast direto como fallback
                try:
                    self.send_direct_message(username, message_obj)
                except:
                    pass

    def connect_to_member(self, room_id, username):
        """Conecta a um membro da sala para chat direto"""
        try:
            # Obter endereço do chat do usuário
            request = {'method': 'get_peer_chat_address', 'username': username}
            response = self.peer.send_request(request)
            
            if not (response and response.get('status') == 'success'):
                return False
            
            ip, port = response['ip'], response['port']
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            
            with self.room_lock:
                if room_id not in self.room_connections:
                    self.room_connections[room_id] = {}
                self.room_connections[room_id][username] = sock
            
            # Iniciar thread para receber mensagens
            threading.Thread(
                target=self.receive_room_messages,
                args=(room_id, username, sock),
                daemon=True
            ).start()
            
            return True
        except Exception as e:
            print(f"Erro ao conectar com {username}: {e}")
            return False

    def receive_room_messages(self, room_id, username, sock):
        """Recebe mensagens de um membro da sala"""
        buffer = ""
        try:
            while True:
                data = sock.recv(4096).decode()
                if not data:
                    break
                
                buffer += data
                while '\n' in buffer:
                    message_str, buffer = buffer.split('\n', 1)
                    try:
                        msg_obj = json.loads(message_str)
                        
                        # Verificar se é uma solicitação de sincronização ou uma mensagem
                        action = msg_obj.get('action')
                        if action in ['sync_room_messages', 'save_room_message']:
                            # Processar solicitação de mensagens
                            response = self.message_manager.handle_peer_message_request(msg_obj)
                            sock.sendall(json.dumps(response).encode() + b'\n')
                        elif action == 'room_message' and msg_obj.get('room_id') == room_id:
                            # Processar mensagem da sala
                            self.process_room_message(msg_obj)
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            print(f"Erro ao receber mensagens de {username}: {e}")
        finally:
            sock.close()
            with self.room_lock:
                if room_id in self.room_connections and username in self.room_connections[room_id]:
                    self.room_connections[room_id].pop(username, None)

    def process_room_message(self, msg_obj):
        """Processa mensagem recebida da sala"""
        room_id = msg_obj.get('room_id')
        sender = msg_obj.get('sender')
        message = msg_obj.get('message')
        timestamp = msg_obj.get('timestamp')
        message_hash = msg_obj.get('hash')
        
        # Verificar se é uma sala ativa
        if room_id not in self.active_rooms:
            return
        
        if message_hash:
            message_obj_full = {
                "hash": message_hash,
                "room_id": room_id,
                "sender": sender,
                "message": message,
                "timestamp": timestamp
            }
            
            request = {'method': 'get_room_info', 'room_id': room_id}
            response = self.peer.send_request(request)
            
            if response and response.get('status') == 'success':
                room_info = response.get('room_info')
                self.message_manager._save_message_distributed(room_id, message_obj_full)
        
        # Salvar no log local para compatibilidade (evitar duplicatas)
        if not self.message_exists_in_log(room_id, message_hash):
            self.save_message_to_log(room_id, sender, message, message_hash, timestamp)
        
        print(f"\n[SALA {room_id}] {sender}: {message}\n{self.peer.prompt}", end='', flush=True)

    def load_room_history(self, room_id):
        """Carrega histórico de mensagens da sala"""
        messages = self.message_manager.get_messages(room_id, limit=50)
        
        # Salvar mensagens no log local para compatibilidade
        for msg in messages:
            sender = msg.get('sender')
            message = msg.get('message') 
            timestamp = msg.get('timestamp')
            message_hash = msg.get('hash')
            
            if sender and message and not self.message_exists_in_log(room_id, message_hash):
                self.save_message_to_log(room_id, sender, message, message_hash, timestamp)

    def refresh_room_members(self, room_id):
        """Atualiza lista de membros da sala"""
        success, members = self.get_room_members(room_id)
        if success:
            with self.room_lock:
                if room_id not in self.room_members:
                    self.room_members[room_id] = {}
                
                # Atualizar lista de membros
                current_members = set(self.room_members[room_id].keys())
                new_members = set(member['username'] for member in members)
                
                # Adicionar novos membros
                for username in new_members - current_members:
                    self.room_members[room_id][username] = None
                
                # Remover membros que saíram
                for username in current_members - new_members:
                    self.room_members[room_id].pop(username, None)
                    # Fechar conexão se existir
                    if room_id in self.room_connections and username in self.room_connections[room_id]:
                        try:
                            self.room_connections[room_id][username].close()
                        except:
                            pass
                        self.room_connections[room_id].pop(username, None)

    def sync_rooms_periodically(self):
        """Sincroniza salas periodicamente - otimizada para evitar conflitos"""
        while True:
            try:
                time.sleep(180)  # Sincronizar a cada 3 minutos
                with self.room_lock:
                    active_room_ids = list(self.active_rooms.keys())
                
                for room_id in active_room_ids:
                    try:
                        self.refresh_room_members(room_id) # Atualizar para quem transmitir
                        
                    except Exception as e:
                        pass
            except Exception as e:
                print(f"Erro na sincronização de salas: {e}")

    # Métodos para gerenciamento de logs locais
    
    def create_room_log_file(self, room_id):
        """Cria arquivo de log para a sala"""
        log_file = os.path.join(self.chat_logs_dir, f"room_{room_id}.log")
        if not os.path.exists(log_file):
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"# Log da sala - (ID: {room_id})\n")
                f.write(f"# Criado em: {datetime.now().isoformat()}\n\n")

    def save_message_to_log(self, room_id, sender, message, message_hash, timestamp=None):
        """Salva mensagem no log local"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
        
        log_file = os.path.join(self.chat_logs_dir, f"room_{room_id}.log")
        
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] {sender}: {message} (hash: {message_hash})\n")
        except Exception as e:
            print(f"Erro ao salvar log: {e}")

    def message_exists_in_log(self, room_id, message_hash):
        """Verifica se a mensagem já existe no log (evitar duplicatas)"""
        log_file = os.path.join(self.chat_logs_dir, f"room_{room_id}.log")
        
        if not os.path.exists(log_file):
            return False
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
                return message_hash in content
        except Exception:
            return False

    def get_room_log(self, room_id, lines=50):
        """Obtém últimas mensagens do log local"""
        log_file = os.path.join(self.chat_logs_dir, f"room_{room_id}.log")
        
        if not os.path.exists(log_file):
            return []
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                # Filtrar linhas de mensagens (não comentários) e remover hash
                message_lines = []
                for line in all_lines:
                    if line.startswith('['):
                        # Remover a parte do hash: (hash: xxxxx)
                        if ' (hash: ' in line:
                            clean_line = line.split(' (hash: ')[0] + '\n'
                            message_lines.append(clean_line)
                        else:
                            message_lines.append(line)
                return message_lines[-lines:] if lines > 0 else message_lines
        except Exception as e:
            print(f"Erro ao ler log: {e}")
            return []
    
    def show_room_history(self, room_id, limit=20):
        """Exibe histórico de mensagens da sala"""
        if room_id not in self.active_rooms:
            return False, "Sala não está ativa"
        
        messages = self.message_manager.get_messages(room_id, limit)
        
        if not messages:
            return True, "Nenhuma mensagem encontrada"

        print(f"\n=== HISTÓRICO DA SALA {room_id} (últimas {len(messages)} mensagens) ===")
        
        for msg in messages:
            sender = msg.get('sender', 'Desconhecido')
            message = msg.get('message', '')
            timestamp = msg.get('timestamp', '')
            
            # Formatar timestamp
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                time_str = dt.strftime('%d/%m %H:%M')
            except:
                time_str = timestamp[:16] if timestamp else ''
            
            print(f"[{time_str}] {sender}: {message}")
        
        print("=" * 50)
        return True, f"Histórico exibido ({len(messages)} mensagens)"

    def sync_room_with_peers(self, room_id):
        """Força sincronização de mensagens com outros peers"""
        if room_id not in self.active_rooms:
            return False, "Sala não está ativa"
        
        # Obter membros da sala
        success, members = self.get_room_members(room_id)
        if not success:
            return False, "Erro ao obter membros da sala"
        
        member_list = [m.get('username') for m in members if m.get('username')]
        
        # Sincronizar mensagens
        self.message_manager.sync_with_peers(room_id, member_list)
        return True, "Sincronização iniciada"

    def send_direct_message(self, username, message_obj):
        """Envia mensagem direta para um peer (fallback quando não há conexão persistente)"""
        try:
            # Obter endereço do peer
            request = {'method': 'get_peer_chat_address', 'username': username}
            response = self.peer.send_request(request)
            
            if not (response and response.get('status') == 'success'):
                return False
            
            ip, port = response['ip'], response['port']
            
            # Conectar e enviar mensagem diretamente
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # Timeout menor para envio direto
            sock.connect((ip, port))
            
            sock.sendall(json.dumps(message_obj).encode() + b'\n')
            sock.close()
            
            return True
        except Exception:
            return False
