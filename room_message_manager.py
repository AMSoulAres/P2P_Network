import json
import hashlib
import os
import socket
import threading
import time
from datetime import datetime
import socket

class RoomMessageManager:
    """
    Gerencia mensagens de sala de chat com armazenamento distribuído.
    """
    
    def __init__(self, peer):
        self.peer = peer
        self.messages_dir = "room_messages"
        self.sync_lock = threading.Lock()
        self._syncing_rooms = set()  # Controle de sincronização concorrente
        
        os.makedirs(self.messages_dir, exist_ok=True)
        
        # Cache de mensagens em memória para salas ativas
        self.message_cache = {}  # room_id -> {"messages": [], "last_sync": timestamp}
        
        # Thread para sincronização periódica
        self.sync_thread = threading.Thread(target=self.sync_messages_periodically, daemon=True)
        self.sync_thread.start()
    
    def get_room_file_path(self, room_id):
        """Retorna o caminho do arquivo de mensagens da sala"""
        return os.path.join(self.messages_dir, f"room_{room_id}.json")
    
    def create_message_hash(self, room_id, sender, message, timestamp):
        """Cria hash único para a mensagem"""
        message_data = f"{room_id}:{sender}:{message}:{timestamp}"
        return hashlib.sha256(message_data.encode()).hexdigest()
    
    def save_message(self, room_id, sender, message, room_info = None):
        """
        Salva uma mensagem na sala.
        
        Returns:
            (sucesso, hash_da_mensagem, mensagem_de_erro)
        """
        timestamp = datetime.now().isoformat()
        message_hash = self.create_message_hash(room_id, sender, message, timestamp)
        
        message_obj = {
            "hash": message_hash,
            "room_id": room_id,
            "sender": sender,
            "message": message,
            "timestamp": timestamp
        }
        
        try:
            return self._save_message_distributed(room_id, message_obj)
        except Exception as e:
            return False, "", f"Erro ao salvar mensagem: {e}"

    def _save_message_distributed(self, room_id, message_obj):
        """Salva mensagem no modo distribuído (todos os membros)"""
        with self.sync_lock:
            messages = self._load_messages_from_file(room_id)
            
            # Verificar duplicata
            if any(msg.get('hash') == message_obj['hash'] for msg in messages):
                return True, message_obj['hash'], ""
            
            messages.append(message_obj)
            
            # Ordenar por timestamp
            messages.sort(key=lambda x: x.get('timestamp', ''))
            
            # Atualizar cache
            self.message_cache[room_id] = {
                "messages": messages.copy(),
                "last_sync": time.time()
            }
            
            # Salvar no arquivo
            self._save_messages_to_file(room_id, messages)
            
        return True, message_obj['hash'], ""
    
    def get_messages(self, room_id, limit = 50):
        """Obtém mensagens da sala"""
        with self.sync_lock:
            # Verificar cache primeiro
            if room_id in self.message_cache:
                cached = self.message_cache[room_id]
                # Cache válido por 30 segundos
                if time.time() - cached["last_sync"] < 30:
                    messages = cached["messages"]
                    return messages[-limit:] if limit > 0 else messages
            
            # Carregar do arquivo
            messages = self._load_messages_from_file(room_id)
            
            # Atualizar cache
            self.message_cache[room_id] = {
                "messages": messages.copy(),
                "last_sync": time.time()
            }
            
            return messages[-limit:] if limit > 0 else messages
    
    def _load_messages_from_file(self, room_id):
        """Carrega mensagens do arquivo JSON"""
        file_path = self.get_room_file_path(room_id)
        
        if not os.path.exists(file_path):
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('messages', [])
        except (json.JSONDecodeError, IOError):
            return []
    
    def _save_messages_to_file(self, room_id, messages):
        """Salva mensagens no arquivo JSON"""
        file_path = self.get_room_file_path(room_id)
        
        data = {
            "room_id": room_id,
            "last_updated": datetime.now().isoformat(),
            "message_count": len(messages),
            "messages": messages
        }
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Erro ao salvar mensagens: {e}")
    
    def sync_with_peers(self, room_id, member_list):
        """Sincroniza mensagens com outros peers (modo distribuído) com controle de concorrência"""
        # Evitar sincronização simultânea da mesma sala
        sync_key = f"sync_{room_id}"
        if hasattr(self, '_syncing_rooms'):
            if sync_key in self._syncing_rooms:
                return  # Já está sincronizando esta sala
        else:
            self._syncing_rooms = set()
        
        self._syncing_rooms.add(sync_key)
        
        try:
            current_messages = self._load_messages_from_file(room_id)
            current_hashes = {msg.get('hash') for msg in current_messages}
            new_messages_added = False
            
            for member in member_list:
                if member == self.peer.username:
                    continue
                
                try:
                    # Solicitar mensagens do peer
                    peer_messages = self._request_messages_from_peer(room_id, member)
                    
                    # Mesclar mensagens
                    for msg in peer_messages:
                        if msg.get('hash') not in current_hashes:
                            current_messages.append(msg)
                            current_hashes.add(msg.get('hash'))
                            new_messages_added = True
                    
                except Exception as e:
                    # Log silencioso para evitar spam
                    pass
            
            # Salvar apenas se houve mudanças
            if new_messages_added and current_messages:
                current_messages.sort(key=lambda x: x.get('timestamp', ''))
                
                with self.sync_lock:
                    self._save_messages_to_file(room_id, current_messages)
                    self.message_cache[room_id] = {
                        "messages": current_messages.copy(),
                        "last_sync": time.time()
                    }
        
        finally:
            self._syncing_rooms.discard(sync_key)
    
    def _request_messages_from_peer(self, room_id, peer_username):
        """Solicita mensagens de outro peer com retry e timeouts melhorados"""
        max_retries = 2
        
        for attempt in range(max_retries):
            try:
                # Verificar se o peer está online primeiro
                online_request = {'method': 'list_online_users'}
                online_response = self.peer.send_request(online_request)
                
                if not (online_response and online_response.get('status') == 'success'):
                    return []
                
                online_users = online_response.get('users', [])
                if peer_username not in online_users:
                    # Peer offline, não tentar conectar
                    return []
                
                # Obter endereço do peer
                request = {'method': 'get_peer_chat_address', 'username': peer_username}
                response = self.peer.send_request(request)
                
                if not (response and response.get('status') == 'success'):
                    return []
                
                ip, port = response['ip'], response['port']
                
                # Conectar ao peer com timeout maior
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(30)  # Timeout generoso para conexão
                sock.connect((ip, port))
                
                # Solicitar mensagens
                sync_request = {
                    "action": "sync_room_messages",
                    "room_id": room_id,
                    "requester": self.peer.username
                }
                
                sock.sendall(json.dumps(sync_request).encode() + b'\n')
                
                # Receber resposta com timeout maior
                sock.settimeout(20)  # Timeout generoso para recebimento
                response_data = sock.recv(16384).decode().strip()  # Buffer maior
                response = json.loads(response_data)
                
                sock.close()
                
                if response.get('status') == 'success':
                    return response.get('messages', [])
                
                # Se chegou aqui, tente novamente
                if attempt < max_retries - 1:
                    time.sleep(1)  # Esperar antes de retry
                    continue
                
            except (socket.timeout, ConnectionRefusedError, OSError) as e:
                # Silenciar timeouts comuns para não fazer spam no console
                if attempt < max_retries - 1:
                    time.sleep(2)  # Aguardar antes de tentar novamente
                    continue
            except Exception as e:
                # Outros erros - apenas no último retry
                if attempt == max_retries - 1:
                    print(f"Erro ao solicitar mensagens de {peer_username}: {e}")
                break
        
        return []
    
    def sync_messages_periodically(self):
        """Thread para sincronização periódica (modo distribuído) com controle inteligente"""
        sync_interval = 120  # Sincronizar a cada 2 minutos ao invés de 60 segundos
        last_sync_times = {}  # room_id -> timestamp da última sync
        
        while True:
            try:
                time.sleep(sync_interval)
                
                # Sincronizar apenas salas com atividade recente
                current_time = time.time()
                
                for room_id in list(self.message_cache.keys()):
                    # Verificar se a sala teve atividade recente (últimos 5 minutos)
                    cache_info = self.message_cache.get(room_id, {})
                    last_activity = cache_info.get('last_sync', 0)
                    
                    # Só sincronizar se:
                    # 1. Houve atividade nos últimos 5 minutos OU
                    # 2. Não sincronizou nos últimos 10 minutos
                    time_since_activity = current_time - last_activity
                    last_room_sync = last_sync_times.get(room_id, 0)
                    time_since_sync = current_time - last_room_sync
                    
                    should_sync = (time_since_activity < 300) or (time_since_sync > 600)
                    
                    if should_sync:
                        # Obter membros da sala (com timeout)
                        try:
                            request = {'method': 'get_room_members', 'room_id': room_id}
                            response = self.peer.send_request(request)
                            
                            if response and response.get('status') == 'success':
                                members = [m.get('username') for m in response.get('members', [])]
                                # Sincronizar apenas com membros ativos limitados
                                active_members = members[:3]  # Limitar a 3 peers por sync
                                
                                # Sincronização assíncrona para não bloquear
                                threading.Thread(
                                    target=self._sync_room_async,
                                    args=(room_id, active_members),
                                    daemon=True
                                ).start()
                                
                                last_sync_times[room_id] = current_time
                        except Exception as e:
                            print(f"Erro ao obter membros para sincronização de {room_id}: {e}")
                
            except Exception as e:
                print(f"Erro na sincronização periódica: {e}")
    
    def _sync_room_async(self, room_id, member_list):
        """Sincronização assíncrona de uma sala específica"""
        try:
            # Usar um subset dos membros para evitar sobrecarga
            import random
            if len(member_list) > 2:
                member_list = random.sample(member_list, 2)
            
            self.sync_with_peers(room_id, member_list)
        except Exception as e:
            print(f"Erro na sincronização assíncrona de {room_id}: {e}")
    
    def handle_peer_message_request(self, request):
        """Processa solicitações relacionadas a mensagens de outros peers"""
        action = request.get('action')
        
        if action == "sync_room_messages":
            room_id = request.get('room_id')
            requester = request.get('requester')
            
            # Verificar se o solicitante é membro da sala
            room_request = {'method': 'get_room_members', 'room_id': room_id}
            response = self.peer.send_request(room_request)
            
            if response and response.get('status') == 'success':
                members = [m.get('username') for m in response.get('members', [])]
                if requester in members:
                    messages = self.get_messages(room_id, limit=0)  # Todas as mensagens
                    return {'status': 'success', 'messages': messages}
            
            return {'status': 'error', 'message': 'Acesso negado'}
        
        elif action == "save_room_message":
            return {'status': 'error', 'message': 'Ação não suportada'}
        
        return {'status': 'error', 'message': 'Ação desconhecida'}
    
    def clear_room_cache(self, room_id):
        """Remove cache de mensagens da sala"""
        with self.sync_lock:
            self.message_cache.pop(room_id, None)
    
    def delete_room_messages(self, room_id):
        """Remove arquivo de mensagens da sala"""
        file_path = self.get_room_file_path(room_id)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
            self.clear_room_cache(room_id)
        except OSError as e:
            print(f"Erro ao remover arquivo de mensagens: {e}")
