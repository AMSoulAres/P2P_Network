import json
import hashlib
import os
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class RoomMessageManager:
    """
    Gerencia mensagens de sala de chat com armazenamento distribuído.
    
    Duas estratégias de armazenamento:
    1. Centralizado no moderador (padrão)
    2. Distribuído entre todos os membros (replicação)
    """
    
    def __init__(self, peer, storage_type="centralized"):
        self.peer = peer
        self.storage_type = storage_type  # "centralized" ou "distributed"
        self.messages_dir = "room_messages"
        self.sync_lock = threading.Lock()
        
        os.makedirs(self.messages_dir, exist_ok=True)
        
        # Cache de mensagens em memória para salas ativas
        self.message_cache = {}  # room_id -> {"messages": [], "last_sync": timestamp}
        
        # Thread para sincronização periódica (apenas para modo distribuído)
        if storage_type == "distributed":
            self.sync_thread = threading.Thread(target=self.sync_messages_periodically, daemon=True)
            self.sync_thread.start()
    
    def get_room_file_path(self, room_id: str) -> str:
        """Retorna o caminho do arquivo de mensagens da sala"""
        return os.path.join(self.messages_dir, f"room_{room_id}.json")
    
    def create_message_hash(self, room_id: str, sender: str, message: str, timestamp: str) -> str:
        """Cria hash único para a mensagem"""
        message_data = f"{room_id}:{sender}:{message}:{timestamp}"
        return hashlib.sha256(message_data.encode()).hexdigest()
    
    def save_message(self, room_id: str, sender: str, message: str, room_info: dict = None) -> Tuple[bool, str, str]:
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
            if self.storage_type == "centralized":
                return self._save_message_centralized(room_id, message_obj, room_info)
            else:
                return self._save_message_distributed(room_id, message_obj)
        except Exception as e:
            return False, "", f"Erro ao salvar mensagem: {e}"
    
    def _save_message_centralized(self, room_id: str, message_obj: dict, room_info: dict) -> Tuple[bool, str, str]:
        """Salva mensagem no modo centralizado (apenas moderador)"""
        # Verificar se é o moderador
        if not room_info or room_info.get('moderator') != self.peer.username:
            # Se não for moderador, enviar mensagem para o moderador salvar
            return self._send_message_to_moderator(room_id, message_obj, room_info)
        
        # É o moderador, salvar localmente
        with self.sync_lock:
            messages = self._load_messages_from_file(room_id)
            
            # Verificar duplicata
            if any(msg.get('hash') == message_obj['hash'] for msg in messages):
                return True, message_obj['hash'], ""
            
            messages.append(message_obj)
            
            # Gerenciar tamanho do histórico
            max_history = room_info.get('max_history', 100)
            if len(messages) > max_history:
                messages = messages[-max_history:]
            
            # Atualizar cache
            self.message_cache[room_id] = {
                "messages": messages.copy(),
                "last_sync": time.time()
            }
            
            # Salvar no arquivo
            self._save_messages_to_file(room_id, messages)
            
        return True, message_obj['hash'], ""
    
    def _save_message_distributed(self, room_id: str, message_obj: dict) -> Tuple[bool, str, str]:
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
    
    def _send_message_to_moderator(self, room_id: str, message_obj: dict, room_info: dict) -> Tuple[bool, str, str]:
        """Envia mensagem para o moderador salvar (modo centralizado)"""
        moderator = room_info.get('moderator')
        if not moderator:
            return False, "", "Moderador não encontrado"
        
        try:
            # Obter endereço do moderador
            request = {'method': 'get_peer_chat_address', 'username': moderator}
            response = self.peer.send_request(request)
            
            if not (response and response.get('status') == 'success'):
                return False, "", "Não foi possível conectar ao moderador"
            
            ip, port = response['ip'], response['port']
            
            # Conectar ao moderador e enviar mensagem
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((ip, port))
            
            # Enviar solicitação para salvar mensagem
            save_request = {
                "action": "save_room_message",
                "room_id": room_id,
                "message_obj": message_obj
            }
            
            sock.sendall(json.dumps(save_request).encode() + b'\n')
            
            # Receber confirmação
            response_data = sock.recv(1024).decode().strip()
            response = json.loads(response_data)
            
            sock.close()
            
            if response.get('status') == 'success':
                return True, message_obj['hash'], ""
            else:
                return False, "", response.get('message', 'Erro ao salvar no moderador')
                
        except Exception as e:
            return False, "", f"Erro ao comunicar com moderador: {e}"
    
    def get_messages(self, room_id: str, limit: int = 50) -> List[dict]:
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
    
    def _load_messages_from_file(self, room_id: str) -> List[dict]:
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
    
    def _save_messages_to_file(self, room_id: str, messages: List[dict]):
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
    
    def sync_with_peers(self, room_id: str, member_list: List[str]):
        """Sincroniza mensagens com outros peers (modo distribuído)"""
        if self.storage_type != "distributed":
            return
        
        current_messages = self._load_messages_from_file(room_id)
        current_hashes = {msg.get('hash') for msg in current_messages}
        
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
                
            except Exception as e:
                print(f"Erro ao sincronizar com {member}: {e}")
        
        # Ordenar e salvar mensagens mescladas
        if current_messages:
            current_messages.sort(key=lambda x: x.get('timestamp', ''))
            
            with self.sync_lock:
                self._save_messages_to_file(room_id, current_messages)
                self.message_cache[room_id] = {
                    "messages": current_messages.copy(),
                    "last_sync": time.time()
                }
    
    def _request_messages_from_peer(self, room_id: str, peer_username: str) -> List[dict]:
        """Solicita mensagens de outro peer"""
        try:
            # Obter endereço do peer
            request = {'method': 'get_peer_chat_address', 'username': peer_username}
            response = self.peer.send_request(request)
            
            if not (response and response.get('status') == 'success'):
                return []
            
            ip, port = response['ip'], response['port']
            
            # Conectar ao peer
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((ip, port))
            
            # Solicitar mensagens
            sync_request = {
                "action": "sync_room_messages",
                "room_id": room_id,
                "requester": self.peer.username
            }
            
            sock.sendall(json.dumps(sync_request).encode() + b'\n')
            
            # Receber resposta
            response_data = sock.recv(8192).decode().strip()
            response = json.loads(response_data)
            
            sock.close()
            
            if response.get('status') == 'success':
                return response.get('messages', [])
            
        except Exception as e:
            print(f"Erro ao solicitar mensagens de {peer_username}: {e}")
        
        return []
    
    def sync_messages_periodically(self):
        """Thread para sincronização periódica (modo distribuído)"""
        while True:
            try:
                time.sleep(30)  # Sincronizar a cada 30 segundos
                
                # Sincronizar salas ativas
                for room_id in list(self.message_cache.keys()):
                    # Obter membros da sala
                    request = {'method': 'get_room_members', 'room_id': room_id}
                    response = self.peer.send_request(request)
                    
                    if response and response.get('status') == 'success':
                        members = [m.get('username') for m in response.get('members', [])]
                        self.sync_with_peers(room_id, members)
                
            except Exception as e:
                print(f"Erro na sincronização periódica: {e}")
    
    def handle_peer_message_request(self, request: dict) -> dict:
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
            # Apenas para modo centralizado
            if self.storage_type == "centralized":
                room_id = request.get('room_id')
                message_obj = request.get('message_obj')
                
                # Obter informações da sala
                room_request = {'method': 'get_room_info', 'room_id': room_id}
                room_response = self.peer.send_request(room_request)
                
                if room_response and room_response.get('status') == 'success':
                    room_info = room_response.get('room_info')
                    success, msg_hash, error = self._save_message_centralized(room_id, message_obj, room_info)
                    
                    if success:
                        return {'status': 'success', 'hash': msg_hash}
                    else:
                        return {'status': 'error', 'message': error}
            
            return {'status': 'error', 'message': 'Ação não suportada'}
        
        return {'status': 'error', 'message': 'Ação desconhecida'}
    
    def clear_room_cache(self, room_id: str):
        """Remove cache de mensagens da sala"""
        with self.sync_lock:
            self.message_cache.pop(room_id, None)
    
    def delete_room_messages(self, room_id: str):
        """Remove arquivo de mensagens da sala"""
        file_path = self.get_room_file_path(room_id)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
            self.clear_room_cache(room_id)
        except OSError as e:
            print(f"Erro ao remover arquivo de mensagens: {e}")
