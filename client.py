import cmd
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import math
import os
import random
import shutil
import socket
import json
import hashlib
from getpass import getpass
import threading
import time
import traceback
from chat_room_manager import ChatRoomManager

CHUNK_SIZE = 1024*1024  # 1MB, tamanho do chunk para download
HEARTBEAT_INTERVAL = 60  # segundos
SCORE_DIVIDER = 1000  # Divisor para calcular o score

class Peer(cmd.Cmd):
    prompt = 'peer> '
    def __init__(self, tracker_host, tracker_port):
        super().__init__()
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.sock = None
        self.logged_in = False
        self.username = None
        self.chunks_served_since_last_heartbeat = 0
        self.base_connections = 2 
        self.score = 0

        self.shared_files = {}  # arquivos completos compartilhados
        self.downloading_files = {}  # arquivos com download em andamento (para seed parcial)
        self.download_dir = "downloads"
        os.makedirs(self.download_dir, exist_ok=True)

        # Iniciar servidor para receber solicitações de chunks
        self.peer_port = random.randint(50000, 60000) # Porta aleatória para evitar conflitos em localhost
        threading.Thread(target=self.start_chunk_server, daemon=True).start()

        # Iniciar servidor de CHAT em porta separada
        self.chat_port = random.randint(40000, 50000)
        threading.Thread(target=self.start_chat_server, daemon=True).start()

        self.download_lock = threading.Lock()  # Lock para sincronizar dados de download
        
        self.chat_connections = {}
        self.chat_lock = threading.Lock()
        self.chat_target_user = None

        # Sistema de salas de chat
        self.room_manager = ChatRoomManager(self)
        self.current_room = None

        self.connect_to_tracker()

    def connect_to_tracker(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.tracker_host, self.tracker_port))
            print("Digite 'help' para visualizar todos os comandos disponíveis")
        except Exception as e:
            print(f"Falha ao conectar ao tracker: {e}")
            self.sock = None

    def start_heartbeat(self):
        while self.logged_in:
            # Coletar métricas
            time_online = HEARTBEAT_INTERVAL
            metrics = {
                'chunks_served': self.chunks_served_since_last_heartbeat,
                'time_online': time_online
            }
            hashes = list(self.shared_files.keys()) + list(self.downloading_files.keys())
            response = self.send_request({
                'method': 'heartbeat',
                'file_hashes': hashes,
                'metrics': metrics
            })

            # Resetar contadores
            self.chunks_serve_since_last_heartbeat = 0

            if response and 'score' in response:
                self.score = response['score']
            time.sleep(HEARTBEAT_INTERVAL) # Vai rodar numa thread separada, então não bloqueia o loop principal

    def start_chunk_server(self):
        hostname = socket.gethostname()
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', self.peer_port))
        server_socket.listen(5)
        print(f"Servidor de chunks ouvindo na porta {self.peer_port}")
        
        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(target=self.handle_peer_request, args=(client_socket,)).start()

    def send_request(self, request):
        """Envia uma requisição para o tracker e retorna a resposta"""
        if not self.sock:
            print("Não conectado ao tracker")
            return None
        try:
            message = json.dumps(request) + '\n'
            self.sock.sendall(message.encode())
            response = ''
            while True:
                data = self.sock.recv(CHUNK_SIZE).decode()
                if not data:
                    break
                response += data
                if '\n' in response:
                    response, _ = response.split('\n', 1)  # separar a resposta JSON
                    break
            return json.loads(response)
        except Exception as e:
            print(f"Erro na comunicação com o tracker: {e}")
            return None

    # O prefixo 'do_' é necessário para que o cmd reconheça os métodos como comandos
    def do_register(self, arg):
        """
        Registra um novo usuário no tracker

        Uso: register <usuário> <senha>
             Ou apenas 'register' para solicitar usuário e senha
        """
        if self.logged_in:
            print("Usuário já autenticado")
            return

        if not arg.strip():
            username = input("Username: ")
            password = getpass("Password: ")
            if not username or not password:
                print("Usuário ou senha não podem ser vazios")
                return
        else:
            args = arg.split()
            if len(args) != 2:
                print("Uso: 'register <usuário> <senha>' ou apenas 'register'")
                return
            username, password = args

        request = {
            'method': 'register',
            'username': username,
            'password': password,
            # 'ip': socket.gethostbyname(socket.gethostname()),
            'ip': '0.0.0.0',
            'port': self.peer_port,
            'chat_port': self.chat_port
        }

        response = self.send_request(request)
        if response:
            if response.get('status') == 'success':
                print("Usuário registrado com sucesso")
            else:
                print(response.get('message'))

    def do_login(self, arg):
        """
        Faz login no tracker
        
        Uso: login <usuário> <senha>
             Ou apenas 'login' para solicitar usuário e senha
        """
        if self.logged_in:
            print("Usuário já autenticado")
            return

        if not arg.strip():
            username = input("Username: ")
            password = getpass("Password: ")
            if not username or not password:
                print("Usuário ou senha não podem ser vazios")
                return
        else:
            args = arg.split()
            if len(args) != 2:
                print("Uso: login <usuário> <senha> ou apenas 'login'")
                return
            username, password = args
        

        request = {
            'method': 'login',
            'username': username,
            'password': password,
            # 'ip': socket.gethostbyname(socket.gethostname()),
            'ip': '0.0.0.0',
            'port': self.peer_port,
            'chat_port': self.chat_port
        }

        response = self.send_request(request)
        if response:
            if response.get('status') == 'success':
                self.logged_in = True
                self.username = username
                self.download_dir = os.path.join(self.download_dir, username)
                os.makedirs(self.download_dir, exist_ok=True)
                print("Login bem-sucedido. Salve, " + username)
                threading.Thread(target=self.start_heartbeat, daemon=True).start()
                self.announce_all_files()
            else:
                print(response.get('message'))

    def do_announce(self, arg):
        """
        Anuncia um arquivo para o tracker
        
        Uso: announce <caminho do arquivo>
             Ou apenas 'announce' para solicitar o caminho do arquivo
        """
        if not arg.strip():
            path = input("Caminho para o arquivo (path): ")
            if not path:
                print("Caminho do arquivo não pode ser vazio")
                return
        else:
            args = arg.split()
            if len(args) != 1:
                print("Uso: announce <caminho do arquivo>")
                return
            path = args[0]
        
        if not self.logged_in:
            print("Autentique-se primeiro")
            return

        # Verifica se o arquivo existe e lê os dados
        # Se o arquivo não existir, retorna uma mensagem de erro
        # Se o arquivo existir, lê os dados e calcula o hash
        try:
            if not os.path.isfile(path):
                print("Arquivo não encontrado")
                return
            name = os.path.basename(path)
            size = os.path.getsize(path) # Tamanho do arquivo em bytes
            file_hash, chunk_hashes = self.compute_file_checksum(path)
        
            # Armazenar localmente metadados do arquivo (para upload de chunks)
            if file_hash in self.shared_files:
                print("INFO: Arquivo listado anteriormente")
            else:
                self.shared_files[file_hash] = {
                    'path': path,
                    'chunks': chunk_hashes,
                    'size': size,
                    'name': name
                }
            print(f"Anunciando arquivo: {name} ({size} bytes, hash: {file_hash}, chunks: {len(chunk_hashes)})")
            
            # Enviar anúncio para tracker
            request = {
                'method': 'announce',
                'name': name,
                'size': size,
                'hash': file_hash,
                'chunks': chunk_hashes  # (necessário, pois quem faz o download precisa saber quais chunks existem para baixar)
            }
            response = self.send_request(request)
            if response:
                print(response.get('message', 'Erro desconhecido'))
        except PermissionError:
            print(f"Permissão negada para ler o arquivo: {path}")
        except Exception as e:
            print(f"Erro ao processar arquivo: {str(e)}")

    def do_exit(self, arg):
        """
        Encerra o cliente (sem argumentos)
        """
        print("Saindo...")
        if self.sock:
            self.sock.close()
        return True

    def do_list_files(self, arg):
        """Lista os arquivos disponíveis para download"""
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        request = {'method': 'list_files'}
        response = self.send_request(request)
        
        if not response or response.get('status') != 'success':
            print("Erro ao obter lista de arquivos:", response.get('message', 'Erro desconhecido'))
            return
        
        files = response.get('files', [])
        if not files:
            print("Nenhum arquivo disponível")
            return
        
        print("Arquivos disponíveis:")
        for file in files:
            print(f"{file['name']} (hash: {file['hash']}, tamanho: {file['size']/CHUNK_SIZE:.2f} MB)")

    def do_download(self, arg):
        """Baixa o arquivo especificado da rede P2P.
        
        Uso: download <file_hash>"""
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Digite o hash do arquivo")
            return
        file_hash = args[0]
        
        start_time = time.time() # Para medir o tempo de download

        # Obter lista de peers com o arquivo
        request = {'method': 'get_peers', 'file_hash': file_hash}
        response = self.send_request(request)
        
        if not response or response.get('status') != 'success':
            print("Erro ao obter peers:", response.get('message', 'Erro desconhecido'))
            return
        
        peers = response.get('peers', [])
        peers = [tuple(peer) for peer in peers if peer[2] != self.username] # Excluir o próprio peer da lista de peers

        if not peers:
            print("Nenhum peer disponível com este arquivo")
            return
        
        print(f"Encontrados {len(peers)} peers com o arquivo.")

        # Obter metadados do arquivo
        request = {'method': 'get_file_metadata', 'file_hash': file_hash}
        response = self.send_request(request)
        
        if not response or response.get('status') != 'success':
            print("Erro ao obter metadados:", response.get('message', 'Erro desconhecido'))
            return
        
        metadata = response.get('metadata', {})
        num_chunks = len(metadata.get('chunk_hashes', []))
        file_name = metadata.get('name', file_hash)
        
        if num_chunks == 0:
            print("Arquivo sem chunks disponíveis, impossível fazer download")
            return
        
        # Preparar download
        download_path = os.path.join(self.download_dir, file_name)
        temp_dir = os.path.join(self.download_dir, f"temp_{file_hash}")
        os.makedirs(temp_dir, exist_ok=True)

        # Registrar arquivo em download (para seed parcial)
        self.downloading_files[file_hash] = {'temp_dir': temp_dir, 'chunks': []}
        
        # Baixar chunks em paralelo
        print(f"Iniciando download de {file_name} ({num_chunks} chunks)")
        start_time = time.time()
        
        # Obter disponibilidade de chunks entre peers
        chunk_availability = self.get_chunk_availability(file_hash, peers)
        max_workers = self.base_connections + math.floor(self.score // SCORE_DIVIDER)  # Ajustar número de workers dinamicamente (incentivo por score)
        if max_workers > 15:
            max_workers = 15
        print(f"Usando até {max_workers} conexões simultâneas (score: {self.score})")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            # Determinar ordem de download dos chunks
            # Ordenar chunks pela raridade (menos peers primeiro)
            chunks_order = sorted(range(num_chunks), key=lambda i: len(chunk_availability.get(i, [])))
            print("\nOrdem de download dos chunks (raridade): \n")
            print(chunks_order)
            
            for chunk_index in chunks_order:
                available_peers = chunk_availability.get(chunk_index, [])
                if not available_peers:
                    print(f"\nNenhum peer possui o chunk {chunk_index}")
                    continue
            
                futures.append(executor.submit(
                    self.download_chunk_w_retry,
                    file_hash,
                    chunk_index,
                    metadata['chunk_hashes'][chunk_index],
                    available_peers, # Responsabilidade do download_chunk_w_retry escolher peers
                    temp_dir
                ))

            # Aguardar conclusão
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Erro no download: {str(e.with_traceback())}")
        
        # Montar arquivo final
        if self.assemble_file(file_hash, metadata['chunk_hashes'], temp_dir, download_path):
            download_time = time.time() - start_time
            file_size = os.path.getsize(download_path)
            print(f"Arquivo salvo em: {download_path}")
            # Adicionar arquivo completo aos compartilhados
            self.shared_files[file_hash] = {
                'path': download_path,
                'chunks': metadata['chunk_hashes'],
                'size': metadata.get('size'),
                'name': file_name
            }
        else:
            print("\nFalha ao montar arquivo final")
            self.downloading_files.pop(file_hash, None)  # Remover download em andamento
            self.shared_files.pop(file_hash, None)  # Remover arquivo incompleto
        # Limpar diretório temporário
        shutil.rmtree(temp_dir, ignore_errors=True)
        self.downloading_files.pop(file_hash, None)  # Remover download em andamento

        end_time = time.time()
        download_time = end_time - start_time
        speed = file_size / download_time if download_time > 0 else 0
        success = os.path.exists(download_path)
        print(f"Download {'completo' if success else 'falhou'} - {file_size/CHUNK_SIZE:.2f} MB em {download_time:.2f} segundos ({speed/CHUNK_SIZE:.2f} MB/s)")

    def get_chunk_availability(self, file_hash, peers):
        """Consulta cada peer para saber quais chunks do arquivo ele possui.
           Retorna um dicionário {chunk_index: [lista_de_peers_com_chunk]}"""
        availability = {}
        for peer in peers:
            peer_ip, peer_port, _, _ = peer
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((peer_ip, peer_port))
                    # Solicitar lista de chunks
                    request = {
                        'action': 'list_chunks',
                        'file_hash': file_hash
                    }
                    s.sendall(json.dumps(request).encode() + b'\n')
                    # Ler resposta JSON
                    data = ""
                    while True:
                        part = s.recv(4096).decode()
                        if not part:
                            break
                        data += part
                        if '\n' in data:
                            break

                    if '\n' in data:
                        message, _ = data.split('\n', 1)
                    else:
                        message = data
                    response = json.loads(message)
                    if response.get('status') == 'success':
                        chunks = response.get('chunks', [])
                        for chunk in chunks:
                            availability.setdefault(chunk, []).append(peer)
            except Exception as e:
                print(f"Erro ao consultar peer {peer_ip}:{peer_port} para chunks do arquivo {file_hash}")
                continue
                
        return availability
    
    def do_list_peers(self, arg):
        """
        Lista os peers disponíveis no tracker
        
        Uso: list_peers <file_hash>
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Digite o hash do arquivo")
            return
        file_hash = args[0]
        
        request = {'method': 'get_peers', 'file_hash': file_hash}
        response = self.send_request(request)
        
        if not response or response.get('status') != 'success':
            print("Erro ao obter peers:", response.get('message', 'Erro desconhecido'))
            return
        
        peers = response.get('peers', [])
        peers = [tuple(peer) for peer in peers if peer[2] != self.username]

        if not peers:
            print("Nenhum peer disponível com este arquivo")
            return
        
        print(f"Encontrados {len(peers)} peers com o arquivo:")
        for peer in peers:
            print(f"{peer[0]}:{peer[1]} ({peer[2]})")

    # Utils
    def compute_file_checksum(self, file_name, chunk_size=CHUNK_SIZE):  # TODO: trocar para 1MB
        sha256 = hashlib.sha256()
        chunk_hashes = []
        
        with open(file_name, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunk_hash = hashlib.sha256(chunk).hexdigest()
                chunk_hashes.append(chunk_hash)
                sha256.update(chunk)
        
        return sha256.hexdigest(), chunk_hashes
    
    def download_chunk_w_retry(self, file_hash, chunk_index, expected_hash, peers_with_chunk, temp_dir):
        """
        Tenta baixar o chunk do arquivo de até dois peers diferentes.
        Se o primeiro peer falhar, tenta um segundo peer não na blacklist.
        """
        attempts = 0
        tried_peers = set() #soft blacklist para evitar tentar o mesmo peer várias vezes
        while attempts < 2:
            # Selecionar peer não tentado e não blacklisted
            candidates = [peer for peer in peers_with_chunk if peer not in tried_peers]
            if not candidates:
                break
            peer = random.choice(candidates)
            tried_peers.add(peer)
            success = self.download_chunk(file_hash, chunk_index, expected_hash, peer, temp_dir)
            if success:
                return True
            else:
                attempts += 1

        # Se chegou aqui, não conseguiu baixar de nenhum dos peers
        print(f"Erro: falha ao baixar chunk {chunk_index} de todos os peers disponíveis.")
        return False
    
    def download_chunk(self, file_hash, chunk_index, expected_hash, peer, temp_dir):
        peer_ip, peer_port, _, _ = peer
        chunk_file = os.path.join(temp_dir, f"{chunk_index}.chunk")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(50)
                s.connect((peer_ip, peer_port))
                
                # Solicitar chunk específico
                request = {
                    'action': 'get_chunk',
                    'file_hash': file_hash,
                    'chunk_index': chunk_index
                }
                s.sendall(json.dumps(request).encode() + b'\n')
                
                # Receber chunk
                chunk_data = b''
                remaining = CHUNK_SIZE
                while remaining > 0:
                    data = s.recv(min(4096, remaining))
                    if not data:
                        break
                    chunk_data += data
                    remaining -= len(data)

                # Só agora verifica o hash e salva o arquivo
                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                if chunk_hash != expected_hash:
                    print(f"Chunk {chunk_index} corrompido: esperado {expected_hash}, recebido {chunk_hash}")
                    return False

                with open(chunk_file, 'wb') as f:
                    f.write(chunk_data)

            # Atualizar estado do download para seed parcial
            self.download_lock.acquire()
            try:
                if file_hash in self.downloading_files:
                    self.downloading_files[file_hash]['chunks'].append(chunk_index)
                    if len(self.downloading_files[file_hash]['chunks']) == 1:
                        # Enviar anúncio para tracker
                        request = {
                            'method': 'partial_announce',
                            'file_hash': file_hash
                        }
                        self.send_request(request)
            finally:
                self.download_lock.release()
                return True
        except Exception as e:
            print(f"Erro ao baixar chunk {chunk_index} de {peer_ip}: {str(e)}")
            traceback.print_exc()
            return False

    def assemble_file(self, file_hash, chunk_hashes, temp_dir, output_path):
        try:
            num_chunks = len(chunk_hashes)
            with open(output_path, 'wb') as outfile:
                for i in range(num_chunks):
                    chunk_file = os.path.join(temp_dir, f"{i}.chunk")
                    
                    if not os.path.exists(chunk_file):
                        print(f"Chunk {i} faltando")
                        return False
                    
                    with open(chunk_file, 'rb') as infile:
                        chunk_data = infile.read()
                        chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                        
                        if chunk_hash != chunk_hashes[i]:
                            print(f"Chunk {i} corrompido")
                            return False
                        
                        outfile.write(chunk_data)
            
            # Verificar hash do arquivo completo
            file_hash_calculated, _ = self.compute_file_checksum(output_path)
            if file_hash_calculated != file_hash:
                print("Arquivo final corrompido")
                os.remove(output_path)
                return False
                
            return True
        except Exception as e:
            print(f"Erro ao montar arquivo: {str(e)}")
            return False
        
    def handle_peer_request(self, client_socket):
        """Lida com solicitações de chunks de outros peers"""
        try:
            # Ler toda a solicitação de uma vez
            data = client_socket.recv(4096).decode()
            if not data:
                return
                
            # Processar apenas a primeira mensagem completa
            if '\n' in data:
                message, _ = data.split('\n', 1)
                request = json.loads(message)
                print(f"Recebido pedido de chunk: {request}")
                self.process_peer_request(client_socket, request)
        except Exception as e:
            print(f"Erro no servidor de chunks: {str(e)}")
        finally:
            client_socket.close()  # Fechar conexão após o envio

    def process_peer_request(self, client_socket, request):
        action = request.get('action')

        if action == 'list_chunks':
            file_hash = request.get('file_hash')

            # Retornar índices de chunks disponíveis neste peer
            chunks_available = []
            if file_hash in self.shared_files:
                chunks_available = list(range(len(self.shared_files[file_hash]['chunks'])))
                print(f"Chunks disponíveis para {file_hash}: {len(chunks_available)}")
            elif file_hash in self.downloading_files:
                chunks_available = sorted(self.downloading_files[file_hash]['chunks'])

            # Enviar resposta
            response = {'status': 'success', 'chunks': chunks_available}
            client_socket.send(json.dumps(response).encode() + b'\n')
            return
        
        if action == 'get_chunk':
            file_hash = request.get('file_hash')
            chunk_index = request.get('chunk_index')
            
            # Caso 1: arquivo completamente compartilhado
            if file_hash in self.shared_files:
                if chunk_index >= len(self.shared_files[file_hash]['chunks']):
                    client_socket.send(b'Invalid request\n')
                    return
                file_path = self.shared_files[file_hash]['path']
                try:
                    with open(file_path, 'rb') as f:
                        f.seek(chunk_index * CHUNK_SIZE)
                        chunk_data = f.read(CHUNK_SIZE)
                        total_sent = 0
                        while total_sent < len(chunk_data):
                            sent = client_socket.send(chunk_data[total_sent:total_sent + 4096])
                            if sent == 0:
                                raise RuntimeError("Conexão fechada durante envio do chunk")
                            total_sent += sent
                        print(f"Chunk {chunk_index} ({len(chunk_data)} bytes) enviado com sucesso")
                        self.chunks_served_since_last_heartbeat += 1
                except FileNotFoundError:
                    print(f"Arquivo {file_hash} não encontrado no diretório compartilhado")
                    client_socket.send(json.dumps({"status": "error", "message": "Chunk não encontrado"})
                                       .encode() + b'\n')
                except Exception as e:
                    print(f"Erro ao ler chunk: {e}")
                    traceback.print_exc()
                    client_socket.send(json.dumps({"status": "error", "message": "Chunk não encontrado"})
                                       .encode() + b'\n')
                return

            # Caso 2: arquivo em download (seed parcial)
            elif file_hash in self.downloading_files:
                if chunk_index not in self.downloading_files[file_hash]['chunks']:
                    client_socket.send(b'Invalid request\n')
                    return
                # Enviar chunk baixado
                chunk_file = os.path.join(self.downloading_files[file_hash]['temp_dir'], f"{chunk_index}.chunk")
                try:
                    with open(chunk_file, 'rb') as f:
                        chunk_data = f.read()
                        total_sent = 0
                        print("Enviando (seed parcial)")
                        while total_sent < len(chunk_data):
                            sent = client_socket.send(chunk_data[total_sent:total_sent + 4096])
                            if sent == 0:
                                raise RuntimeError("Conexão fechada durante envio do chunk")
                            total_sent += sent
                        print(f"Chunk {chunk_index} enviado ({len(chunk_data)} bytes)")
                        self.chunks_served_since_last_heartbeat += 1
                except Exception as e:
                    print(f"Erro ao ler chunk parcial: {str(e)}")
                    client_socket.send(json.dumps({"status": "error", "message": "Chunk não encontrado"})
                                       .encode() + b'\n')
                return
            else:
                client_socket.send(json.dumps({"status": "error", "message": "Requisição inválida"})
                                   .encode() + b'\n')
                return
            
    def announce_all_files(self):
        """Anuncia todos os arquivos da pasta downloads/{username} ao tracker."""
        if not self.logged_in or not self.username:
            return
        user_dir = self.download_dir
        if not os.path.isdir(user_dir):
            return
        
        # Anúncio de arquivos completos
        for fname in os.listdir(user_dir):
            fpath = os.path.join(user_dir, fname)
            if os.path.isfile(fpath):
                try:
                    name = os.path.basename(fpath)
                    size = os.path.getsize(fpath)
                    file_hash, chunk_hashes = self.compute_file_checksum(fpath)
                    if file_hash in self.shared_files:
                        continue
                    self.shared_files[file_hash] = {
                        'path': fpath,
                        'chunks': chunk_hashes,
                        'size': size,
                        'name': name
                    }
                    print(f"Auto-anunciando arquivo: {name} ({size} bytes, hash: {file_hash}, chunks: {len(chunk_hashes)})")
                    request = {
                        'method': 'announce',
                        'name': name,
                        'size': size,
                        'hash': file_hash,
                        'chunks': chunk_hashes
                    }
                    response = self.send_request(request)
                    if response:
                        print(response.get('message', 'Erro desconhecido'))
                except Exception as e:
                    print(f"Erro ao anunciar {fpath}: {str(e)}")
        
        # Anunciar arquivos parciais (downloads interrompidos)
        for fname in os.listdir(user_dir):
            temp_dir = os.path.join(user_dir, fname)
            if os.path.isdir(temp_dir) and fname.startswith("temp_"):
                file_hash = fname.replace("temp_", "")
                chunk_files = [f for f in os.listdir(temp_dir) if f.endswith(".chunk")]
                if not chunk_files:
                    continue
                # Descobrir quais chunks já foram baixados
                chunk_indices = [int(f.split(".")[0]) for f in chunk_files if f.split(".")[0].isdigit()]
                if not chunk_indices:
                    continue
                print(f"Auto-anunciando arquivo parcial: {file_hash} (chunks: {sorted(chunk_indices)})")
                
                self.downloading_files[file_hash] = {'temp_dir': temp_dir, 'chunks': []}
                self.downloading_files[file_hash]['chunks'].extend(chunk_indices)
                request = {
                    'method': 'partial_announce',
                    'file_hash': file_hash,
                    'chunks': sorted(chunk_indices)
                }
                response = self.send_request(request)
                if response:
                    print(response.get('message', 'Erro desconhecido'))

    def do_score(self, arg):
        """Exibe a pontuação atual do usuário."""
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        print(f"Sua pontuação atual é: {self.score:.1f}, permitindo ter até {self.base_connections + math.floor(self.score // SCORE_DIVIDER)} conexões simultâneas.")
        
    # <<<< SALAS DE CHAT >>>>
    
    def do_create_room(self, arg):
        """Cria uma nova sala de chat privada.
        
        Uso: create_room <room_id> <nome> [max_history]
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if len(args) < 1:
            print("Uso: create_room <name> [max_history]")
            return
        
        room_id = args[0]
        max_history = 100
        
        try:
            if len(args) > 1 and args[-1].isdigit():
                max_history = int(args[-1])
                room_id = ' '.join(args[0:-1])
        except ValueError:
            pass

        success, message = self.room_manager.create_room(room_id, max_history)
        print(message)
    
    def do_delete_room(self, arg):
        """Remove uma sala de chat (apenas moderador).
        
        Uso: delete_room <room_id>
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Uso: delete_room <room_id>")
            return
        
        room_id = args[0]
        success, message = self.room_manager.delete_room(room_id)
        print(message)
    
    def do_invite(self, arg):
        """Adiciona um usuário à sala (apenas moderador).
        
        Uso: invite <room_id> <username>
        """
        if not self.logged_in:
            print("Autentique-se primeira")
            return
        
        args = arg.split()
        if len(args) < 2:
            print("Uso: invite <room_id> <username>")
            return
        
        room_id, username = args[0], args[1]
        success, message = self.room_manager.add_member(room_id, username)
        print(message)
    
    def do_remove_from_room(self, arg):
        """Remove um usuário da sala (moderador ou próprio usuário).
        
        Uso: remove_from_room <room_id> <username>
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if len(args) < 2:
            print("Uso: remove_from_room <room_id> <username>")
            return
        
        room_id, username = args[0], args[1]
        success, message = self.room_manager.remove_member(room_id, username)
        print(message)
    
    def do_list_rooms(self, arg):
        """Lista todas as salas de chat disponíveis."""
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        success, rooms = self.room_manager.list_rooms()
        if not success:
            print(f"Erro: {rooms}")
            return
        
        if not rooms:
            print("Nenhuma sala encontrada")
            return
        
        print("Salas disponíveis:")
        for room in rooms:
            status = "MEMBRO" if room['is_member'] else ""
            print(f"  {room['room_id']} (Moderador: {room['moderator']}) [{status}]")
    
    def do_join_room(self, arg):
        """Entra como membro e sincroniza mensagens em uma sala de chat.
        
        Uso: join_room <room_id>
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Uso: join_room <room_id>")
            return
        
        room_id = args[0]
        success, message = self.room_manager.join_room(room_id)
        print(message)
    
    def do_leave_room(self, arg):
        """Sai de uma sala de chat.
        
        Uso: leave_room <room_id>
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Uso: leave_room <room_id>")
            return
        
        room_id = args[0]
        success, message = self.room_manager.leave_room(room_id)
        print(message)
        
        if self.current_room == room_id:
            self.current_room = None
            self.prompt = 'peer> '
    
    def do_room_members(self, arg):
        """Lista os membros de uma sala.
        
        Uso: room_members <room_id>
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Uso: room_members <room_id>")
            return
        
        room_id = args[0]
        success, members = self.room_manager.get_room_members(room_id)
        
        if not success:
            print(f"Erro: {members}")
            return
        
        if not members:
            print("Nenhum membro encontrado")
            return
        
        print(f"Membros da sala {room_id}:")
        for member in members:
            print(f"  {member['username']} (desde {member['joined_at']})")
    
    def do_enter_room(self, arg):
        """Entra no modo de chat da sala.
        
        Uso: enter_room <room_id>
        """
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Uso: enter_room <room_id>")
            return
        
        room_id = args[0]
        
        # Verificar se é membro da sala
        if room_id not in self.room_manager.active_rooms:
            print("Você não é membro desta sala. Use 'join_room' primeiro.")
            return
        
        self.current_room = room_id
        room_id = self.room_manager.active_rooms[room_id]['room_id']
        self.prompt = f'sala({room_id})> '
        
        print(f"--- Entrando na sala {room_id}. Digite '/exit' para sair do modo sala. ---")
        
        # Mostrar histórico recente
        history = self.room_manager.get_room_log(room_id, 10)
        if history:
            print("Histórico recente:")
            for line in history:
                print(f"  {line.strip()}")
    
    # <<<< CHAT >>>>
    def start_chat_server(self):
        """Inicia o servidor de chat para aceitar conexões de outros peers."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', self.chat_port))
        server_socket.listen(5)
        print(f"Servidor de CHAT ouvindo na porta {self.chat_port}")
        
        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(target=self.handle_chat_session, args=(client_socket,)).start()

    def handle_chat_session(self, sock):
        """Lida com uma sessão de chat recebida, ouvindo mensagens em loop."""
        buffer = ""
        peer_name = None
        try:
            while True:
                data = sock.recv(4096).decode()
                if not data:
                    break  # Conexão fechada pelo outro peer
                
                buffer += data
                while '\n' in buffer:
                    message_str, buffer = buffer.split('\n', 1)
                    try:
                        msg_obj = json.loads(message_str)
                        
                        if msg_obj.get('action') == 'chat_message':
                            # Chat direto 1:1
                            if peer_name is None:
                                peer_name = msg_obj.get('from')
                                if peer_name:
                                    with self.chat_lock:
                                        self.chat_connections[peer_name] = sock
                                    print(f"\n--- Chat iniciado por {peer_name}. Use 'chat {peer_name}' para responder. ---")
                            
                            print(f"\n[CHAT de {peer_name}]: {msg_obj.get('message')}\n{self.prompt}", end='', flush=True)
                        
                        elif msg_obj.get('action') == 'room_message':
                            # Mensagem de sala
                            self.room_manager.process_room_message(msg_obj)
                            
                    except json.JSONDecodeError:
                        pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if peer_name:
                print(f"\n[INFO] Conexão com {peer_name} encerrada.")
                self.close_chat_session(peer_name, announce=False)
            else:
                sock.close()

    def chat_receiver_loop(self, sock, peer_name):
        """Recebe mensagens em uma sessão de chat que este peer iniciou."""
        buffer = ""
        try:
            while True:
                data = sock.recv(4096).decode()
                if not data:
                    break # Conexão fechada pelo outro peer
                
                buffer += data
                while '\n' in buffer:
                    message_str, buffer = buffer.split('\n', 1)
                    try:
                        msg_obj = json.loads(message_str)
                        print(f"\n[CHAT de {peer_name}]: {msg_obj.get('message')}\n{self.prompt}", end='', flush=True)
                    except json.JSONDecodeError:
                        pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            print(f"\n[INFO] Conexão com {peer_name} foi perdida.")
            self.close_chat_session(peer_name, announce=False)

    def do_list_online(self, arg):
        """Lista os peers online"""
        request = {'method': 'list_online_users'}
        response = self.send_request(request)
        if response and response.get('status') == 'success':
            print("Usuários online:")
            for user in response['users']:
                print(f"- {user}")
        else:
            print("Erro ao obter lista")

    def do_chat(self, arg):
        """Inicia uma sessão de chat interativa com um usuário.
        Uso: chat <username>"""
        if not self.logged_in:
            print("Autentique-se primeiro")
            return
        
        args = arg.split()
        if not args:
            print("Uso: chat <username>")
            return
        target_user = args[0]

        if target_user == self.username:
            print("Você não pode conversar consigo mesmo.")
            return

        # Verifica ou cria a conexão
        with self.chat_lock:
            if target_user not in self.chat_connections:
                request = {'method': 'get_peer_chat_address', 'username': target_user}
                response = self.send_request(request)
                
                if not (response and response.get('status') == 'success'):
                    print(f"Usuário '{target_user}' não encontrado ou offline.")
                    return

                ip, port = response['ip'], response['port']
                try:
                    chat_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    chat_sock.connect((ip, port))
                    self.chat_connections[target_user] = chat_sock
                    threading.Thread(target=self.chat_receiver_loop, args=(chat_sock, target_user), daemon=True).start()
                except Exception as e:
                    print(f"Erro ao iniciar chat com {target_user}: {e}")
                    return

        # Entra no modo chat
        if self.chat_target_user is None:
            print(f"--- Entrou no modo chat com {target_user}. Digite '/exit' para sair. ---")
        self.chat_target_user = target_user
        self.prompt = f'chat({target_user})> '
        
    def close_chat_session(self, username, announce=True):
        """Fecha e limpa uma sessão de chat com um usuário específico."""
        with self.chat_lock:
            if username in self.chat_connections:
                sock = self.chat_connections.pop(username)
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass # Socket já pode estar fechado
                sock.close()
                if announce:
                    print(f"Sessão de chat com {username} foi encerrada.")

    def do_close_chat(self, arg):
        """Encerra uma sessão de chat ativa.
        Uso: close_chat <username>"""
        args = arg.split()
        if not args:
            print("Uso: close_chat <username>")
            return
        target_user = args[0]
        self.close_chat_session(target_user)

    def precmd(self, line):
        """Intercepta comandos quando em modo de chat ou sala."""
        # Modo sala de chat ativo
        if self.current_room:
            stripped_line = line.strip().lower()
            
            if stripped_line == '/exit':
                room_id = self.room_manager.active_rooms[self.current_room]['room_id']
                print(f"--- Saindo do modo de sala {room_id}. Você continua como membro. ---")
                self.current_room = None
                self.prompt = 'peer> '
                self.lastcmd = ''
                return ''
            
            # Qualquer outra coisa é uma mensagem para a sala
            message = line
            if not message:
                return ''
            
            success, msg = self.room_manager.send_message(self.current_room, message)
            if not success:
                print(f"Erro ao enviar mensagem: {msg}")
            
            return ''
        
        # Modo chat 1:1 ativo
        elif self.chat_target_user:
            stripped_line = line.strip().lower()
            
            # Modo chat ativo
            if stripped_line == '/exit':
                print(f"--- Saindo do modo de chat com {self.chat_target_user}. A conexão permanece aberta. ---")
                self.chat_target_user = None
                self.prompt = 'peer> '
                self.lastcmd = '' # Impede que uma linha vazia reative o chat
                return ''  # Impede que o cmd processe '/exit' como um comando

            # Qualquer outra coisa digitada é uma mensagem de chat
            message = line
            if not message: # Ignora linhas em branco
                return ''

            try:
                with self.chat_lock:
                    chat_sock = self.chat_connections[self.chat_target_user]
                
                chat_sock.sendall(json.dumps({
                    'action': 'chat_message',
                    'from': self.username,
                    'message': message
                }).encode() + b'\n')
            except (KeyError, BrokenPipeError, ConnectionResetError):
                print(f"A conexão com {self.chat_target_user} foi perdida. Saindo do modo chat.")
                self.close_chat_session(self.chat_target_user, announce=False)
                self.chat_target_user = None
                self.prompt = 'peer> '
            
            return '' # Impede que o cmd processe a mensagem como um comando
        else:
            return line # Modo normal, processa o comando como de costume
if __name__ == '__main__':
    cli = Peer('localhost', 5000)
    cli.cmdloop()
