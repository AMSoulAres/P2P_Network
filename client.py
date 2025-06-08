import cmd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import random
import shutil
import socket
import json
import hashlib
from getpass import getpass
import threading
import time

class Peer(cmd.Cmd):
    prompt = 'peer> '
    def __init__(self, tracker_host, tracker_port):
        super().__init__()
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.sock = None
        self.logged_in = False
        self.username = None

        self.shared_files = {}  # {file_hash: {'path': str, 'chunks': [chunk_hashes]}}
        self.download_dir = "downloads"
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Iniciar servidor para receber solicitações de chunks
        self.chunk_port = 6000  # Porta para transferência P2P
        threading.Thread(target=self.start_chunk_server, daemon=True).start()

        self.connect_to_tracker()

    def connect_to_tracker(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.tracker_host, self.tracker_port))
        except Exception as e:
            print(f"Falha ao conectar ao tracker: {e}")
            self.sock = None

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
                data = self.sock.recv(1024).decode()
                if not data:
                    break
                response += data
                if '\n' in response:
                    response, _ = response.split('\n', 1)  # Add _ (item vazio) para separar a resposta e mandar no JSON
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
            'password': password
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
            'password': password
        }

        response = self.send_request(request)
        if response:
            if response.get('status') == 'success':
                self.logged_in = True
                self.username = username
                print("Login bem-sucedido. Salve, " + username)
            else:
                print(response.get('message'))

    def do_announce(self, arg):
        """
        Anuncia um arquivo para o tracker
        
        Uso: announce <caminho do arquivo>
             Ou apenas 'announce' para solicitar o caminho do arquivo
        """

        # Verificações iniciais (se o usuário está logado e se o caminho do arquivo é válido)

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
        
            # Armazenar localmente metadados do arquivo (servirá para upload de chunks)
            if file_hash in self.shared_files:
                print("INFO: Arquivo listado anteriormente")
            else:
                self.shared_files[file_hash] = {
                    'path': path,
                    'chunks': chunk_hashes,
                    'size': size,
                    'name': name
                }
            
            # Enviar anúncio para tracker
            request = {
                'method': 'announce',
                'name': name,
                'size': size,
                'hash': file_hash,
                'chunk_hashes': chunk_hashes  # Adicionar hashes dos chunks (necessário, pois quem faz o download precisa saber quais chunks existem para baixar)
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

    def do_download(self, arg):
        """Baixa o arquivo especificado arquivo da rede P2P.
         
        Uso: download <file_hash>"""
        # Considera-se que  hash do arquivo foi obtido previamente (link, anúncio, etc.)

        if not self.logged_in:
            print("Autentique-se primeiro")
            return
            
        file_hash = arg.strip()
        if not file_hash:
            print("Digite o hash do arquivo")
            return
            
        # Obter lista de peers com o arquivo
        request = {'method': 'get_peers', 'file_hash': file_hash}
        response = self.send_request(request)
        
        if not response or response.get('status') != 'success':
            print("Erro ao obter peers:", response.get('message', 'Erro desconhecido'))
            return
            
        peers = response.get('peers', [])
        if not peers:
            print("Nenhum peer disponível com este arquivo")
            return
            
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
        
        # Baixar chunks em paralelo
        print(f"Iniciando download de {file_name} ({num_chunks} chunks)")
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for chunk_index in range(num_chunks):
                # Escolher peer aleatório para cada chunk
                peer = random.choice(peers)
                futures.append(executor.submit(
                    self.download_chunk,
                    file_hash,
                    chunk_index,
                    chunk_hash,
                    peer,
                    temp_dir
                ))

            num_downloaded_chunks = 0
            
            # Aguardar conclusão
            for future in as_completed(futures):
                try:
                    result = future.result()
                    downloaded_chunks += 1 if result else 0
                    if not result:
                        print("Erro ao baixar chunk - ERROR")

                    progress = int(40 * num_downloaded_chunks / num_chunks)
                    bar = '[' + '#' * progress + '-' * (40 - progress) + ']'
                    print(f'\rBaixando: {bar} {num_downloaded_chunks}/{num_chunks} chunks', end='', flush=True)
                except Exception as e:
                    print(f"Erro no download: {str(e)}")
        
        # Montar arquivo final
        if self.assemble_file(file_hash, metadata['chunk_hashes'], temp_dir, download_path):
            download_time = time.time() - start_time
            file_size = os.path.getsize(download_path)
            print(f"Download concluído! {file_size/1048576:.2f} MB em {download_time:.1f} segundos")
            print(f"Arquivo salvo em: {download_path}")
        else:
            print("Falha ao montar arquivo final")
        
        # Limpar diretório temporário
        shutil.rmtree(temp_dir, ignore_errors=True)

    # Utils

    def compute_file_checksum(self, file_name, chunk_size=1048576):  # Chunk de 1MB
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
    
    def download_chunk(self, file_hash, chunk_index, peer, temp_dir):
        peer_ip, peer_port = peer
        chunk_file = os.path.join(temp_dir, f"{chunk_index}.chunk")
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((peer_ip, peer_port))
                
                # Solicitar chunk específico
                request = {
                    'action': 'get_chunk',
                    'file_hash': file_hash,
                    'chunk_index': chunk_index
                }
                s.send(json.dumps(request).encode() + b'\n')
                
                # Receber chunk
                chunk_data = b''
                while True:
                    data = s.recv(4096)
                    if not data:
                        break
                    chunk_data += data
                
                # Verificar integridade
                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                with open(chunk_file, 'wb') as f:
                    f.write(chunk_data)
                
                return True
        except Exception as e:
            print(f"Erro ao baixar chunk {chunk_index} de {peer_ip}: {str(e)}")
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
                return False
                
            return True
        except Exception as e:
            print(f"Erro ao montar arquivo: {str(e)}")
            return False
        
    def handle_chunk_request(self, client_socket):
        try:
            buffer = ''
            while True:
                data = client_socket.recv(4096).decode()
                if not data:
                    break
                buffer += data
                if '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    request = json.loads(message)
                    self.process_chunk_request(client_socket, request)
        except Exception as e:
            print(f"Erro no servidor de chunks: {str(e)}")
        finally:
            client_socket.close()

    def process_chunk_request(self, client_socket, request):
        action = request.get('action')
        
        if action == 'get_chunk':
            file_hash = request.get('file_hash')
            chunk_index = request.get('chunk_index')
            
            if (file_hash not in self.shared_files or 
                chunk_index >= len(self.shared_files[file_hash]['chunks'])):
                client_socket.send(b'Invalid request\n')
                return
            
            file_path = self.shared_files[file_hash]['path']
            chunk_size = 1024 * 1024  # 1MB
            
            try:
                with open(file_path, 'rb') as f:
                    f.seek(chunk_index * chunk_size)
                    chunk_data = f.read(chunk_size)
                    client_socket.sendall(chunk_data)
            except Exception as e:
                print(f"Erro ao ler chunk: {str(e)}")


if __name__ == '__main__':
    cli = Peer('localhost', 5000)
    cli.cmdloop()